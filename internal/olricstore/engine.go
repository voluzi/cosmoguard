package olricstore

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	"github.com/voluzi/olric/pkg/storage"

	"github.com/voluzi/cosmoguard/v6/internal/bytebudget"
)

var ErrCapacity = errors.New("olric response storage capacity exhausted")
var ErrClosed = errors.New("olric storage closed")
var ErrInvalidEntry = errors.New("invalid native olric entry")

type Policy uint8

const (
	Response Policy = iota
	Security
)
const fragmentCharge = 512
const headerSize = 48

type PoolStats struct {
	Capacity, Allocated, Inuse, Entries, PutRejected, RawRejected, ImportDropped uint64
	Codec                                                                        bytebudget.Snapshot
}

// Observer is called outside the allocator lock.
type Observer func(string)
type Pool struct {
	mu                                                     sync.Mutex
	arena                                                  arena
	policy                                                 Policy
	observer                                               Observer
	engines                                                *Engine
	nextID                                                 uint64
	used, entries, putRejected, rawRejected, importDropped uint64
	codec                                                  *bytebudget.Budget
	cancel                                                 context.CancelFunc
	done                                                   chan struct{}
	closed                                                 bool
}
type Engine struct {
	exportID               int
	exportLow, exportHigh  uint64
	p                      *Pool
	next                   *Engine
	id                     uint64
	buckets                [32]uint64
	head, tail, generation uint64
	bytes, length          int
	closed, destroyed      bool
}

var _ storage.Engine = (*Engine)(nil)

func NewPool(limit uint64, policy Policy, observer Observer) *Pool {
	scratch := uint64(8 << 20)
	if policy == Security {
		scratch = 4 << 20
	}
	return &Pool{arena: arena{limit: limit}, policy: policy, observer: observer, codec: bytebudget.New(scratch)}
}
func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	if p.cancel != nil {
		return nil
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				p.mu.Lock()
				for e := p.engines; e != nil; e = e.next {
					e.sweepLocked(time.Now().UnixMilli())
				}
				p.mu.Unlock()
			}
		}
	}()
	return nil
}
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	if p.cancel != nil {
		p.cancel()
	}
	done := p.done
	for p.engines != nil {
		p.engines.destroyLocked()
	}
	p.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (p *Pool) Snapshot() PoolStats {
	p.mu.Lock()
	s := PoolStats{Capacity: p.arena.limit, Allocated: p.arena.allocated, Inuse: p.used, Entries: p.entries, PutRejected: p.putRejected, RawRejected: p.rawRejected, ImportDropped: p.importDropped}
	p.mu.Unlock()
	s.Codec = p.codec.Snapshot()
	return s
}
func NewEngine(p *Pool) *Engine             { return &Engine{p: p} }
func (e *Engine) SetConfig(*storage.Config) {}
func (e *Engine) SetLogger(*log.Logger)     {}
func (e *Engine) Name() string              { return "cosmoguard-slab" }
func (e *Engine) NewEntry() storage.Entry   { return NewEntry() }
func (e *Engine) Start() error              { e.p.mu.Lock(); defer e.p.mu.Unlock(); return e.readyLocked() }
func (e *Engine) readyLocked() error {
	if e.closed || e.destroyed || e.p.closed {
		return ErrClosed
	}
	return nil
}
func (e *Engine) registerLocked() error {
	if err := e.readyLocked(); err != nil {
		return err
	}
	if e.id != 0 {
		return nil
	}
	if !e.p.arena.reserve(fragmentCharge) {
		return ErrCapacity
	}
	e.p.nextID++
	e.id = e.p.nextID
	e.next = e.p.engines
	e.p.engines = e
	e.p.used += fragmentCharge
	return nil
}
func (e *Engine) Fork(*storage.Config) (storage.Engine, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if err := e.readyLocked(); err != nil {
		return nil, err
	}
	child := NewEngine(e.p)
	if err := child.registerLocked(); err != nil {
		return nil, err
	}
	return child, nil
}
func field(b []byte, off int) uint64       { return binary.LittleEndian.Uint64(b[off : off+8]) }
func setField(b []byte, off int, n uint64) { binary.LittleEndian.PutUint64(b[off:off+8], n) }
func rawRecord(b []byte) []byte {
	return b[headerSize : headerSize+int(binary.LittleEndian.Uint32(b[40:44]))]
}
func blockCharge(b []byte) int { return int(binary.LittleEndian.Uint32(b[44:48])) }
func (e *Engine) findLocked(h uint64) (uint64, uint64) {
	var prev uint64
	for loc := e.buckets[h%32]; loc != 0; {
		b := e.p.arena.block(loc)
		if field(b, 0) == h {
			return loc, prev
		}
		prev = loc
		loc = field(b, 8)
	}
	return 0, prev
}
func (e *Engine) unlinkOrderLocked(loc uint64) {
	b := e.p.arena.block(loc)
	prev, next := field(b, 16), field(b, 24)
	if prev == 0 {
		e.head = next
	} else {
		setField(e.p.arena.block(prev), 24, next)
	}
	if next == 0 {
		e.tail = prev
	} else {
		setField(e.p.arena.block(next), 16, prev)
	}
}
func (e *Engine) appendOrderLocked(loc uint64) {
	b := e.p.arena.block(loc)
	setField(b, 16, e.tail)
	setField(b, 24, 0)
	e.generation++
	setField(b, 32, e.generation)
	if e.tail == 0 {
		e.head = loc
	} else {
		setField(e.p.arena.block(e.tail), 24, loc)
	}
	e.tail = loc
}
func (e *Engine) removeLocked(loc, prev uint64) {
	b := e.p.arena.block(loc)
	h, next, size := field(b, 0), field(b, 8), blockCharge(b)
	s := e.p.arena.slab(loc)
	if prev == 0 {
		e.buckets[h%32] = next
	} else {
		setField(e.p.arena.block(prev), 8, next)
	}
	e.unlinkOrderLocked(loc)
	e.bytes -= size
	e.length--
	e.p.used -= uint64(size)
	e.p.entries--
	e.p.arena.free(loc, size)
	if s.live > 0 && s.owner == e.id {
		e.p.assignOwnerLocked(s)
	}
}

// Slab backing is attributed to one deterministic live fragment, avoiding
// double charging shared backing in independent Olric Stats calls.
func (p *Pool) assignOwnerLocked(s *slab) {
	s.owner = 0
	for e := p.engines; e != nil; e = e.next {
		for loc := e.head; loc != 0; loc = field(p.arena.block(loc), 24) {
			if uint32(loc>>32) == s.id && (s.owner == 0 || e.id < s.owner) {
				s.owner = e.id
				break
			}
		}
	}
}
func expiredRaw(b []byte, now int64) bool {
	k := int(b[0])
	ttl := int64(binary.BigEndian.Uint64(b[1+k : 9+k]))
	return ttl != 0 && ttl <= now
}
func (e *Engine) sweepLocked(now int64) {
	for loc := e.head; loc != 0; {
		b := e.p.arena.block(loc)
		next := field(b, 24)
		if expiredRaw(rawRecord(b), now) {
			h := field(b, 0)
			_, prev := e.findLocked(h)
			e.removeLocked(loc, prev)
		}
		loc = next
	}
}
func (e *Engine) Put(h uint64, v storage.Entry) error {
	if len(v.Key()) > 255 {
		return storage.ErrKeyTooLarge
	}
	if len(v.Value()) >= MaxEntryBytes-29-len(v.Key()) {
		return storage.ErrEntryTooLarge
	}
	return e.put(h, v.Encode(), false)
}
func (e *Engine) PutRaw(h uint64, b []byte) error {
	if len(b) >= MaxEntryBytes {
		return storage.ErrEntryTooLarge
	}
	if !validRaw(b) {
		return ErrInvalidEntry
	}
	return e.put(h, b, true)
}
func (e *Engine) put(h uint64, v []byte, isRaw bool) error {
	e.p.mu.Lock()
	err := e.putLocked(h, v)
	if errors.Is(err, ErrCapacity) {
		if isRaw {
			e.p.rawRejected++
		} else {
			e.p.putRejected++
		}
	}
	e.p.mu.Unlock()
	if errors.Is(err, ErrCapacity) && e.p.observer != nil {
		path := "put"
		if isRaw {
			path = "put_raw"
		}
		e.p.observer(path)
	}
	return err
}
func (e *Engine) putLocked(h uint64, v []byte) error {
	if err := e.registerLocked(); err != nil {
		return err
	}
	size := leafSize
	for size < len(v)+headerSize {
		size *= 2
	}
	loc, prev := e.findLocked(h)
	if loc != 0 && blockCharge(e.p.arena.block(loc)) == size {
		b := e.p.arena.block(loc)
		e.unlinkOrderLocked(loc)
		binary.LittleEndian.PutUint32(b[40:44], uint32(len(v)))
		copy(b[headerSize:], v)
		e.appendOrderLocked(loc)
		return nil
	}
	dest, ok := e.p.arena.allocate(size)
	if !ok {
		return ErrCapacity
	}
	s := e.p.arena.slab(dest)
	if s.owner == 0 || e.id < s.owner {
		s.owner = e.id
	}
	if loc != 0 {
		e.removeLocked(loc, prev)
	}
	b := e.p.arena.block(dest)
	setField(b, 0, h)
	setField(b, 8, e.buckets[h%32])
	binary.LittleEndian.PutUint32(b[40:44], uint32(len(v)))
	binary.LittleEndian.PutUint32(b[44:48], uint32(size))
	copy(b[headerSize:], v)
	e.buckets[h%32] = dest
	e.appendOrderLocked(dest)
	e.bytes += size
	e.length++
	e.p.used += uint64(size)
	e.p.entries++
	if s.owner == 0 || e.id < s.owner {
		s.owner = e.id
	}
	return nil
}
func (e *Engine) lookupLocked(h uint64) ([]byte, error) {
	if err := e.readyLocked(); err != nil {
		return nil, err
	}
	loc, _ := e.findLocked(h)
	if loc == 0 {
		return nil, storage.ErrKeyNotFound
	}
	return rawRecord(e.p.arena.block(loc)), nil
}
func (e *Engine) Get(h uint64) (storage.Entry, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return nil, err
	}
	v := NewEntry()
	v.Decode(append([]byte(nil), b...))
	k := int(b[0])
	binary.BigEndian.PutUint64(b[17+k:25+k], uint64(time.Now().UnixNano()))
	return v, nil
}
func (e *Engine) GetRaw(h uint64) ([]byte, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), b...), nil
}
func (e *Engine) GetTTL(h uint64) (int64, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return 0, err
	}
	k := int(b[0])
	return int64(binary.BigEndian.Uint64(b[1+k : 9+k])), nil
}
func (e *Engine) GetLastAccess(h uint64) (int64, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return 0, err
	}
	k := int(b[0])
	return int64(binary.BigEndian.Uint64(b[17+k : 25+k])), nil
}
func (e *Engine) GetKey(h uint64) (string, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return "", err
	}
	return string(b[1 : 1+int(b[0])]), nil
}
func (e *Engine) UpdateTTL(h uint64, v storage.Entry) error {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	b, err := e.lookupLocked(h)
	if err != nil {
		return err
	}
	k := int(b[0])
	binary.BigEndian.PutUint64(b[1+k:9+k], uint64(v.TTL()))
	binary.BigEndian.PutUint64(b[9+k:17+k], uint64(v.Timestamp()))
	loc, _ := e.findLocked(h)
	e.unlinkOrderLocked(loc)
	e.appendOrderLocked(loc)
	return nil
}
func (e *Engine) Delete(h uint64) error {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if err := e.readyLocked(); err != nil {
		return err
	}
	loc, prev := e.findLocked(h)
	if loc == 0 {
		return storage.ErrKeyNotFound
	}
	e.removeLocked(loc, prev)
	return nil
}
func (e *Engine) Check(h uint64) bool {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	_, err := e.lookupLocked(h)
	return err == nil
}
func (e *Engine) Stats() storage.Stats {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if e.destroyed || e.id == 0 {
		return storage.Stats{}
	}
	s := storage.Stats{Allocated: fragmentCharge, Inuse: e.bytes + fragmentCharge, Length: e.length}
	for slab := e.p.arena.head; slab != nil; slab = slab.next {
		if slab.owner == e.id {
			s.Allocated += slabCharge
			s.NumTables++
		}
	}
	return s
}

type token struct{ hash, generation uint64 }

func (e *Engine) batch(after, high uint64) ([32]token, int) {
	var out [32]token
	var n int
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if e.readyLocked() != nil {
		return out, 0
	}
	for loc := e.head; loc != 0; loc = field(e.p.arena.block(loc), 24) {
		b := e.p.arena.block(loc)
		g := field(b, 32)
		if g > after && g <= high {
			out[n] = token{field(b, 0), g}
			n++
			if n == len(out) {
				break
			}
		}
	}
	return out, n
}
func (e *Engine) copyToken(t token) (storage.Entry, bool) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if e.readyLocked() != nil {
		return nil, false
	}
	loc, _ := e.findLocked(t.hash)
	if loc == 0 {
		return nil, false
	}
	b := e.p.arena.block(loc)
	if field(b, 32) != t.generation {
		return nil, false
	}
	v := NewEntry()
	v.Decode(append([]byte(nil), rawRecord(b)...))
	return v, true
}
func (e *Engine) highWater() uint64 { e.p.mu.Lock(); defer e.p.mu.Unlock(); return e.generation }
func (e *Engine) walk(after, high uint64, f func(token) bool) {
	for {
		batch, n := e.batch(after, high)
		if n == 0 {
			return
		}
		for _, t := range batch[:n] {
			after = t.generation
			if !f(t) {
				return
			}
		}
	}
}
func (e *Engine) RangeHKey(f func(uint64) bool) {
	e.walk(0, e.highWater(), func(t token) bool { _, ok := e.copyToken(t); return !ok || f(t.hash) })
}
func (e *Engine) Range(f func(uint64, storage.Entry) bool) {
	high := e.highWater()
	start := uint64(0)
	if high > 0 {
		start = rand.Uint64N(high)
	}
	stopped := false
	visit := func(t token) bool {
		v, ok := e.copyToken(t)
		if ok && !f(t.hash, v) {
			stopped = true
			return false
		}
		return true
	}
	e.walk(start, high, visit)
	if !stopped {
		e.walk(0, start, visit)
	}
}
func (e *Engine) Scan(c uint64, n int, f func(storage.Entry) bool) (uint64, error) {
	return e.scan(c, n, nil, f)
}
func (e *Engine) ScanRegexMatch(c uint64, match string, n int, f func(storage.Entry) bool) (uint64, error) {
	r, err := regexp.Compile(match)
	if err != nil {
		return 0, err
	}
	return e.scan(c, n, r, f)
}
func (e *Engine) scan(c uint64, n int, r *regexp.Regexp, f func(storage.Entry) bool) (uint64, error) {
	e.p.mu.Lock()
	err := e.readyLocked()
	e.p.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, nil
	}
	var next uint64
	e.walk(c, e.highWater(), func(t token) bool {
		v, ok := e.copyToken(t)
		if !ok || (r != nil && !r.MatchString(v.Key())) {
			return true
		}
		n--
		if !f(v) || n == 0 {
			next = t.generation
			return false
		}
		return true
	})
	return next, nil
}
func (e *Engine) Compaction() (bool, error) {
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if err := e.readyLocked(); err != nil {
		return false, err
	}
	e.sweepLocked(time.Now().UnixMilli())
	return true, nil
}
func (e *Engine) Close() error { e.p.mu.Lock(); defer e.p.mu.Unlock(); e.closed = true; return nil }
func (e *Engine) destroyLocked() {
	if e.destroyed {
		return
	}
	for e.head != 0 {
		h := field(e.p.arena.block(e.head), 0)
		_, prev := e.findLocked(h)
		e.removeLocked(e.head, prev)
	}
	if e.id != 0 {
		var prev *Engine
		for x := e.p.engines; x != nil; x = x.next {
			if x == e {
				if prev == nil {
					e.p.engines = e.next
				} else {
					prev.next = e.next
				}
				break
			}
			prev = x
		}
		e.p.arena.allocated -= fragmentCharge
		e.p.used -= fragmentCharge
	}
	e.closed = true
	e.destroyed = true
	e.next = nil
}
func (e *Engine) Destroy() error { e.p.mu.Lock(); defer e.p.mu.Unlock(); e.destroyLocked(); return nil }
