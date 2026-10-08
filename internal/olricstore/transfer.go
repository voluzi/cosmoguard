package olricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/olric-data/olric/pkg/storage"
	"github.com/vmihailenco/msgpack/v5"
)

var ErrScratch = errors.New("olric codec scratch capacity exhausted")
var ErrInvalidPack = errors.New("invalid native olric transfer")

const codecCharge = 4 << 20
const maxNativeRecords = (MaxEntryBytes - 1) / 29

// Field names and types are the upstream v0.7.4 table.Pack wire contract.
type nativePack struct {
	Offset, Allocated, Inuse, Garbage uint64
	RecycledAt                        int64
	State                             uint8
	HKeys                             map[uint64]uint64
	OffsetIndex, Memory               []byte
}
type transfer struct{ e *Engine }

func (e *Engine) TransferIterator() storage.TransferIterator { return &transfer{e: e} }
func (t *transfer) Next() bool {
	e := t.e
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if e.readyLocked() != nil {
		return false
	}
	e.sweepLocked(time.Now().UnixMilli())
	return e.length > 0
}
func (t *transfer) Export() ([]byte, int, error) {
	e := t.e
	l, ok := e.p.codec.TryAcquire(codecCharge)
	if !ok {
		return nil, 0, ErrScratch
	}
	defer l.Release()
	p := nativePack{Allocated: MaxEntryBytes, State: 2, HKeys: make(map[uint64]uint64, 4096), Memory: make([]byte, 0, MaxEntryBytes)}
	idx := roaring64.New()
	e.p.mu.Lock()
	if err := e.readyLocked(); err != nil {
		e.p.mu.Unlock()
		return nil, 0, err
	}
	e.sweepLocked(time.Now().UnixMilli())
	e.exportID++
	id := e.exportID
	e.exportLow = 0
	e.exportHigh = 0
	for loc := e.head; loc != 0; loc = field(e.p.arena.block(loc), 24) {
		b := e.p.arena.block(loc)
		raw := rawRecord(b)
		if len(p.HKeys) > 0 && (len(p.Memory)+len(raw) > 256<<10 || len(p.HKeys) >= 4096) {
			break
		}
		off := uint64(len(p.Memory))
		p.HKeys[field(b, 0)] = off
		idx.Add(off)
		p.Memory = append(p.Memory, raw...)
		g := field(b, 32)
		if e.exportLow == 0 {
			e.exportLow = g
		}
		e.exportHigh = g
	}
	e.p.mu.Unlock()
	p.Offset = uint64(len(p.Memory))
	p.Inuse = p.Offset
	var err error
	p.OffsetIndex, err = idx.MarshalBinary()
	if err != nil {
		return nil, 0, err
	}
	data, err := msgpack.Marshal(p)
	return data, id, err
}
func (t *transfer) Drop(id int) error {
	e := t.e
	e.p.mu.Lock()
	defer e.p.mu.Unlock()
	if err := e.readyLocked(); err != nil {
		return err
	}
	if id != e.exportID {
		return ErrInvalidPack
	}
	for loc := e.head; loc != 0; {
		b := e.p.arena.block(loc)
		next, g := field(b, 24), field(b, 32)
		if g >= e.exportLow && g <= e.exportHigh {
			h := field(b, 0)
			_, prev := e.findLocked(h)
			e.removeLocked(loc, prev)
		}
		loc = next
	}
	e.exportLow = 0
	e.exportHigh = 0
	return nil
}

type hashOffset struct{ hash, offset uint64 }
type decodedPack struct {
	offset, allocated, inuse, garbage uint64
	state                             uint64
	keys                              []hashOffset
	memory, bitmap                    []byte
}

// bytes.Reader implements ReadByte, so the msgpack decoder cannot read ahead.
// Borrow spans only after checking their declared lengths against the payload.
func span(d *msgpack.Decoder, r *bytes.Reader, data []byte, max int) ([]byte, error) {
	n, err := d.DecodeBytesLen()
	if err != nil || n < 0 || n > max || n > r.Len() {
		return nil, ErrInvalidPack
	}
	off := len(data) - r.Len()
	if _, err := r.Seek(int64(n), io.SeekCurrent); err != nil {
		return nil, err
	}
	return data[off : off+n], nil
}
func decodePack(data []byte) (decodedPack, error) {
	var p decodedPack
	r := bytes.NewReader(data)
	d := msgpack.NewDecoder(r)
	n, err := d.DecodeMapLen()
	if err != nil || n != 9 {
		return p, ErrInvalidPack
	}
	var fields uint16
	for range n {
		name, err := span(d, r, data, 16)
		if err != nil {
			return p, err
		}
		var bit uint16
		switch string(name) {
		case "Offset":
			bit = 1
			p.offset, err = d.DecodeUint64()
		case "Allocated":
			bit = 2
			p.allocated, err = d.DecodeUint64()
		case "Inuse":
			bit = 4
			p.inuse, err = d.DecodeUint64()
		case "Garbage":
			bit = 8
			p.garbage, err = d.DecodeUint64()
		case "RecycledAt":
			bit = 16
			_, err = d.DecodeInt64()
		case "State":
			bit = 32
			p.state, err = d.DecodeUint64()
		case "HKeys":
			bit = 64
			var count int
			count, err = d.DecodeMapLen()
			if err == nil && (count < 0 || count > maxNativeRecords || count > r.Len()/2) {
				err = ErrInvalidPack
			}
			if err == nil {
				p.keys = make([]hashOffset, count)
				for i := range p.keys {
					p.keys[i].hash, err = d.DecodeUint64()
					if err != nil {
						break
					}
					p.keys[i].offset, err = d.DecodeUint64()
					if err != nil {
						break
					}
				}
			}
		case "OffsetIndex":
			bit = 128
			p.bitmap, err = span(d, r, data, 256<<10)
		case "Memory":
			bit = 256
			p.memory, err = span(d, r, data, MaxEntryBytes-1)
		default:
			return p, ErrInvalidPack
		}
		if err != nil {
			return p, err
		}
		if fields&bit != 0 {
			return p, ErrInvalidPack
		}
		fields |= bit
	}
	if r.Len() != 0 || fields != 511 || p.allocated == 0 || p.allocated > MaxEntryBytes || p.offset != uint64(len(p.memory)) || p.offset >= p.allocated || p.inuse > p.offset || p.garbage > p.offset || p.inuse+p.garbage != p.offset || p.state < 1 || p.state > 3 {
		return p, ErrInvalidPack
	}
	return p, nil
}

// Native offsets are below 1MiB. Parse Roaring's bounded container format
// directly rather than letting forged container counts allocate a bitmap.
func validateBitmap(data []byte, memory []byte) ([]uint64, error) {
	bits := make([]uint64, (len(memory)+63)/64)
	if len(data) < 8 {
		return nil, ErrInvalidPack
	}
	high := binary.LittleEndian.Uint64(data)
	data = data[8:]
	if high == 0 {
		if len(data) != 0 {
			return nil, ErrInvalidPack
		}
		return bits, nil
	}
	if high != 1 || len(data) < 8 || binary.LittleEndian.Uint32(data) != 0 {
		return nil, ErrInvalidPack
	}
	data = data[4:]
	cookie := binary.LittleEndian.Uint32(data)
	data = data[4:]
	var count int
	var runs []byte
	switch cookie & 65535 {
	case 12346:
		if cookie != 12346 || len(data) < 4 {
			return nil, ErrInvalidPack
		}
		count = int(binary.LittleEndian.Uint32(data))
		data = data[4:]
	case 12347:
		count = int(cookie>>16) + 1
		n := (count + 7) / 8
		if n > len(data) {
			return nil, ErrInvalidPack
		}
		runs = data[:n]
		data = data[n:]
	default:
		return nil, ErrInvalidPack
	}
	if count < 1 || count > 16 || len(data) < count*4 {
		return nil, ErrInvalidPack
	}
	headers := data[:count*4]
	data = data[count*4:]
	if runs == nil || count >= 4 {
		if len(data) < count*4 {
			return nil, ErrInvalidPack
		}
		data = data[count*4:]
	}
	last := -1
	entries := 0
	add := func(v int) bool {
		if v < 0 || v >= len(memory) || bits[v/64]&(uint64(1)<<uint(v%64)) != 0 {
			return false
		}
		b := memory[v:]
		if len(b) < 29+int(b[0]) {
			return false
		}
		k := int(b[0])
		n := uint64(29+k) + uint64(binary.BigEndian.Uint32(b[25+k:29+k]))
		if n >= MaxEntryBytes || n > uint64(len(b)) {
			return false
		}
		bits[v/64] |= uint64(1) << uint(v%64)
		entries++
		return entries <= maxNativeRecords
	}
	for i := range count {
		key := int(binary.LittleEndian.Uint16(headers[i*4:]))
		card := int(binary.LittleEndian.Uint16(headers[i*4+2:])) + 1
		if key <= last || key >= 16 {
			return nil, ErrInvalidPack
		}
		last = key
		base := key << 16
		actual := 0
		if runs != nil && runs[i/8]&(1<<uint(i%8)) != 0 {
			if len(data) < 2 {
				return nil, ErrInvalidPack
			}
			n := int(binary.LittleEndian.Uint16(data))
			data = data[2:]
			if n > card || n*4 > len(data) {
				return nil, ErrInvalidPack
			}
			for j := range n {
				start := int(binary.LittleEndian.Uint16(data[j*4:]))
				length := int(binary.LittleEndian.Uint16(data[j*4+2:]))
				if start+length > 65535 {
					return nil, ErrInvalidPack
				}
				for v := start; v <= start+length; v++ {
					if !add(base + v) {
						return nil, ErrInvalidPack
					}
					actual++
				}
			}
			data = data[n*4:]
		} else if card <= 4096 {
			if card*2 > len(data) {
				return nil, ErrInvalidPack
			}
			for j := range card {
				if !add(base + int(binary.LittleEndian.Uint16(data[j*2:]))) {
					return nil, ErrInvalidPack
				}
				actual++
			}
			data = data[card*2:]
		} else {
			if len(data) < 8192 {
				return nil, ErrInvalidPack
			}
			for j := 0; j < 65536; j++ {
				if data[j/8]&(1<<uint(j%8)) != 0 {
					if !add(base + j) {
						return nil, ErrInvalidPack
					}
					actual++
				}
			}
			data = data[8192:]
		}
		if actual != card {
			return nil, ErrInvalidPack
		}
	}
	if len(data) != 0 {
		return nil, ErrInvalidPack
	}
	return bits, nil
}
func (e *Engine) Import(data []byte, f func(uint64, storage.Entry) error) error {
	l, ok := e.p.codec.TryAcquire(codecCharge)
	if !ok {
		return ErrScratch
	}
	defer l.Release()
	e.p.mu.Lock()
	err := e.readyLocked()
	e.p.mu.Unlock()
	if err != nil {
		return err
	}
	p, err := decodePack(data)
	if err != nil {
		return err
	}
	bits, err := validateBitmap(p.bitmap, p.memory)
	if err != nil {
		return err
	}
	sort.Slice(p.keys, func(i, j int) bool { return p.keys[i].hash < p.keys[j].hash })
	for i, k := range p.keys {
		if i > 0 && p.keys[i-1].hash == k.hash {
			return ErrInvalidPack
		}
		off := k.offset
		if off >= p.offset || bits[off/64]&(uint64(1)<<uint(off%64)) == 0 {
			return ErrInvalidPack
		}
		bits[off/64] &^= uint64(1) << uint(off%64)
		b := p.memory[off:]
		kl := int(b[0])
		n := 29 + kl + int(binary.BigEndian.Uint32(b[25+kl:29+kl]))
		if !validRaw(b[:n]) {
			return ErrInvalidPack
		}
	}
	for _, k := range p.keys {
		e.p.mu.Lock()
		err := e.readyLocked()
		e.p.mu.Unlock()
		if err != nil {
			return err
		}
		b := p.memory[k.offset:]
		kl := int(b[0])
		n := 29 + kl + int(binary.BigEndian.Uint32(b[25+kl:29+kl]))
		b = b[:n]
		if expiredRaw(b, time.Now().UnixMilli()) {
			continue
		}
		v := NewEntry()
		v.Decode(b)
		if err := f(k.hash, v); err != nil {
			if e.p.policy == Response && errors.Is(err, ErrCapacity) {
				e.p.mu.Lock()
				e.p.importDropped++
				e.p.mu.Unlock()
				continue
			}
			return err
		}
	}
	return nil
}
