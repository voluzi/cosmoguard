package olricstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/olric-data/olric/pkg/storage"
)

func testEngine(t *testing.T, limit uint64, policy Policy) (*Pool, *Engine) {
	t.Helper()
	p := NewPool(limit, policy, nil)
	if err := p.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	r := NewEngine(p)
	x, err := r.Fork(nil)
	if err != nil {
		t.Fatal(err)
	}
	return p, x.(*Engine)
}
func item(key string, n int) *Entry {
	e := NewEntry()
	e.SetKey(key)
	e.SetValue(bytes.Repeat([]byte{42}, n))
	e.SetTimestamp(123)
	e.SetLastAccess(456)
	return e
}
func TestEngineBufferOwnership(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	v := item("key", 100)
	if err := e.Put(1, v); err != nil {
		t.Fatal(err)
	}
	v.Value()[0] = 0
	r, err := e.Get(1)
	if err != nil || r.Value()[0] != 42 {
		t.Fatal("put ownership", err)
	}
	raw, _ := e.GetRaw(1)
	r.Value()[0] = 0
	raw[len(raw)-1] = 0
	r2, _ := e.Get(1)
	if r2.Value()[0] != 42 || r2.Value()[99] != 42 {
		t.Fatal("get ownership")
	}
	_ = e.Destroy()
	if r2.Value()[0] != 42 {
		t.Fatal("destroy alias")
	}
}
func TestEngineOverwriteAtomicOnCapacity(t *testing.T) {
	p, e := testEngine(t, slabCharge+fragmentCharge, Response)
	for i := uint64(0); i < 4; i++ {
		if err := e.Put(i, item(fmt.Sprint(i), 256<<10)); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := e.GetRaw(0)
	if err := e.Put(0, item("0", 900<<10)); !errors.Is(err, ErrCapacity) {
		t.Fatal("expected capacity", err)
	}
	now, _ := e.GetRaw(0)
	if !bytes.Equal(old, now) {
		t.Fatal("failed growth changed entry")
	}
	if p.Snapshot().Allocated > p.Snapshot().Capacity {
		t.Fatal("cap")
	}
}
func TestPoolConcurrentAdmissionAllWritePaths(t *testing.T) {
	p, e := testEngine(t, 4<<20, Response)
	var wg sync.WaitGroup
	for n := range 8 {
		wg.Go(func() {
			for i := range 40 {
				h := uint64(n*100 + i)
				v := item(fmt.Sprint(h), 256<<10)
				var err error
				if n%2 == 0 {
					err = e.Put(h, v)
				} else {
					err = e.PutRaw(h, v.Encode())
				}
				if err != nil && !errors.Is(err, ErrCapacity) {
					t.Error(err)
				}
				if p.Snapshot().Allocated > 4<<20 {
					t.Error("cap exceeded")
				}
			}
		})
	}
	wg.Wait()
	if p.Snapshot().PutRejected == 0 || p.Snapshot().RawRejected == 0 {
		t.Fatal("missing rejection")
	}
}
func TestEngineLastAccessNativeSemantics(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	v := item("k", 5)
	_ = e.Put(1, v)
	raw, _ := e.GetRaw(1)
	if !bytes.Equal(raw, v.Encode()) {
		t.Fatal("raw changed access")
	}
	r, _ := e.Get(1)
	if r.LastAccess() != 456 {
		t.Fatal("previous access")
	}
	access, _ := e.GetLastAccess(1)
	if access <= 456 {
		t.Fatal("not updated")
	}
}
func TestEngineStaticScanAllKeys(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	for i := uint64(0); i < 100; i++ {
		_ = e.Put(i*32, item(fmt.Sprint(i), 10))
	}
	seen := map[string]bool{}
	var c uint64
	for {
		var err error
		c, err = e.Scan(c, 7, func(v storage.Entry) bool {
			if seen[v.Key()] {
				t.Fatal("duplicate")
			}
			seen[v.Key()] = true
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		if c == 0 {
			break
		}
	}
	if len(seen) != 100 {
		t.Fatal(len(seen))
	}
	if _, err := e.ScanRegexMatch(0, "[", 2, func(storage.Entry) bool { return true }); err == nil {
		t.Fatal("regex")
	}
	count := 0
	e.Range(func(h uint64, v storage.Entry) bool { count++; _ = e.Delete(h); return false })
	if count != 1 || e.Stats().Length != 99 {
		t.Fatal("reentry/stop")
	}
}
func TestEngineExpiryAllDMapsAndBackups(t *testing.T) {
	p, security := testEngine(t, 8<<20, Security)
	_ = security.Put(1, item("persistent", 1))
	response := NewPool(8<<20, Response, nil)
	_ = response.Start(t.Context())
	t.Cleanup(func() { _ = response.Close(context.Background()) })
	var children []*Engine
	for range 16 {
		x, err := NewEngine(response).Fork(nil)
		if err != nil {
			t.Fatal(err)
		}
		e := x.(*Engine)
		children = append(children, e)
		v := item("expiring", 1024)
		v.SetTTL(time.Now().Add(20 * time.Millisecond).UnixMilli())
		_ = e.Put(1, v)
	}
	deadline := time.Now().Add(3 * time.Second)
	for response.Snapshot().Entries != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if response.Snapshot().Entries != 0 || response.Snapshot().Allocated != uint64(len(children))*fragmentCharge {
		t.Fatal(response.Snapshot())
	}
	if p.Snapshot().Entries != 1 || !security.Check(1) {
		t.Fatal("security lost")
	}
}
func TestEngineStatsAggregateAccounting(t *testing.T) {
	p, e := testEngine(t, 8<<20, Response)
	x, err := NewEngine(p).Fork(nil)
	if err != nil {
		t.Fatal(err)
	}
	other := x.(*Engine)
	_ = e.Put(1, item("a", 100))
	_ = other.Put(2, item("b", 100))
	if uint64(e.Stats().Allocated+other.Stats().Allocated) != p.Snapshot().Allocated {
		t.Fatal("attribution")
	}
	_ = e.Destroy()
	if uint64(other.Stats().Allocated) != p.Snapshot().Allocated {
		t.Fatal("ownership transfer")
	}
}
func TestEngineTTLAndClose(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	v := item("k", 10)
	_ = e.Put(1, v)
	v.SetTTL(-1)
	v.SetTimestamp(999)
	if err := e.UpdateTTL(1, v); err != nil {
		t.Fatal(err)
	}
	done, err := e.Compaction()
	if !done || err != nil || e.Check(1) {
		t.Fatal("expiry")
	}
	_ = e.Close()
	if err := e.Put(1, v); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := e.Get(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	_ = e.Destroy()
	_ = e.Destroy()
	if err := e.Start(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func FuzzNativeEntryDecode(f *testing.F) {
	f.Add(item("key", 1).Encode())
	f.Add([]byte{255})
	f.Fuzz(func(t *testing.T, b []byte) {
		e := NewEntry()
		e.Decode(b)
		if validRaw(b) && !bytes.Equal(e.Encode(), b) {
			t.Fatal("round trip")
		}
	})
}
func TestEngineRawEntryValidation(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	for _, b := range [][]byte{nil, {255}, make([]byte, 28), append(item("k", 1).Encode(), 0)} {
		if err := e.PutRaw(1, b); !errors.Is(err, ErrInvalidEntry) {
			t.Fatal(err)
		}
	}
	if err := e.Put(1, item(string(make([]byte, 256)), 0)); !errors.Is(err, storage.ErrKeyTooLarge) {
		t.Fatal(err)
	}
	if err := e.Put(1, item("k", MaxEntryBytes-30)); !errors.Is(err, storage.ErrEntryTooLarge) {
		t.Fatal(err)
	}
	if err := e.Put(1, item("k", MaxEntryBytes-31)); err != nil {
		t.Fatal(err)
	}
}
func TestEngineCloseDestroyRaces(t *testing.T) {
	p, e := testEngine(t, 8<<20, Response)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				_ = e.Put(1, item("k", 100))
				_, _ = e.Get(1)
				_, _ = e.Compaction()
				e.RangeHKey(func(uint64) bool { return true })
			}
		})
	}
	wg.Go(func() { _ = e.Close(); _ = e.Destroy() })
	wg.Wait()
	if p.Snapshot().Allocated != 0 {
		t.Fatal("destroy leak")
	}
}
