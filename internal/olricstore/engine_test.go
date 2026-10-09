package olricstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/voluzi/olric/pkg/storage"
)

func testEngine(t *testing.T, limit uint64, policy Policy) (*Pool, *Engine) {
	t.Helper()
	p := NewPool(limit, policy, nil)
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
func TestEngineOverwriteReclaimsNeighbors(t *testing.T) {
	p, e := testEngine(t, slabCharge+fragmentCharge, Response)
	for i := uint64(0); i < 4; i++ {
		if err := e.Put(i, item(fmt.Sprint(i), 256<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Put(0, item("0", 900<<10)); err != nil {
		t.Fatal("larger overwrite should reclaim eligible neighbors", err)
	}
	got, err := e.Get(0)
	if err != nil || len(got.Value()) != 900<<10 {
		t.Fatal("replacement", err)
	}
	if p.Snapshot().PressureEvictions == 0 || p.Snapshot().Allocated > p.Snapshot().Capacity {
		t.Fatal("pressure accounting", p.Snapshot())
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
	foreign, err := e.Fork(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := foreign.Put(10000, item("foreign", 900<<10)); !errors.Is(err, ErrCapacity) {
		t.Fatal("empty fragment must reject", err)
	}
	if err := foreign.PutRaw(10000, item("foreign", 900<<10).Encode()); !errors.Is(err, ErrCapacity) {
		t.Fatal("empty raw fragment must reject", err)
	}
	if p.Snapshot().PutRejected == 0 || p.Snapshot().RawRejected == 0 || p.Snapshot().PressureEvictions == 0 {
		t.Fatal("missing pressure/rejection accounting", p.Snapshot())
	}
}
func TestEngineLastAccessNativeSemantics(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	v := item("k", 5)
	_ = e.PutRaw(1, v.Encode())
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
	for _, tc := range []struct {
		pattern string
		want    int
	}{{"^.*$", 100}, {"^missing$", 0}} {
		seen := map[string]bool{}
		cursor := uint64(0)
		for {
			var err error
			cursor, err = e.ScanRegexMatch(cursor, tc.pattern, 7, func(v storage.Entry) bool {
				if seen[v.Key()] || !bytes.Equal(v.Value(), bytes.Repeat([]byte{42}, 10)) {
					t.Fatal("invalid regex scan entry", v.Key())
				}
				seen[v.Key()] = true
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			if cursor == 0 {
				break
			}
		}
		if len(seen) != tc.want {
			t.Fatal(tc.pattern, len(seen), tc.want)
		}
	}
	count := 0
	e.Range(func(h uint64, v storage.Entry) bool { count++; _ = e.Delete(h); return false })
	if count != 1 || e.Stats().Length != 99 {
		t.Fatal("reentry/stop")
	}
}

func TestEngineScanTerminatesWhileReadingEachPage(t *testing.T) {
	for _, regex := range []bool{false, true} {
		t.Run(fmt.Sprint(regex), func(t *testing.T) {
			_, e := testEngine(t, 8<<20, Response)
			const count = 100
			for h := uint64(0); h < count; h++ {
				if err := e.Put(h, item(fmt.Sprint(h), 10)); err != nil {
					t.Fatal(err)
				}
			}
			seen := make(map[string]bool)
			readOtherKeys := true
			var cursor uint64
			for page := 0; page <= count; page++ {
				var keys []string
				visit := func(v storage.Entry) bool {
					if readOtherKeys {
						readOtherKeys = false
						for h := uint64(0); h < count; h++ {
							if _, err := e.Get(h); err != nil {
								t.Fatal(err)
							}
						}
					}
					keys = append(keys, v.Key())
					seen[v.Key()] = true
					return true
				}
				var err error
				if regex {
					cursor, err = e.ScanRegexMatch(cursor, "^[0-9]+$", 7, visit)
				} else {
					cursor, err = e.Scan(cursor, 7, visit)
				}
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range keys {
					var h uint64
					_, _ = fmt.Sscan(key, &h)
					if _, err := e.Get(h); err != nil {
						t.Fatal(err)
					}
				}
				if cursor == 0 {
					if len(seen) != count {
						t.Fatal("scan omitted unchanged keys", len(seen))
					}
					return
				}
			}
			t.Fatal("scan did not terminate while reading unchanged keys")
		})
	}
}
func TestEngineExpiryAllDMapsAndBackups(t *testing.T) {
	p, security := testEngine(t, 8<<20, Security)
	_ = security.Put(1, item("persistent", 1))
	response := NewPool(8<<20, Response, nil)
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
		for _, child := range children {
			_, _ = child.Compaction()
		}
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
	engines := []*Engine{e}
	for range 2 {
		child, err := NewEngine(p).Fork(nil)
		if err != nil {
			t.Fatal(err)
		}
		engines = append(engines, child.(*Engine))
	}
	for h := range uint64(2) {
		if err := engines[h].Put(h, item("large", 900<<10)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Put(3, item("small", 100)); err != nil {
		t.Fatal(err)
	}
	check := func() {
		var allocated, inuse, entries uint64
		tables := 0
		for _, engine := range engines {
			stats := engine.Stats()
			allocated += uint64(stats.Allocated)
			inuse += uint64(stats.Inuse)
			entries += uint64(stats.Length)
			tables += stats.NumTables
		}
		pool := p.Snapshot()
		if allocated != pool.Allocated || inuse != pool.Inuse || entries != pool.Entries || tables != 2 {
			t.Fatal("shared accounting", allocated, inuse, entries, tables, pool)
		}
	}
	check()
	if err := engines[2].Destroy(); err != nil {
		t.Fatal(err)
	}
	check()
}
func TestEngineKeyAndTTLMetadata(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	v := item("key-π", 10)
	v.SetTTL(time.Now().Add(time.Minute).UnixMilli())
	if err := e.Put(1, v); err != nil {
		t.Fatal(err)
	}
	key, err := e.GetKey(1)
	if err != nil || key != v.Key() {
		t.Fatal("stored key", key, err)
	}
	ttl, err := e.GetTTL(1)
	if err != nil || ttl != v.TTL() {
		t.Fatal("stored deadline", ttl, err)
	}
	if _, err := e.GetKey(2); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatal("missing key", err)
	}
	if _, err := e.GetTTL(2); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatal("missing deadline", err)
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

func TestEnginePutInitializesNativeLastAccess(t *testing.T) {
	_, bounded := testEngine(t, 8<<20, Response)
	for _, e := range []storage.Engine{nativeEngine(t), bounded} {
		v := item("k", 5)
		before := time.Now().UnixNano()
		if err := e.Put(1, v); err != nil {
			t.Fatal(err)
		}
		access, err := e.GetLastAccess(1)
		if err != nil || access < before || access > time.Now().UnixNano() {
			t.Fatalf("Put must initialize access time: %d, %v", access, err)
		}
		if v.LastAccess() != 456 {
			t.Fatal("Put mutated caller entry")
		}
	}
}

func TestEngineRangeHKeyDoesNotCopyPayloads(t *testing.T) {
	_, e := testEngine(t, 32<<20, Response)
	for i := uint64(0); i < 10; i++ {
		if err := e.Put(i, item(fmt.Sprint(i), 900<<10)); err != nil {
			t.Fatal(err)
		}
	}
	seen := 0
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	e.RangeHKey(func(uint64) bool { seen++; return true })
	runtime.ReadMemStats(&after)
	if seen != 10 {
		t.Fatalf("visited %d hashes", seen)
	}
	if after.TotalAlloc-before.TotalAlloc >= 1<<20 {
		t.Fatal("hash traversal copied value payloads")
	}
}

func TestEngineUpdateTTLNativeLastAccess(t *testing.T) {
	_, bounded := testEngine(t, 8<<20, Response)
	for _, e := range []storage.Engine{nativeEngine(t), bounded} {
		v := item("key", 5)
		if err := e.PutRaw(1, v.Encode()); err != nil {
			t.Fatal(err)
		}
		v.SetTTL(time.Now().Add(time.Hour).UnixMilli())
		v.SetTimestamp(789)
		before := time.Now().UnixNano()
		if err := e.UpdateTTL(1, v); err != nil {
			t.Fatal(err)
		}
		access, err := e.GetLastAccess(1)
		if err != nil || access < before || access > time.Now().UnixNano() {
			t.Fatalf("UpdateTTL access: %d, %v", access, err)
		}
		raw, err := e.GetRaw(1)
		if err != nil {
			t.Fatal(err)
		}
		out := NewEntry()
		out.Decode(raw)
		if out.Timestamp() != 789 || out.TTL() != v.TTL() || !bytes.Equal(out.Value(), v.Value()) {
			t.Fatal("TTL update changed payload or omitted metadata")
		}
	}
}

func TestPoolCountsFragmentAdmissionFailures(t *testing.T) {
	var observed []string
	var p *Pool
	p = NewPool(fragmentCharge-1, Response, func(path string) {
		if p.Snapshot().Allocated != 0 {
			t.Error("failed registration retained backing")
		}
		observed = append(observed, path)
	})
	t.Cleanup(func() { _ = p.Close(context.Background()) })
	e := NewEngine(p)
	if _, err := e.Fork(nil); !errors.Is(err, ErrCapacity) {
		t.Fatal("fork admission", err)
	}
	if err := e.Put(1, item("key", 1)); !errors.Is(err, ErrCapacity) {
		t.Fatal("put registration", err)
	}
	if err := e.PutRaw(1, item("key", 1).Encode()); !errors.Is(err, ErrCapacity) {
		t.Fatal("raw registration", err)
	}
	if got := fmt.Sprint(observed); got != "[fork put put_raw]" {
		t.Fatal("missing receiving rejection", got)
	}
	if s := p.Snapshot(); s.ForkRejected != 1 || s.PutRejected != 1 || s.RawRejected != 1 {
		t.Fatal("registration rejection counters", s)
	}
}
