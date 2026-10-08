package olricstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/voluzi/olric/config"
	"github.com/voluzi/olric/pkg/storage"
)

func nativeEngine(t *testing.T) storage.Engine {
	t.Helper()
	c := config.NewEngine()
	if err := c.Sanitize(); err != nil {
		t.Fatal(err)
	}
	e, err := c.Implementation.Fork(storage.NewConfig(c.Config))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close(); _ = e.Destroy() })
	return e
}
func TestNativeWireBothDirections(t *testing.T) {
	for _, direction := range []string{"native-to-bounded", "bounded-to-native"} {
		t.Run(direction, func(t *testing.T) {
			_, bounded := testEngine(t, 8<<20, Response)
			native := nativeEngine(t)
			var src, dst storage.Engine = bounded, native
			if direction == "native-to-bounded" {
				src, dst = native, bounded
			}
			v := item("binary\x00key", 16384)
			v.SetTTL(time.Now().Add(time.Hour).UnixMilli())
			raw := v.Encode()
			for i := uint64(0); i < 40; i++ {
				if err := src.PutRaw(i, raw); err != nil {
					t.Fatal(err)
				}
			}
			it := src.TransferIterator()
			for it.Next() {
				data, idx, err := it.Export()
				if err != nil {
					t.Fatal(err)
				}
				if err := dst.Import(data, func(h uint64, e storage.Entry) error { return dst.PutRaw(h, e.Encode()) }); err != nil {
					t.Fatal(err)
				}
				if err := it.Drop(idx); err != nil {
					t.Fatal(err)
				}
			}
			for i := uint64(0); i < 40; i++ {
				b, err := dst.GetRaw(i)
				if err != nil || !bytes.Equal(b, raw) {
					t.Fatalf("native wire mismatch %d: %v", i, err)
				}
			}
		})
	}
}
func TestTransferDropOnlyAcknowledgedGeneration(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	for h := uint64(1); h <= 4; h++ {
		if err := e.Put(h, item(fmt.Sprint(h), 10)); err != nil {
			t.Fatal(err)
		}
	}
	it := e.TransferIterator()
	_, idx, err := it.Export()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Get(1); err != nil {
		t.Fatal(err)
	}
	ttl := item("2", 10)
	ttl.SetTTL(time.Now().Add(time.Hour).UnixMilli())
	if err := e.UpdateTTL(2, ttl); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(3, item("overwrite", 10)); err != nil {
		t.Fatal(err)
	}
	if err := e.Put(5, item("insert", 10)); err != nil {
		t.Fatal(err)
	}
	if err := it.Drop(idx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		hash uint64
		key  string
	}{{1, "1"}, {2, "2"}, {3, "overwrite"}, {5, "insert"}} {
		v, err := e.Get(tc.hash)
		if err != nil || v.Key() != tc.key || !bytes.Equal(v.Value(), bytes.Repeat([]byte{42}, 10)) {
			t.Fatalf("dropped touched key %d: %v", tc.hash, err)
		}
	}
	if ttl, err := e.GetTTL(2); err != nil || ttl <= time.Now().UnixMilli() {
		t.Fatal("lost renewed TTL", err)
	}
	if _, err := e.Get(4); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatal("retained untouched export", err)
	}
}
func TestTransferResponseFullMakesProgress(t *testing.T) {
	_, src := testEngine(t, 8<<20, Response)
	_ = src.Put(1, item("k", 1024))
	data, _, err := src.TransferIterator().Export()
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []Policy{Response, Security} {
		p, dst := testEngine(t, fragmentCharge, policy)
		err := dst.Import(data, func(h uint64, v storage.Entry) error { return dst.Put(h, v) })
		if policy == Response {
			if err != nil || p.Snapshot().ImportDropped != 1 {
				t.Fatal(err, p.Snapshot())
			}
		} else {
			if !errors.Is(err, ErrCapacity) || p.Snapshot().ImportDropped != 0 {
				t.Fatal("security error swallowed", err)
			}
		}
	}
}
func TestTransferFailurePreservesSource(t *testing.T) {
	_, src := testEngine(t, 8<<20, Response)
	_ = src.Put(1, item("k", 1024))
	it := src.TransferIterator()
	data, _, err := it.Export()
	if err != nil {
		t.Fatal(err)
	}
	_, dst := testEngine(t, 8<<20, Response)
	sentinel := errors.New("interrupted")
	if err := dst.Import(data, func(uint64, storage.Entry) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if !src.Check(1) {
		t.Fatal("source lost")
	}
	if err := dst.Import(data, func(h uint64, v storage.Entry) error { return dst.Put(h, v) }); err != nil {
		t.Fatal(err)
	}
}
func TestTransferScratchRejectedThenRetry(t *testing.T) {
	p, e := testEngine(t, 8<<20, Response)
	_ = e.Put(1, item("k", 1))
	l, ok := p.codec.TryAcquire(8 << 20)
	if !ok {
		t.Fatal("lease")
	}
	if _, _, err := e.TransferIterator().Export(); !errors.Is(err, ErrScratch) {
		t.Fatal(err)
	}
	l.Release()
	if _, _, err := e.TransferIterator().Export(); err != nil {
		t.Fatal(err)
	}
	if p.Snapshot().Codec.Reserved != 0 {
		t.Fatal("leak")
	}
}
func TestNativeRunContainerPackRoundTrip(t *testing.T) {
	data, err := os.ReadFile("testdata/native-run-pack.msgpack")
	if err != nil {
		t.Fatal(err)
	}
	var pack nativePack
	if err := msgpack.Unmarshal(data, &pack); err != nil {
		t.Fatal(err)
	}
	// Roaring's run cookie follows the 64-bit bucket count and 32-bit bucket key.
	if binary.LittleEndian.Uint32(pack.OffsetIndex[12:16])&65535 != 12347 || pack.OffsetIndex[16]&1 == 0 {
		t.Fatal("fixture does not contain a run container")
	}
	_, bounded := testEngine(t, 8<<20, Response)
	native := nativeEngine(t)
	for _, engine := range []storage.Engine{bounded, native} {
		if err := engine.Import(data, func(h uint64, v storage.Entry) error { return engine.PutRaw(h, v.Encode()) }); err != nil {
			t.Fatal(err)
		}
		raw, err := engine.GetRaw(42)
		if err != nil || !bytes.Equal(raw, pack.Memory) {
			t.Fatal("native run record", err)
		}
	}
	exported, _, err := bounded.TransferIterator().Export()
	if err != nil {
		t.Fatal(err)
	}
	if err := native.Import(exported, func(h uint64, v storage.Entry) error { return native.PutRaw(h, v.Encode()) }); err != nil {
		t.Fatal(err)
	}
	raw, err := native.GetRaw(42)
	if err != nil || !bytes.Equal(raw, pack.Memory) {
		t.Fatal("native run round trip", err)
	}
}

func FuzzNativePackImport(f *testing.F) {
	p := nativePack{Allocated: MaxEntryBytes, State: 2, HKeys: map[uint64]uint64{}, OffsetIndex: make([]byte, 8)}
	b, _ := msgpack.Marshal(p)
	f.Add(b)
	for _, name := range []string{"native-pack.msgpack", "native-run-pack.msgpack"} {
		seed, err := os.ReadFile("testdata/" + name)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(seed)
	}
	f.Add([]byte{0xdf, 255, 255, 255, 255})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 2<<20 {
			return
		}
		p, e := testEngine(t, 8<<20, Response)
		_ = e.Import(b, func(h uint64, v storage.Entry) error { return e.Put(h, v) })
		if p.Snapshot().Codec.Reserved != 0 || p.Snapshot().Allocated > 8<<20 {
			t.Fatal("budget leak")
		}
	})
}
func TestNativeEntryGolden(t *testing.T) {
	b, err := os.ReadFile("testdata/native-entry.bin")
	if err != nil {
		t.Fatal(err)
	}
	e := nativeEngine(t).NewEntry()
	e.Decode(b)
	v := NewEntry()
	v.Decode(b)
	if v.Key() != "binary\x00key" || v.Timestamp() != 123 || v.LastAccess() != 456 || !bytes.Equal(v.Encode(), e.Encode()) || !bytes.Equal(v.Encode(), b) {
		t.Fatal("native entry contract")
	}
}
func TestNativePackGoldenDecode(t *testing.T) {
	b, err := os.ReadFile("testdata/native-pack.msgpack")
	if err != nil {
		t.Fatal(err)
	}
	_, e := testEngine(t, 8<<20, Response)
	if err := e.Import(b, func(h uint64, v storage.Entry) error { return e.PutRaw(h, v.Encode()) }); err != nil {
		t.Fatal(err)
	}
	v, err := e.Get(42)
	if err != nil || v.Key() != "binary\x00key" || v.Timestamp() != 123 || !bytes.Equal(v.Value(), []byte{0, 1, 254, 255}) {
		t.Fatal("golden pack", err)
	}
}

func TestSparseExportWorkspaceScalesWithFragment(t *testing.T) {
	for _, size := range []int{0, 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			_, e := testEngine(t, 8<<20, Response)
			if size != 0 {
				if err := e.PutRaw(1, item("key", size).Encode()); err != nil {
					t.Fatal(err)
				}
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for range 10 {
				data, _, err := e.TransferIterator().Export()
				if err != nil {
					t.Fatal(err)
				}
				var p nativePack
				if err := msgpack.Unmarshal(data, &p); err != nil {
					t.Fatal(err)
				}
				if p.Allocated != MaxEntryBytes || len(p.HKeys) != min(size, 1) {
					t.Fatal("native table envelope", p.Allocated, len(p.HKeys))
				}
			}
			runtime.ReadMemStats(&after)
			if average := (after.TotalAlloc - before.TotalAlloc) / 10; average > 64<<10 {
				t.Fatalf("sparse export workspace: %d bytes/call, want under 64KiB", average)
			}
		})
	}
}

func TestNativeTableBoundaryPacksRemainImportable(t *testing.T) {
	for _, sizes := range [][]int{{MaxEntryBytes / 2, MaxEntryBytes / 2}, {MaxEntryBytes - 1}} {
		t.Run(fmt.Sprint(sizes), func(t *testing.T) {
			native := nativeEngine(t)
			_, bounded := testEngine(t, 8<<20, Response)
			originals := make(map[uint64][]byte)
			for i, size := range sizes {
				raw := item("k", size-30).Encode()
				if len(raw) != size {
					t.Fatalf("fixture size %d, want %d", len(raw), size)
				}
				h := uint64(i + 1)
				originals[h] = raw
				if err := native.PutRaw(h, raw); err != nil {
					t.Fatal(err)
				}
			}
			if native.Stats().NumTables != len(sizes) {
				t.Fatal("native admitted an exactly full table", native.Stats())
			}
			it := native.TransferIterator()
			packs := 0
			for it.Next() {
				data, id, err := it.Export()
				if err != nil {
					t.Fatal(err)
				}
				var pack nativePack
				if err := msgpack.Unmarshal(data, &pack); err != nil {
					t.Fatal(err)
				}
				if pack.Offset >= pack.Allocated || len(pack.Memory) >= MaxEntryBytes {
					t.Fatal("native exported an exactly full table")
				}
				if err := bounded.Import(data, func(h uint64, e storage.Entry) error { return bounded.PutRaw(h, e.Encode()) }); err != nil {
					t.Fatal(err)
				}
				if err := it.Drop(id); err != nil {
					t.Fatal(err)
				}
				packs++
			}
			if packs != len(sizes) {
				t.Fatalf("exported %d tables, want %d", packs, len(sizes))
			}
			for h, want := range originals {
				got, err := bounded.GetRaw(h)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("boundary transfer %d: %v", h, err)
				}
			}
		})
	}
}
