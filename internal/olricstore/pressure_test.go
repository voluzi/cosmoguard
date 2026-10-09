package olricstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/voluzi/olric/pkg/storage"
)

func TestEnginePressureLocalOrder(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprint(raw), func(t *testing.T) {
			p, e := testEngine(t, slabCharge+2*fragmentCharge, Response)
			sibling, err := e.Fork(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := sibling.PutRaw(100, item("foreign", 256<<10).Encode()); err != nil {
				t.Fatal(err)
			}
			for h := uint64(1); h <= 3; h++ {
				if err := e.PutRaw(h, item(fmt.Sprint(h), 256<<10).Encode()); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := e.Get(1); err != nil {
				t.Fatal(err)
			}
			incoming := item("incoming", 256<<10)
			incoming.SetTTL(time.Now().Add(time.Hour).UnixMilli())
			if raw {
				err = e.PutRaw(4, incoming.Encode())
			} else {
				err = e.Put(4, incoming)
			}
			if err != nil {
				t.Fatal("pressure must admit", err)
			}
			if _, err := e.GetRaw(2); !errors.Is(err, storage.ErrKeyNotFound) {
				t.Fatal("oldest retained", err)
			}
			for _, h := range []uint64{1, 3, 4} {
				if _, err := e.GetRaw(h); err != nil {
					t.Fatal(h, err)
				}
			}
			if _, err := sibling.GetRaw(100); err != nil {
				t.Fatal("foreign victim", err)
			}
			got, err := e.GetRaw(4)
			if err != nil {
				t.Fatal(err)
			}
			if raw && !bytes.Equal(got, incoming.Encode()) {
				t.Fatal("native metadata changed")
			}
			if p.Snapshot().Allocated > p.Snapshot().Capacity {
				t.Fatal("cap")
			}
		})
	}
}

func TestEnginePressureSecurityAndProtectedTarget(t *testing.T) {
	for _, policy := range []Policy{Response, Security} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			p, e := testEngine(t, slabCharge+2*fragmentCharge, policy)
			sibling, err := e.Fork(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.PutRaw(1, item("1", 256<<10).Encode()); err != nil {
				t.Fatal(err)
			}
			for h := uint64(2); h <= 4; h++ {
				if err := sibling.PutRaw(h, item(fmt.Sprint(h), 256<<10).Encode()); err != nil {
					t.Fatal(err)
				}
			}
			old, _ := e.GetRaw(1)
			err = e.PutRaw(1, item("1", 900<<10).Encode())
			if !errors.Is(err, ErrCapacity) {
				t.Fatal("must preserve target on failure", err)
			}
			got, _ := e.GetRaw(1)
			if !bytes.Equal(old, got) {
				t.Fatal("target changed")
			}
			if policy == Security && p.Snapshot().Entries != 4 {
				t.Fatal("security eviction")
			}
		})
	}
}

func TestEnginePressureBoundedWork(t *testing.T) {
	p, e := testEngine(t, slabCharge+fragmentCharge+8192, Response)
	// A 1MiB allocation needs more than 32 of these 16KiB blocks.
	for h := uint64(1); h <= 128; h++ {
		if err := e.PutRaw(h, item(fmt.Sprint(h), 8<<10).Encode()); err != nil {
			t.Fatal(err)
		}
	}
	before := p.Snapshot()
	if err := e.PutRaw(1000, item("large", 900<<10).Encode()); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	after := p.Snapshot()
	if before.Entries-after.Entries != 32 {
		t.Fatalf("victims=%d, want 32", before.Entries-after.Entries)
	}
	if err := e.PutRaw(1001, []byte{0}); !errors.Is(err, ErrInvalidEntry) {
		t.Fatal(err)
	}
	if p.Snapshot().Entries != after.Entries {
		t.Fatal("invalid entry evicted")
	}
	if err := e.PutRaw(1002, make([]byte, MaxEntryBytes)); !errors.Is(err, storage.ErrEntryTooLarge) {
		t.Fatal(err)
	}
	if p.Snapshot().Entries != after.Entries {
		t.Fatal("oversized entry evicted")
	}

}

func TestEnginePressureRecomputesOverwritePredecessor(t *testing.T) {
	p, e := testEngine(t, slabCharge+fragmentCharge, Response)
	const target = uint64(1)
	victim := uint64(2)
	for e.bucket(victim) != e.bucket(target) {
		victim++
	}
	others := []uint64{}
	for h := victim + 1; len(others) < 2; h++ {
		if e.bucket(h) != e.bucket(target) {
			others = append(others, h)
		}
	}
	for _, h := range []uint64{target, victim, others[0], others[1]} {
		if err := e.PutRaw(h, item(fmt.Sprint(h), 256<<10).Encode()); err != nil {
			t.Fatal(err)
		}
	}
	// The oldest eligible victim is also the target's hash-chain predecessor.
	p.mu.Lock()
	loc, prev := e.findLocked(target)
	predecessor := uint64(0)
	if prev != 0 {
		predecessor = field(e.p.arena.block(prev), 0)
	}
	p.mu.Unlock()
	if loc == 0 || prev == 0 || predecessor != victim {
		t.Fatal("fixture must place predecessor before target")
	}
	replacement := item("replacement", 900<<10).Encode()
	if err := e.PutRaw(target, replacement); err != nil {
		t.Fatal(err)
	}
	got, err := e.GetRaw(target)
	if err != nil || !bytes.Equal(got, replacement) {
		t.Fatal("replacement chain corrupted", err)
	}
	if s := p.Snapshot(); s.PressureEvictions == 0 || s.PressureEvictions > 3 || s.Entries != 4-s.PressureEvictions || s.Allocated > s.Capacity {
		t.Fatal("overwrite accounting", s)
	}
	if e.Check(victim) {
		t.Fatal("hash predecessor must be the first pressure victim")
	}
	if err := e.Delete(target); err != nil {
		t.Fatal(err)
	}
	if e.Check(target) {
		t.Fatal("stale hash chain resurrected the overwritten target")
	}
}
