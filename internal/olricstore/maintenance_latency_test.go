package olricstore

import (
	"context"
	"testing"
	"time"
)

func TestEngineSmallEntryMaintenance(t *testing.T) {
	p := NewPool(256<<20, Response, nil)
	engines := make([]*Engine, 271)
	for i := range engines {
		child, err := NewEngine(p).Fork(nil)
		if err != nil {
			t.Fatal(err)
		}
		engines[i] = child.(*Engine)
	}
	expired := NewEntry()
	expired.SetKey("small")
	expired.SetValue(make([]byte, 100))
	expired.SetTTL(time.Now().Add(-time.Hour).UnixMilli())
	live := NewEntry()
	live.SetKey("small")
	live.SetValue(make([]byte, 100))
	live.SetTTL(time.Now().Add(time.Hour).UnixMilli())
	expiredRaw, liveRaw := expired.Encode(), live.Encode()
	const entries = 500000
	for i := range entries {
		raw := liveRaw
		if i%271 == 0 {
			raw = expiredRaw
		}
		if err := engines[i%271].PutRaw(uint64(i), raw); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	_, err := engines[0].Compaction()
	sweep := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	const deletes = 512
	last := entries - 1 - (entries-2)%271
	for i := range deletes {
		if err := engines[1].Delete(uint64(last - i*271)); err != nil {
			t.Fatal(err)
		}
	}
	burst := time.Since(start)
	t.Logf("sweep=%s", sweep)
	t.Logf("512 deletes=%s", burst)
	stats := p.Snapshot()
	if stats.Entries != entries-uint64((entries+270)/271)-deletes || stats.Allocated > stats.Capacity {
		t.Fatal("maintenance lost live entries or exceeded cap", stats)
	}
	if sweep > time.Second || burst > time.Second {
		t.Fatalf("pool stalled: sweep=%s, 512 deletes=%s", sweep, burst)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
