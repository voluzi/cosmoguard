package olricstore

import (
	"context"
	"testing"
	"time"
)

func BenchmarkEngineSmallEntryMaintenance(b *testing.B) {
	for range b.N {
		b.StopTimer()
		p := NewPool(256<<20, Response, nil)
		engines := make([]*Engine, 271)
		for i := range engines {
			child, err := NewEngine(p).Fork(nil)
			if err != nil {
				b.Fatal(err)
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
				b.Fatal(err)
			}
		}
		b.StartTimer()
		start := time.Now()
		_, err := engines[0].Compaction()
		sweep := time.Since(start)
		if err != nil {
			b.Fatal(err)
		}
		start = time.Now()
		const deletes = 512
		last := entries - 1 - (entries-2)%271
		for i := range deletes {
			if err := engines[1].Delete(uint64(last - i*271)); err != nil {
				b.Fatal(err)
			}
		}
		burst := time.Since(start)
		b.StopTimer()
		b.ReportMetric(float64(sweep.Nanoseconds())/1e6, "sweep-ms")
		b.ReportMetric(float64(burst.Nanoseconds())/1e6, "512-deletes-ms")
		stats := p.Snapshot()
		if stats.Entries != entries-uint64((entries+270)/271)-deletes || stats.Allocated > stats.Capacity {
			b.Fatal("maintenance lost live entries or exceeded cap", stats)
		}
		if err := p.Close(context.Background()); err != nil {
			b.Fatal(err)
		}
		if sweep > 100*time.Millisecond || burst > 100*time.Millisecond {
			b.Fatalf("pool stalled: sweep=%s, 512 deletes=%s", sweep, burst)
		}
	}
}
