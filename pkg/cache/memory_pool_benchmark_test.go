package cache

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func memoryHitBench[K comparable](b *testing.B, shared bool, protocols int, parallel, mixed bool, key K) {
	p := NewMemoryPool(0, 0)
	defer p.Close()
	keys := make([]K, 1024)
	for i := range keys {
		switch any(key).(type) {
		case string:
			keys[i] = any(fmt.Sprintf("key-%d", i)).(K)
		case uint64:
			keys[i] = any(uint64(i + 1000)).(K)
		}
	}
	caches := make([]Cache[K, []byte], protocols)
	for i := range caches {
		var opts []Option
		if shared {
			opts = append(opts, WithMemoryPool(p))
		}
		c, err := NewMemoryCache[K, []byte](fmt.Sprint(i), opts...)
		if err != nil {
			b.Fatal(err)
		}
		defer c.Close()
		caches[i] = c
		for _, key := range keys {
			if err := c.Set(context.Background(), key, []byte("cached"), time.Hour); err != nil {
				b.Fatal(err)
			}
		}
	}
	var workers atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		// Exercise every protocol even when there are fewer CPUs than adapters.
		procs := runtime.GOMAXPROCS(0)
		b.SetParallelism((protocols + procs - 1) / procs)
		b.RunParallel(func(pb *testing.PB) {
			c := caches[int(workers.Add(1)-1)%protocols]
			i := 0
			for pb.Next() {
				if mixed && i%64 == 0 {
					if err := c.Set(context.Background(), keys[i%len(keys)], []byte("cached"), time.Hour); err != nil {
						b.Error(err)
					}
				} else {
					if _, err := c.Get(context.Background(), keys[i%len(keys)]); err != nil {
						b.Error(err)
					}
				}
				i++
			}
		})
	} else {
		for i := 0; i < b.N; i++ {
			if _, err := caches[i%protocols].Get(context.Background(), keys[i%len(keys)]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

func BenchmarkMemoryPoolHits(b *testing.B) {
	for _, shared := range []bool{false, true} {
		for _, protocols := range []int{1, 4, 8} {
			for _, mode := range []string{"sequential", "parallel", "mixed"} {
				name := fmt.Sprintf("shared=%v/protocols=%d/%s", shared, protocols, mode)
				b.Run(name+"/string", func(b *testing.B) { memoryHitBench(b, shared, protocols, mode != "sequential", mode == "mixed", "key") })
				b.Run(name+"/uint64", func(b *testing.B) {
					memoryHitBench(b, shared, protocols, mode != "sequential", mode == "mixed", uint64(1000))
				})
			}
		}
	}
}

func TestMemoryPoolHitPerformance(t *testing.T) {
	for _, keyType := range []string{"string", "uint64"} {
		t.Run(keyType, func(t *testing.T) {
			p := NewMemoryPool(0, 0)
			defer p.Close()
			if keyType == "string" {
				c := pooled[string, []byte](t, p, "allocations")
				require.NoError(t, c.Set(t.Context(), "key", nil, time.Hour))
				require.Zero(t, testing.AllocsPerRun(1000, func() { _, _ = c.Get(t.Context(), "key") }))
			} else {
				c := pooled[uint64, []byte](t, p, "allocations")
				require.NoError(t, c.Set(t.Context(), 1000, nil, time.Hour))
				require.Zero(t, testing.AllocsPerRun(1000, func() { _, _ = c.Get(t.Context(), 1000) }))
			}
			// Race instrumentation changes timing and short samples include cleaner
			// startup allocations. Allocation gates above exercise warmed hits.
			if benchmarkRaceEnabled {
				return
			}
			var samples [2][]int64
			for round := range 3 {
				for j := range 2 {
					mode := (round + j) % 2
					result := testing.Benchmark(func(b *testing.B) {
						if keyType == "string" {
							memoryHitBench(b, mode == 1, 1, false, false, "key")
						} else {
							memoryHitBench(b, mode == 1, 1, false, false, uint64(1000))
						}
					})
					samples[mode] = append(samples[mode], result.NsPerOp())
				}
			}
			for i := range samples {
				sort.Slice(samples[i], func(a, b int) bool { return samples[i][a] < samples[i][b] })
			}
			t.Logf("legacy=%v shared=%v ns/op", samples[0], samples[1])
			require.LessOrEqual(t, samples[1][1], samples[0][1]*2, "gross hot-path regression")
		})
	}
}
