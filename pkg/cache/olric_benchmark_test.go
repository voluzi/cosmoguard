package cache

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// Step 8 of the v4 plan decided whether to keep the standalone memory
// backend. The decision hinged on this benchmark: if olric embedded was
// within ~2x of ttlcache for Set/Get on a single instance, the memory
// backend would have been dropped. Two payload shapes per backend:
//
//   - Cache[string, int]    — struct-shaped payload, exercises msgpack on
//                             the non-bypass path.
//   - Cache[string, []byte] — byte payload, exercises olric's []byte bypass
//                             (the path response cache + JSON-RPC cache use
//                             at runtime).
//
// Reproduce with:
//
//	go test -run=^$ -bench=BenchmarkCache -benchmem -count=3 \
//	    ./pkg/cache/
//
// The benchmark uses sequential keys with TTL=10s so neither cache evicts
// during the run.
//
// Measured on Apple M5 Pro, darwin/arm64, 2026-05-27 (median of three
// 2-second runs):
//
//	BenchmarkCacheMemorySetInt        ~400 ns/op    3 allocs/op
//	BenchmarkCacheMemoryGetInt         ~61 ns/op    0 allocs/op
//	BenchmarkCacheMemorySetBytes      ~405 ns/op    3 allocs/op
//	BenchmarkCacheMemoryGetBytes       ~60 ns/op    0 allocs/op
//	BenchmarkCacheOlricSetInt         ~450 ns/op   16 allocs/op (~1.1x)
//	BenchmarkCacheOlricGetInt         ~395 ns/op   19 allocs/op (~6.5x)
//	BenchmarkCacheOlricSetBytes       ~660 ns/op   15 allocs/op (~1.6x)
//	BenchmarkCacheOlricGetBytes       ~353 ns/op   17 allocs/op (~5.9x)
//
// Set is within striking distance of ttlcache; Get is ~6x slower because
// the embedded olric path still walks its partition map and runs the
// EmbeddedClient request plumbing even though the operation never leaves
// the process. ~6x is well outside the "within ~2x" gate, so the memory
// backend stays as an explicit opt-in for the rare single-instance
// deployment that profiles Get as the hot spot. Default (olric) remains
// right for >95% of deployments because the embedded-only Get cost is
// still measured in hundreds of nanoseconds — orders of magnitude below
// the upstream-call latency the cache sits in front of.

const benchTTL = 10 * time.Second

func benchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}
	return keys
}

func benchBytesPayload() []byte {
	// 512 B — representative of small JSON-RPC response payloads.
	b := make([]byte, 512)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// --- memory (ttlcache) ---------------------------------------------------

func BenchmarkCacheMemorySetInt(b *testing.B) {
	cache, err := NewMemoryCache[string, int]("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	keys := benchKeys(b.N)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cache.Set(ctx, keys[i], i, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheMemoryGetInt(b *testing.B) {
	cache, err := NewMemoryCache[string, int]("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	ctx := context.Background()
	keys := benchKeys(1024)
	for i, k := range keys {
		if err := cache.Set(ctx, k, i, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(ctx, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheMemorySetBytes(b *testing.B) {
	cache, err := NewMemoryCache[string, []byte]("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	payload := benchBytesPayload()
	keys := benchKeys(b.N)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cache.Set(ctx, keys[i], payload, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheMemoryGetBytes(b *testing.B) {
	cache, err := NewMemoryCache[string, []byte]("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	payload := benchBytesPayload()
	ctx := context.Background()
	keys := benchKeys(1024)
	for _, k := range keys {
		if err := cache.Set(ctx, k, payload, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(ctx, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

// --- olric embedded ------------------------------------------------------

func BenchmarkCacheOlricSetInt(b *testing.B) {
	client := embeddedOlric(b)
	cache, err := NewOlricCache[string, int](client, "bench-set-int")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	ctx := context.Background()
	keys := benchKeys(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cache.Set(ctx, keys[i], i, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheOlricGetInt(b *testing.B) {
	client := embeddedOlric(b)
	cache, err := NewOlricCache[string, int](client, "bench-get-int")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	ctx := context.Background()
	keys := benchKeys(1024)
	for i, k := range keys {
		if err := cache.Set(ctx, k, i, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(ctx, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheOlricSetBytes(b *testing.B) {
	client := embeddedOlric(b)
	cache, err := NewOlricCache[string, []byte](client, "bench-set-bytes")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	payload := benchBytesPayload()
	keys := benchKeys(b.N)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cache.Set(ctx, keys[i], payload, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheOlricGetBytes(b *testing.B) {
	client := embeddedOlric(b)
	cache, err := NewOlricCache[string, []byte](client, "bench-get-bytes")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	payload := benchBytesPayload()
	ctx := context.Background()
	keys := benchKeys(1024)
	for _, k := range keys {
		if err := cache.Set(ctx, k, payload, benchTTL); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Get(ctx, keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}
