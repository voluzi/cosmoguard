package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

type rejectedSeedCache struct{ cache.Cache[string, []byte] }

func (rejectedSeedCache) Set(context.Context, string, []byte, time.Duration) error {
	return olricstore.ErrCapacity
}

func TestSparseSeedRequiresSuccessfulWrites(t *testing.T) {
	err := seedResponsePartitions(t.Context(), rejectedSeedCache{}, "cosmoguard-lcd", time.Minute)
	if !errors.Is(err, olricstore.ErrCapacity) {
		t.Fatalf("seed error=%v, want capacity rejection", err)
	}
}

func TestSparseSeedCoversEveryPartition(t *testing.T) {
	cc, err := cache.NewMemoryCache[string, []byte]("cosmoguard-lcd")
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	if err := seedResponsePartitions(t.Context(), cc, "cosmoguard-lcd", time.Minute); err != nil {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for k := 0; k < 10000; k++ {
		key := fmt.Sprintf("seed-%d", k)
		value, err := cc.Get(t.Context(), key)
		if errors.Is(err, cache.ErrNotFound) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if string(value) != "seed" {
			t.Fatalf("unexpected seed value %q", value)
		}
		p := xxhash.Sum64String("cosmoguard-lcd"+key) % 271
		if seen[p] {
			t.Fatalf("partition %d seeded twice", p)
		}
		seen[p] = true
	}
	if len(seen) != 271 {
		t.Fatalf("seeded %d partitions, want 271", len(seen))
	}
}
