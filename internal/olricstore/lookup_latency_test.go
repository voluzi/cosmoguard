package olricstore

import (
	"errors"
	"testing"
	"time"
)

func TestEngineIndexGrowthCannotExceedPoolCapacity(t *testing.T) {
	p, e := testEngine(t, slabCharge+fragmentCharge, Response)
	for h := uint64(0); h < 128; h++ {
		if err := e.Put(h, item("small", 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Put(128, item("new", 8)); !errors.Is(err, ErrCapacity) {
		t.Fatal("index growth must reject before exceeding the pool cap", err)
	}
	for h := uint64(0); h < 128; h++ {
		if !e.Check(h) {
			t.Fatal("failed index growth lost an existing key", h)
		}
	}
	if e.Check(128) || p.Snapshot().Allocated > p.Snapshot().Capacity {
		t.Fatal("failed index growth admitted a key or exceeded the cap", p.Snapshot())
	}
}

func TestEngineLargeFragmentLookupLatency(t *testing.T) {
	p, e := testEngine(t, 32<<20, Response)
	const count = 100000
	raw := item("small", 8).Encode()
	populate := time.Now()
	for i := range count {
		if i%1024 == 0 && time.Since(populate) > time.Minute {
			t.Fatalf("put stalled populating a 100k-key fragment: %d keys in %s", i, time.Since(populate))
		}
		if err := e.PutRaw(uint64(i)*16, raw); err != nil {
			t.Fatal(err)
		}
	}
	const operations = 1024
	for _, operation := range []string{"get", "put", "delete"} {
		start := time.Now()
		for i := range operations {
			h := uint64(i) * 16
			var err error
			switch operation {
			case "get":
				_, err = e.Get(h)
			case "put":
				err = e.PutRaw(h, raw)
			case "delete":
				err = e.Delete(h)
			}
			if err != nil {
				t.Fatal(operation, err)
			}
		}
		elapsed := time.Since(start)
		t.Logf("%s: %d operations in %s", operation, operations, elapsed)
		if elapsed > time.Second {
			t.Errorf("%s stalled a large fragment for %s", operation, elapsed)
		}
	}
	start := time.Now()
	visited := 0
	e.RangeHKey(func(uint64) bool {
		visited++
		return time.Since(start) < 10*time.Second
	})
	if visited != count-operations {
		t.Fatalf("range stalled after %d of %d keys in %s", visited, count-operations, time.Since(start))
	}
	t.Logf("range: %d keys in %s", visited, time.Since(start))
	if p.Snapshot().Allocated > p.Snapshot().Capacity {
		t.Fatal("index growth exceeded the pool cap", p.Snapshot())
	}
}

func TestEngineRangeHKeyCanDeleteCurrentAndUpcomingKeys(t *testing.T) {
	_, e := testEngine(t, 8<<20, Response)
	for h := uint64(0); h < 100; h++ {
		if err := e.Put(h, item("small", 8)); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[uint64]bool)
	e.RangeHKey(func(h uint64) bool {
		if seen[h] || h == 32 {
			t.Fatal("visited a duplicate or deleted key", h)
		}
		seen[h] = true
		if h == 0 {
			if err := e.Delete(32); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Delete(h); err != nil {
			t.Fatal(err)
		}
		return true
	})
	if len(seen) != 99 || e.Stats().Length != 0 {
		t.Fatal("range skipped remaining keys", len(seen), e.Stats())
	}
}
