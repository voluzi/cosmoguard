package util

import (
	"sync"
	"testing"
)

func TestUniqueID_ID(t *testing.T) {
	u := &UniqueID{}

	// Generate multiple IDs and ensure they're unique
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := u.ID()
		if ids[id] {
			t.Errorf("Duplicate ID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestUniqueID_Release(t *testing.T) {
	u := &UniqueID{}

	id := u.ID()
	u.Release(id)

	if _, ok := u.generated.Load(id); ok {
		t.Errorf("ID %s found in map after release", id)
	}
}

func TestUniqueID_ReleaseChurn(t *testing.T) {
	u := &UniqueID{}

	for i := 0; i < 1000; i++ {
		id := u.ID()
		u.Release(id)
	}

	entries := 0
	u.generated.Range(func(_, _ any) bool {
		entries++
		return true
	})
	if entries != 0 {
		t.Errorf("generated map contains %d entries after release churn", entries)
	}
}

func TestUniqueID_Concurrent(t *testing.T) {
	u := &UniqueID{}
	var wg sync.WaitGroup
	ids := make(chan string, 1000)
	release := make(chan struct{})

	for i := 0; i < cap(ids); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := u.ID()
			ids <- id
			<-release
			u.Release(id)
		}()
	}

	generated := make(map[string]bool)
	for i := 0; i < cap(ids); i++ {
		id := <-ids
		if generated[id] {
			t.Errorf("Duplicate ID generated concurrently: %s", id)
		}
		generated[id] = true
	}

	close(release)
	wg.Wait()

	entries := 0
	u.generated.Range(func(_, _ any) bool {
		entries++
		return true
	})
	if entries != 0 {
		t.Errorf("generated map contains %d entries after concurrent release", entries)
	}
}

func TestSliceContainsStringIgnoreCase(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		key      string
		expected bool
	}{
		{
			name:     "exact match",
			slice:    []string{"GET", "POST", "PUT"},
			key:      "GET",
			expected: true,
		},
		{
			name:     "case insensitive match lowercase",
			slice:    []string{"GET", "POST", "PUT"},
			key:      "get",
			expected: true,
		},
		{
			name:     "case insensitive match mixed",
			slice:    []string{"GET", "POST", "PUT"},
			key:      "GeT",
			expected: true,
		},
		{
			name:     "no match",
			slice:    []string{"GET", "POST", "PUT"},
			key:      "DELETE",
			expected: false,
		},
		{
			name:     "empty slice",
			slice:    []string{},
			key:      "GET",
			expected: false,
		},
		{
			name:     "empty key",
			slice:    []string{"GET", "POST"},
			key:      "",
			expected: false,
		},
		{
			name:     "empty key in slice",
			slice:    []string{"GET", "", "POST"},
			key:      "",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SliceContainsStringIgnoreCase(tt.slice, tt.key)
			if result != tt.expected {
				t.Errorf("SliceContainsStringIgnoreCase(%v, %q) = %v, want %v",
					tt.slice, tt.key, result, tt.expected)
			}
		})
	}
}
