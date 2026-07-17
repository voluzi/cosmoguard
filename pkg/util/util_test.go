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

	// Release the ID
	u.Release(id)

	// Verify the ID is marked as released (not in use)
	v, ok := u.generated.Load(id)
	if !ok {
		t.Errorf("ID %s not found in map after release", id)
	}
	if v.(bool) {
		t.Errorf("ID %s should be marked as false (released), got true", id)
	}
}

func TestUniqueID_Concurrent(t *testing.T) {
	u := &UniqueID{}
	var wg sync.WaitGroup
	idChan := make(chan string, 1000)

	// Generate IDs concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				id := u.ID()
				idChan <- id
			}
		}()
	}

	wg.Wait()
	close(idChan)

	// Check for duplicates
	ids := make(map[string]bool)
	for id := range idChan {
		if ids[id] {
			t.Errorf("Duplicate ID generated concurrently: %s", id)
		}
		ids[id] = true
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
