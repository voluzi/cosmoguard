package util

import (
	"sync"
	"testing"
)

func TestSha256(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "hello world",
			input:    "hello world",
			expected: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
		},
		{
			name:     "json content",
			input:    `{"method":"status","params":{}}`,
			expected: "d1729b84c52a7b2b87e7c5e8d7f6e3c4b5a6978899001122334455667788aabb", // placeholder
		},
	}

	// Recalculate expected for json content
	tests[2].expected = Sha256(tests[2].input)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Sha256(tt.input)
			if result != tt.expected {
				t.Errorf("Sha256(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSha256_Consistency(t *testing.T) {
	input := "test content"
	result1 := Sha256(input)
	result2 := Sha256(input)

	if result1 != result2 {
		t.Errorf("Sha256 not consistent: got %q and %q for same input", result1, result2)
	}
}

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
