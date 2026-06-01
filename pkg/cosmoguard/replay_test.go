package cosmoguard

import (
	"context"
	"testing"
	"time"
)

// TestMemoryReplayStore_FirstStoreOk: first call returns seen=false
// and inserts the key.
func TestMemoryReplayStore_FirstStoreOk(t *testing.T) {
	s := &memoryReplayStore{seen: map[string]time.Time{}, stopCh: make(chan struct{})}
	defer s.Close()

	seen, err := s.SeenOrStore(context.Background(), "jti-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if seen {
		t.Fatal("first store should not be seen")
	}
}

// TestMemoryReplayStore_RepeatBlocked: a second store within TTL
// returns seen=true.
func TestMemoryReplayStore_RepeatBlocked(t *testing.T) {
	s := &memoryReplayStore{seen: map[string]time.Time{}, stopCh: make(chan struct{})}
	defer s.Close()

	_, _ = s.SeenOrStore(context.Background(), "jti-1", time.Minute)
	seen, err := s.SeenOrStore(context.Background(), "jti-1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !seen {
		t.Fatal("repeat within TTL should be seen")
	}
}

// TestMemoryReplayStore_ExpiredAllowed: after the TTL elapses the
// same jti is admitted again. Tested by inserting with a negative
// time-of-expiry directly in the map (avoids real-time sleep).
func TestMemoryReplayStore_ExpiredAllowed(t *testing.T) {
	s := &memoryReplayStore{seen: map[string]time.Time{}, stopCh: make(chan struct{})}
	defer s.Close()
	s.seen["jti-old"] = time.Now().Add(-time.Hour)

	seen, err := s.SeenOrStore(context.Background(), "jti-old", time.Minute)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if seen {
		t.Fatal("expired entry should not block")
	}
}
