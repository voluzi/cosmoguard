package testharness

import "testing"

// TestFreePortsAreDistinct pins the property the harness depends on:
// two listeners must never be handed the same port, or cosmoguard.New
// refuses the config with "listener port collision" and the suite
// fails for a reason that has nothing to do with the code under test.
func TestFreePortsAreDistinct(t *testing.T) {
	for range 50 {
		seen := make(map[int]bool, 7)
		for _, port := range freePorts(t, 7) {
			if seen[port] {
				t.Fatalf("freePorts handed out port %d twice", port)
			}
			seen[port] = true
		}
	}
}
