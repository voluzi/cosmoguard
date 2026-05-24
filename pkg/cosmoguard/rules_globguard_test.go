package cosmoguard

import (
	"strings"
	"testing"
	"time"
)

func TestGlobLengthGuard(t *testing.T) {
	// The pathological input from the fuzz freeze: long char-class pattern.
	pat := strings.Repeat("[a-z]", 500) // 2500 bytes
	start := time.Now()
	r := &HttpRule{Priority: 1, Action: RuleActionAllow, Paths: []string{pat}, Methods: []string{"GET"}, Query: map[string]string{"k": pat}}
	err := r.Compile()
	d := time.Since(start)
	if err == nil {
		t.Fatalf("over-long glob pattern (%d bytes) must be rejected, got nil err", len(pat))
	}
	if d > 50*time.Millisecond {
		t.Fatalf("rejection must be fast, took %v", d)
	}
	// A normal pattern still compiles fine.
	r2 := &HttpRule{Priority: 1, Action: RuleActionAllow, Paths: []string{"/cosmos/*/balance"}, Methods: []string{"GET"}}
	if err := r2.Compile(); err != nil {
		t.Fatalf("normal pattern must compile: %v", err)
	}
}
