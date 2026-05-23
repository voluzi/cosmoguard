package cosmoguard

import (
	"strings"
	"testing"
)

// TestPrepareConfig_RejectsUnknownUpstreamStrategy: typos in
// upstream.strategy should fail PrepareConfig at startup so the
// operator notices, instead of silently falling back to the default.
func TestPrepareConfig_RejectsUnknownUpstreamStrategy(t *testing.T) {
	cfg := &Config{
		Upstream: UpstreamConfig{Strategy: "lest-conn"},
	}
	err := PrepareConfig(cfg)
	if err == nil || !strings.Contains(err.Error(), "unknown value") {
		t.Fatalf("expected unknown-strategy error; got %v", err)
	}
}

func TestPrepareConfig_AcceptsKnownStrategies(t *testing.T) {
	for _, s := range []string{"weighted-round-robin", "round-robin", "least-conn", "primary-failover"} {
		cfg := &Config{Upstream: UpstreamConfig{Strategy: s}}
		if err := PrepareConfig(cfg); err != nil {
			t.Fatalf("strategy %q rejected: %v", s, err)
		}
	}
}

// TestPrepareConfig_TracingValidation: invalid tracing inputs must
// fail before any span is emitted.
func TestPrepareConfig_TracingValidation(t *testing.T) {
	cases := []struct {
		name string
		cfg  TracingConfig
		want string
	}{
		{"bad protocol", TracingConfig{Enable: true, Protocol: "h2"}, "tracing.protocol"},
		{"sample negative", TracingConfig{Enable: true, SampleRate: -0.1}, "out of range"},
		{"sample too big", TracingConfig{Enable: true, SampleRate: 1.5}, "out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := PrepareConfig(&Config{Tracing: tc.cfg})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestPrepareConfig_TracingDisabledSkipsValidation(t *testing.T) {
	// Disabled tracing with otherwise-invalid fields must NOT error.
	cfg := &Config{Tracing: TracingConfig{Enable: false, Protocol: "garbage", SampleRate: 99}}
	if err := PrepareConfig(cfg); err != nil {
		t.Fatalf("disabled tracing should not validate: %v", err)
	}
}
