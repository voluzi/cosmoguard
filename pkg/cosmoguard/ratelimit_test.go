package cosmoguard

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

func TestParseRate(t *testing.T) {
	tests := []struct {
		input   string
		wantPS  float64
		wantErr bool
	}{
		{"100", 100, false},
		{"100/s", 100, false},
		{"30/min", 0.5, false},
		{"60/hour", 60.0 / 3600, false},
		{"1/5s", 0.2, false},
		{"250/250ms", 1000, false},
		{"", 0, true},
		{"/s", 0, true},
		{"100/", 0, true},
		{"abc/s", 0, true},
		{"100/banana", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseRate(tt.input)
			if tt.wantErr {
				assert.Assert(t, err != nil, "expected error")
				return
			}
			assert.NilError(t, err)
			// Allow tiny float drift for division results.
			diff := got.PerSecond - tt.wantPS
			if diff < 0 {
				diff = -diff
			}
			assert.Assert(t, diff < 1e-6,
				"%s: got %v want %v", tt.input, got.PerSecond, tt.wantPS)
		})
	}
}

// TestMemoryRateLimiter_Allow verifies the in-process limiter respects the
// configured burst — first N requests pass instantly, the next is denied
// with a non-zero retry-after.
func TestMemoryRateLimiter_Allow(t *testing.T) {
	cfg := RateLimitConfig{
		Rate:  Rate{PerSecond: 10},
		Burst: 3,
		Scope: RateLimitScopePerIP,
	}
	// No cacheCfg → forced to memory limiter.
	l, err := NewRateLimiter(cfg, nil, "test")
	assert.NilError(t, err)
	defer l.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ok, _, err := l.Allow(ctx, "client-a")
		assert.NilError(t, err)
		assert.Equal(t, ok, true, "burst slot %d should pass", i)
	}
	ok, retryAfter, err := l.Allow(ctx, "client-a")
	assert.NilError(t, err)
	assert.Equal(t, ok, false, "fourth request must be denied")
	assert.Assert(t, retryAfter > 0, "retry-after must be non-zero")
}

// TestMemoryRateLimiter_SeparateBuckets verifies that different keys
// (i.e. different scope values) get independent buckets.
func TestMemoryRateLimiter_SeparateBuckets(t *testing.T) {
	cfg := RateLimitConfig{
		Rate:  Rate{PerSecond: 1},
		Burst: 1,
	}
	l, err := NewRateLimiter(cfg, nil, "test")
	assert.NilError(t, err)
	defer l.Close()

	ctx := context.Background()
	okA, _, err := l.Allow(ctx, "ip-A")
	assert.NilError(t, err)
	assert.Equal(t, okA, true)

	// Client A is now over budget.
	okADenied, _, _ := l.Allow(ctx, "ip-A")
	assert.Equal(t, okADenied, false)

	// Client B has its own bucket.
	okB, _, err := l.Allow(ctx, "ip-B")
	assert.NilError(t, err)
	assert.Equal(t, okB, true)
}

// TestMemoryRateLimiter_Refill verifies that tokens come back after the
// configured interval. Uses a fast rate to keep the test snappy.
func TestMemoryRateLimiter_Refill(t *testing.T) {
	cfg := RateLimitConfig{
		Rate:  Rate{PerSecond: 50}, // 1 token every 20ms
		Burst: 1,
	}
	l, err := NewRateLimiter(cfg, nil, "test")
	assert.NilError(t, err)
	defer l.Close()

	ctx := context.Background()
	ok1, _, _ := l.Allow(ctx, "x")
	assert.Equal(t, ok1, true)
	ok2, _, _ := l.Allow(ctx, "x")
	assert.Equal(t, ok2, false)

	// Wait for the bucket to refill.
	time.Sleep(50 * time.Millisecond)
	ok3, _, _ := l.Allow(ctx, "x")
	assert.Equal(t, ok3, true, "token should have been refilled by now")
}

// TestRateLimitKey_PerIPDifferentIPs proves that two requests from
// different IPs against the same rule get different bucket keys.
func TestRateLimitKey_PerIPDifferentIPs(t *testing.T) {
	withTrustAll(t)
	r1 := &http.Request{
		Method:     "GET",
		URL:        &url.URL{Path: "/"},
		Header:     http.Header{},
		RemoteAddr: "10.0.0.1:1111",
	}
	r1.Header.Set("X-Real-Ip", "1.2.3.4")
	r2 := &http.Request{
		Method:     "GET",
		URL:        &url.URL{Path: "/"},
		Header:     http.Header{},
		RemoteAddr: "10.0.0.1:2222",
	}
	r2.Header.Set("X-Real-Ip", "5.6.7.8")

	k1 := rateLimitKey(RateLimitScopePerIP, 42, r1, "")
	k2 := rateLimitKey(RateLimitScopePerIP, 42, r2, "")
	assert.Assert(t, k1 != k2, "different IPs must yield different keys")
	assert.Assert(t, strings.Contains(k1, "1.2.3.4"), "key should embed IP: %s", k1)
}

// TestRateLimitKey_GlobalCollapses verifies the global scope produces ONE
// key regardless of source.
func TestRateLimitKey_GlobalCollapses(t *testing.T) {
	r1 := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
	r1.Header.Set("X-Real-Ip", "1.1.1.1")
	r2 := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
	r2.Header.Set("X-Real-Ip", "2.2.2.2")

	k1 := rateLimitKey(RateLimitScopeGlobal, 7, r1, "")
	k2 := rateLimitKey(RateLimitScopeGlobal, 7, r2, "")
	assert.Equal(t, k1, k2)
}

// TestRateLimitKey_PerIdentitySplitsByIdentity: same IP + different
// authenticated identities → separate buckets.
func TestRateLimitKey_PerIdentitySplitsByIdentity(t *testing.T) {
	withTrustAll(t)
	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}}
	r.Header.Set("X-Real-Ip", "1.1.1.1")

	k1 := rateLimitKey(RateLimitScopePerIdentity, 99, r, "alice")
	k2 := rateLimitKey(RateLimitScopePerIdentity, 99, r, "bob")
	assert.Assert(t, k1 != k2, "different identities must produce different keys")
	assert.Assert(t, strings.Contains(k1, "alice"), "key should embed identity: %s", k1)
}

// TestRateLimitKey_PerIdentityFallsBackToIP: anonymous request degrades
// to a per-IP bucket so unauth traffic is still capped.
func TestRateLimitKey_PerIdentityFallsBackToIP(t *testing.T) {
	withTrustAll(t)
	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}, RemoteAddr: "10.0.0.1:9999"}
	r.Header.Set("X-Real-Ip", "1.1.1.1")

	k := rateLimitKey(RateLimitScopePerIdentity, 99, r, "")
	assert.Assert(t, strings.Contains(k, "1.1.1.1"), "anonymous per-identity should be per-IP: %s", k)
}

// TestRateLimitKey_CompoundSplitsByIPAndIdentity: same identity from
// different IPs gets distinct buckets — the compound scope captures
// (identity, source) tuples so token sharing across IPs doesn't slip
// through.
func TestRateLimitKey_CompoundSplitsByIPAndIdentity(t *testing.T) {
	withTrustAll(t)
	r1 := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}, RemoteAddr: "10.0.0.1:1111"}
	r1.Header.Set("X-Real-Ip", "1.1.1.1")
	r2 := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}, RemoteAddr: "10.0.0.1:2222"}
	r2.Header.Set("X-Real-Ip", "2.2.2.2")

	k1 := rateLimitKey(RateLimitScopeCompound, 13, r1, "alice")
	k2 := rateLimitKey(RateLimitScopeCompound, 13, r2, "alice")
	assert.Assert(t, k1 != k2, "compound must split by IP within an identity: %s == %s", k1, k2)
	assert.Assert(t, strings.Contains(k1, "alice"), "compound key should embed identity: %s", k1)
	assert.Assert(t, strings.Contains(k1, "1.1.1.1"), "compound key should embed IP: %s", k1)
}

// TestRateLimitKey_CompoundFallsBackOnAnonymous: anonymous compound
// collapses to per-IP — same correctness story as per-identity.
func TestRateLimitKey_CompoundFallsBackOnAnonymous(t *testing.T) {
	withTrustAll(t)
	r := &http.Request{Method: "GET", URL: &url.URL{Path: "/"}, Header: http.Header{}, RemoteAddr: "10.0.0.1:9999"}
	r.Header.Set("X-Real-Ip", "3.3.3.3")
	k := rateLimitKey(RateLimitScopeCompound, 13, r, "")
	assert.Assert(t, strings.Contains(k, "3.3.3.3"), "anonymous compound should be per-IP: %s", k)
}
