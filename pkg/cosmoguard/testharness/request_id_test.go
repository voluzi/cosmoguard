package testharness_test

import (
	"net/http"
	"regexp"
	"testing"

	"gotest.tools/assert"

	"github.com/voluzi/cosmoguard/pkg/cosmoguard/testharness"
)

// TestH_RequestID_Generated: an incoming request with no X-Request-Id
// gets one assigned. The response carries the same ID; the upstream
// sees the same ID.
func TestH_RequestID_Generated(t *testing.T) {
	h := testharness.New(t,
		testharness.WithLCDResponse(http.MethodGet, "/x", `{}`),
	)

	resp := h.GET(t, h.LCDURL+"/x")
	id := resp.Header.Get("X-Request-Id")
	assert.Assert(t, id != "", "X-Request-Id should be set on response")
	// 16-hex-char ID from 8 random bytes.
	matched, err := regexp.MatchString(`^[0-9a-f]{16}$`, id)
	assert.NilError(t, err)
	assert.Assert(t, matched, "X-Request-Id should be 16 hex chars: %q", id)

	// Upstream saw the same ID.
	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 1)
	assert.Equal(t, calls[0].Headers.Get("X-Request-Id"), id,
		"upstream should see the same X-Request-Id cosmoguard assigned")
}

// TestH_RequestID_Preserved: an incoming request with an existing
// X-Request-Id has it preserved (not replaced). End-to-end correlation
// from L7 LBs that already stamp the header.
func TestH_RequestID_Preserved(t *testing.T) {
	h := testharness.New(t,
		testharness.WithLCDResponse(http.MethodGet, "/x", `{}`),
	)

	req, _ := http.NewRequest(http.MethodGet, h.LCDURL+"/x", nil)
	req.Header.Set("X-Request-Id", "operator-supplied-trace-id-001")

	resp := h.Do(t, req)
	assert.Equal(t, resp.Header.Get("X-Request-Id"), "operator-supplied-trace-id-001")
	calls := h.Upstream.LCD.Calls()
	assert.Equal(t, len(calls), 1)
	assert.Equal(t, calls[0].Headers.Get("X-Request-Id"), "operator-supplied-trace-id-001")
}
