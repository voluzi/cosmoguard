package cosmoguard

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gotest.tools/assert"
)

func enabledLog(t *testing.T, maxEntries int) *requestLog {
	t.Helper()
	// 24h window so tests can use arbitrary "fresh" timestamps
	// without bumping against the time-window eviction.
	cfg := &RequestLogConfig{Enable: true, MaxEntries: maxEntries, MaxAge: 24 * time.Hour}
	return newRequestLog(cfg)
}

// TestRequestLog_DisabledDropsEverything pins the off-by-default
// behaviour: Record + Snapshot are no-ops when the operator hasn't
// enabled the ring.
func TestRequestLog_DisabledDropsEverything(t *testing.T) {
	rl := newRequestLog(nil)
	rl.Record(RequestLogEntry{Section: "lcd", Method: "GET", Status: 200})
	assert.Equal(t, len(rl.Snapshot(nil, 10)), 0)

	rl = newRequestLog(&RequestLogConfig{Enable: false})
	rl.Record(RequestLogEntry{Section: "lcd", Method: "GET", Status: 200})
	assert.Equal(t, len(rl.Snapshot(nil, 10)), 0)
}

// TestRequestLog_HardCapDropsOldest verifies that the maxEntries
// hard cap drops the globally oldest entry by timestamp when the
// total across sections exceeds it. Time-windowed entries that are
// also within the window are still subject to the cap.
func TestRequestLog_HardCapDropsOldest(t *testing.T) {
	rl := enabledLog(t, 3)
	now := time.Now().UnixMilli()
	for i := 0; i < 5; i++ {
		rl.Record(RequestLogEntry{
			Section:     "lcd",
			Method:      "GET",
			Status:      200,
			TimestampMs: now + int64(i),
		})
	}
	snap := rl.Snapshot([]string{"lcd"}, 10)
	assert.Equal(t, len(snap), 3, "hard cap evicted oldest down to 3")
	assert.Equal(t, snap[0].TimestampMs, now+4)
	assert.Equal(t, snap[1].TimestampMs, now+3)
	assert.Equal(t, snap[2].TimestampMs, now+2)
}

// TestRequestLog_TimeWindowEvicts verifies entries older than maxAge
// are dropped from Snapshot. Push doesn't have to actively trim them
// because Snapshot filters by cutoff; either path works.
func TestRequestLog_TimeWindowEvicts(t *testing.T) {
	cfg := &RequestLogConfig{Enable: true, MaxEntries: 100, MaxAge: 50 * time.Millisecond}
	rl := newRequestLog(cfg)
	rl.Record(RequestLogEntry{Section: "rpc", TimestampMs: time.Now().UnixMilli(), Status: 200})
	time.Sleep(80 * time.Millisecond)
	rl.Record(RequestLogEntry{Section: "rpc", TimestampMs: time.Now().UnixMilli(), Status: 200})

	snap := rl.Snapshot([]string{"rpc"}, 10)
	assert.Equal(t, len(snap), 1, "old entry must have aged out")
}

// TestRequestLog_IncludeFilters confirms the success/denied
// toggles drop entries that don't match.
func TestRequestLog_IncludeFilters(t *testing.T) {
	success := false
	denied := true
	rl := newRequestLog(&RequestLogConfig{
		Enable:         true,
		MaxEntries:     10,
		IncludeSuccess: &success,
		IncludeDenied:  &denied,
	})
	rl.Record(RequestLogEntry{Section: "lcd", Status: 200})                 // drop
	rl.Record(RequestLogEntry{Section: "lcd", Status: 401})                 // keep
	rl.Record(RequestLogEntry{Section: "lcd", Status: 200, Action: "deny"}) // keep (deny)
	snap := rl.Snapshot(nil, 10)
	assert.Equal(t, len(snap), 2)
}

// TestRequestLog_SnapshotMergesAcrossSections newest-first merges
// per-section slices into one timeline.
func TestRequestLog_SnapshotMergesAcrossSections(t *testing.T) {
	rl := enabledLog(t, 10)
	now := time.Now().UnixMilli()
	rl.Record(RequestLogEntry{Section: "lcd", TimestampMs: now + 100, Status: 200})
	rl.Record(RequestLogEntry{Section: "rpc", TimestampMs: now + 200, Status: 200})
	rl.Record(RequestLogEntry{Section: "lcd", TimestampMs: now + 300, Status: 200})
	rl.Record(RequestLogEntry{Section: "rpc", TimestampMs: now + 400, Status: 200})

	all := rl.Snapshot(nil, 10)
	assert.Equal(t, len(all), 4)
	for i := 1; i < len(all); i++ {
		assert.Assert(t, all[i-1].TimestampMs > all[i].TimestampMs, "expect newest-first")
	}

	onlyRPC := rl.Snapshot([]string{"rpc"}, 10)
	assert.Equal(t, len(onlyRPC), 2)
	for _, e := range onlyRPC {
		assert.Equal(t, e.Section, "rpc")
	}
}

// TestRequestLog_ApplyConfigSwapsAtomically pins hot-reload: toggling
// enable via ApplyConfig must take effect on subsequent Records
// without rebuilding the requestLog.
func TestRequestLog_ApplyConfigSwapsAtomically(t *testing.T) {
	rl := newRequestLog(nil) // disabled
	rl.Record(RequestLogEntry{Section: "lcd", TimestampMs: time.Now().UnixMilli()})
	assert.Equal(t, len(rl.Snapshot(nil, 10)), 0, "no entries while disabled")

	rl.ApplyConfig(&RequestLogConfig{Enable: true, MaxEntries: 10, MaxAge: time.Hour})
	rl.Record(RequestLogEntry{Section: "lcd", TimestampMs: time.Now().UnixMilli(), Status: 200})
	assert.Equal(t, len(rl.Snapshot(nil, 10)), 1, "enable now in effect after ApplyConfig")

	rl.ApplyConfig(&RequestLogConfig{Enable: false})
	rl.Record(RequestLogEntry{Section: "lcd", TimestampMs: time.Now().UnixMilli(), Status: 200})
	assert.Equal(t, len(rl.Snapshot(nil, 10)), 0, "disabled state returns nothing on Snapshot")
}

// TestRequestLog_TimestampDefaulted Record fills TimestampMs when
// the caller leaves it zero.
func TestRequestLog_TimestampDefaulted(t *testing.T) {
	rl := enabledLog(t, 5)
	before := time.Now().UnixMilli()
	rl.Record(RequestLogEntry{Section: "lcd", Method: "GET", Status: 200})
	after := time.Now().UnixMilli()
	snap := rl.Snapshot(nil, 1)
	assert.Equal(t, len(snap), 1)
	ts := snap[0].TimestampMs
	assert.Assert(t, ts >= before && ts <= after, "ts=%d not in [%d,%d]", ts, before, after)
}

// TestListRecentRequests_Envelope checks the handler returns the
// disabled envelope when the log is off and honors limit/section
// query params when enabled.
func TestListRecentRequests_Envelope(t *testing.T) {
	t.Run("disabled returns enabled=false", func(t *testing.T) {
		cg := &CosmoGuard{requestLog: newRequestLog(nil)}
		r := httptest.NewRequest("GET", "/api/v1/requests/recent", nil)
		out := listRecentRequests(cg, r)
		assert.Equal(t, out["enabled"], false)
	})

	t.Run("enabled honors limit + section", func(t *testing.T) {
		cg := &CosmoGuard{requestLog: enabledLog(t, 10)}
		now := time.Now().UnixMilli()
		cg.requestLog.Record(RequestLogEntry{Section: "lcd", TimestampMs: now + 1})
		cg.requestLog.Record(RequestLogEntry{Section: "rpc", TimestampMs: now + 2})
		cg.requestLog.Record(RequestLogEntry{Section: "rpc", TimestampMs: now + 3})

		r := httptest.NewRequest("GET", "/api/v1/requests/recent?section=rpc&limit=10", nil)
		out := listRecentRequests(cg, r)
		assert.Equal(t, out["enabled"], true)
		entries := out["requests"].([]RequestLogEntry)
		assert.Equal(t, len(entries), 2)
		for _, e := range entries {
			assert.Equal(t, e.Section, "rpc")
		}

		// limit caps the response
		r2 := httptest.NewRequest("GET", "/api/v1/requests/recent?limit=1", nil)
		out2 := listRecentRequests(cg, r2)
		assert.Equal(t, len(out2["requests"].([]RequestLogEntry)), 1)
	})
}

// TestAggregateRequests_FlagsEnabledFromAnyPeer pins the cluster
// aggregator's enabled semantics: a single peer reporting enabled=true
// flips the cluster envelope's `enabled` to true, since the operator
// expects the dashboard to render data the moment any pod has the
// log on.
func TestAggregateRequests_FlagsEnabledFromAnyPeer(t *testing.T) {
	peer := func(body string) peerResponse { return peerResponse{Body: []byte(body)} }
	rs := []peerResponse{
		peer(`{"requests":[],"enabled":false}`),
		peer(`{"requests":[{"timestamp_ms":1,"section":"lcd"}],"enabled":true}`),
	}
	entries, enabled := aggregateRequests(rs)
	assert.Equal(t, enabled, true)
	assert.Equal(t, len(entries), 1)
}

// TestAggregateRequests_NewestFirstAcrossPeers merges and sorts.
func TestAggregateRequests_NewestFirstAcrossPeers(t *testing.T) {
	body := func(ts ...int64) string {
		var sb strings.Builder
		sb.WriteString(`{"requests":[`)
		for i, t := range ts {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`{"timestamp_ms":`)
			sb.WriteString(fmtInt(t))
			sb.WriteByte('}')
		}
		sb.WriteString(`],"enabled":true}`)
		return sb.String()
	}
	rs := []peerResponse{
		{Body: []byte(body(100, 300))},
		{Body: []byte(body(200, 400))},
	}
	entries, _ := aggregateRequests(rs)
	assert.Equal(t, len(entries), 4)
	for i := 1; i < len(entries); i++ {
		assert.Assert(t, entries[i-1].TimestampMs > entries[i].TimestampMs)
	}
}

func fmtInt(i int64) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = digits[i%10]
		i /= 10
	}
	return string(buf[n:])
}
