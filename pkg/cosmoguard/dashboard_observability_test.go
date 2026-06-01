package cosmoguard

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestReloadDelta: ruleFingerprintsLocked-style maps zipped through
// reloadDelta produce a section row per key in either side, with the
// missing side filled in as zero. Sections that go N→0 or 0→N stay
// visible in the delta — that's the operator's signal for "rule
// disappeared in this reload". The Before/After counts mirror the
// slice lengths; Added/Removed/Modified come from the multiset diff.
func TestReloadDelta(t *testing.T) {
	// Distinct fingerprints; we only care about Before/After + the
	// pure-add / pure-remove case here.
	before := map[string][]uint64{
		"lcd":  {1, 2, 3},
		"rpc":  {4},
		"grpc": {5, 6},
	}
	after := map[string][]uint64{
		"lcd":     {1, 2, 3, 7, 8}, // +7, +8
		"rpc":     {},              // -4
		"evm.rpc": {9, 10, 11, 12}, // +9, +10, +11, +12
	}

	delta := reloadDelta(before, after)
	want := map[string]ReloadSection{
		"lcd":     {Before: 3, After: 5, Added: 2},
		"rpc":     {Before: 1, After: 0, Removed: 1},
		"grpc":    {Before: 2, After: 0, Removed: 2},
		"evm.rpc": {Before: 0, After: 4, Added: 4},
	}
	if len(delta) != len(want) {
		t.Fatalf("delta length: got %d want %d (%v)", len(delta), len(want), delta)
	}
	for k, w := range want {
		got, ok := delta[k]
		if !ok {
			t.Fatalf("missing section %q in delta", k)
		}
		if got != w {
			t.Fatalf("section %q: got %+v want %+v", k, got, w)
		}
	}
}

// TestReloadDelta_DetectsInPlaceModification pins the user-visible
// invariant the dashboard panel needs: editing a TTL on an existing
// LCD rule changes the rule's Fingerprint but leaves the rule count
// unchanged. The diff MUST report Modified=1 (not "no change") so an
// operator who just saved a config file can see their edit landed.
func TestReloadDelta_DetectsInPlaceModification(t *testing.T) {
	before := map[string][]uint64{
		"lcd": {0xAAAA, 0xBBBB, 0xCCCC}, // 3 rules
	}
	after := map[string][]uint64{
		// Same count; one fingerprint changed (operator bumped a TTL
		// on the rule that previously hashed to 0xBBBB).
		"lcd": {0xAAAA, 0xDDDD, 0xCCCC},
	}
	delta := reloadDelta(before, after)
	got, ok := delta["lcd"]
	if !ok {
		t.Fatalf("missing lcd section: %+v", delta)
	}
	want := ReloadSection{Before: 3, After: 3, Modified: 1}
	if got != want {
		t.Fatalf("lcd delta: got %+v want %+v", got, want)
	}
}

// TestReloadDelta_AddPlusModify covers the mixed case: one rule is
// added AND one rule is modified in the same reload. The multiset
// diff should split correctly into Added=1 + Modified=1 instead of
// reporting "+2 added, -1 removed".
func TestReloadDelta_AddPlusModify(t *testing.T) {
	before := map[string][]uint64{
		"lcd": {0xAAAA, 0xBBBB},
	}
	after := map[string][]uint64{
		// 0xBBBB was modified → 0xCCCC; 0xDDDD is brand new.
		"lcd": {0xAAAA, 0xCCCC, 0xDDDD},
	}
	delta := reloadDelta(before, after)
	got := delta["lcd"]
	want := ReloadSection{Before: 2, After: 3, Added: 1, Modified: 1}
	if got != want {
		t.Fatalf("mixed-case delta: got %+v want %+v", got, want)
	}
}

// TestReloadDelta_TTLEditFlowsThroughRealCompile is the end-to-end
// regression for the user-reported "I edited a TTL and the dashboard
// still says no change" bug. The synthetic-fingerprint tests above
// only prove the diff logic; this test proves the FULL chain works:
//
//  1. Two configs identical except for one LCD rule's cache TTL
//  2. PrepareConfig → Compile populates Fingerprint on both
//  3. ruleFingerprintsLocked reads the populated values
//  4. reloadDelta detects the in-place change
//  5. RecordReload stamps the dashboard
//  6. listReloadStatus emits sections.lcd.modified=1 in the JSON shape
//     the UI keys off
//
// A regression anywhere in this chain (Compile dropping TTL from the
// fingerprint, capture site reading wrong slice, JSON tag drift,
// endpoint shape change) surfaces as a single failure here instead of
// silently looking like "no change" on the operator's dashboard.
func TestReloadDelta_TTLEditFlowsThroughRealCompile(t *testing.T) {
	mk := func(ttl time.Duration) *Config {
		return &Config{
			Host: "127.0.0.1",
			LCD: LcdConfig{
				Default: RuleActionAllow,
				Rules: []*HttpRule{{
					Priority: 100,
					Action:   RuleActionAllow,
					Paths:    []string{"/cosmos/bank/v1beta1/balances/*"},
					Cache:    &RuleCache{Enable: true, TTL: ttl},
				}},
			},
			RPC: RpcConfig{
				Default: RuleActionAllow,
				JsonRpc: JsonRpcConfig{Default: RuleActionAllow},
			},
			GRPC: GrpcConfig{Default: RuleActionAllow},
			Nodes: []NodeConfig{
				{Host: "127.0.0.1", LcdPort: 1317, RpcPort: 26657, GrpcPort: 9090},
			},
		}
	}
	oldCfg := mk(10 * time.Second)
	newCfg := mk(20 * time.Second)
	if err := PrepareConfig(oldCfg); err != nil {
		t.Fatalf("PrepareConfig oldCfg: %v", err)
	}
	if err := PrepareConfig(newCfg); err != nil {
		t.Fatalf("PrepareConfig newCfg: %v", err)
	}

	// Construct a minimal CosmoGuard with just the bits tryReload's
	// diff path touches — no listeners, no proxies. This keeps the
	// regression tight to the diff chain without needing ephemeral
	// ports or background goroutines.
	cg := &CosmoGuard{cfg: oldCfg, dashboard: newDashboardObservability()}

	// Mirror tryReload's exact sequence: snapshot, swap, snapshot,
	// stamp. If any step regresses, the assertion below fires.
	before := cg.ruleFingerprintsLocked()
	cg.cfg = newCfg
	after := cg.ruleFingerprintsLocked()
	cg.dashboard.RecordReload(true, "", reloadDelta(before, after))

	if cg.dashboard.lastReload == nil {
		t.Fatal("lastReload is nil after RecordReload")
	}
	lcd, ok := cg.dashboard.lastReload.Sections["lcd"]
	if !ok {
		t.Fatalf("lcd section missing: %+v", cg.dashboard.lastReload.Sections)
	}
	want := ReloadSection{Before: 1, After: 1, Modified: 1}
	if lcd != want {
		t.Fatalf("lcd delta: got %+v want %+v\n  before fp = %#x\n  after  fp = %#x",
			lcd, want, before["lcd"], after["lcd"])
	}

	// Confirm the endpoint payload shape the UI consumes.
	out := listReloadStatus(cg)
	rs, ok := out["reload"].(ReloadStatus)
	if !ok {
		t.Fatalf("reload field shape unexpected: %T", out["reload"])
	}
	if rs.Sections["lcd"].Modified != 1 {
		t.Fatalf("listReloadStatus().reload.sections.lcd.modified: got %d want 1",
			rs.Sections["lcd"].Modified)
	}
}

// TestRecordReload_FailureStampsError: a failing reload stamps the
// dashboard with Success=false plus the error message so the pill
// renders the failure state instead of an outdated success.
func TestRecordReload_FailureStampsError(t *testing.T) {
	d := newDashboardObservability()
	d.RecordReload(false, "parse error: bad indent", nil)

	if d.lastReload == nil {
		t.Fatalf("RecordReload should have set lastReload")
	}
	if d.lastReload.Success {
		t.Fatalf("expected Success=false on failed reload")
	}
	if d.lastReload.Error != "parse error: bad indent" {
		t.Fatalf("error not preserved: %q", d.lastReload.Error)
	}
	if d.lastReload.TimestampMs == 0 {
		t.Fatalf("TimestampMs should be populated")
	}
}

// TestListReloadStatus_DefaultsWhenUnset: before any reload has
// happened the endpoint payload still carries a zero-valued
// ReloadStatus so the dashboard pill can render "no reloads yet"
// rather than choking on a null.
func TestListReloadStatus_DefaultsWhenUnset(t *testing.T) {
	cg := &CosmoGuard{dashboard: newDashboardObservability()}
	out := listReloadStatus(cg)
	rs, ok := out["reload"].(ReloadStatus)
	if !ok {
		t.Fatalf("reload field shape unexpected: %T", out["reload"])
	}
	if rs.Sections == nil {
		t.Fatalf("Sections should be non-nil for stable JSON shape")
	}
}

// TestDiscoverer_ReconcileOnce_EmitsEvents: a reconcile that resolves
// to a fresh IP set must push a "tick" event with the resolved list,
// "remove" events for each dropped IP, and "add" events for each new
// one. Operators look at this stream to spot a flapping deployment.
func TestDiscoverer_ReconcileOnce_EmitsEvents(t *testing.T) {
	dash := newDashboardObservability()
	lookupResult := []string{"10.0.0.2", "10.0.0.3"}
	lookup := func(ctx context.Context, host string) ([]string, error) {
		return lookupResult, nil
	}
	d := NewDiscoverer(nil, nil, lookup)
	d.SetDashboard(dash)

	current := map[string]struct{}{"10.0.0.1": {}, "10.0.0.2": {}}
	tmpl := NodeConfig{Name: "validator", Discovery: &DiscoveryConfig{Host: "validators.svc"}}
	d.reconcileOnce(context.Background(), tmpl, current)

	events := dash.discoveryLog.Snapshot()
	// Snapshot is newest-first. Expect (in some order across types):
	//   tick(resolved=[.2, .3]), remove(.1), add(.3)
	var (
		tickSeen, addSeen, removeSeen bool
		tick                          DiscoveryEvent
	)
	for _, e := range events {
		switch e.Type {
		case "tick":
			tickSeen = true
			tick = e
		case "add":
			if e.IP == "10.0.0.3" {
				addSeen = true
			}
		case "remove":
			if e.IP == "10.0.0.1" {
				removeSeen = true
			}
		}
	}
	if !tickSeen {
		t.Fatalf("expected a tick event with resolved IPs; events=%v", events)
	}
	if len(tick.Resolved) != 2 || tick.Resolved[0] != "10.0.0.2" || tick.Resolved[1] != "10.0.0.3" {
		t.Fatalf("tick resolved set: got %v", tick.Resolved)
	}
	if !addSeen {
		t.Fatalf("expected add event for 10.0.0.3; events=%v", events)
	}
	if !removeSeen {
		t.Fatalf("expected remove event for 10.0.0.1; events=%v", events)
	}
}

// TestDiscoverer_ReconcileOnce_LookupErrorEmitsError: when the
// resolver fails, the loop must keep the previous pool membership
// AND record an "error" event so the operator's dashboard surfaces
// the DNS hiccup instead of silently masking it.
func TestDiscoverer_ReconcileOnce_LookupErrorEmitsError(t *testing.T) {
	dash := newDashboardObservability()
	lookup := func(ctx context.Context, host string) ([]string, error) {
		return nil, errors.New("dns server unreachable")
	}
	d := NewDiscoverer(nil, nil, lookup)
	d.SetDashboard(dash)

	current := map[string]struct{}{"10.0.0.1": {}}
	tmpl := NodeConfig{Name: "validator", Discovery: &DiscoveryConfig{Host: "validators.svc"}}
	d.reconcileOnce(context.Background(), tmpl, current)

	// Membership must be untouched on lookup failure.
	if _, ok := current["10.0.0.1"]; !ok || len(current) != 1 {
		t.Fatalf("current should be untouched on lookup failure: %v", current)
	}
	events := dash.discoveryLog.Snapshot()
	if len(events) != 1 || events[0].Type != "error" || events[0].Error == "" {
		t.Fatalf("expected single error event with non-empty Error; got %v", events)
	}
}

// TestListDiscoveryLog_DefaultsWhenEmpty: the endpoint returns an
// empty (non-nil) slice when no events have been recorded yet so the
// dashboard can iterate without a nil check.
func TestListDiscoveryLog_DefaultsWhenEmpty(t *testing.T) {
	cg := &CosmoGuard{dashboard: newDashboardObservability()}
	out := listDiscoveryLog(cg)
	events, ok := out["events"].([]DiscoveryEvent)
	if !ok {
		t.Fatalf("events field shape unexpected: %T", out["events"])
	}
	if events == nil {
		t.Fatalf("events should be non-nil (empty slice) for stable JSON shape")
	}
	if len(events) != 0 {
		t.Fatalf("expected 0 events; got %d", len(events))
	}
}

// TestListCardinality_GroupsByRule: distinct request keys recorded
// under the same rule tag collapse to one row whose DistinctKeys
// matches the unique count. The endpoint exists to surface "rule X
// is writing N keys"; grouping is the entire point.
func TestListCardinality_GroupsByRule(t *testing.T) {
	d := newDashboardObservability()
	for i := 0; i < 30; i++ {
		d.RecordCardinality("lcd", "hot-rule", iToReq(i))
	}
	for i := 0; i < 5; i++ {
		d.RecordCardinality("lcd", "cold-rule", iToReq(i))
	}

	cg := &CosmoGuard{dashboard: d}
	out := listCardinality(cg)
	sections, ok := out["sections"].(map[string][]CardinalityRule)
	if !ok {
		t.Fatalf("sections field shape unexpected: %T", out["sections"])
	}
	lcd := sections["lcd"]
	if len(lcd) != 2 {
		t.Fatalf("expected 2 rule rows in lcd, got %d (%v)", len(lcd), lcd)
	}
	for _, row := range lcd {
		switch row.RuleTag {
		case "hot-rule":
			if row.DistinctKeys != 30 {
				t.Fatalf("hot-rule DistinctKeys: got %d want 30", row.DistinctKeys)
			}
			if len(row.HotSamples) == 0 || len(row.HotSamples) > maxCardinalityHotSamples {
				t.Fatalf("hot-rule HotSamples bound violated: %v", row.HotSamples)
			}
		case "cold-rule":
			if row.DistinctKeys != 5 {
				t.Fatalf("cold-rule DistinctKeys: got %d want 5", row.DistinctKeys)
			}
		default:
			t.Fatalf("unexpected rule tag: %q", row.RuleTag)
		}
	}
}

// iToReq is a small helper for the cardinality test — keeps the
// distinct-key generator readable without pulling fmt into the test
// file's hot path.
func iToReq(i int) string {
	return time.Unix(int64(i), 0).Format("v1.0-150405") + "-" +
		time.Unix(int64(i), 0).Format("k=0405")
}
