package cosmoguard

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"
)

// TestListRules_RaceWithReload pins the configMutex snapshot in
// listRules. tryReload swaps cg.cfg under configMutex; without the
// snapshot, a /api/v1/rules request landing during a reload races
// on the slice-header read of cfg.LCD.Rules and the race detector
// catches it. The test loops one mutator and many readers under
// -race; a regression that drops the snapshot fails immediately.
func TestListRules_RaceWithReload(t *testing.T) {
	cfgA := &Config{
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{
				{Priority: 1, Action: RuleActionAllow, Paths: []string{"/a"}, Methods: []string{http.MethodGet}},
			},
		},
		RPC:  RpcConfig{Default: RuleActionAllow, JsonRpc: JsonRpcConfig{Default: RuleActionAllow}},
		GRPC: GrpcConfig{Default: RuleActionAllow},
	}
	cfgB := &Config{
		LCD: LcdConfig{
			Default: RuleActionAllow,
			Rules: []*HttpRule{
				{Priority: 2, Action: RuleActionDeny, Paths: []string{"/b/*"}, Methods: []string{http.MethodPost}},
			},
		},
		RPC:  RpcConfig{Default: RuleActionAllow, JsonRpc: JsonRpcConfig{Default: RuleActionAllow}},
		GRPC: GrpcConfig{Default: RuleActionAllow},
	}
	assert.NilError(t, PrepareConfig(cfgA))
	assert.NilError(t, PrepareConfig(cfgB))

	cg := &CosmoGuard{cfg: cfgA}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Mutator: swaps cfg under configMutex on a tight loop, exactly
	// mirroring tryReload's write pattern.
	wg.Add(1)
	go func() {
		defer wg.Done()
		flip := false
		for !stop.Load() {
			cg.configMutex.Lock()
			if flip {
				cg.cfg = cfgA
			} else {
				cg.cfg = cfgB
			}
			flip = !flip
			cg.configMutex.Unlock()
		}
	}()

	// Readers: hammer the same code path the dashboard handler uses.
	// With the snapshot in place these only read a stable *Config
	// captured under the lock; without it -race flags the cfg.LCD.Rules
	// slice header read.
	readers := 8
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for !stop.Load() {
				_ = listRules(cg)
				_ = listIdentities(cg)
			}
		}()
	}

	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
}

// TestInstallDashboardServer_HasTimeouts pins the slowloris
// hardening: a forgotten ReadHeaderTimeout / ReadTimeout /
// WriteTimeout / IdleTimeout on a public-facing http.Server lets
// a single attacker dripping bytes pin a goroutine indefinitely.
// gosec G112 surfaces this; the test catches a regression that
// drops the timeouts from installDashboardServer's literal.
func TestInstallDashboardServer_HasTimeouts(t *testing.T) {
	on := true
	cg := &CosmoGuard{cfg: &Config{}}
	srv := installDashboardServer(cg, &DashboardConfig{Enable: &on, Port: 19999}, "127.0.0.1")
	assert.Assert(t, srv != nil, "expected a server when dashboard is enabled")
	assert.Assert(t, srv.ReadHeaderTimeout > 0, "ReadHeaderTimeout must be set (got %v)", srv.ReadHeaderTimeout)
	assert.Assert(t, srv.ReadTimeout > 0, "ReadTimeout must be set (got %v)", srv.ReadTimeout)
	assert.Assert(t, srv.WriteTimeout > 0, "WriteTimeout must be set (got %v)", srv.WriteTimeout)
	assert.Assert(t, srv.IdleTimeout > 0, "IdleTimeout must be set (got %v)", srv.IdleTimeout)
}

// TestDashboardInfo_ClusterSection — the UI uses /api/v1/info to
// decide whether to surface the scope toggle and the Cluster tab.
// "cluster.enabled" follows ClusterConfig.Enable verbatim;
// "peer_count" is 0 unless cluster is enabled AND olric reports
// members. We test the cluster-off path here because spinning up
// an embedded olric just to assert "peer_count >= 1" overshoots
// what this unit test should know about.
func TestDashboardInfo_ClusterSection(t *testing.T) {
	cg := &CosmoGuard{cfg: &Config{}, startedAt: time.Now()}
	info := dashboardInfo(cg)
	cluster, ok := info["cluster"].(map[string]any)
	assert.Assert(t, ok, "info.cluster must be a map")
	assert.Equal(t, cluster["enabled"], false)
	assert.Equal(t, cluster["peer_count"], 0)

	cg2 := &CosmoGuard{
		cfg: &Config{Cache: CacheGlobalConfig{
			Cluster: &ClusterConfig{BindPort: 3320},
		}},
		startedAt: time.Now(),
	}
	info2 := dashboardInfo(cg2)
	cluster2 := info2["cluster"].(map[string]any)
	// No cluster runtime wired in the test fixture so peer_count
	// stays 0 even when Enable is true — the UI keeps the toggle
	// hidden in this state (see updateScopeToggle in app.js).
	assert.Equal(t, cluster2["enabled"], true)
	assert.Equal(t, cluster2["peer_count"], 0)
}

// TestJoinNonEmpty_ActuallyFiltersEmpties pins the fix for the
// latent bug where joinNonEmpty was misnamed — it inserted a
// separator before every element regardless of content, so a slice
// like {"a", "", "b"} produced "a  b" with a double space. The
// summary helpers don't pass empty strings today, but a future
// caller would otherwise corrupt the match-summary output.
func TestJoinNonEmpty_ActuallyFiltersEmpties(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want string
	}{
		{[]string{}, ""},
		{[]string{""}, ""},
		{[]string{"", ""}, ""},
		{[]string{"a"}, "a"},
		{[]string{"a", "b"}, "a b"},
		{[]string{"a", "", "b"}, "a b"},
		{[]string{"", "a", "b"}, "a b"},
		{[]string{"a", "b", ""}, "a b"},
		{[]string{"", "a", "", "b", ""}, "a b"},
	} {
		got := joinNonEmpty(tc.in)
		assert.Equal(t, got, tc.want, "joinNonEmpty(%v) = %q, want %q", tc.in, got, tc.want)
		assert.Assert(t, !strings.Contains(got, "  "),
			"output must not contain double spaces: %q", got)
	}
}
