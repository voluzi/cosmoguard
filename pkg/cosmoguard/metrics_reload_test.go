package cosmoguard

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestConfigReloadOutcomeCounter(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	for _, entry := range os.Environ() {
		if name, _, _ := strings.Cut(entry, "="); strings.HasPrefix(name, "COSMOGUARD_") {
			t.Setenv(name, "")
		}
	}
	prevTrusted := snapshotTrustedProxies()
	t.Cleanup(func() { restoreTrustedProxies(prevTrusted) })
	registerSharedMetrics()

	counts := func() map[string]float64 {
		t.Helper()
		families, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			t.Fatal(err)
		}
		result := make(map[string]float64)
		for _, family := range families {
			if family.GetName() != "cosmoguard_config_reloads_total" {
				continue
			}
			for _, metric := range family.GetMetric() {
				labels := metric.GetLabel()
				if len(labels) != 1 || labels[0].GetName() != "outcome" || metric.Counter == nil {
					t.Fatalf("unexpected reload metric: %v", metric)
				}
				outcome := labels[0].GetValue()
				switch outcome {
				case "applied", "restart_required", "invalid":
				default:
					t.Fatalf("unexpected reload outcome: %q", outcome)
				}
				result[outcome] = metric.GetCounter().GetValue()
			}
		}
		return result
	}

	previous := parseRestartConfig(t, "lcd: {rules: [{paths: [/old], action: allow}]}")
	path := filepath.Join(t.TempDir(), "config.yaml")
	cg := &CosmoGuard{cfg: previous, cfgFile: path, origNodes: previous.Nodes, dashboard: newDashboardObservability(),
		lcdProxy: &HttpProxy{}, rpcProxy: &HttpProxy{}, grpcProxy: &GrpcProxy{}, jsonRpcHandler: &JsonRpcHandler{},
	}
	cg.applyRulesLocked()
	want := counts()
	for _, tc := range []struct {
		name, raw, outcome string
	}{
		{"applied", "lcd: {rules: [{paths: [/new], action: deny}]}", "applied"},
		{"restart required", "server: {writeTimeout: 1s}\nlcd: {rules: [{paths: [/rejected], action: allow}]}", "restart_required"},
		{"invalid YAML", "lcd: [", "invalid"},
		{"invalid settings", "grpc: {maxRecvMsgSize: -1}", "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.raw), 0600); err != nil {
				t.Fatal(err)
			}
			cg.tryReload()
			want[tc.outcome]++
			got := counts()
			for _, outcome := range []string{"applied", "restart_required", "invalid"} {
				if got[outcome] != want[outcome] {
					t.Errorf("reload counter{%s} = %v; want %v", outcome, got[outcome], want[outcome])
				}
			}
			if status := cg.dashboard.lastReload; status == nil || status.Success != (tc.outcome == "applied") {
				t.Fatalf("reload status = %+v; outcome = %s", status, tc.outcome)
			}
			if cg.cfg.LCD.Rules[0].Paths[0] != "/new" || cg.lcdProxy.rules[0].Paths[0] != "/new" {
				t.Fatal("last applied rules must remain active after rejected reloads")
			}
		})
	}
}
