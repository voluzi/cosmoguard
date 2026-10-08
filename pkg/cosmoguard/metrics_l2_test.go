package cosmoguard

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRuntimeMetricsSnapshotAndCleanup(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(l2Metrics)
	families, err := reg.Gather()
	require.NoError(t, err)
	baseline := map[string]float64{}
	for _, f := range families {
		for _, m := range f.Metric {
			if m.Gauge != nil {
				key := f.GetName()
				if len(m.Label) > 0 {
					key += m.Label[0].GetValue()
				}
				baseline[key] = m.Gauge.GetValue()
			}
		}
	}
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 8 << 20, L2WorkBytes: 16 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	dm, err := cr.Client().NewDMap("untrusted-user-namespace")
	require.NoError(t, err)
	require.NoError(t, dm.Put(t.Context(), "secret-key", []byte("value")))
	families, err = reg.Gather()
	require.NoError(t, err)
	seen := false
	for _, f := range families {
		for _, m := range f.Metric {
			require.LessOrEqual(t, len(m.Label), 1)
			for _, l := range m.Label {
				require.Equal(t, "pool", l.GetName())
				require.Contains(t, []string{"response", "security"}, l.GetValue())
				if f.GetName() == "cosmoguard_l2_storage_allocated_bytes" && l.GetValue() == "response" {
					require.Greater(t, m.Gauge.GetValue(), baseline[f.GetName()+"response"])
					seen = true
				}
			}
		}
	}
	require.True(t, seen)
	var cpuSeen, limiterSeen bool
	for _, f := range families {
		if f.GetName() == "cosmoguard_gc_cpu_seconds_total" {
			require.Len(t, f.Metric, 1)
			require.Empty(t, f.Metric[0].Label)
			require.NotNil(t, f.Metric[0].Counter)
			cpuSeen = true
		}
		if f.GetName() == "cosmoguard_gc_limiter_last_enabled_cycle" {
			require.Len(t, f.Metric, 1)
			require.Empty(t, f.Metric[0].Label)
			require.NotNil(t, f.Metric[0].Gauge)
			limiterSeen = true
		}
	}
	require.True(t, cpuSeen, "the soak ledger needs process GC CPU")
	require.True(t, limiterSeen, "the soak ledger needs GC limiter activation")

	require.NoError(t, cr.Close(context.Background()))
	families, err = reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), "cosmoguard_l2_") {
			continue
		}
		for _, m := range f.Metric {
			if m.Gauge != nil {
				key := f.GetName()
				if len(m.Label) > 0 {
					key += m.Label[0].GetValue()
				}
				require.Equal(t, baseline[key], m.Gauge.GetValue())
			}
		}
	}
}
