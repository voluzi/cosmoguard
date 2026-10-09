package cosmoguard

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
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

func TestPressureVictimsAreNotCapacityRejections(t *testing.T) {
	before := testutil.ToFloat64(l2StoragePressureEvictions)
	rejected := testutil.ToFloat64(l2StorageRejections.WithLabelValues("put_raw"))
	var p *olricstore.Pool
	p = olricstore.NewPool(3<<20, olricstore.Response, func(event string) { _ = p.Snapshot(); recordL2StorageRejection(event) })
	defer p.Close(t.Context())
	e := olricstore.NewEngine(p)
	for h := uint64(0); h < 5; h++ {
		v := olricstore.NewEntry()
		v.SetKey("key")
		v.SetValue(make([]byte, 256<<10))
		require.NoError(t, e.PutRaw(h, v.Encode()))
	}
	require.Equal(t, before+1, testutil.ToFloat64(l2StoragePressureEvictions))
	require.Equal(t, rejected, testutil.ToFloat64(l2StorageRejections.WithLabelValues("put_raw")))
	require.Equal(t, uint64(1), p.Snapshot().PressureEvictions)
	foreign, err := e.Fork(nil)
	require.NoError(t, err)
	v := olricstore.NewEntry()
	v.SetKey("foreign")
	v.SetValue(make([]byte, 256<<10))
	require.ErrorIs(t, foreign.PutRaw(100, v.Encode()), olricstore.ErrCapacity)
	require.Equal(t, rejected+1, testutil.ToFloat64(l2StorageRejections.WithLabelValues("put_raw")))
	require.Equal(t, before+1, testutil.ToFloat64(l2StoragePressureEvictions))
}
