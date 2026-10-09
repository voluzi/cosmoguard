package cosmoguard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
	"github.com/voluzi/olric"
)

func TestRuntimeEngineSelectionSeparatesPools(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 4 << 20, ResponseLRUBytesPerDMap: 0})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cr.Close(context.Background())) })
	response, err := cr.Client().NewDMap("pressure")
	require.NoError(t, err)
	for i := range 12 {
		_ = response.Put(t.Context(), fmt.Sprint(i), make([]byte, 900<<10))
	}
	require.Positive(t, cr.responsePool.Snapshot().PutRejected)
	before := cr.responsePool.Snapshot()
	for _, name := range evictionExemptDMaps {
		dm, err := cr.Client().NewDMap(name)
		require.NoError(t, err)
		require.NoError(t, dm.Put(t.Context(), "same-key", []byte(name)))
		r, err := dm.Get(t.Context(), "same-key")
		require.NoError(t, err)
		b, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte(name), b)
	}
	require.Equal(t, before.Allocated, cr.responsePool.Snapshot().Allocated)
	require.Equal(t, uint64(4), cr.securityPool.Snapshot().Entries)
}
func TestRuntimeSecurityPoolWhenL2Unlimited(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	for _, name := range []string{"response", replayJTIDMap} {
		dm, err := cr.Client().NewDMap(name)
		require.NoError(t, err)
		require.NoError(t, dm.Put(t.Context(), "k", []byte("v")))
	}
	require.Equal(t, uint64(1), cr.responsePool.Snapshot().Entries)
	require.Equal(t, uint64(1), cr.securityPool.Snapshot().Entries)
}
func TestRuntimePoolExpiry(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 8 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	before := float64(time.Now().UnixMilli()) / 1000
	for _, name := range []string{"expiry", replayJTIDMap} {
		dm, err := cr.Client().NewDMap(name)
		require.NoError(t, err)
		require.NoError(t, dm.Put(t.Context(), "anchor", []byte("live")))
		require.NoError(t, dm.Put(t.Context(), "k", []byte("v"), olric.PX(10*time.Millisecond)))
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(l2Metrics)
	require.Eventually(t, func() bool {
		families, err := registry.Gather()
		if err != nil {
			return false
		}
		swept := map[string]bool{}
		for _, family := range families {
			if family.GetName() != "cosmoguard_l2_last_compaction_timestamp_seconds" {
				continue
			}
			for _, metric := range family.Metric {
				swept[metric.Label[0].GetValue()] = metric.Gauge.GetValue() >= before
			}
		}
		response, security := cr.responsePool.Snapshot(), cr.securityPool.Snapshot()
		return swept["response"] && swept["security"] && response.Entries == 1 && security.Entries == 1 &&
			float64(response.LastCompactionUnixMilli)/1000 >= before && float64(security.LastCompactionUnixMilli)/1000 >= before
	}, 3*time.Second, 20*time.Millisecond, "both pools must complete storage sweeps, not just sampled eviction")
	require.NoError(t, cr.Close(context.Background()))
	require.Zero(t, cr.responsePool.Snapshot().Allocated)
}
func TestCapacityRejectionStillFillsL1(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	c, err := newResponseCache[string, []byte](nil, cr.Client(), "capacity-l1", CacheBudget{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.Error(t, c.Set(t.Context(), "k", []byte("cached locally"), time.Minute))
	v, err := c.Get(t.Context(), "k")
	require.NoError(t, err)
	require.Equal(t, []byte("cached locally"), v)
	require.Zero(t, cr.responsePool.Snapshot().Entries)
	require.Positive(t, cr.responsePool.Snapshot().PutRejected)
}
func TestRuntimeGatesAreIndependent(t *testing.T) {
	a, err := newClusterRuntime(clusterRuntimeOptions{L2WorkBytes: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(context.Background()) })
	b, err := newClusterRuntime(clusterRuntimeOptions{L2WorkBytes: 16 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	for i, cr := range []*clusterRuntime{a, b} {
		c, err := newResponseCache[string, []byte](nil, cr.Client(), "standalone-gate", CacheBudget{}, cr.ResponseOperations())
		require.NoError(t, err)
		t.Cleanup(func() { _ = c.Close() })
		_, err = c.Get(t.Context(), "missing")
		if i == 0 {
			require.ErrorIs(t, err, boundedcall.ErrRejected)
		} else {
			require.ErrorIs(t, err, cache.ErrNotFound)
		}
	}
}
func TestResponseBytePressureDoesNotConsumeLimiterOrReplaySlots(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{L2WorkBytes: 1})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	c, err := newResponseCache[string, []byte](nil, cr.Client(), "blocked-responses", CacheBudget{}, cr.ResponseOperations())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.ErrorIs(t, c.Set(t.Context(), "key", []byte("value"), time.Second), boundedcall.ErrRejected)
	dm, err := cr.Client().NewDMap(replayJTIDMap)
	require.NoError(t, err)
	require.NoError(t, dm.Put(t.Context(), "jti", []byte("seen"), olric.NX(), olric.EX(time.Minute)))
	require.ErrorIs(t, dm.Put(t.Context(), "jti", []byte("twice"), olric.NX()), olric.ErrKeyFound)
	limiter, err := newRuleRateLimiter(RateLimitConfig{Rate: Rate{PerSecond: 0.001}, Burst: 1}, &CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "pressure")
	require.NoError(t, err)
	t.Cleanup(func() { _ = limiter.Close() })
	ok, _, err := limiter.Allow(t.Context(), "subject")
	require.NoError(t, err)
	require.True(t, ok)
	ok, _, err = limiter.Allow(t.Context(), "subject")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDMapLockLeaseAndTokenUnlock(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 4 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cr.Close(context.Background())) })
	dm, err := cr.Client().NewDMap(rateLimitLocksDMap)
	require.NoError(t, err)
	lock, err := dm.LockWithTimeout(t.Context(), "lease", time.Second, time.Second)
	require.NoError(t, err)
	require.NoError(t, lock.Lease(t.Context(), 2*time.Second))
	r, err := dm.Get(t.Context(), "lease")
	require.NoError(t, err)
	require.Greater(t, r.TTL(), time.Now().Add(time.Second).UnixMilli())
	require.NoError(t, lock.Unlock(t.Context()))
	next, err := dm.LockWithTimeout(t.Context(), "lease", time.Second, time.Second)
	require.NoError(t, err)
	require.Error(t, lock.Unlock(t.Context()))
	require.NoError(t, next.Unlock(t.Context()))
}
