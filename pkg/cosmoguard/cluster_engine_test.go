package cosmoguard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	dm, err := cr.Client().NewDMap("expiry")
	require.NoError(t, err)
	require.NoError(t, dm.Put(t.Context(), "k", []byte("v"), olric.PX(10*time.Millisecond)))
	require.Eventually(t, func() bool { return cr.responsePool.Snapshot().Entries == 0 }, 3*time.Second, 20*time.Millisecond)
	require.NoError(t, cr.Close(context.Background()))
	require.Zero(t, cr.responsePool.Snapshot().Allocated)
}
func TestCapacityRejectionStillFillsL1(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	c, err := newResponseCache[string, []byte](nil, cr.Client(), "capacity-l1", CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.Error(t, c.Set(t.Context(), "k", []byte("cached locally"), time.Minute))
	v, err := c.Get(t.Context(), "k")
	require.NoError(t, err)
	require.Equal(t, []byte("cached locally"), v)
	require.Zero(t, cr.responsePool.Snapshot().Entries)
	require.Positive(t, cr.responsePool.Snapshot().PutRejected)
}
