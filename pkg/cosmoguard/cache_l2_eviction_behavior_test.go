package cosmoguard

import (
	"context"
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestL2Eviction_CacheEvictsButExemptDMapsDoNot drives a real embedded olric
// (configured through the production newClusterRuntime → applyL2EvictionConfig
// path) past a small per-node cap and asserts:
//   - a response-cache DMap (name NOT in the exempt list) sheds entries, so
//     the L2 working set stays bounded (issue #15);
//   - an exempt DMap (the JWT replay set) keeps every entry under the same
//     write pressure — the security-critical guarantee that memory pressure
//     can't silently drop replay-protection or rate-limit state.
func TestL2Eviction_CacheEvictsButExemptDMapsDoNot(t *testing.T) {
	const capBytes = 1 << 20 // 1 MiB per-node cap → forces eviction quickly
	ctx := context.Background()
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 8 << 20, ResponseLRUBytesPerDMap: capBytes})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(ctx) })
	client := cr.Client()
	require.NotNil(t, client)
	value := make([]byte, 8<<10) // 8 KiB entries

	// Exempt DMap: write a modest, fixed set we expect to survive intact.
	exempt, err := client.NewDMap(replayJTIDMap)
	require.NoError(t, err)
	const exemptKeys = 200
	for i := 0; i < exemptKeys; i++ {
		require.NoError(t, exempt.Put(ctx, fmt.Sprintf("jti-%d", i), value))
	}

	// Cache DMap: write far past the cap so LRU eviction must engage.
	cacheDM, err := client.NewDMap("cosmoguard-prodrpc") // cache.Key + proxy — not exempt
	require.NoError(t, err)
	const cacheWrites = 4000 // 4000 × 8 KiB ≈ 32 MiB >> 1 MiB cap
	for i := 0; i < cacheWrites; i++ {
		require.NoError(t, cacheDM.Put(ctx, fmt.Sprintf("resp-%d", i), value))
	}

	// The cache DMap must have shed entries (bounded working set), so a
	// large fraction of the early keys are gone.
	present := 0
	for i := 0; i < cacheWrites; i++ {
		if _, err := cacheDM.Get(ctx, fmt.Sprintf("resp-%d", i)); err == nil {
			present++
		}
	}
	assert.Less(t, present, cacheWrites,
		"cache DMap should have evicted entries under the per-node cap (present=%d of %d)", present, cacheWrites)

	// The exempt DMap must be fully intact despite the concurrent pressure —
	// NONE eviction policy means nothing is ever dropped.
	for i := 0; i < exemptKeys; i++ {
		_, err := exempt.Get(ctx, fmt.Sprintf("jti-%d", i))
		assert.NoErrorf(t, err, "exempt DMap key jti-%d must survive (never evicted)", i)
	}
}

func TestL2NativeLRUAndLocalPressureSequence(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 3 << 20, ResponseLRUBytesPerDMap: 32 << 20})
	require.NoError(t, err)
	defer cr.Close(context.Background())
	const name = "pressure-sequence"
	dm, err := cr.Client().NewDMap(name)
	require.NoError(t, err)
	keys := []string{}
	for i := 0; len(keys) < 5; i++ {
		key := fmt.Sprint(i)
		if len(keys) == 0 || xxhash.Sum64String(name+key)%16 == xxhash.Sum64String(name+keys[0])%16 {
			keys = append(keys, key)
		}
	}
	for _, name := range evictionExemptDMaps {
		security, err := cr.Client().NewDMap(name)
		require.NoError(t, err)
		require.NoError(t, security.Put(t.Context(), "sentinel", []byte("security")))
	}
	for _, key := range keys[:4] {
		require.NoError(t, dm.Put(t.Context(), key, make([]byte, 256<<10)))
	}
	_, err = dm.Get(t.Context(), keys[3])
	require.NoError(t, err)
	require.NoError(t, dm.Put(t.Context(), keys[4], make([]byte, 900<<10)), "native candidate processing must finish before pressure retries")
	require.Positive(t, cr.responsePool.Snapshot().PressureEvictions)
	for _, key := range []string{keys[3], keys[4]} {
		r, err := dm.Get(t.Context(), key)
		require.NoError(t, err)
		v, err := r.Byte()
		require.NoError(t, err)
		want := 256 << 10
		if key == keys[4] {
			want = 900 << 10
		}
		require.Equal(t, make([]byte, want), v)
	}
	for _, name := range evictionExemptDMaps {
		security, err := cr.Client().NewDMap(name)
		require.NoError(t, err)
		r, err := security.Get(t.Context(), "sentinel")
		require.NoError(t, err)
		v, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte("security"), v)
	}
	require.LessOrEqual(t, cr.responsePool.Snapshot().Allocated, cr.responsePool.Snapshot().Capacity)
}
