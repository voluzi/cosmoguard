package cosmoguard

import (
	"context"
	"fmt"
	"testing"

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
	cr, err := newClusterRuntime(clusterRuntimeOptions{L2MaxBytesPerNode: capBytes})
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
