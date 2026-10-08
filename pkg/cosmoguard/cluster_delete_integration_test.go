//go:build integration

package cosmoguard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"
)

func samePartitionKeys(name string, count int) []string {
	keys := make([]string, 0, count)
	for i := 0; len(keys) < count; i++ {
		key := fmt.Sprint(i)
		if len(keys) == 0 || xxhash.Sum64String(name+key)%271 == xxhash.Sum64String(name+keys[0])%271 {
			keys = append(keys, key)
		}
	}
	return keys
}

func removeMixedBackup(t *testing.T, clients []*redis.Client, name, key string) {
	t.Helper()
	for _, client := range clients {
		if _, err := client.Do(t.Context(), "dm.getentry", name, key, "RC").Text(); err != nil {
			continue
		}
		require.NoError(t, client.Do(t.Context(), "dm.delentry", name, key, "RC").Err())
		_, err := client.Do(t.Context(), "dm.getentry", name, key, "RC").Text()
		require.Error(t, err)
		return
	}
	t.Fatal("replica missing before removal")
}

func TestMixedEngineLRUEvictsWithMissingBackup(t *testing.T) {
	configure := func(c *config.Config, _ *mixedNode) {
		applyL2EvictionConfig(c.DMaps, 16<<20, 2)
		c.DMaps.MaxKeys = 1
		c.DMaps.MaxInuse = 0
		c.DMaps.CheckEmptyFragmentsInterval = time.Hour
	}
	a := startMixedNodeConfigured(t, true, "", 16<<20, configure)
	b := startMixedNodeConfigured(t, true, a.gossip, 16<<20, configure)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	const name = "lru-missing-backup"
	dm, err := a.db.NewEmbeddedClient().NewDMap(name)
	require.NoError(t, err)
	keys := samePartitionKeys(name, 2)
	clients := mixedReplicaClients(t, a, b)
	mixedConfirmReplica(t, dm, name, keys[0], []byte("candidate"), time.Now().Add(time.Hour).UnixMilli(), clients)
	removeMixedBackup(t, clients, name, keys[0])
	require.NoError(t, dm.Put(t.Context(), keys[1], []byte("replacement")))
	_, err = dm.Get(t.Context(), keys[0])
	require.ErrorIs(t, err, olric.ErrKeyNotFound)
	got, err := dm.Get(t.Context(), keys[1])
	require.NoError(t, err)
	value, err := got.Byte()
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), value)
}

type beforeBucketRead struct {
	olric.DMap
	before func()
}

func (d beforeBucketRead) Get(ctx context.Context, key string) (*olric.GetResponse, error) {
	d.before()
	return d.DMap.Get(ctx, key)
}

func TestMixedEngineLimiterUnlocksWithMissingBackup(t *testing.T) {
	configure := func(c *config.Config, _ *mixedNode) { c.DMaps.CheckEmptyFragmentsInterval = time.Hour }
	a := startMixedNodeConfigured(t, true, "", 16<<20, configure)
	b := startMixedNodeConfigured(t, true, a.gossip, 16<<20, configure)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	limiter, err := newOlricRateLimiter(a.db.NewEmbeddedClient(), RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 3}, "missing-backup")
	require.NoError(t, err)
	peer, err := newOlricRateLimiter(b.db.NewEmbeddedClient(), RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 3}, "missing-backup")
	require.NoError(t, err)
	clients := mixedReplicaClients(t, a, b)
	limiter.dm = beforeBucketRead{limiter.dm, func() { removeMixedBackup(t, clients, rateLimitLocksDMap, "missing-backup:identity") }}
	allowed, _, err := limiter.Allow(t.Context(), "identity")
	require.NoError(t, err)
	require.True(t, allowed)
	allowed, _, err = peer.Allow(t.Context(), "identity")
	require.NoError(t, err)
	require.True(t, allowed, "unlock must release the token before its two-second TTL")
}
