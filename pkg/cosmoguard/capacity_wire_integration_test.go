//go:build integration

package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"
)

func TestMixedEngineRemoteCapacitySkipsUseDebugLogs(t *testing.T) {
	configure := func(c *config.Config, _ *mixedNode) { c.ReplicaCount = 1 }
	a := startMixedNodeConfigured(t, true, "", 8<<20, configure)
	b := startMixedNodeConfigured(t, true, a.gossip, 1<<20, configure)
	cluster, err := olric.NewClusterClient([]string{a.address}, olric.WithPassword(testClusterEncryptionKey))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.Close(context.Background())) })
	const name = "remote-capacity"
	var key string
	require.Eventually(t, func() bool {
		routes, err := cluster.RoutingTable(t.Context())
		if err != nil {
			return false
		}
		for i := range 10000 {
			candidate := fmt.Sprint(i)
			owners := routes[xxhash.Sum64String(name+candidate)%271].PrimaryOwners
			if len(owners) > 0 && owners[len(owners)-1] == b.address {
				key = candidate
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	dm, err := a.db.NewEmbeddedClient().NewDMap(name)
	require.NoError(t, err)
	rawErr := dm.Put(t.Context(), key, []byte("value"))
	require.Error(t, rawErr)
	t.Logf("remote embedded Put: %T %v; sentinel=%v", rawErr, rawErr, errors.Is(rawErr, olricstore.ErrCapacity))
	var reason string
	operations := cache.RecoveringOperations(8, time.Second, 8<<20, nil, func(r string) { reason = r }, nil)
	t.Cleanup(operations.CloseOperations)
	c, err := cache.NewOlricCache[string, []byte](a.db.NewEmbeddedClient(), name, operations)
	require.NoError(t, err)
	err = c.Set(t.Context(), key, []byte("value"), time.Minute)
	require.ErrorIs(t, err, cache.ErrL2Skipped)
	require.Equal(t, "storage_capacity", reason)
	require.ErrorIs(t, err, olricstore.ErrCapacity)
	var out bytes.Buffer
	logger := newEntry(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	logCacheBackendError(logger, err, "cache failed")
	require.Contains(t, out.String(), "level=DEBUG")
	require.NotContains(t, out.String(), "level=ERROR")
}
