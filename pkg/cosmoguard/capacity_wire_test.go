package cosmoguard

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
)

func TestFullResponsePoolClassifiesRealClientCapacitySkip(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{ResponsePoolBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cr.Close(context.Background()) })
	dm, err := cr.Client().NewDMap("capacity-wire")
	require.NoError(t, err)
	rawErr := dm.Put(t.Context(), "key", []byte("value"))
	require.Error(t, rawErr)
	t.Logf("real embedded Put capacity result: %T %v; sentinel=%v", rawErr, rawErr, errors.Is(rawErr, olricstore.ErrCapacity))
	var reason string
	operations := cache.RecoveringOperations(8, time.Second, 8<<20, nil, func(r string) { reason = r }, nil)
	t.Cleanup(operations.CloseOperations)
	c, err := cache.NewOlricCache[string, []byte](cr.Client(), "capacity-wire", operations)
	require.NoError(t, err)
	err = c.Set(t.Context(), "key", []byte("value"), time.Minute)
	require.ErrorIs(t, err, cache.ErrL2Skipped)
	require.Equal(t, "storage_capacity", reason)
	require.ErrorIs(t, err, olricstore.ErrCapacity)
	var out bytes.Buffer
	logger := newEntry(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	logCacheBackendError(logger, err, "cache failed")
	require.Contains(t, out.String(), "level=DEBUG")
	require.NotContains(t, out.String(), "level=ERROR")
}
