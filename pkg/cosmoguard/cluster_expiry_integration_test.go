//go:build integration

package cosmoguard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"
	"github.com/voluzi/olric/pkg/storage"
)

type pausedLRUEngine struct{ storage.Engine }

func (e *pausedLRUEngine) Fork(c *storage.Config) (storage.Engine, error) {
	child, err := e.Engine.Fork(c)
	if err != nil {
		return nil, err
	}
	return &pausedLRUEngine{child}, nil
}
func (e *pausedLRUEngine) Range(f func(uint64, storage.Entry) bool) {
	e.Engine.Range(func(h uint64, v storage.Entry) bool {
		time.Sleep(1100 * time.Millisecond)
		return f(h, v)
	})
}

func TestMixedEngineExpiryCannotInvalidateLockedLRUCandidate(t *testing.T) {
	node := startMixedNodeConfigured(t, true, "", 8<<20, func(c *config.Config, _ *mixedNode) {
		applyL2EvictionConfig(c.DMaps, 16<<20, 1)
		c.DMaps.MaxKeys = 271
		c.DMaps.MaxInuse = 0
		c.DMaps.TriggerCompactionInterval = time.Second
		c.DMaps.Engine.Implementation = &pausedLRUEngine{c.DMaps.Engine.Implementation}
	})
	dm, err := node.db.NewEmbeddedClient().NewDMap("lru-expiry")
	require.NoError(t, err)
	keys := make([]string, 0, 2)
	for i := 0; len(keys) < 2; i++ {
		k := fmt.Sprint(i)
		if len(keys) == 0 || xxhash.Sum64String("lru-expiry"+k)%271 == xxhash.Sum64String("lru-expiry"+keys[0])%271 {
			keys = append(keys, k)
		}
	}
	require.NoError(t, dm.Put(context.Background(), keys[0], []byte("expired candidate"), olric.PX(20*time.Millisecond)))
	require.NoError(t, dm.Put(context.Background(), keys[1], []byte("replacement")))
	got, err := dm.Get(t.Context(), keys[1])
	require.NoError(t, err)
	value, err := got.Byte()
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), value)
}
