//go:build integration

package cosmoguard

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/config"
	"github.com/voluzi/olric/pkg/storage"
)

type pausedLRUEngine struct {
	storage.Engine
	captured        chan struct{}
	expire, release <-chan struct{}
	expired         chan error
	compacted       chan struct{}
}

func (e *pausedLRUEngine) Fork(c *storage.Config) (storage.Engine, error) {
	child, err := e.Engine.Fork(c)
	if err != nil {
		return nil, err
	}
	return &pausedLRUEngine{Engine: child, captured: e.captured, expire: e.expire, release: e.release, expired: e.expired, compacted: e.compacted}, nil
}
func (e *pausedLRUEngine) Range(f func(uint64, storage.Entry) bool) {
	e.Engine.Range(func(h uint64, v storage.Entry) bool {
		e.captured <- struct{}{}
		<-e.expire
		v.SetTTL(time.Now().Add(-time.Hour).UnixMilli())
		e.expired <- e.Engine.UpdateTTL(h, v)
		<-e.release
		return f(h, v)
	})
}

func (e *pausedLRUEngine) Compaction() (bool, error) {
	done, err := e.Engine.Compaction()
	select {
	case e.compacted <- struct{}{}:
	default:
	}
	return done, err
}

func TestMixedEngineExpiryCannotInvalidateLockedLRUCandidate(t *testing.T) {
	captured, expire, release := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	expired, compacted := make(chan error, 1), make(chan struct{}, 1)
	var expireOnce, releaseOnce sync.Once
	trigger := func() { expireOnce.Do(func() { close(expire) }) }
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer trigger()
	defer unblock()
	node := startMixedNodeConfigured(t, true, "", 8<<20, func(c *config.Config, _ *mixedNode) {
		applyL2EvictionConfig(c.DMaps, 16<<20, 1)
		c.DMaps.MaxKeys = 271
		c.DMaps.MaxInuse = 0
		c.DMaps.TriggerCompactionInterval = time.Second
		c.DMaps.Engine.Implementation = &pausedLRUEngine{Engine: c.DMaps.Engine.Implementation, captured: captured, expire: expire, release: release, expired: expired, compacted: compacted}
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
	require.NoError(t, dm.Put(t.Context(), keys[0], []byte("candidate")))
	putDone := make(chan error, 1)
	go func() { putDone <- dm.Put(t.Context(), keys[1], []byte("replacement")) }()
	select {
	case <-captured:
	case err := <-putDone:
		t.Fatalf("LRU did not capture candidate: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("LRU did not enter Range")
	}
	trigger()
	select {
	case err := <-expired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("candidate was not expired after capture")
	}
	// Hold the captured candidate across the periodic compaction tick.
	time.Sleep(1100 * time.Millisecond)
	unblock()
	select {
	case err := <-putDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("replacement Put did not finish")
	}
	select {
	case <-compacted:
	case <-time.After(5 * time.Second):
		t.Fatal("expiry sweep did not finish")
	}
	got, err := dm.Get(t.Context(), keys[1])
	require.NoError(t, err)
	value, err := got.Byte()
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), value)
}
