package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olric-data/olric"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v5/internal/boundedcall"
)

type blockedCacheDMap struct {
	olric.DMap
	release <-chan struct{}
	calls   atomic.Int32
}

func (d *blockedCacheDMap) Get(context.Context, string) (*olric.GetResponse, error) {
	d.calls.Add(1)
	<-d.release
	return nil, olric.ErrKeyNotFound
}
func (d *blockedCacheDMap) Put(context.Context, string, any, ...olric.PutOption) error {
	d.calls.Add(1)
	<-d.release
	return nil
}

type blockedCacheClient struct{ dm olric.DMap }

func (c blockedCacheClient) NewDMap(string, ...olric.DMapOption) (olric.DMap, error) {
	return c.dm, nil
}

func TestOlricCacheBoundsAllOperations(t *testing.T) {
	for _, method := range []string{"get", "expiry", "has", "set"} {
		t.Run(method, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			safety := time.AfterFunc(200*time.Millisecond, unblock)
			defer func() { safety.Stop(); unblock() }()
			dm := &blockedCacheDMap{release: release}
			c, err := NewOlricCache[string, []byte](blockedCacheClient{dm}, "test", BoundedOperations(1, 10*time.Millisecond, nil))
			require.NoError(t, err)
			start := time.Now()
			switch method {
			case "get":
				_, err = c.Get(t.Context(), "key")
			case "expiry":
				_, _, err = c.GetWithExpiry(t.Context(), "key")
			case "has":
				_, err = c.Has(t.Context(), "key")
			case "set":
				err = c.Set(t.Context(), "key", []byte("value"), time.Second)
			}
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(start), 100*time.Millisecond)
			_, err = c.Get(t.Context(), "other")
			require.ErrorIs(t, err, boundedcall.ErrRejected)
			require.Equal(t, int32(1), dm.calls.Load())
			unblock()
			require.Eventually(t, func() bool { _, err := c.Get(t.Context(), "recovered"); return err == ErrNotFound }, time.Second, time.Millisecond)
		})
	}
}
