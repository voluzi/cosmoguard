package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olric-data/olric"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
)

type blockedCacheDMap struct {
	olric.DMap
	release <-chan struct{}
	calls   atomic.Int32
	entered chan struct{}
}

func (d *blockedCacheDMap) Get(context.Context, string) (*olric.GetResponse, error) {
	n := d.calls.Add(1)
	if n == 1 && d.entered != nil {
		d.entered <- struct{}{}
	}
	<-d.release
	return nil, olric.ErrKeyNotFound
}
func (d *blockedCacheDMap) Put(context.Context, string, any, ...olric.PutOption) error {
	n := d.calls.Add(1)
	if n == 1 && d.entered != nil {
		d.entered <- struct{}{}
	}
	<-d.release
	return nil
}

func TestOlricCacheBoundsAllOperations(t *testing.T) {
	for _, method := range []string{"get", "expiry", "has", "set"} {
		t.Run(method, func(t *testing.T) {
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			dm := &blockedCacheDMap{release: release, entered: make(chan struct{}, 1)}
			options := defaultOptions()
			BoundedOperations(1, 10*time.Millisecond, nil)(options)
			c := &OlricCache[string, []byte]{dm: dm, cfg: options, namespace: "test"}
			done := make(chan error, 1)
			go func() {
				var err error
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
				done <- err
			}()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.DeadlineExceeded)
			case <-time.After(5 * time.Second):
				t.Fatal("cache operation did not stop waiting")
			}
			select {
			case <-dm.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("backend operation did not enter")
			}
			_, err := c.Get(t.Context(), "other")
			require.ErrorIs(t, err, boundedcall.ErrRejected)
			require.Equal(t, int32(1), dm.calls.Load())
			unblock()
			require.Eventually(t, func() bool { _, err := c.Get(t.Context(), "recovered"); return err == ErrNotFound }, time.Second, time.Millisecond)
		})
	}
}

func TestOlricCacheRejectsOversizedPayloadBeforePut(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	dm := &blockedCacheDMap{release: release}
	options := defaultOptions()
	BoundedOperations(1, 10*time.Millisecond, nil)(options)
	c := &OlricCache[string, []byte]{dm: dm, cfg: options, namespace: "test"}
	err := c.Set(t.Context(), "key", make([]byte, 1<<20+1), time.Minute)
	require.ErrorIs(t, err, olric.ErrEntryTooLarge)
	require.Zero(t, dm.calls.Load())
}
