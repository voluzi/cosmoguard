package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/olric"
)

type outageDMap struct {
	olric.DMap
	err   error
	calls atomic.Int32
}

func (d *outageDMap) Get(context.Context, string) (*olric.GetResponse, error) {
	d.calls.Add(1)
	return nil, d.err
}
func (d *outageDMap) Put(context.Context, string, any, ...olric.PutOption) error {
	d.calls.Add(1)
	return d.err
}

func TestOlricOutageRecoversOnMissAndCapacity(t *testing.T) {
	for _, capacity := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			var unavailable atomic.Bool
			opts := defaultOptions()
			RecoveringOperations(8, time.Second, 32<<20, nil, nil, unavailable.Store)(opts)
			dm := &outageDMap{err: olric.ErrOperationTimeout}
			c := &OlricCache[string, []byte]{dm: dm, cfg: opts}
			for range 3 {
				_, err := c.Get(t.Context(), "key")
				require.ErrorIs(t, err, boundedcall.ErrTimeout)
			}
			require.True(t, unavailable.Load())
			for range 10 {
				_, err := c.Get(t.Context(), "another-partition")
				require.ErrorIs(t, err, boundedcall.ErrUnavailable)
			}
			err := c.Set(t.Context(), "key", []byte("value"), time.Second)
			require.True(t, IsExpectedL2Skip(err))
			require.Equal(t, "unavailable", writeSkipReason(err))
			require.Equal(t, int32(3), dm.calls.Load())
			time.Sleep(time.Second)
			if capacity {
				dm.err = olricstore.ErrCapacity
				err = c.Set(t.Context(), "key", []byte("value"), time.Second)
				require.ErrorIs(t, err, olricstore.ErrCapacity)
			} else {
				dm.err = olric.ErrKeyNotFound
				_, err = c.Get(t.Context(), "key")
				require.ErrorIs(t, err, ErrNotFound)
			}
			require.False(t, unavailable.Load(), "a valid backend reply closes the outage")
			dm.err = olric.ErrKeyNotFound
			_, err = c.Get(t.Context(), "key")
			require.ErrorIs(t, err, ErrNotFound)
			require.Equal(t, int32(5), dm.calls.Load())
		})
	}
}
