package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/olric"
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
			BoundedOperations(1, 10*time.Millisecond, 0, nil, nil)(options)
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
	BoundedOperations(1, 10*time.Millisecond, 0, nil, nil)(options)
	c := &OlricCache[string, []byte]{dm: dm, cfg: options, namespace: "test"}
	err := c.Set(t.Context(), "key", make([]byte, 1<<20+1), time.Minute)
	require.ErrorIs(t, err, olric.ErrEntryTooLarge)
	require.Zero(t, dm.calls.Load())
}

type heldReadDMap struct {
	olric.DMap
	entered chan<- struct{}
	release <-chan struct{}
}

func (d heldReadDMap) Get(ctx context.Context, key string) (*olric.GetResponse, error) {
	d.entered <- struct{}{}
	<-d.release
	return d.DMap.Get(ctx, key)
}

func TestOlricCacheSmallReadsAt250MiProfile(t *testing.T) {
	client := embeddedOlric(t)
	option := BoundedOperations(128, 100*time.Millisecond, 16<<20, nil, nil)
	c, err := NewOlricCache[string, []byte](client, "concurrent-small-reads", option)
	require.NoError(t, err)
	require.NoError(t, c.Set(t.Context(), "key", []byte("shared response"), time.Minute))
	const readers = 7
	entered := make(chan struct{}, readers)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	c.dm = heldReadDMap{c.dm, entered, release}
	done := make(chan error, readers)
	for range readers {
		go func() {
			value, err := c.Get(t.Context(), "key")
			if err == nil && string(value) != "shared response" {
				err = fmt.Errorf("wrong shared response: %q", value)
			}
			done <- err
		}()
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	admitted := 0
wait:
	for admitted < readers {
		select {
		case <-entered:
			admitted++
		case <-timer.C:
			break wait
		}
	}
	require.Equal(t, readers, admitted, "250Mi profile must admit more than two concurrent L2 reads")
	reserved, capacity := option.OperationBytes()
	require.LessOrEqual(t, reserved, capacity)
	unblock()
	for range readers {
		require.NoError(t, <-done)
	}
	reserved, _ = option.OperationBytes()
	require.Zero(t, reserved)
}

var heldReadDecode struct {
	entered chan struct{}
	release chan struct{}
}

type heldReadValue struct{ Body []byte }

func (v *heldReadValue) UnmarshalMsgpack(raw []byte) error {
	heldReadDecode.entered <- struct{}{}
	<-heldReadDecode.release
	body, err := DecodeValue[[]byte](raw)
	v.Body = body
	return err
}

func TestOlricCacheShrinksReadsBeforeDecode(t *testing.T) {
	client := embeddedOlric(t)
	dm, err := client.NewDMap("read-decode-charge")
	require.NoError(t, err)
	encoded, err := EncodeValue([]byte("shared response"))
	require.NoError(t, err)
	require.NoError(t, dm.Put(t.Context(), "key", encoded, olric.EX(time.Minute)))
	option := BoundedOperations(128, 100*time.Millisecond, 16<<20, nil, nil)
	c, err := NewOlricCache[string, heldReadValue](client, "read-decode-charge", option)
	require.NoError(t, err)
	heldReadDecode.entered = make(chan struct{}, 32)
	heldReadDecode.release = make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(heldReadDecode.release) }) }
	defer unblock()
	done := make(chan error, 32)
	launched, admitted := 0, 0
	for range 32 {
		launched++
		go func() {
			value, err := c.Get(t.Context(), "key")
			if err == nil && string(value.Body) != "shared response" {
				err = fmt.Errorf("wrong decoded response: %q", value.Body)
			}
			done <- err
		}()
		select {
		case <-heldReadDecode.entered:
			admitted++
		case <-time.After(50 * time.Millisecond):
		}
		if admitted != launched {
			break
		}
	}
	reserved, _ := option.OperationBytes()
	unblock()
	var failures []error
	for range launched {
		if err := <-done; err != nil {
			failures = append(failures, err)
		}
	}
	require.Equal(t, 32, admitted, "known small values must free capacity before decoding completes")
	require.LessOrEqual(t, reserved, uint64(512<<10))
	require.Empty(t, failures)
	reserved, _ = option.OperationBytes()
	require.Zero(t, reserved)
}
