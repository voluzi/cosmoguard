package cache

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// embeddedOlric spins up a single-node olric daemon on loopback with
// ephemeral ports. It returns a ready EmbeddedClient and registers cleanup
// so the daemon is shut down at test end. Accepts testing.TB so the same
// helper serves benchmarks via olric_benchmark_test.go.
func embeddedOlric(t testing.TB) *olric.EmbeddedClient {
	t.Helper()

	c := config.New("local")
	c.PartitionCount = 7 // small for tests; matches olric's own internal pattern

	mc := memberlist.DefaultLocalConfig()
	mc.BindAddr = "127.0.0.1"
	mc.BindPort = 0
	// memberlist rejects both LogOutput and Logger being set; olric
	// always installs its own Logger onto MemberlistConfig at start
	// (see olric/internal/discovery/discovery.go:167), so we have to
	// leave LogOutput nil here even though the default config wires it
	// to os.Stderr.
	mc.LogOutput = nil
	c.MemberlistConfig = mc

	port, err := freePort()
	require.NoError(t, err)
	c.BindAddr = "127.0.0.1"
	c.BindPort = port
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))

	// olric refuses to start if both LogOutput and Logger are set, so pin
	// Logger only and route everything through a discard writer for tests.
	c.LogOutput = nil
	c.Logger = log.New(io.Discard, "", 0)
	c.LeaveTimeout = 200 * time.Millisecond

	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())

	ready := make(chan struct{})
	c.Started = func() { close(ready) }

	db, err := olric.New(c)
	require.NoError(t, err)

	var startErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = db.Start()
	}()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatalf("olric failed to start within 10s")
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = db.Shutdown(ctx)
		wg.Wait()
		assert.NoError(t, startErr)
	})

	return db.NewEmbeddedClient()
}

func freePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func testOlricCache[K comparable, V any](t *testing.T, namespace string, testItems []testCase[K, V]) {
	client := embeddedOlric(t)
	cache, err := NewOlricCache[K, V](client, namespace)
	require.NoError(t, err)
	for _, tc := range testItems {
		err := cache.Set(context.Background(), tc.Key, tc.Value, tc.TTL)
		assert.NoError(t, err)
		if tc.Wait > 0 {
			time.Sleep(tc.Wait)
		}
		v, err := cache.Get(context.Background(), tc.Key)
		assert.ErrorIs(t, err, tc.Expected.Err)
		assert.Equal(t, tc.Expected.Value, v)
	}
}

func TestOlricCacheStringInt(t *testing.T) {
	items := []testCase[string, int]{
		{
			Key:   "A",
			Value: 1,
			TTL:   500 * time.Millisecond,
			Wait:  100 * time.Millisecond,
			Expected: testCaseExpect[int]{
				Value: 1,
				Err:   nil,
			},
		},
		{
			Key:   "B",
			Value: 1,
			TTL:   500 * time.Millisecond,
			Wait:  900 * time.Millisecond,
			Expected: testCaseExpect[int]{
				Value: 0,
				Err:   ErrNotFound,
			},
		},
	}
	testOlricCache(t, "string-int", items)
}

func TestOlricCacheStringString(t *testing.T) {
	items := []testCase[string, string]{
		{
			Key:   "C",
			Value: "hello",
			TTL:   0,
			Expected: testCaseExpect[string]{
				Value: "hello",
				Err:   nil,
			},
		},
	}
	testOlricCache(t, "string-string", items)
}

func TestOlricCacheStringStruct(t *testing.T) {
	items := []testCase[string, testStruct]{
		{
			Key: "C",
			Value: testStruct{
				S: "test",
				I: 5,
			},
			TTL: 0,
			Expected: testCaseExpect[testStruct]{
				Value: testStruct{
					S: "test",
					I: 5,
				},
				Err: nil,
			},
		},
	}
	testOlricCache(t, "string-struct", items)
}

// TestOlricCacheBytesBypass exercises the []byte hot path: the value should
// round-trip without msgpack wrapping and the cache must hand back a slice
// equal to the input (we don't assert pointer identity since the
// implementation defensively copies on Set).
func TestOlricCacheBytesBypass(t *testing.T) {
	client := embeddedOlric(t)
	cache, err := NewOlricCache[string, []byte](client, "bytes")
	require.NoError(t, err)

	payload := []byte("\x00\x01\x02 raw binary \xff\xfe")
	require.NoError(t, cache.Set(context.Background(), "k", payload, time.Second))

	got, err := cache.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, payload, got)

	has, err := cache.Has(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, has)

	missing, err := cache.Has(context.Background(), "nope")
	require.NoError(t, err)
	assert.False(t, missing)
}

func TestOlricCacheGetMissing(t *testing.T) {
	client := embeddedOlric(t)
	cache, err := NewOlricCache[string, int](client, "missing")
	require.NoError(t, err)

	_, err = cache.Get(context.Background(), "absent")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestOlricCacheNamespaceIsolation(t *testing.T) {
	client := embeddedOlric(t)
	a, err := NewOlricCache[string, int](client, "ns-a")
	require.NoError(t, err)
	b, err := NewOlricCache[string, int](client, "ns-b")
	require.NoError(t, err)

	require.NoError(t, a.Set(context.Background(), "k", 1, time.Second))
	require.NoError(t, b.Set(context.Background(), "k", 2, time.Second))

	va, err := a.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, 1, va)

	vb, err := b.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, 2, vb)
}

func TestOlricCacheNilClient(t *testing.T) {
	_, err := NewOlricCache[string, int](nil, "x")
	assert.Error(t, err)
}
