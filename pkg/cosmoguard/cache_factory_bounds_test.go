package cosmoguard

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"sync"
	"testing"
	"time"
)

type pausedResponseEncoding struct{ release <-chan struct{} }

func (v pausedResponseEncoding) CacheEncodedSize() uint64 { return 64 }
func (v pausedResponseEncoding) EncodeMsgpack(enc *msgpack.Encoder) error {
	<-v.release
	return enc.EncodeString("cached")
}

func TestResponseCacheDefaultsToBoundedOperations(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)
	responses, err := newResponseCache[string, pausedResponseEncoding](&CacheGlobalConfig{Cluster: &ClusterConfig{}}, cr.Client(), "default-admission", CacheBudget{}, nil)
	require.NoError(t, err)
	defer responses.Close()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan error, 1)
	go func() {
		done <- responses.Set(context.Background(), "key", pausedResponseEncoding{release}, time.Minute)
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, boundedcall.ErrTimeout)
	case <-time.After(3 * time.Second):
		t.Fatal("public response cache ran an unbounded encode")
	}
	unblock()
	require.NoError(t, responses.Close())
	_, err = responses.Get(t.Context(), "never-cached")
	require.ErrorIs(t, err, boundedcall.ErrRejected, "closing the cache closes its default gate")
}
