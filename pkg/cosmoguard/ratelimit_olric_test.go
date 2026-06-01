package cosmoguard

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOlricRateLimiter_BurstAndRefill is the single-node sanity check
// for the olric-backed limiter. Same shape as TestMemoryRateLimiter but
// against a clustered runtime so the lock + DMap path is exercised end
// to end. If this passes but the cluster test below fails, the bug is
// in the cross-node partition routing — not in the limiter math.
func TestOlricRateLimiter_BurstAndRefill(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)

	cfg := RateLimitConfig{
		Rate:  Rate{PerSecond: 50}, // 1 token every 20 ms
		Burst: 3,
	}
	l, err := newOlricRateLimiter(cr.Client(), cfg, "test")
	require.NoError(t, err)
	defer l.Close()

	ctx := context.Background()
	// Burst window: first 3 admits.
	for i := 0; i < 3; i++ {
		ok, _, err := l.Allow(ctx, "client-a")
		require.NoError(t, err)
		require.True(t, ok, "burst slot %d should pass", i)
	}
	// Next one denied.
	ok, retry, err := l.Allow(ctx, "client-a")
	require.NoError(t, err)
	require.False(t, ok)
	require.Greater(t, retry, time.Duration(0))

	// Wait for refill, then a single token should be available again.
	time.Sleep(50 * time.Millisecond)
	ok2, _, err := l.Allow(ctx, "client-a")
	require.NoError(t, err)
	require.True(t, ok2, "token should have refilled by now")
}

// TestOlricRateLimiter_SeparateBucketsPerKey verifies that two distinct
// keys (different IPs, different identities, etc.) get independent
// budgets — the bucket key is derived from the input verbatim. Catches
// a regression where the limiter accidentally shares state across keys.
func TestOlricRateLimiter_SeparateBucketsPerKey(t *testing.T) {
	cr := newEmbeddedClusterRuntimeForTest(t)

	cfg := RateLimitConfig{Rate: Rate{PerSecond: 1}, Burst: 1}
	l, err := newOlricRateLimiter(cr.Client(), cfg, "test")
	require.NoError(t, err)
	defer l.Close()

	ctx := context.Background()
	okA, _, _ := l.Allow(ctx, "a")
	require.True(t, okA)
	okADenied, _, _ := l.Allow(ctx, "a")
	require.False(t, okADenied)
	okB, _, _ := l.Allow(ctx, "b")
	require.True(t, okB, "B has its own bucket")
}

// TestOlricRateLimiter_ClusterSharedBudget is the Step 3 contract test:
// when two pods rate-limit the same key, the cluster total admits no
// more than the configured burst. Today's per-pod limiter would let 2×
// the configured rate through under a 2-pod HPA — that's the gap the
// olric limiter closes, and this test guards against regressing back.
func TestOlricRateLimiter_ClusterSharedBudget(t *testing.T) {
	a, b := newTwoNodeClusterForTest(t)

	const burst = 5
	cfg := RateLimitConfig{
		Rate:  Rate{PerSecond: 1}, // 1 req/s steady state
		Burst: burst,
	}
	lA, err := newOlricRateLimiter(a.Client(), cfg, "ratelimit-cluster")
	require.NoError(t, err)
	lB, err := newOlricRateLimiter(b.Client(), cfg, "ratelimit-cluster")
	require.NoError(t, err)

	// Drive 5×burst requests from both nodes in parallel for the SAME
	// bucket key. The bucket starts full, refills at 1/s. Over the
	// ~500ms this test takes that refill contributes < 1 token, so
	// total admits should be exactly burst (5) ± 1 for refill timing.
	var (
		mu       sync.Mutex
		admitted int
	)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hit := func(l *olricRateLimiter, n int) {
		defer wg.Done()
		for i := 0; i < n; i++ {
			ok, _, err := l.Allow(ctx, "ip-1.2.3.4")
			if err != nil {
				continue
			}
			if ok {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}
	}
	wg.Add(2)
	go hit(lA, 5*burst)
	go hit(lB, 5*burst)
	wg.Wait()

	// Tolerance: at most burst + 1 admits (a refill might land
	// mid-test on slow CI). Critically, we expect WAY less than the
	// "10 admits if each pod ran independently" failure mode.
	require.LessOrEqual(t, admitted, burst+1,
		"cluster-wide admits should be ≈ burst (got %d, burst=%d)", admitted, burst)
	require.GreaterOrEqual(t, admitted, burst,
		"all burst tokens should have been spent (got %d, burst=%d)", admitted, burst)
}

// newEmbeddedClusterRuntimeForTest spins up a single-node embedded-only
// clusterRuntime and registers shutdown cleanup. Sufficient for the
// single-node limiter tests where cross-node correctness isn't the
// thing being checked.
func newEmbeddedClusterRuntimeForTest(t *testing.T) *clusterRuntime {
	t.Helper()
	cr, err := newClusterRuntime(clusterRuntimeOptions{})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cr.Close(ctx)
	})
	return cr
}

// newTwoNodeClusterForTest brings up a 2-node cluster on loopback with
// static discovery, waits for membership to converge to 2, and returns
// both runtimes. Mirrors the setup in TestClusterRuntimeTwoNodeStatic
// Discovery but factored out so other cluster-mode tests can reuse it.
func newTwoNodeClusterForTest(t *testing.T) (*clusterRuntime, *clusterRuntime) {
	t.Helper()
	ports := reserveLoopbackPorts(t, 4)
	bindA, gossipA, bindB, gossipB := ports[0], ports[1], ports[2], ports[3]
	addrA := net.JoinHostPort("127.0.0.1", strconv.Itoa(gossipA))
	addrB := net.JoinHostPort("127.0.0.1", strconv.Itoa(gossipB))
	peers := []string{addrA, addrB}

	mk := func(bind, gossip int) *clusterRuntime {
		cfg := &ClusterConfig{
			BindAddr:     "127.0.0.1",
			BindPort:     bind,
			GossipPort:   gossip,
			ReplicaCount: 2,
			Quorum:       1,
			Discovery: &ClusterDiscoveryConfig{
				Mode:   "static",
				Static: &StaticDiscoveryConfig{Peers: peers},
			},
		}
		cr, err := newClusterRuntime(clusterRuntimeOptions{
			Cluster:      cfg,
			StartTimeout: 30 * time.Second,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = cr.Close(ctx)
		})
		return cr
	}

	a := mk(bindA, gossipA)
	b := mk(bindB, gossipB)

	require.Eventually(t, func() bool {
		ma, err := a.Client().Members(context.Background())
		if err != nil || len(ma) != 2 {
			return false
		}
		mb, err := b.Client().Members(context.Background())
		return err == nil && len(mb) == 2
	}, 20*time.Second, 100*time.Millisecond, "2-node cluster never converged")

	return a, b
}
