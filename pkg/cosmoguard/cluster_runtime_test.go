package cosmoguard

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClusterEncryptionKey is a valid base64-encoded 32-byte memberlist
// key shared by cluster-forming tests (runtime, rate-limiter, replication).
const testClusterEncryptionKey = "Y29zbW9ndWFyZC10ZXN0LWtleS0zMi1ieXRlcyEhISE="

// TestClusterRuntimeEmbeddedStartStop verifies that the embedded-only
// default cluster runtime starts, exposes a usable client, and shuts down
// cleanly. This is the path that runs on every single-instance install,
// even with cluster mode disabled.
func TestClusterRuntimeEmbeddedStartStop(t *testing.T) {
	cr, err := newClusterRuntime(clusterRuntimeOptions{})
	require.NoError(t, err)
	require.NotNil(t, cr.Client())

	// Smoke: a DMap should be obtainable from the embedded client.
	dm, err := cr.Client().NewDMap("smoke")
	require.NoError(t, err)
	require.NoError(t, dm.Put(context.Background(), "k", []byte("v")))

	resp, err := dm.Get(context.Background(), "k")
	require.NoError(t, err)
	got, err := resp.Byte()
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, cr.Close(ctx))
}

func TestClusterRuntimeNilCloseSafe(t *testing.T) {
	var cr *clusterRuntime
	require.NoError(t, cr.Close(context.Background()))
	require.Nil(t, cr.Client())
}

// TestClusterRuntimeTwoNodeStaticDiscovery is the Step 2 integration test
// from the v4 plan: spin up two clustered olric runtimes on loopback with
// static discovery wiring them together, then assert that a Set on node A
// is Get-able from node B. This proves the ClusterConfig → ServiceDiscovery
// plumbing (BindPort/GossipPort split, ReplicaCount/Quorum, ServiceDiscovery
// plugin registration, peer pre-population, self-filter) actually wires up
// to a working cluster end-to-end.
func TestClusterRuntimeTwoNodeStaticDiscovery(t *testing.T) {
	// Reserve four loopback ports — two RESP, two gossip. We can't ask
	// the kernel for ports during runtime startup because olric's
	// validator rejects BindPort=0 in cluster mode, and the discovery
	// plugin needs the gossip addresses upfront to feed olric's initial
	// peer list.
	ports := reserveLoopbackPorts(t, 4)
	bindA, gossipA, bindB, gossipB := ports[0], ports[1], ports[2], ports[3]

	addrA := net.JoinHostPort("127.0.0.1", strconv.Itoa(gossipA))
	addrB := net.JoinHostPort("127.0.0.1", strconv.Itoa(gossipB))
	staticPeers := []string{addrA, addrB}

	newNode := func(bind, gossip int) *clusterRuntime {
		cfg := &ClusterConfig{
			BindAddr:      "127.0.0.1",
			BindPort:      bind,
			GossipPort:    gossip,
			ReplicaCount:  2,
			Quorum:        1,
			EncryptionKey: testClusterEncryptionKey,
			Discovery: &ClusterDiscoveryConfig{
				Mode:   "static",
				Static: &StaticDiscoveryConfig{Peers: staticPeers},
			},
		}
		cr, err := newClusterRuntime(clusterRuntimeOptions{
			Cluster:      cfg,
			StartTimeout: 30 * time.Second,
		})
		require.NoError(t, err)
		return cr
	}

	a := newNode(bindA, gossipA)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Close(ctx)
	})

	b := newNode(bindB, gossipB)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	// Wait for cluster convergence. memberlist gossip converges in a
	// couple of probe intervals; on a quiet loopback this is sub-second
	// but we give a generous ceiling because CI machines vary.
	require.Eventually(t, func() bool {
		members, err := a.Client().Members(context.Background())
		if err != nil {
			return false
		}
		return len(members) == 2
	}, 20*time.Second, 100*time.Millisecond, "node A never saw 2 members")

	require.Eventually(t, func() bool {
		members, err := b.Client().Members(context.Background())
		if err != nil {
			return false
		}
		return len(members) == 2
	}, 20*time.Second, 100*time.Millisecond, "node B never saw 2 members")

	// Set on A, Get from B. This is the contract: the cache backend
	// must serve reads from any node regardless of which node owns the
	// partition the key hashes to.
	dmA, err := a.Client().NewDMap("xnode")
	require.NoError(t, err)
	require.NoError(t, dmA.Put(context.Background(), "k", []byte("hello-from-A")))

	dmB, err := b.Client().NewDMap("xnode")
	require.NoError(t, err)

	// Allow a brief window for the write to settle across partitions.
	// On a freshly-formed cluster the first read can lose a routing
	// table refresh race; assert.Eventually handles that without
	// hard-coding a sleep.
	var got []byte
	require.Eventually(t, func() bool {
		resp, err := dmB.Get(context.Background(), "k")
		if err != nil {
			return false
		}
		got, err = resp.Byte()
		return err == nil
	}, 10*time.Second, 100*time.Millisecond, "node B never observed the key written on A")

	assert.Equal(t, []byte("hello-from-A"), got)
}

// reserveLoopbackPorts grabs n distinct free TCP ports on 127.0.0.1 and
// returns them. The probe listeners are held open across the whole
// allocation so the kernel doesn't hand the same port out twice for
// successive calls; then closed all at once. A small TOCTOU window
// remains between Close and olric's bind — acceptable in tests, and the
// only alternative (BindPort=0) is rejected by olric's validator in
// cluster mode.
func reserveLoopbackPorts(t *testing.T, n int) []int {
	t.Helper()
	listeners := make([]*net.TCPListener, 0, n)
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		l, err := net.ListenTCP("tcp", addr)
		require.NoError(t, err)
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	for _, l := range listeners {
		require.NoError(t, l.Close())
	}
	return ports
}
