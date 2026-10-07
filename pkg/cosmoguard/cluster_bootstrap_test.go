package cosmoguard

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/olric-data/olric"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// bootstrapMember mirrors olric's member metadata on the gossip/RESP wire.
type bootstrapMember struct {
	Name      string
	NameHash  uint64
	ID        uint64
	Birthdate int64
}

type bootstrapDelegate struct{ meta []byte }

func (d bootstrapDelegate) NodeMeta(int) []byte           { return d.meta }
func (bootstrapDelegate) NotifyMsg([]byte)                {}
func (bootstrapDelegate) GetBroadcasts(int, int) [][]byte { return nil }
func (bootstrapDelegate) LocalState(bool) []byte          { return nil }
func (bootstrapDelegate) MergeRemoteState([]byte, bool)   {}

func TestClusterRuntimeWaitsForRoutingTable(t *testing.T) {
	ports := reserveLoopbackPorts(t, 3)
	coordinator := bootstrapMember{Name: "127.0.0.1:1", ID: 42, Birthdate: 1}
	meta, err := msgpack.Marshal(coordinator)
	require.NoError(t, err)
	mc := memberlist.DefaultLocalConfig()
	mc.Name, mc.BindAddr, mc.BindPort = coordinator.Name, "127.0.0.1", ports[0]
	mc.AdvertisePort, mc.LogOutput, mc.Delegate = ports[0], io.Discard, bootstrapDelegate{meta: meta}
	mc.SecretKey, err = DecodeClusterEncryptionKey(testClusterEncryptionKey)
	require.NoError(t, err)
	peers, err := memberlist.Create(mc)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peers.Shutdown() })
	cfg := &ClusterConfig{BindAddr: "127.0.0.1", BindPort: ports[1], GossipPort: ports[2], ReplicaCount: 1, Quorum: 1, EncryptionKey: testClusterEncryptionKey,
		Discovery: &ClusterDiscoveryConfig{Mode: "static", Static: &StaticDiscoveryConfig{Peers: []string{net.JoinHostPort("127.0.0.1", strconv.Itoa(ports[0]))}}}}
	type outcome struct {
		cr  *clusterRuntime
		err error
	}
	result := make(chan outcome, 1)
	returned := false
	t.Cleanup(func() {
		if returned {
			return
		}
		select {
		case r := <-result:
			if r.cr != nil {
				_ = r.cr.Close(context.Background())
			}
		case <-time.After(3 * time.Second):
			t.Error("runtime did not terminate")
		}
	})
	go func() {
		cr, err := newClusterRuntime(clusterRuntimeOptions{Cluster: cfg, StartTimeout: 2 * time.Second})
		result <- outcome{cr, err}
	}()
	var joiner bootstrapMember
	require.Eventually(t, func() bool {
		for _, m := range peers.Members() {
			if m.Name != coordinator.Name {
				return msgpack.Unmarshal(m.Meta, &joiner) == nil
			}
		}
		return false
	}, time.Second, time.Millisecond)
	// A joiner has services running but no routing table until the coordinator sends it.
	select {
	case r := <-result:
		returned = true
		if r.cr != nil {
			_ = r.cr.Close(context.Background())
		}
		t.Fatalf("runtime returned before routing delivery: %v", r.err)
	case <-time.After(150 * time.Millisecond):
	}
	table := map[uint64]any{}
	for id := uint64(0); id < 271; id++ {
		table[id] = map[string]any{"Owners": []bootstrapMember{joiner}, "Backups": []bootstrapMember{}}
	}
	payload, err := msgpack.Marshal(table)
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: joiner.Name, Password: testClusterEncryptionKey})
	defer client.Close()
	require.NoError(t, client.Do(t.Context(), "internal.node.updaterouting", payload, coordinator.ID).Err())
	select {
	case r := <-result:
		returned = true
		require.NoError(t, r.err)
		require.NotNil(t, r.cr)
		defer r.cr.Close(context.Background())
		dm, err := r.cr.Client().NewDMap("grpc")
		require.NoError(t, err)
		require.NoError(t, dm.Put(t.Context(), "key", "value"))
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not finish after routing delivery")
	}
}

type bootstrapProbe struct {
	calls int
	open  func(int) error
}

func (p *bootstrapProbe) NewDMap(string, ...olric.DMapOption) (olric.DMap, error) {
	p.calls++
	return nil, p.open(p.calls)
}

func TestClusterBootstrapRetriesOnlyTransientErrors(t *testing.T) {
	permanent := errors.New("authentication rejected")
	for _, tc := range []struct {
		name     string
		failures []error
		want     error
		calls    int
	}{
		{"transient", []error{olric.ErrOperationTimeout, olric.ErrClusterQuorum}, nil, 3},
		{"permanent", []error{permanent}, permanent, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &bootstrapProbe{open: func(n int) error {
				if n <= len(tc.failures) {
					return tc.failures[n-1]
				}
				return nil
			}}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := waitClusterBootstrap(ctx, probe)
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, tc.calls, probe.calls)
		})
	}
}

func TestClusterRuntimeBootstrapDeadlineCleansUp(t *testing.T) {
	ports := reserveLoopbackPorts(t, 2)
	cfg := &ClusterConfig{BindAddr: "127.0.0.1", BindPort: ports[0], GossipPort: ports[1], ReplicaCount: 2, Quorum: 2, EncryptionKey: testClusterEncryptionKey,
		Discovery: &ClusterDiscoveryConfig{Mode: "static", Static: &StaticDiscoveryConfig{}}}
	started := time.Now()
	cr, err := newClusterRuntime(clusterRuntimeOptions{Cluster: cfg, StartTimeout: 150 * time.Millisecond})
	if cr != nil {
		defer cr.Close(context.Background())
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, cr)
	require.Less(t, time.Since(started), time.Second)
	for _, port := range ports {
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		require.NoError(t, err, "daemon must release its listeners")
		require.NoError(t, l.Close())
	}
}
