//go:build integration

package cosmoguard

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"

	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
)

type mixedNode struct {
	db              *olric.Olric
	pool, security  *olricstore.Pool
	address, gossip string
	closed          bool
}

func startMixedNode(t *testing.T, bounded bool, peer string, cap uint64) *mixedNode {
	return startMixedNodeConfigured(t, bounded, peer, cap, nil)
}
func startMixedNodeConfigured(t *testing.T, bounded bool, peer string, cap uint64, configure func(*config.Config, *mixedNode)) *mixedNode {
	t.Helper()
	ports := reserveLoopbackPorts(t, 2)
	c := config.New("local")
	c.PartitionCount = 271
	c.ReplicaCount = 2
	c.ReadQuorum = 1
	c.WriteQuorum = 1
	c.MemberCountQuorum = 1
	c.BindAddr = "127.0.0.1"
	c.BindPort = ports[0]
	c.MemberlistConfig.BindAddr = c.BindAddr
	c.MemberlistConfig.BindPort = ports[1]
	c.MemberlistConfig.AdvertisePort = ports[1]
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	key, err := DecodeClusterEncryptionKey(testClusterEncryptionKey)
	require.NoError(t, err)
	c.MemberlistConfig.SecretKey = key
	c.Authentication = &config.Authentication{Password: testClusterEncryptionKey}
	c.LogOutput = nil
	c.Logger = stdlog.New(io.Discard, "", 0)
	c.RoutingTablePushInterval = 100 * time.Millisecond
	c.TriggerBalancerInterval = 100 * time.Millisecond
	// Empty fragments stop a balancer scan until the existing janitor removes them.
	c.DMaps.CheckEmptyFragmentsInterval = 100 * time.Millisecond
	c.LeaveTimeout = 500 * time.Millisecond
	n := &mixedNode{address: c.MemberlistConfig.Name, gossip: net.JoinHostPort(c.BindAddr, strconv.Itoa(ports[1]))}
	if bounded {
		c.DMaps.TriggerCompactionInterval = time.Second
		n.pool = olricstore.NewPool(cap, olricstore.Response, nil)
		n.security = olricstore.NewPool(0, olricstore.Security, nil)
		c.DMaps.Engine = &config.Engine{Implementation: olricstore.NewEngine(n.pool)}
		c.DMaps.Custom = map[string]config.DMap{}
		for _, name := range evictionExemptDMaps {
			c.DMaps.Custom[name] = config.DMap{Engine: &config.Engine{Implementation: olricstore.NewEngine(n.security)}, EvictionPolicy: config.EvictionPolicy("NONE")}
		}
	}
	if configure != nil {
		configure(c, n)
	}
	t.Cleanup(func() { n.close(t) })
	if peer != "" {
		c.Peers = []string{peer}
	}
	ready := make(chan struct{})
	c.Started = func() { close(ready) }
	require.NoError(t, c.Sanitize())
	require.NoError(t, c.Validate())
	n.db, err = olric.New(c)
	require.NoError(t, err)
	errors := make(chan error, 1)
	go func() { errors <- n.db.Start() }()
	select {
	case <-ready:
	case err := <-errors:
		require.NoError(t, err)
		t.Fatal("returned before ready")
	case <-time.After(20 * time.Second):
		t.Fatal("startup timeout")
	}
	return n
}

// The accelerated janitor can retire an empty fragment between lookup and write.
// Native Close permits a stale write to succeed, so confirm the primary too.
func mixedPut(t *testing.T, dm olric.DMap, key string, value []byte, options ...olric.PutOption) {
	t.Helper()
	require.Eventually(t, func() bool {
		if dm.Put(t.Context(), key, value, options...) != nil {
			return false
		}
		r, err := dm.Get(t.Context(), key)
		if err != nil {
			return false
		}
		stored, err := r.Byte()
		return err == nil && bytes.Equal(stored, value)
	}, 20*time.Second, 10*time.Millisecond)
}

func mixedReplicaClients(t *testing.T, nodes ...*mixedNode) []*redis.Client {
	t.Helper()
	var clients []*redis.Client
	for _, node := range nodes {
		client := redis.NewClient(&redis.Options{Addr: node.address, Password: testClusterEncryptionKey, Protocol: 2})
		t.Cleanup(func() { _ = client.Close() })
		clients = append(clients, client)
	}
	return clients
}
func mixedConfirmReplica(t *testing.T, dm olric.DMap, name, key string, value []byte, deadline int64, clients []*redis.Client) {
	t.Helper()
	encoded := value
	require.Eventually(t, func() bool {
		// Quorum one may succeed with a failed backup; verify the replica itself.
		if dm.Put(t.Context(), key, value, olric.PXAT(time.Duration(deadline)*time.Millisecond)) != nil {
			return false
		}
		for _, client := range clients {
			raw, err := client.Do(t.Context(), "dm.getentry", name, key, "RC").Text()
			if err != nil {
				continue
			}
			e := olricstore.NewEntry()
			e.Decode([]byte(raw))
			if e.TTL() == deadline && bytes.Equal(e.Value(), encoded) {
				return true
			}
		}
		return false
	}, 20*time.Second, 10*time.Millisecond, "confirmed replica for %s/%s", name, key)
}

func (n *mixedNode) close(t *testing.T) {
	t.Helper()
	if n.closed {
		return
	}
	n.closed = true
	if n.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, n.db.Shutdown(ctx))
	}
	if n.pool != nil {
		require.NoError(t, n.pool.Close(context.Background()))
		require.NoError(t, n.security.Close(context.Background()))
	}
}
func (n *mixedNode) counts(t *testing.T) (int, int) {
	t.Helper()
	s, err := n.db.NewEmbeddedClient().Stats(t.Context(), n.address)
	require.NoError(t, err)
	primary, backup := 0, 0
	for _, p := range s.Partitions {
		for name, d := range p.DMaps {
			if !isSecurityMap(name) {
				primary += d.Length
			}
		}
	}
	for _, p := range s.Backups {
		for name, d := range p.DMaps {
			if !isSecurityMap(name) {
				backup += d.Length
			}
		}
	}
	return primary, backup
}
func isSecurityMap(name string) bool {
	for _, n := range evictionExemptDMaps {
		if n == name {
			return true
		}
	}
	return false
}

func (n *mixedNode) awaitRouting(t *testing.T, replicas int, departed string) {
	t.Helper()
	require.Eventually(t, func() bool {
		s, err := n.db.NewEmbeddedClient().Stats(t.Context(), n.address)
		if err != nil || len(s.Partitions) == 0 {
			return false
		}
		for _, part := range s.Partitions {
			if len(part.PreviousOwners) != 0 || (replicas >= 0 && len(part.Backups) != replicas) {
				return false
			}
			for _, owner := range part.Backups {
				if owner.Name == departed {
					return false
				}
			}
		}
		return true
	}, 40*time.Second, 100*time.Millisecond, "routing convergence")
}

type mixedItem struct {
	namespace, key string
	marker         byte
}

var mixedNamespaces = []string{"cosmoguard-grpc", "cosmoguard-lcd", "cosmoguard-jsonrpc", "cosmoguard-rpc", "cosmoguard-evm_jsonrpc", "cosmoguard-evm_rpc", "cosmoguard-evm_jsonrpc_ws", "cosmoguard-evm_rpc_ws"}

func mixedRoundTrip(t *testing.T, first, second bool) {
	for _, maps := range []int{4, 8} {
		t.Run(fmt.Sprint(maps), func(t *testing.T) {
			a := startMixedNode(t, first, "", 256<<20)
			deadline := time.Now().Add(time.Hour).UnixMilli()
			var items []mixedItem
			for _, name := range mixedNamespaces[:maps] {
				dm, err := a.db.NewEmbeddedClient().NewDMap(name)
				require.NoError(t, err)
				seen := map[uint64]bool{}
				for i := 0; len(seen) < 271; i++ {
					key := fmt.Sprintf("key-%d", i)
					partition := xxhash.Sum64String(name+key) % 271
					if seen[partition] {
						continue
					}
					seen[partition] = true
					marker := byte(i % 251)
					mixedPut(t, dm, key, bytes.Repeat([]byte{marker}, 1024), olric.PXAT(time.Duration(deadline)*time.Millisecond))
					items = append(items, mixedItem{name, key, marker})
				}
			}
			populated, _ := a.counts(t)
			require.Equal(t, len(items), populated, "confirmed pre-join population")
			b := startMixedNode(t, second, a.gossip, 256<<20)
			var lastPrimaryA, lastPrimaryB int
			require.Eventually(t, func() bool {
				pa, _ := a.counts(t)
				pb, _ := b.counts(t)
				lastPrimaryA, lastPrimaryB = pa, pb
				return pa > 0 && pb > 0 && pa+pb == len(items)
			}, 40*time.Second, 100*time.Millisecond, "primary migration: last %d + %d, expected %d", lastPrimaryA, lastPrimaryB, len(items))
			a.awaitRouting(t, 1, "")
			b.awaitRouting(t, 1, "")
			verify := func(n *mixedNode, size int) {
				for _, it := range items {
					dm, err := n.db.NewEmbeddedClient().NewDMap(it.namespace)
					require.NoError(t, err)
					r, err := dm.Get(t.Context(), it.key)
					require.NoError(t, err)
					value, err := r.Byte()
					require.NoError(t, err)
					require.Equal(t, bytes.Repeat([]byte{it.marker}, size), value)
					require.Equal(t, deadline, r.TTL())
				}
			}
			verify(a, 1024)
			verify(b, 1024)
			clients := mixedReplicaClients(t, a, b)
			for _, it := range items {
				dm, err := b.db.NewEmbeddedClient().NewDMap(it.namespace)
				require.NoError(t, err)
				mixedConfirmReplica(t, dm, it.namespace, it.key, bytes.Repeat([]byte{it.marker}, 16384), deadline, clients)
			}
			require.Eventually(t, func() bool { _, ba := a.counts(t); _, bb := b.counts(t); return ba+bb == len(items) }, 20*time.Second, 100*time.Millisecond, "post-join replicas")
			verify(a, 16384)
			verify(b, 16384)
			a.close(t)
			require.Eventually(t, func() bool {
				members, err := b.db.NewEmbeddedClient().Members(t.Context())
				return err == nil && len(members) == 1
			}, 20*time.Second, 100*time.Millisecond)
			b.awaitRouting(t, -1, a.address)
			verify(b, 16384)
			if b.pool != nil {
				require.LessOrEqual(t, b.pool.Snapshot().Allocated, uint64(256<<20))
			}
		})
	}
}
func TestMixedEngineNativeThenBounded(t *testing.T)  { mixedRoundTrip(t, false, true) }
func TestMixedEngineBoundedThenNative(t *testing.T)  { mixedRoundTrip(t, true, false) }
func TestMixedEngineBoundedThenBounded(t *testing.T) { mixedRoundTrip(t, true, true) }
func TestMixedEngineSecurityMigration(t *testing.T) {
	a := startMixedNode(t, false, "", 0)
	for _, name := range evictionExemptDMaps {
		dm, err := a.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		mixedPut(t, dm, "sentinel", []byte(name), olric.EX(time.Hour))
	}
	b := startMixedNode(t, true, a.gossip, 4<<20)
	require.Eventually(t, func() bool {
		members, err := b.db.NewEmbeddedClient().Members(t.Context())
		return err == nil && len(members) == 2
	}, 20*time.Second, 100*time.Millisecond)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	clients := mixedReplicaClients(t, a, b)
	for _, name := range evictionExemptDMaps {
		dm, err := b.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		r, err := dm.Get(t.Context(), "sentinel")
		require.NoError(t, err)
		value, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte(name), value)
		mixedConfirmReplica(t, dm, name, "sentinel", value, r.TTL(), clients)
	}
	require.Eventually(t, func() bool {
		total := 0
		for _, node := range []*mixedNode{a, b} {
			st, err := node.db.NewEmbeddedClient().Stats(t.Context(), node.address)
			require.NoError(t, err)
			for _, part := range st.Backups {
				for name, dm := range part.DMaps {
					if isSecurityMap(name) {
						total += dm.Length
					}
				}
			}
		}
		return total == len(evictionExemptDMaps)
	}, 20*time.Second, 100*time.Millisecond, "confirmed security replicas")
	a.close(t)
	require.Eventually(t, func() bool {
		members, err := b.db.NewEmbeddedClient().Members(t.Context())
		return err == nil && len(members) == 1
	}, 20*time.Second, 100*time.Millisecond)
	b.awaitRouting(t, -1, a.address)
	for _, name := range evictionExemptDMaps {
		dm, err := b.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		r, err := dm.Get(t.Context(), "sentinel")
		require.NoError(t, err)
		value, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte(name), value)
	}
}

func TestMixedEngineDMapNXTTLAndLock(t *testing.T) {
	a := startMixedNode(t, true, "", 32<<20)
	b := startMixedNode(t, true, a.gossip, 32<<20)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	var maps []olric.DMap
	for _, node := range []*mixedNode{a, b} {
		dm, err := node.db.NewEmbeddedClient().NewDMap(replayJTIDMap)
		require.NoError(t, err)
		maps = append(maps, dm)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			err := maps[i%2].Put(t.Context(), "replay", []byte("verified-identity"), olric.NX(), olric.PX(time.Second))
			if err == nil {
				successes.Add(1)
			} else {
				require.ErrorIs(t, err, olric.ErrKeyFound)
			}
		})
	}
	wg.Wait()
	require.Equal(t, int32(1), successes.Load())
	require.ErrorIs(t, maps[0].Put(t.Context(), "absent", []byte("x"), olric.XX()), olric.ErrKeyNotFound)
	require.NoError(t, maps[1].Put(t.Context(), "replay", []byte("updated"), olric.XX(), olric.PX(100*time.Millisecond)))
	require.Eventually(t, func() bool { _, err := maps[0].Get(t.Context(), "replay"); return err == olric.ErrKeyNotFound }, 3*time.Second, 10*time.Millisecond)
	require.NoError(t, maps[0].Put(t.Context(), "replay", []byte("new-window"), olric.NX(), olric.EX(time.Hour)))
	locksA, err := a.db.NewEmbeddedClient().NewDMap("ratelimit-locks")
	require.NoError(t, err)
	locksB, err := b.db.NewEmbeddedClient().NewDMap("ratelimit-locks")
	require.NoError(t, err)
	lock, err := locksA.LockWithTimeout(t.Context(), "lease", time.Second, time.Second)
	require.NoError(t, err)
	_, err = locksB.LockWithTimeout(t.Context(), "lease", time.Second, 20*time.Millisecond)
	require.Error(t, err)
	require.NoError(t, lock.Unlock(t.Context()))
	next, err := locksB.LockWithTimeout(t.Context(), "lease", time.Second, time.Second)
	require.NoError(t, err)
	require.Error(t, lock.Unlock(t.Context()), "an old token must not release a new lock")
	require.NoError(t, next.Unlock(t.Context()))
}

func TestMixedEngineFullPoolReplicaAdmission(t *testing.T) {
	a := startMixedNode(t, false, "", 0)
	dm, err := a.db.NewEmbeddedClient().NewDMap(mixedNamespaces[0])
	require.NoError(t, err)
	for i := range 1000 {
		mixedPut(t, dm, fmt.Sprint(i), bytes.Repeat([]byte{byte(i % 251)}, 16384), olric.EX(time.Hour))
	}
	security, err := a.db.NewEmbeddedClient().NewDMap(replayJTIDMap)
	require.NoError(t, err)
	mixedPut(t, security, "full-sentinel", []byte("unexpired-security"), olric.EX(time.Hour))
	b := startMixedNode(t, true, a.gossip, 3<<20)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	require.Positive(t, b.pool.Snapshot().ImportDropped)
	require.LessOrEqual(t, b.pool.Snapshot().Allocated, uint64(3<<20))
	remote, err := b.db.NewEmbeddedClient().NewDMap(mixedNamespaces[0])
	require.NoError(t, err)
	var hits int
	for i := range 1000 {
		r, err := remote.Get(t.Context(), fmt.Sprint(i))
		if err != nil {
			require.ErrorIs(t, err, olric.ErrKeyNotFound)
			continue
		}
		value, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i % 251)}, 16384), value)
		hits++
	}
	require.Positive(t, hits)
	sec, err := b.db.NewEmbeddedClient().NewDMap(replayJTIDMap)
	require.NoError(t, err)
	r, err := sec.Get(t.Context(), "full-sentinel")
	require.NoError(t, err)
	value, err := r.Byte()
	require.NoError(t, err)
	require.Equal(t, []byte("unexpired-security"), value)
}

func TestMixedEngineTransferDisconnectRetry(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		t.Run(fmt.Sprint(bounded), func(t *testing.T) {
			receiver := startMixedNode(t, bounded, "", 16<<20)
			pool := olricstore.NewPool(16<<20, olricstore.Response, nil)
			t.Cleanup(func() { _ = pool.Close(context.Background()) })
			source, err := olricstore.NewEngine(pool).Fork(nil)
			require.NoError(t, err)
			name, key := mixedNamespaces[0], "disconnect-retry"
			value := bytes.Repeat([]byte{123}, 900<<10)
			e := source.NewEntry()
			e.SetKey(key)
			e.SetValue(value)
			e.SetTTL(time.Now().Add(time.Hour).UnixMilli())
			e.SetTimestamp(time.Now().UnixNano())
			hash := xxhash.Sum64String(name + key)
			require.NoError(t, source.Put(hash, e))
			iterator := source.TransferIterator()
			require.True(t, iterator.Next())
			payload, id, err := iterator.Export()
			require.NoError(t, err)
			packetBody := struct {
				PartID  uint64
				Kind    int
				Name    string
				Payload []byte
			}{hash % 271, 1, name, payload}
			packet, err := msgpack.Marshal(packetBody)
			require.NoError(t, err)
			frame := func(args ...[]byte) []byte {
				b := []byte(fmt.Sprintf("*%d\r\n", len(args)))
				for _, arg := range args {
					b = append(b, []byte(fmt.Sprintf("$%d\r\n", len(arg)))...)
					b = append(b, arg...)
					b = append(b, '\r', '\n')
				}
				return b
			}
			for _, complete := range []bool{false, true} {
				conn, err := net.DialTimeout("tcp", receiver.address, time.Second)
				require.NoError(t, err)
				require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
				_, err = conn.Write(frame([]byte("auth"), []byte(testClusterEncryptionKey)))
				require.NoError(t, err)
				ack, err := bufio.NewReader(conn).ReadString('\n')
				require.NoError(t, err)
				require.Equal(t, "+OK\r\n", ack)
				request := frame([]byte("internal.node.movefragment"), packet)
				if !complete {
					request = request[:len(request)/2]
				}
				_, err = conn.Write(request)
				require.NoError(t, err)
				// Disconnect before reading the ACK. The caller retains the source.
				require.NoError(t, conn.Close())
				raw, err := source.Get(hash)
				require.NoError(t, err)
				require.Equal(t, value, raw.Value())
			}
			// The intervening reads touched the source. Acknowledge a fresh export.
			payload, id, err = iterator.Export()
			require.NoError(t, err)
			packetBody.Payload = payload
			packet, err = msgpack.Marshal(packetBody)
			require.NoError(t, err)
			clients := mixedReplicaClients(t, receiver)
			ack, err := clients[0].Do(t.Context(), "internal.node.movefragment", packet).Text()
			require.NoError(t, err)
			require.Equal(t, "OK", ack)
			require.NoError(t, iterator.Drop(id))
			require.Zero(t, source.Stats().Length)
			dm, err := receiver.db.NewEmbeddedClient().NewDMap(name)
			require.NoError(t, err)
			r, err := dm.Get(t.Context(), key)
			require.NoError(t, err)
			out, err := r.Byte()
			require.NoError(t, err)
			require.Equal(t, value, out)
			require.Equal(t, e.TTL(), r.TTL())
		})
	}
}

func TestMixedEngineRollbackToNative(t *testing.T) {
	a := startMixedNode(t, false, "", 0)
	deadline := time.Now().Add(time.Hour).UnixMilli()
	for _, name := range append(append([]string{}, mixedNamespaces[:4]...), evictionExemptDMaps...) {
		dm, err := a.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		mixedPut(t, dm, "rollback", []byte(name), olric.PXAT(time.Duration(deadline)*time.Millisecond))
	}
	b := startMixedNode(t, true, a.gossip, 16<<20)
	a.awaitRouting(t, 1, "")
	b.awaitRouting(t, 1, "")
	first := mixedReplicaClients(t, a, b)
	for _, name := range append(append([]string{}, mixedNamespaces[:4]...), evictionExemptDMaps...) {
		dm, err := b.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		mixedConfirmReplica(t, dm, name, "rollback", []byte(name), deadline, first)
	}
	a.close(t)
	require.Eventually(t, func() bool {
		members, err := b.db.NewEmbeddedClient().Members(t.Context())
		return err == nil && len(members) == 1
	}, 20*time.Second, 100*time.Millisecond)
	b.awaitRouting(t, -1, a.address)
	c := startMixedNode(t, false, b.gossip, 0)
	b.awaitRouting(t, 1, "")
	c.awaitRouting(t, 1, "")
	second := mixedReplicaClients(t, b, c)
	for _, name := range append(append([]string{}, mixedNamespaces[:4]...), evictionExemptDMaps...) {
		dm, err := c.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		r, err := dm.Get(t.Context(), "rollback")
		require.NoError(t, err)
		value, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte(name), value)
		require.Equal(t, deadline, r.TTL())
		mixedConfirmReplica(t, dm, name, "rollback", value, deadline, second)
	}
	b.close(t)
	require.Eventually(t, func() bool {
		members, err := c.db.NewEmbeddedClient().Members(t.Context())
		return err == nil && len(members) == 1
	}, 20*time.Second, 100*time.Millisecond)
	c.awaitRouting(t, -1, b.address)
	for _, name := range append(append([]string{}, mixedNamespaces[:4]...), evictionExemptDMaps...) {
		dm, err := c.db.NewEmbeddedClient().NewDMap(name)
		require.NoError(t, err)
		r, err := dm.Get(t.Context(), "rollback")
		require.NoError(t, err)
		value, err := r.Byte()
		require.NoError(t, err)
		require.Equal(t, []byte(name), value)
		require.Equal(t, deadline, r.TTL())
	}
}

func TestMixedEngineSharedResponseBudgetPressure(t *testing.T) {
	retained := []int{}
	for _, divisor := range []uint64{4, 1} {
		t.Run(fmt.Sprint(divisor), func(t *testing.T) {
			const cap = 8 << 20
			configure := func(c *config.Config, _ *mixedNode) {
				// Avoid the pinned memberlist shutdown race with an in-flight periodic probe.
				c.MemberlistConfig.ProbeInterval = time.Hour
				applyL2EvictionConfig(c.DMaps, cap/divisor, 2)
				c.DMaps.CheckEmptyFragmentsInterval = time.Hour
			}
			a := startMixedNodeConfigured(t, true, "", cap, configure)
			b := startMixedNodeConfigured(t, true, a.gossip, cap, configure)
			a.awaitRouting(t, 1, "")
			b.awaitRouting(t, 1, "")
			const name = "shared-budget-lcd"
			client := a.db.NewEmbeddedClient()
			for _, other := range []string{"grpc", "jsonrpc", "rpc", "evm_jsonrpc", "evm_rpc", "evm_jsonrpc_ws", "evm_rpc_ws"} {
				dm, err := client.NewDMap(other)
				require.NoError(t, err)
				require.NoError(t, dm.Put(t.Context(), "idle", []byte("idle")))
			}
			dm, err := client.NewDMap(name)
			require.NoError(t, err)
			value := bytes.Repeat([]byte{42}, 8<<10)
			for i := range 900 {
				err := dm.Put(t.Context(), fmt.Sprint(i), value, olric.EX(time.Hour))
				if err != nil {
					require.True(t, errors.Is(err, olricstore.ErrCapacity) || errors.Is(err, olric.ErrWriteQuorum) || err.Error() == olricstore.ErrCapacity.Error(), "unexpected write error: %v", err)
				}
				for _, node := range []*mixedNode{a, b} {
					s := node.pool.Snapshot()
					require.Equal(t, uint64(cap), s.Capacity)
					require.LessOrEqual(t, s.Allocated, s.Capacity)
				}
			}
			hits := 0
			for i := range 900 {
				r, err := dm.Get(t.Context(), fmt.Sprint(i))
				if err != nil {
					require.ErrorIs(t, err, olric.ErrKeyNotFound)
					continue
				}
				v, err := r.Byte()
				require.NoError(t, err)
				require.Equal(t, value, v)
				hits++
			}
			retained = append(retained, hits)
			clients := mixedReplicaClients(t, a, b)
			keys := samePartitionKeys(name, 2)
			deadline := time.Now().Add(time.Hour).UnixMilli()
			mixedConfirmReplica(t, dm, name, keys[0], value, deadline, clients)
			require.ErrorIs(t, dm.Put(t.Context(), keys[0], value, olric.NX()), olric.ErrKeyFound)
			require.NoError(t, dm.Put(t.Context(), keys[0], value, olric.XX(), olric.PXAT(time.Duration(deadline)*time.Millisecond)))
			_, err = dm.Delete(t.Context(), keys[0])
			require.NoError(t, err)
			_, err = dm.Get(t.Context(), keys[0])
			require.ErrorIs(t, err, olric.ErrKeyNotFound)
			mixedConfirmReplica(t, dm, name, keys[0], value, deadline, clients)
			if divisor == 1 {
				require.Positive(t, a.pool.Snapshot().PressureEvictions+b.pool.Snapshot().PressureEvictions)
			}
			// Fill remaining blocks in foreign engines before testing an empty fragment.
			for _, node := range []*mixedNode{a, b} {
				filler := olricstore.NewEngine(node.pool)
				record := olricstore.NewEntry()
				record.SetKey("filler")
				record.SetValue([]byte{1})
				raw := record.Encode()
				full := false
				for h := uint64(0); h < 100000; h++ {
					before := node.pool.Snapshot().PressureEvictions
					err := filler.PutRaw(h, raw)
					if errors.Is(err, olricstore.ErrCapacity) || node.pool.Snapshot().PressureEvictions > before {
						full = true
						break
					}
					require.NoError(t, err)
				}
				require.True(t, full, "fixture must reach local backing pressure")
			}
			empty, err := client.NewDMap("late-empty-fragment")
			require.NoError(t, err)
			pressure := a.pool.Snapshot().PressureEvictions + b.pool.Snapshot().PressureEvictions
			rejected := a.pool.Snapshot().PutRejected + a.pool.Snapshot().RawRejected + b.pool.Snapshot().PutRejected + b.pool.Snapshot().RawRejected
			err = empty.Put(t.Context(), "large", make([]byte, 900<<10))
			require.Error(t, err)
			require.Equal(t, pressure, a.pool.Snapshot().PressureEvictions+b.pool.Snapshot().PressureEvictions, "empty fragments must not reclaim foreign records")
			require.Greater(t, a.pool.Snapshot().PutRejected+a.pool.Snapshot().RawRejected+b.pool.Snapshot().PutRejected+b.pool.Snapshot().RawRejected, rejected)

		})
	}
	require.Len(t, retained, 2)
	require.Greater(t, retained[1], retained[0], "full-budget thresholds must retain more than the quarter share")
}
