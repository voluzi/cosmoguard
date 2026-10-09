package cosmoguard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestProductionResponseL2UsesTotalBudget(t *testing.T) {
	for _, evm := range []bool{false, true} {
		for _, automatic := range []bool{false, true} {
			for _, replicas := range []int{1, 2} {
				t.Run(fmt.Sprintf("evm=%v/auto=%v/rf=%d", evm, automatic, replicas), func(t *testing.T) {
					withMemoryLimit(t, 64<<20, true)
					cfg := startupHealthConfig(t)
					if replicas == 1 {
						cfg.Cache.Cluster = nil
					} else {
						cfg.Cache.Cluster.Quorum = 1
					}
					disabled := false
					cfg.Metrics.Enable = &disabled
					cfg.EnableEvm = evm
					ports := reserveLoopbackPorts(t, 2)
					cfg.EvmRpcPort, cfg.EvmRpcWsPort = ports[0], ports[1]
					if !automatic {
						cfg.Cache.Memory.DistributedMaxBytesPerNode = int64p(32 << 20)
					}
					budget := cfg.Cache.ResolveBudget()
					cg, err := New(cfg)
					require.NoError(t, err)
					defer cg.Shutdown(context.Background())
					dm, err := cg.cluster.Client().NewDMap(cfg.Cache.Key + "lcd")
					require.NoError(t, err)
					writes, minRetained := 700, 550
					if !evm && !automatic && replicas == 1 {
						writes, minRetained = 1600, 1100
					}
					for i := range writes {
						require.NoError(t, dm.Put(t.Context(), fmt.Sprint(i), make([]byte, 8<<10)))
					}
					retained := 0
					for i := range writes {
						if _, err := dm.Get(t.Context(), fmt.Sprint(i)); err == nil {
							retained++
						}
					}
					require.Greater(t, retained, minRetained, "one protocol must grow beyond the old quarter/eighth share")
					stats := cg.cluster.responsePool.Snapshot()
					require.Equal(t, budget.L2MaxBytesPerNode, stats.Capacity)
					require.LessOrEqual(t, stats.Allocated, stats.Capacity)
				})
			}
		}
	}
}

func TestProductionResponseL1SharesAllProtocols(t *testing.T) {
	for _, evm := range []bool{false, true} {
		t.Run(fmt.Sprint(evm), func(t *testing.T) {
			cfg := startupHealthConfig(t)
			cfg.Cache.Cluster = nil
			disabled := false
			cfg.Metrics.Enable = &disabled
			cfg.EnableEvm = evm
			ports := reserveLoopbackPorts(t, 2)
			cfg.EvmRpcPort, cfg.EvmRpcWsPort = ports[0], ports[1]
			cfg.Cache.Memory.MaxBytes = int64p(1 << 20)
			cfg.Cache.Memory.MaxItems = int64p(16)
			cfg.Cache.Memory.DistributedMaxBytesPerNode = int64p(8 << 20)
			cg, err := New(cfg)
			require.NoError(t, err)
			defer cg.Shutdown(context.Background())
			// Make L2 misses fail, so every successful read proves local retention.
			cg.cluster.responseOperations.CloseOperations()
			type consumer struct {
				set func(int)
				get func(int) error
			}
			consumers := []consumer{}
			addHTTP := func(p *HttpProxy) {
				consumers = append(consumers, consumer{
					set: func(i int) {
						_ = p.cache.Set(t.Context(), fmt.Sprint(i), CachedResponse{Data: make([]byte, 32<<10)}, time.Hour)
					},
					get: func(i int) error { _, err := p.cache.Get(t.Context(), fmt.Sprint(i)); return err },
				})
			}
			addJSON := func(h *JsonRpcHandler) {
				consumers = append(consumers, consumer{
					set: func(i int) { _ = h.cache.Set(t.Context(), uint64(i), &JsonRpcMsg{}, time.Hour) },
					get: func(i int) error { _, err := h.cache.Get(t.Context(), uint64(i)); return err },
				})
			}
			addHTTP(cg.lcdProxy)
			addHTTP(cg.rpcProxy)
			addJSON(cg.jsonRpcHandler)
			consumers = append(consumers, consumer{
				set: func(i int) {
					_ = cg.grpcProxy.grpcCache.Set(t.Context(), fmt.Sprint(i), grpcCachedResponse{}, time.Hour)
				},
				get: func(i int) error { _, err := cg.grpcProxy.grpcCache.Get(t.Context(), fmt.Sprint(i)); return err },
			})
			if evm {
				addHTTP(cg.evmRpcProxy)
				addHTTP(cg.evmRpcWsProxy)
				addJSON(cg.evmJsonRpcHandler)
				addJSON(cg.evmJsonRpcWsHandler)
			}
			for _, c := range consumers {
				for i := range 12 {
					c.set(i)
				}
				for i := range 12 {
					require.NoError(t, c.get(i), "each production adapter can exceed the old share")
				}
			}
			// A shared count cap leaves only the last 16 entries, across all owners.
			hits := 0
			for _, c := range consumers {
				for i := range 12 {
					if c.get(i) == nil {
						hits++
					}
				}
			}
			require.Equal(t, 16, hits)
			require.NoError(t, cg.cluster.memoryPool.Close())
			for _, c := range consumers {
				require.Error(t, c.get(11))
			}
		})
	}
}

func TestSharedMemoryOwnerConstructorFailureDoesNotLeak(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	ports := reserveLoopbackPorts(t, 2)
	cr, err := newClusterRuntime(clusterRuntimeOptions{L1MaxBytes: 4096, Cluster: &ClusterConfig{
		BindAddr: "127.0.0.1", BindPort: ports[0], GossipPort: ports[1], ReplicaCount: 2, Quorum: 3, EncryptionKey: testClusterEncryptionKey,
	}})
	require.Error(t, err)
	require.Nil(t, cr)
	require.Eventually(t, func() bool { return goleak.Find(baseline) == nil }, 3*time.Second, 10*time.Millisecond, "failed runtime must join its shared L1 cleaner")
}
