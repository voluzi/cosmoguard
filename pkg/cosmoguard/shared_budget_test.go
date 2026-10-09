package cosmoguard

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProductionResponseL2UsesTotalBudget(t *testing.T) {
	for _, evm := range []bool{false, true} {
		for _, automatic := range []bool{false, true} {
			t.Run(fmt.Sprintf("evm=%v/auto=%v", evm, automatic), func(t *testing.T) {
				withMemoryLimit(t, 64<<20, true)
				cfg := startupHealthConfig(t)
				cfg.Cache.Cluster = nil
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
				for i := range 700 {
					require.NoError(t, dm.Put(t.Context(), fmt.Sprint(i), make([]byte, 8<<10)))
				}
				retained := 0
				for i := range 700 {
					if _, err := dm.Get(t.Context(), fmt.Sprint(i)); err == nil {
						retained++
					}
				}
				require.Greater(t, retained, 550, "one protocol must grow beyond the old quarter/eighth share")
				stats := cg.cluster.responsePool.Snapshot()
				require.Equal(t, budget.L2MaxBytesPerNode, stats.Capacity)
				require.LessOrEqual(t, stats.Allocated, stats.Capacity)
			})
		}
	}
}
