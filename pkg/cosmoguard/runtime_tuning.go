package cosmoguard

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"go.uber.org/automaxprocs/maxprocs"
)

// goMemLimitRatio is the fraction of the detected memory limit set as
// GOMEMLIMIT. Below this soft ceiling the Go GC runs normally; as the heap
// approaches it the GC works progressively harder, trading CPU to avoid the
// hard cgroup limit (and the OOMKill that follows). 0.90 leaves ~10% for
// non-heap RSS (goroutine stacks, mmap'd runtime structures) that GOMEMLIMIT
// does not govern.
const goMemLimitRatio = 0.90

// memoryLimitProvider returns the container/cgroup memory limit in bytes and
// whether a finite limit was found. It is a package var so tests can inject a
// deterministic limit instead of reading the host's cgroup. Production reads
// the cgroup (v1/v2) via automemlimit.
var memoryLimitProvider = detectMemoryLimit

// detectMemoryLimit reads the cgroup memory limit. Returns (0, false) when no
// finite limit is set (unconstrained host, non-Linux, or cgroup read error) —
// callers fall back to a fixed default budget in that case.
func detectMemoryLimit() (uint64, bool) {
	limit, err := memlimit.FromCgroup()
	if err != nil {
		// ErrNoLimit (unconstrained) and unsupported-platform errors are
		// expected off Kubernetes; treat them all as "no limit detected".
		if !errors.Is(err, memlimit.ErrNoLimit) {
			slog.Debug("no cgroup memory limit detected; using fixed cache budget", "error", err)
		}
		return 0, false
	}
	if limit == 0 {
		return 0, false
	}
	return limit, true
}

// SetupRuntimeTuning aligns the Go runtime with the container's resource
// limits: GOMAXPROCS to the CPU quota (avoids scheduler thrash when the pod is
// capped below the host core count) and GOMEMLIMIT to goMemLimitRatio of the
// memory limit (the primary defence against cache-driven OOMKills). Both are
// no-ops when the respective limit is unset, and neither is fatal — a tuning
// failure must never stop the proxy from starting.
func SetupRuntimeTuning(logger *slog.Logger) {
	if _, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...interface{}) {
		logger.Info(fmt.Sprintf(format, args...), "component", "runtime-tuning")
	})); err != nil {
		logger.Warn("failed to set GOMAXPROCS from cpu quota", "error", err)
	}

	if _, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(goMemLimitRatio),
		memlimit.WithProvider(memlimit.FromCgroup),
		memlimit.WithLogger(logger),
	); err != nil && !errors.Is(err, memlimit.ErrNoLimit) {
		logger.Warn("failed to set GOMEMLIMIT from memory limit", "error", err)
	}
}
