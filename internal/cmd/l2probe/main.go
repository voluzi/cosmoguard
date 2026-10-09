// l2probe is a diagnostic engine workload, not a whole-application soak.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"

	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
	"github.com/voluzi/cosmoguard/v6/pkg/cosmoguard"
)

func port() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	p := l.Addr().(*net.TCPAddr).Port
	return p, l.Close()
}
func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() (result error) {
	maps := flag.Int("dmaps", 4, "response namespaces: 4 or 8")
	limit := flag.Uint64("limit-mib", 250, "expected cgroup memory limit; sets GOMEMLIMIT at 90%")
	size := flag.Int("size", 16384, "payload bytes")
	duration := flag.Duration("duration", 30*time.Second, "load duration")
	idle := flag.Duration("idle", time.Minute, "automatic-GC idle observation")
	ttl := flag.Duration("ttl", 10*time.Second, "response TTL")
	writers := flag.Int("writers", 1, "distinct concurrent caller buffers")
	work := flag.Uint64("work-mib", 16, "diagnostic response-work allowance")
	l1 := flag.Bool("l1", true, "populate real MemoryCache L1 alongside L2")
	guardTargets := flag.String("guard-targets", "", "JSON target file for real guard API traffic")
	rps := flag.Int("rps", 0, "guard offered requests/second; 0 is unthrottled")
	profile := flag.String("heap-profile", "", "write an unforced end-of-run heap profile")
	flag.Parse()
	if *guardTargets != "" {
		return guardRun(context.Background(), *guardTargets, *duration, *writers, *size, *rps)
	}
	if *profile != "" {
		defer func() {
			f, err := os.Create(*profile)
			if err != nil {
				result = errors.Join(result, err)
				return
			}
			result = errors.Join(result, pprof.WriteHeapProfile(f), f.Close())
		}()
	}
	if *maps != 4 && *maps != 8 {
		return errors.New("dmaps must be 4 or 8")
	}
	if *ttl <= 0 {
		return errors.New("diagnostic TTL must be positive")
	}
	if *size < 1 || *size > 900<<10 || *writers < 1 {
		return errors.New("invalid diagnostic workload")
	}
	debug.SetMemoryLimit(int64(*limit<<20) * 9 / 10)
	cosmoguard.SetupRuntimeTuning(slog.Default())
	budget := (&cosmoguard.CacheGlobalConfig{}).ResolveBudget()
	per := budget.PerCache(*maps)
	response := olricstore.NewPool(budget.L2MaxBytesPerNode, olricstore.Response, nil)
	security := olricstore.NewPool(0, olricstore.Security, nil)
	defer response.Close(context.Background())
	defer security.Close(context.Background())
	c := config.New("local")
	c.PartitionCount = 271
	c.ReplicaCount = 2
	c.ReadQuorum = 1
	c.WriteQuorum = 1
	c.MemberCountQuorum = 1
	c.BindAddr = "127.0.0.1"
	var err error
	c.BindPort, err = port()
	if err != nil {
		return err
	}
	c.MemberlistConfig.BindAddr = c.BindAddr
	c.MemberlistConfig.BindPort, err = port()
	if err != nil {
		return err
	}
	c.MemberlistConfig.AdvertisePort = c.MemberlistConfig.BindPort
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))
	c.LogOutput = nil
	c.Logger = log.New(io.Discard, "", 0)
	c.LeaveTimeout = 500 * time.Millisecond
	c.DMaps.TriggerCompactionInterval = time.Second
	c.DMaps.Engine = &config.Engine{Implementation: olricstore.NewEngine(response)}
	c.DMaps.EvictionPolicy = config.LRUEviction
	// This probe has one member, so RF2 creates no resident backup copies.
	c.DMaps.MaxInuse = int(per.L2MaxBytesPerNode)
	c.DMaps.MaxKeys = c.DMaps.MaxInuse / 512
	c.DMaps.LRUSamples = 10
	c.DMaps.Custom = map[string]config.DMap{}
	for _, name := range []string{"ratelimit", "ratelimit-locks", "cosmoguard:jti", "observability"} {
		c.DMaps.Custom[name] = config.DMap{Engine: &config.Engine{Implementation: olricstore.NewEngine(security)}, EvictionPolicy: config.EvictionPolicy("NONE")}
	}
	ready := make(chan struct{})
	c.Started = func() { close(ready) }
	if err := c.Sanitize(); err != nil {
		return err
	}
	db, err := olric.New(c)
	if err != nil {
		return err
	}
	defer db.Shutdown(context.Background())
	startErrors := make(chan error, 1)
	go func() { startErrors <- db.Start() }()
	select {
	case <-ready:
	case err := <-startErrors:
		return err
	case <-time.After(30 * time.Second):
		return errors.New("start timeout")
	}
	client := db.NewEmbeddedClient()
	names := []string{"cosmoguard-grpc", "cosmoguard-lcd", "cosmoguard-jsonrpc", "cosmoguard-rpc", "cosmoguard-evm_jsonrpc", "cosmoguard-evm_rpc", "cosmoguard-evm_jsonrpc_ws", "cosmoguard-evm_rpc_ws"}
	operations := cache.RecoveringOperations(128, 100*time.Millisecond, *work<<20, nil, nil, nil)
	defer operations.CloseOperations()
	var caches []cache.Cache[string, []byte]
	for _, name := range names[:*maps] {
		l2, err := cache.NewOlricCache[string, []byte](client, name, operations)
		if err != nil {
			return err
		}
		var cc cache.Cache[string, []byte] = l2
		if *l1 {
			local, err := cache.NewMemoryCache[string, []byte](name, cache.MaxCost(per.L1MaxBytes))
			if err != nil {
				return err
			}
			cc, err = cache.NewTieredCache[string, []byte](local, l2)
			if err != nil {
				return err
			}
		}
		defer cc.Close()
		caches = append(caches, cc)
	}
	var attempts, skips atomic.Uint64
	started := time.Now()
	last := started
	var lastGC uint32
	var lastCPU float64
	snapshot := func(stage string) error {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		now := time.Now()
		seconds := now.Sub(last).Seconds()
		sm := []metrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/limiter/last-enabled:gc-cycle"}}
		metrics.Read(sm)
		var ru syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
			return err
		}
		cpu := float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
		rss := uint64(0)
		if b, err := os.ReadFile("/proc/self/statm"); err == nil {
			fields := strings.Fields(string(b))
			if len(fields) > 1 {
				n, _ := strconv.ParseUint(fields[1], 10, 64)
				rss = n * uint64(os.Getpagesize())
			}
		}
		read := func(name string) string {
			b, _ := os.ReadFile("/sys/fs/cgroup/" + name)
			return strings.TrimSpace(string(b))
		}
		r, n := operations.OperationBytes()
		out := map[string]any{"stage": stage, "elapsed": now.Sub(started).Seconds(), "heap_alloc": m.HeapAlloc, "heap_inuse": m.HeapInuse, "heap_sys": m.HeapSys, "go_managed": m.Sys - m.HeapReleased, "next_gc": m.NextGC, "rss": rss, "gc_rate": float64(m.NumGC-lastGC) / seconds, "gc_cpu": sm[0].Value.Float64(), "gc_limiter_cycle": sm[1].Value.Uint64(), "cpu_millicores": 1000 * (cpu - lastCPU) / seconds, "response": response.Snapshot(), "security": security.Snapshot(), "operation_bytes": r, "operation_capacity": n, "attempts": attempts.Load(), "skips": skips.Load(), "cgroup_current": read("memory.current"), "cgroup_peak": read("memory.peak"), "cgroup_events": read("memory.events"), "cpu_stat": read("cpu.stat"), "cgroup_stat": read("memory.stat")}
		last, lastGC, lastCPU = now, m.NumGC, cpu
		return json.NewEncoder(os.Stdout).Encode(out)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"go": runtime.Version(), "arch": runtime.GOARCH, "budget": budget, "dmaps": *maps, "size": *size, "writers": *writers, "ttl": ttl.String(), "seed": 42}); err != nil {
		return err
	}
	if err := snapshot("empty"); err != nil {
		return err
	}
	for i, name := range names[:*maps] {
		if err := seedResponsePartitions(context.Background(), caches[i], name, *ttl); err != nil {
			return err
		}
	}
	for _, name := range []string{"ratelimit", "ratelimit-locks", "cosmoguard:jti", "observability"} {
		dm, err := client.NewDMap(name)
		if err != nil {
			return err
		}
		if err := dm.Put(context.Background(), "sentinel", []byte("persistent")); err != nil {
			return err
		}
	}
	if err := snapshot("sparse"); err != nil {
		return err
	}
	loadCtx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var wg sync.WaitGroup
	for w := range *writers {
		wg.Go(func() {
			for loadCtx.Err() == nil {
				value := make([]byte, *size)
				for i := range value {
					value[i] = byte(i + w)
				}
				j := attempts.Add(1)
				cc := caches[int(j%uint64(*maps))]
				key := fmt.Sprintf("write-%d", j)
				if err := cc.Set(loadCtx, key, value, *ttl); err != nil {
					skips.Add(1)
				}
				if response.Snapshot().Allocated > budget.L2MaxBytesPerNode {
					panic("response cap exceeded")
				}
			}
		})
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for loadCtx.Err() == nil {
		select {
		case <-loadCtx.Done():
		case <-ticker.C:
			if err := snapshot("load"); err != nil {
				return err
			}
		}
	}
	wg.Wait()
	if err := snapshot("after_writes"); err != nil {
		return err
	}
	end := time.Now().Add(*idle)
	for time.Now().Before(end) {
		time.Sleep(min(5*time.Second, time.Until(end)))
		if err := snapshot("idle"); err != nil {
			return err
		}
	}
	return nil
}

func seedResponsePartitions(ctx context.Context, cc cache.Cache[string, []byte], name string, ttl time.Duration) error {
	seen := map[uint64]bool{}
	for k := 0; len(seen) < 271; k++ {
		key := fmt.Sprintf("seed-%d", k)
		p := xxhash.Sum64String(name+key) % 271
		if seen[p] {
			continue
		}
		if err := cc.Set(ctx, key, []byte("seed"), ttl); err != nil {
			return fmt.Errorf("seed %s partition %d: %w", name, p, err)
		}
		seen[p] = true
	}
	return nil
}
