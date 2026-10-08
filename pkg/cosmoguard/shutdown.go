package cosmoguard

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// The binary cannot read the pod grace period: 5s propagation, up to 19s traffic,
// 2s cleanup, then 3s Olric leave fit 30s with 1s margin. Every phase is capped
// by the same absolute deadline; shorter caller budgets can curtail any phase.
const shutdownTotal = 29 * time.Second
const shutdownPropagation = 5 * time.Second
const shutdownTraffic = 24 * time.Second
const shutdownCleanup = 2 * time.Second
const shutdownLeave = 3 * time.Second

// DrainAndShutdown fails readiness, serves during endpoint propagation, then
// drains listeners concurrently. Shutdown interrupts the propagation hold.
func (f *CosmoGuard) DrainAndShutdown(ctx context.Context) error { return f.shutdown(ctx, true) }

// Shutdown skips the propagation hold, including on failed startup. Cleanup is
// started only once; an uncooperative cleanup retains ownership after the deadline.
// The first call fixes that deadline; later callers may stop waiting earlier.
func (f *CosmoGuard) Shutdown(ctx context.Context) error { return f.shutdown(ctx, false) }

func (f *CosmoGuard) shutdown(ctx context.Context, hold bool) error {
	first := false
	f.shutdownOnce.Do(func() {
		first = true
		f.draining.Store(true)
		f.shutdownDone = make(chan struct{})
		f.shutdownHold = make(chan struct{})
		started := time.Now()
		deadline := started.Add(shutdownTotal)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		go func() {
			defer close(f.shutdownDone)
			totalCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
			defer cancel()
			if hold {
				timer := time.NewTimer(time.Until(minTime(started.Add(shutdownPropagation), deadline)))
				select {
				case <-timer.C:
				case <-f.shutdownHold:
				case <-ctx.Done():
				}
				timer.Stop()
			}
			trafficCtx, stopTraffic := context.WithDeadline(ctx, minTime(started.Add(shutdownTraffic), deadline))
			f.shutdownErr = f.stopListeners(trafficCtx)
			stopTraffic()
			cleanupCtx, stopCleanup := context.WithDeadline(totalCtx, minTime(time.Now().Add(shutdownCleanup), started.Add(shutdownTraffic+shutdownCleanup)))
			f.shutdownErr = errors.Join(f.shutdownErr, f.closeConsumers(cleanupCtx))
			stopCleanup()
			if f.cluster != nil {
				leaveCtx, stopLeave := context.WithDeadline(totalCtx, minTime(time.Now().Add(shutdownLeave), deadline))
				f.shutdownErr = errors.Join(f.shutdownErr, shutdownTasks(leaveCtx, func() error { return f.cluster.Close(leaveCtx) }))
				stopLeave()
			}
		}()
	})
	if !hold {
		f.shutdownHoldOnce.Do(func() { close(f.shutdownHold) })
	}

	if first {
		<-f.shutdownDone
		return f.shutdownErr
	}
	select {
	case <-f.shutdownDone:
		return f.shutdownErr
	default:
	}
	select {
	case <-f.shutdownDone:
		return f.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (f *CosmoGuard) stopListeners(ctx context.Context) error {
	f.constructed.Store(false)
	if f.runDone != nil {
		f.runDoneOnce.Do(func() { close(f.runDone) })
	}
	var tasks []func() error
	if w := f.configWatcher.Swap(nil); w != nil {
		tasks = append(tasks, w.Close)
	}
	if f.discovery != nil {
		tasks = append(tasks, func() error { f.discovery.Stop(); return nil })
	}
	for _, srv := range []*http.Server{f.metricsServer, f.dashboardServer, f.peerApiServer} {
		if srv != nil {
			tasks = append(tasks, func() error {
				err := srv.Shutdown(ctx)
				if err != nil {
					_ = srv.Close()
				}
				return err
			})
		}
	}
	for _, p := range []*HttpProxy{f.lcdProxy, f.rpcProxy, f.evmRpcProxy, f.evmRpcWsProxy} {
		if p != nil {
			tasks = append(tasks, func() error { return p.Shutdown(ctx) })
		}
	}
	if f.grpcProxy != nil {
		tasks = append(tasks, func() error { return f.grpcProxy.Shutdown(ctx) })
	}
	for _, h := range []*JsonRpcHandler{f.jsonRpcHandler, f.evmJsonRpcHandler, f.evmJsonRpcWsHandler} {
		if h != nil && h.wsProxy != nil {
			tasks = append(tasks, func() error { h.wsProxy.drainConnections(ctx); return nil })
		}
	}
	return shutdownTasks(ctx, tasks...)
}

func (f *CosmoGuard) closeConsumers(ctx context.Context) error {
	var tasks []func() error
	for _, h := range []*JsonRpcHandler{f.jsonRpcHandler, f.evmJsonRpcHandler, f.evmJsonRpcWsHandler} {
		if h != nil {
			tasks = append(tasks, h.Shutdown)
		}
	}
	if f.auth != nil {
		tasks = append(tasks, f.auth.Close)
	}
	if f.tracingShutdown != nil {
		tasks = append(tasks, func() error { return f.tracingShutdown(ctx) })
	}
	if f.obsReplicator != nil {
		tasks = append(tasks, func() error { return f.obsReplicator.Close(ctx) })
	}
	return shutdownTasks(ctx, tasks...)
}

// A timed-out cleanup can still return into its buffered result channel; its
// captured resources are not discarded or handed to a duplicate cleanup.
func shutdownTasks(ctx context.Context, tasks ...func() error) error {
	results := make(chan error, len(tasks))
	for _, task := range tasks {
		go func() { results <- task() }()
	}
	var err error
	for range tasks {
		select {
		case result := <-results:
			err = errors.Join(err, result)
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	return err
}
