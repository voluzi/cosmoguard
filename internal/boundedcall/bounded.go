// Package boundedcall limits caller waiting and outstanding backend operations.
package boundedcall

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/voluzi/cosmoguard/v6/internal/bytebudget"
)

var ErrRejected = errors.New("backend operation capacity exhausted")
var ErrTimeout = errors.New("backend operation wait timed out")

func IsFailure(err error) bool {
	return errors.Is(err, ErrTimeout) || errors.Is(err, ErrRejected) || errors.Is(err, ErrUnavailable)
}

type Gate struct {
	closed  atomic.Bool
	slots   chan struct{}
	budget  time.Duration
	observe func(string)
	wait    bool
	outage  *outage
}

func New(capacity int, budget time.Duration, observe func(string)) *Gate {
	return &Gate{slots: make(chan struct{}, capacity), budget: budget, observe: observe}
}

// NewWaiting bounds admission separately so late arrivals retain the full
// operation budget. Security decisions must not fail open just from a burst.
func NewWaiting(capacity int, budget time.Duration, observe func(string)) *Gate {
	gate := New(capacity, budget, observe)
	gate.wait = true
	return gate
}

// Do leaves a slot occupied until fn actually returns, even if it ignores
// cancellation. A nil gate preserves synchronous, unbounded local calls.
func Do[T any](ctx context.Context, gate *Gate, fn func(context.Context) (T, error)) (T, error) {
	return DoWeighted(ctx, gate, nil, 0, func(ctx context.Context, _ *bytebudget.Lease) (T, error) { return fn(ctx) })
}

func (g *Gate) Close() {
	if g != nil {
		g.closed.Store(true)
		g.outage.close()
	}
}

// DoWeighted transfers both reservations to the worker. Caller cancellation
// discards its result but cannot release a worker still using temporary bytes.
func DoWeighted[T any](ctx context.Context, gate *Gate, bytes *bytebudget.Budget, charge uint64, fn func(context.Context, *bytebudget.Lease) (T, error)) (T, error) {
	if gate == nil {
		return fn(ctx, nil)
	}
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	op, admitted := gate.outage.admit()
	unavailable := func() (T, error) {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if gate.observe != nil {
			gate.observe("unavailable")
		}
		return zero, ErrUnavailable
	}
	if !admitted {
		return unavailable()
	}
	abort := func() {
		gate.outage.resolve(op, false, false, false)
	}
	expired := func(waitCtx context.Context, executed bool) (T, error) {
		if err := ctx.Err(); err != nil {
			gate.outage.resolve(op, false, false, false)
			return zero, err
		}
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			gate.outage.resolve(op, true, false, executed)
			if gate.observe != nil {
				gate.observe("timeout")
			}
			return zero, fmt.Errorf("%w: %w", ErrTimeout, waitCtx.Err())
		}
		return zero, waitCtx.Err()
	}
	rejected := func() (T, error) {
		abort()
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if gate.observe != nil {
			gate.observe("rejected")
		}
		return zero, ErrRejected
	}
	if gate.closed.Load() {
		return rejected()
	}
	if gate.wait {
		admissionCtx, cancel := context.WithTimeout(ctx, gate.budget)
		select {
		case gate.slots <- struct{}{}:
		case <-admissionCtx.Done():
			cancel()
			return expired(admissionCtx, false)
		}
		if admissionCtx.Err() != nil {
			<-gate.slots
			cancel()
			return expired(admissionCtx, false)
		}
		cancel()
	} else {
		select {
		case gate.slots <- struct{}{}:
		default:
			return rejected()
		}
	}
	if gate.closed.Load() {
		<-gate.slots
		return rejected()
	}
	var lease *bytebudget.Lease
	if bytes != nil {
		var ok bool
		lease, ok = bytes.TryAcquire(charge)
		if !ok {
			<-gate.slots
			return rejected()
		}
	}
	var current bool
	op, current = gate.outage.current(op)
	if !current {
		<-gate.slots
		lease.Release()
		abort()
		return unavailable()
	}
	if gate.closed.Load() {
		<-gate.slots
		lease.Release()
		return rejected()
	}
	waitCtx, cancel := context.WithTimeout(ctx, gate.budget)
	defer cancel()
	if waitCtx.Err() != nil {
		<-gate.slots
		lease.Release()
		abort()
		return expired(waitCtx, false)
	}

	done := make(chan callResult[T])
	workerDone := make(chan struct{})
	go runWorker(waitCtx, gate.slots, lease, fn, done, workerDone)
	select {
	case <-waitCtx.Done():
		return expired(waitCtx, true)
	case res := <-done:
		if ctx.Err() != nil || waitCtx.Err() != nil {
			value, err := expired(waitCtx, true)
			<-workerDone
			return value, err
		}
		timedOut := errors.Is(res.err, ErrTimeout)
		gate.outage.resolve(op, timedOut, !timedOut, true)
		if timedOut && gate.observe != nil {
			gate.observe("timeout")
		}
		<-workerDone
		return res.value, res.err
	}
}

type callResult[T any] struct {
	value T
	err   error
}

func runWorker[T any](ctx context.Context, slots chan struct{}, lease *bytebudget.Lease, fn func(context.Context, *bytebudget.Lease) (T, error), done chan<- callResult[T], workerDone chan<- struct{}) {
	defer close(workerDone)
	var res callResult[T]
	defer func() {
		if v := recover(); v != nil {
			res.err = fmt.Errorf("backend operation panicked: %v", v)
			slog.Error("backend operation panicked", "error", res.err, "stack", string(debug.Stack()))
		}
		select {
		case done <- res:
		case <-ctx.Done():
		}
		res = callResult[T]{}
		lease.Release()
		<-slots
	}()
	res.value, res.err = fn(ctx, lease)
}
