// Package boundedcall limits caller waiting and outstanding backend operations.
package boundedcall

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

var ErrRejected = errors.New("backend operation capacity exhausted")
var ErrTimeout = errors.New("backend operation wait timed out")

func IsFailure(err error) bool {
	return errors.Is(err, ErrTimeout) || errors.Is(err, ErrRejected)
}

type Gate struct {
	slots   chan struct{}
	budget  time.Duration
	observe func(string)
	wait    bool
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
	if gate == nil {
		return fn(ctx)
	}
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	expired := func(waitCtx context.Context) (T, error) {
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			if gate.observe != nil {
				gate.observe("timeout")
			}
			return zero, fmt.Errorf("%w: %w", ErrTimeout, waitCtx.Err())
		}
		return zero, waitCtx.Err()
	}
	if gate.wait {
		admissionCtx, cancel := context.WithTimeout(ctx, gate.budget)
		select {
		case gate.slots <- struct{}{}:
		case <-admissionCtx.Done():
			cancel()
			return expired(admissionCtx)
		}
		if admissionCtx.Err() != nil {
			<-gate.slots
			cancel()
			return expired(admissionCtx)
		}
		cancel()
	} else {
		select {
		case gate.slots <- struct{}{}:
		default:
			if gate.observe != nil {
				gate.observe("rejected")
			}
			return zero, ErrRejected
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, gate.budget)
	defer cancel()
	if waitCtx.Err() != nil {
		<-gate.slots
		return expired(waitCtx)
	}

	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		var res result
		defer func() {
			if v := recover(); v != nil {
				res.err = fmt.Errorf("backend operation panicked: %v", v)
				slog.Error("backend operation panicked", "error", res.err, "stack", string(debug.Stack()))
			}
			<-gate.slots
			done <- res
		}()
		res.value, res.err = fn(waitCtx)
	}()
	select {
	case <-waitCtx.Done():
		return expired(waitCtx)
	case res := <-done:
		if waitCtx.Err() != nil {
			return expired(waitCtx)
		}
		return res.value, res.err
	}
}
