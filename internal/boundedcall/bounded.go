// Package boundedcall limits caller waiting and outstanding backend operations.
package boundedcall

import (
	"context"
	"errors"
	"fmt"
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
}

func New(capacity int, budget time.Duration, observe func(string)) *Gate {
	return &Gate{slots: make(chan struct{}, capacity), budget: budget, observe: observe}
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
	select {
	case gate.slots <- struct{}{}:
	default:
		if gate.observe != nil {
			gate.observe("rejected")
		}
		return zero, ErrRejected
	}
	waitCtx, cancel := context.WithTimeout(ctx, gate.budget)
	defer cancel()
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
			}
			<-gate.slots
			done <- res
		}()
		res.value, res.err = fn(waitCtx)
	}()
	expired := func() (T, error) {
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			if gate.observe != nil {
				gate.observe("timeout")
			}
			return zero, fmt.Errorf("%w: %w", ErrTimeout, waitCtx.Err())
		}
		return zero, waitCtx.Err()
	}
	select {
	case <-waitCtx.Done():
		return expired()
	case res := <-done:
		if waitCtx.Err() != nil {
			return expired()
		}
		return res.value, res.err
	}
}
