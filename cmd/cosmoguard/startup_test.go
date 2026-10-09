package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestStartupSignalDoesNotHideOtherFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		err      error
		graceful bool
	}{{fmt.Errorf("bootstrap: %w", context.Canceled), true}, {errors.New("bad config"), false}, {context.DeadlineExceeded, false}} {
		if got := startupCanceledBySignal(ctx, tc.err); got != tc.graceful {
			t.Fatalf("error %v: graceful=%t want %t", tc.err, got, tc.graceful)
		}
	}
	if startupCanceledBySignal(context.Background(), context.Canceled) {
		t.Fatal("cancellation without a signal was hidden")
	}
}
