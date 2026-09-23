package cosmoguard

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCoalescerPreservesAbortHandler(t *testing.T) {
	var c coalescer[int]
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	panics := make(chan any, 2)
	call := func(ctx context.Context) {
		defer func() { panics <- recover() }()
		_, _ = c.do(ctx, "key", func() (int, error) {
			calls.Add(1)
			close(started)
			<-release
			panic(http.ErrAbortHandler)
		})
	}
	go call(t.Context())
	<-started
	observed := make(chan struct{})
	go call(&observedDoneContext{Context: t.Context(), observed: observed})
	<-observed
	canceledCtx, cancel := context.WithCancel(t.Context())
	canceledObserved := make(chan struct{})
	canceledResult := make(chan error, 1)
	go func() {
		_, err := c.do(&observedDoneContext{Context: canceledCtx, observed: canceledObserved}, "key", func() (int, error) {
			return 0, nil
		})
		canceledResult <- err
	}()
	<-canceledObserved
	cancel()
	require.ErrorIs(t, <-canceledResult, context.Canceled)
	close(release)
	for i := 0; i < 2; i++ {
		got := <-panics
		require.True(t, got == http.ErrAbortHandler, "got panic %T: %v", got, got)
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestCoalescerRefreshAbortHandler(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	var c coalescer[int]
	started := make(chan struct{})
	c.refresh("key", func() (int, error) {
		close(started)
		panic(http.ErrAbortHandler)
	})
	<-started
	require.Eventually(t, func() bool {
		_, active := c.inflight.Load("key")
		return !active
	}, time.Second, time.Millisecond)
	require.Empty(t, logs.String())
	second := make(chan struct{})
	c.refresh("key", func() (int, error) { close(second); return 1, nil })
	<-second
	require.Eventually(t, func() bool {
		_, active := c.inflight.Load("key")
		return !active
	}, time.Second, time.Millisecond)
}

func TestCoalescerWorkerPanicIsolation(t *testing.T) {
	if mode := os.Getenv("COSMOGUARD_COALESCER_PANIC_CHILD"); mode != "" {
		var c coalescer[int]
		switch mode {
		case "foreground":
			recovered := false
			func() {
				defer func() { recovered = recover() != nil }()
				_, _ = c.do(context.Background(), "key", func() (int, error) {
					panic("foreground panic")
				})
			}()
			require.True(t, recovered)
		case "refresh":
			c.refresh("key", func() (int, error) {
				panic("refresh panic")
			})
			deadline := time.Now().Add(time.Second)
			for {
				if _, active := c.inflight.Load("key"); !active {
					return
				}
				if time.Now().After(deadline) {
					t.Fatal("panicked refresh remained in flight")
				}
				runtime.Gosched()
			}
		default:
			t.Fatalf("unknown child mode %q", mode)
		}
		return
	}

	for _, mode := range []string{"foreground", "refresh"} {
		t.Run(mode, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestCoalescerWorkerPanicIsolation$")
			cmd.Env = append(os.Environ(), "COSMOGUARD_COALESCER_PANIC_CHILD="+mode)
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}
