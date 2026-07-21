package cosmoguard

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
