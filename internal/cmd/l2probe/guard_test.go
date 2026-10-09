package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGuardRejectsUnsupportedRate(t *testing.T) {
	err := guardRun(t.Context(), "missing-targets", time.Second, 1, 1024, 1000000001)
	if err == nil || err.Error() != "invalid guard workload" {
		t.Fatalf("unsupported rate accepted: %v", err)
	}
}
func TestGuardRejectsInvalidTargetSizeBeforeRequests(t *testing.T) {
	t.Setenv("PROBE_JWT_SECRET", "local-fixture-secret")
	for _, size := range []int{-1, 2097153} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			file := filepath.Join(t.TempDir(), "targets.json")
			if err := os.WriteFile(file, []byte(fmt.Sprintf(`[{"protocol":"http","address":"127.0.0.1:1","size":%d}]`, size)), 0600); err != nil {
				t.Fatal(err)
			}
			err := guardRun(t.Context(), file, time.Second, 1, 1024, 1)
			if err == nil || !strings.Contains(err.Error(), "target size") {
				t.Fatalf("invalid target size accepted: %v", err)
			}
		})
	}
}
