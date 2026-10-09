package main

import (
	"encoding/json"
	"flag"
	"io"
	"math"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestProbeSnapshotRates(t *testing.T) {
	if os.Getenv("L2PROBE_SNAPSHOT_CHILD") == "1" {
		flag.CommandLine = flag.NewFlagSet("l2probe", flag.ExitOnError)
		os.Args = []string{"l2probe", "-duration=1ms", "-idle=1ms", "-ttl=1h", "-l1=false"}
		if err := run(); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProbeSnapshotRates$")
	cmd.Env = append(os.Environ(), "L2PROBE_SNAPSHOT_CHILD=1")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("probe failed: %v", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(out)))
	var stages []string
	for {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		stage, ok := row["stage"].(string)
		if !ok {
			continue
		}
		stages = append(stages, stage)
		for _, name := range []string{"gc_rate", "cpu_millicores"} {
			value, present := row[name]
			if stage == "empty" {
				if present {
					t.Errorf("initial snapshot reports %s=%v without a baseline", name, value)
				}
				continue
			}
			rate, ok := value.(float64)
			if !present || !ok || rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
				t.Errorf("%s snapshot has invalid %s=%v", stage, name, value)
			}
		}
	}
	if want := []string{"empty", "sparse", "after_writes", "idle"}; !slices.Equal(stages, want) {
		t.Fatalf("got snapshot stages %v, want %v", stages, want)
	}
}
