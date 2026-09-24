// Command cosmoguard-compat checks that cosmoguard answers like the node
// behind it. It discovers the node's query endpoints (gRPC reflection,
// the LCD routes annotated on them, CometBFT RPC, EVM JSON-RPC and
// WebSocket subscriptions), calls each one on the node and through
// cosmoguard at one pinned height, and reports every difference.
//
// It is a local test tool and is not shipped in releases:
//
//	make compat CHAIN=nibiru
//	go run ./cmd/cosmoguard-compat --chain allora --guard-lcd http://... (etc.)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/voluzi/cosmoguard/internal/compat"
)

// presets are public endpoints of chains cosmoguard is run in front of.
var presets = map[string]compat.Endpoints{
	"nibiru": {
		LCD:  "https://lcd.nibiru.fi",
		RPC:  "https://rpc.nibiru.fi",
		GRPC: "https://grpc.nibiru.fi",
		EVM:  "https://evm-rpc.nibiru.fi",
		// evm-rpc-ws.nibiru.fi refuses eth_subscribe without
		// credentials, so EVM subscriptions need --node-evm-ws.
	},
	"allora": {
		LCD:  "https://allora-api.mainnet.allora.network",
		RPC:  "https://allora-rpc.mainnet.allora.network",
		GRPC: "https://allora-grpc.mainnet.allora.network",
	},
}

func main() {
	os.Exit(run())
}

func run() int {
	var (
		chain       = flag.String("chain", "nibiru", "node preset: "+strings.Join(presetNames(), ", ")+" (ignored when any --node-* URL is set)")
		spawnBin    = flag.String("spawn", "", "cosmoguard binary to start in front of the node (instead of guard-* flags)")
		report      = flag.String("report", "", "write the full JSON report, including skip reasons, to this file")
		only        = flag.String("only", "", "comma-separated protocols to run: grpc,lcd,rpc,evm,ws (default all)")
		height      = flag.Int64("height", 0, "height to pin queries to (default: node latest - 5)")
		concurrency = flag.Int("concurrency", 4, "comparisons in flight at once")
		timeout     = flag.Duration("timeout", 20*time.Second, "timeout per request")
		roundDelay  = flag.Duration("round-delay", 0, "pause between comparison rounds of a differing endpoint; set above cosmoguard's cache TTL (default with --spawn: 3s)")
		node        compat.Endpoints
		guard       compat.Endpoints
		params      = compat.Params{}
	)
	flag.Func("param", "request field value, `name=value`, for chain-specific fields discovery cannot fill (repeatable)", func(s string) error {
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" {
			return fmt.Errorf("want name=value, got %q", s)
		}
		params[k] = v
		return nil
	})
	endpointFlags(&node, "node", "the node")
	endpointFlags(&guard, "guard", "cosmoguard")
	flag.Parse()

	if node == (compat.Endpoints{}) {
		p, ok := presets[*chain]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown chain %q and no --node-* URLs\n", *chain)
			return 2
		}
		node = p
	}
	if node.LCD == "" || node.RPC == "" || node.GRPC == "" {
		fmt.Fprintln(os.Stderr, "need --node-lcd, --node-rpc and --node-grpc (or a --chain preset)")
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var spawned *spawned
	if *spawnBin != "" {
		s, g, err := spawn(ctx, *spawnBin, *chain, node)
		if err != nil {
			fmt.Fprintf(os.Stderr, "starting cosmoguard: %v\n", err)
			return 2
		}
		spawned, guard = s, g
		if *roundDelay == 0 {
			*roundDelay = spawnRoundDelay
		}
	}
	code := compare(ctx, *chain, node, guard, *height, *concurrency, *timeout, *roundDelay, *only, *report, params, spawned != nil)
	if spawned != nil {
		spawned.stop()
		if log := spawned.cleanup(code != 0); log != "" {
			fmt.Fprintf(os.Stderr, "cosmoguard log: %s\n", log)
		}
	}
	return code
}

// compare runs the comparison and returns the exit code: 1 when cosmoguard
// answered differently, refused a request under the allow-all spawn config,
// or nothing could be compared.
func compare(ctx context.Context, chain string, node, guard compat.Endpoints, height int64, concurrency int,
	timeout, roundDelay time.Duration, only, report string, params compat.Params, spawned bool) int {
	if guard.LCD == "" || guard.RPC == "" || guard.GRPC == "" {
		fmt.Fprintln(os.Stderr, "need --spawn or --guard-lcd, --guard-rpc and --guard-grpc")
		return 2
	}
	if guard.EVM == "" {
		node.EVM, node.EVMWS = "", ""
	}

	protocols := map[string]bool{}
	for _, p := range strings.Split(only, ",") {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		switch p {
		case compat.ProtoGRPC, compat.ProtoLCD, compat.ProtoRPC, compat.ProtoEVM, compat.ProtoWS:
			protocols[p] = true
		default:
			fmt.Fprintf(os.Stderr, "unknown protocol %q in --only\n", p)
			return 2
		}
	}

	rep, err := compat.Run(ctx, compat.Options{
		Chain:       chain,
		Node:        node,
		Guard:       guard,
		Height:      height,
		Concurrency: concurrency,
		Timeout:     timeout,
		Protocols:   protocols,
		Params:      params,
		RoundDelay:  roundDelay,
		Log:         os.Stderr,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "compat: %v\n", err)
		return 2
	}
	fmt.Println()
	rep.WriteSummary(os.Stdout)
	if report != "" {
		if err := rep.WriteJSON(report); err != nil {
			fmt.Fprintf(os.Stderr, "writing report: %v\n", err)
			return 2
		}
		fmt.Printf("\nfull report: %s\n", report)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return 130
	}
	switch {
	case rep.HasDifferences():
		return 1
	case spawned && rep.Count(compat.Denied) > 0:
		// The spawned config allows everything; a refusal is a defect.
		return 1
	case rep.Count(compat.Identical) == 0:
		fmt.Fprintln(os.Stderr, "nothing was compared")
		return 1
	}
	return 0
}

func endpointFlags(e *compat.Endpoints, prefix, who string) {
	flag.StringVar(&e.LCD, prefix+"-lcd", "", "LCD base URL of "+who)
	flag.StringVar(&e.RPC, prefix+"-rpc", "", "CometBFT RPC base URL of "+who)
	flag.StringVar(&e.GRPC, prefix+"-grpc", "", "gRPC target of "+who+" (https://host[:port] for TLS, http://host:port plaintext)")
	flag.StringVar(&e.EVM, prefix+"-evm", "", "EVM JSON-RPC URL of "+who+" (optional)")
	flag.StringVar(&e.EVMWS, prefix+"-evm-ws", "", "EVM WebSocket URL of "+who+" (optional)")
}

func presetNames() []string {
	names := make([]string, 0, len(presets))
	for n := range presets {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
