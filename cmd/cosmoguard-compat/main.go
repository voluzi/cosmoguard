// Command cosmoguard-compat checks that cosmoguard answers like the node
// behind it. It discovers the node's query endpoints (gRPC reflection,
// the LCD routes annotated on them, CometBFT RPC, EVM JSON-RPC and
// WebSocket subscriptions), calls each one on the node and through
// cosmoguard at one pinned height, and reports every difference.
//
// It is a local test tool and is not shipped in releases:
//
//	make compat        # a node port-forwarded to localhost's standard ports
//	go run ./cmd/cosmoguard-compat --node-lcd https://... --guard-lcd http://... (etc.)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/voluzi/cosmoguard/internal/compat"
)

// defaultNode is a raw node reached through a port-forward on its standard
// ports, e.g. kubectl port-forward svc/<node> 1317 26657 9090 8545 8546.
var defaultNode = compat.Endpoints{
	LCD:   "http://localhost:1317",
	RPC:   "http://localhost:26657",
	GRPC:  "http://localhost:9090",
	EVM:   "http://localhost:8545",
	EVMWS: "ws://localhost:8546",
}

func main() {
	os.Exit(run())
}

func run() int {
	var (
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

	explicit := node
	node, guard = trimSlashes(overlay(defaultNode, node)), trimSlashes(guard)

	protocols := map[string]bool{}
	for _, p := range strings.Split(*only, ",") {
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	enabled := func(p string) bool { return len(protocols) == 0 || protocols[p] }
	if missing := requiredURLs(node, enabled, true); len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "need %s\n", strings.Join(missing, ", "))
		return 2
	}

	node, err := resolveNodeEVM(node, explicit, enabled, reachable)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	if missing := requiredURLs(guard, enabled, false); *spawnBin == "" && len(missing) > 0 {
		fmt.Fprintf(os.Stderr, "need --spawn or %s\n", strings.Join(missing, ", "))
		return 2
	}

	var spawned *spawned
	if *spawnBin != "" {
		s, g, err := spawn(ctx, *spawnBin, node)
		if err != nil {
			fmt.Fprintf(os.Stderr, "starting cosmoguard: %v\n", err)
			return 2
		}
		spawned, guard = s, g
		if *roundDelay == 0 {
			*roundDelay = spawnRoundDelay
		}
	}
	code := compare(ctx, node, guard, *height, *concurrency, *timeout, *roundDelay, protocols, *report, params, spawned != nil)
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
func compare(ctx context.Context, node, guard compat.Endpoints, height int64, concurrency int,
	timeout, roundDelay time.Duration, protocols map[string]bool, report string, params compat.Params, spawned bool) int {
	if guard.EVM == "" {
		node.EVM = ""
	}
	if guard.EVMWS == "" {
		node.EVMWS = ""
	}

	rep, err := compat.Run(ctx, compat.Options{
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
		if errors.Is(ctx.Err(), context.Canceled) {
			return 130 // interrupted during setup
		}
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
	def := func(v string) string {
		if prefix == "node" {
			return " (default " + v + ")"
		}
		return ""
	}
	flag.StringVar(&e.LCD, prefix+"-lcd", "", "LCD base URL of "+who+def(defaultNode.LCD))
	flag.StringVar(&e.RPC, prefix+"-rpc", "", "CometBFT RPC base URL of "+who+def(defaultNode.RPC))
	flag.StringVar(&e.GRPC, prefix+"-grpc", "", "gRPC target of "+who+" (https://host[:port] for TLS, http://host:port plaintext)"+def(defaultNode.GRPC))
	flag.StringVar(&e.EVM, prefix+"-evm", "", "EVM JSON-RPC URL of "+who+def(defaultNode.EVM))
	flag.StringVar(&e.EVMWS, prefix+"-evm-ws", "", "EVM WebSocket URL of "+who+def(defaultNode.EVMWS))
}

// requiredURLs lists the flags a run of the enabled protocols needs. The
// node's LCD and RPC are always needed: they give the pinned height and
// the live request values. Its gRPC is needed for discovery (gRPC and LCD).
func requiredURLs(e compat.Endpoints, enabled func(string) bool, isNode bool) []string {
	prefix := "--guard-"
	if isNode {
		prefix = "--node-"
	}
	var missing []string
	need := func(url, name string, when bool) {
		if when && url == "" {
			missing = append(missing, prefix+name)
		}
	}
	need(e.LCD, "lcd", isNode || enabled(compat.ProtoLCD))
	need(e.RPC, "rpc", isNode || enabled(compat.ProtoRPC) || enabled(compat.ProtoWS))
	need(e.GRPC, "grpc", enabled(compat.ProtoGRPC) || (isNode && enabled(compat.ProtoLCD)))
	return missing
}

// resolveNodeEVM settles the node's EVM URLs. A chain without EVM leaves
// the default EVM ports closed. A port is only probed when a selected
// protocol uses it; the EVM URL also serves newHeads, as the spawned
// cosmoguard proxies EVM WebSocket only with EVM on.
func resolveNodeEVM(node, explicit compat.Endpoints, enabled func(string) bool, reach func(string) error) (compat.Endpoints, error) {
	var err error
	if !enabled(compat.ProtoEVM) && !enabled(compat.ProtoWS) {
		node.EVM = ""
	} else if node.EVM, err = resolveEVM(node.EVM, explicit.EVM != "", reach(node.EVM)); err != nil {
		return node, fmt.Errorf("--node-evm: %w", err)
	}
	if !enabled(compat.ProtoWS) {
		node.EVMWS = ""
	} else if node.EVMWS, err = resolveEVM(node.EVMWS, explicit.EVMWS != "", reach(node.EVMWS)); err != nil {
		return node, fmt.Errorf("--node-evm-ws: %w", err)
	}
	return node, nil
}

// resolveEVM decides whether an EVM endpoint is compared, given whether it
// was set explicitly and whether it could be reached. A default endpoint
// that refuses the connection belongs to a chain without EVM and is
// dropped (""); an explicit one that cannot be reached is an error.
// Other failures are left for the comparison to report.
func resolveEVM(url string, explicit bool, reachErr error) (string, error) {
	switch {
	case reachErr == nil:
		return url, nil
	case explicit:
		return "", fmt.Errorf("%s is unreachable: %w", url, reachErr)
	case errors.Is(reachErr, syscall.ECONNREFUSED):
		return "", nil
	}
	return url, nil
}

// reachable opens and closes a TCP connection to url's host.
func reachable(url string) error {
	u, err := neturl.Parse(url)
	if err != nil {
		return err
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" || u.Scheme == "wss" {
			port = "443"
		}
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(u.Hostname(), port), 5*time.Second)
	if err != nil {
		return err
	}
	return conn.Close()
}

// overlay returns defaults with every URL set in flags replacing its own.
func overlay(defaults, flags compat.Endpoints) compat.Endpoints {
	pick := func(flag, def string) string {
		if flag != "" {
			return flag
		}
		return def
	}
	return compat.Endpoints{
		LCD:   pick(flags.LCD, defaults.LCD),
		RPC:   pick(flags.RPC, defaults.RPC),
		GRPC:  pick(flags.GRPC, defaults.GRPC),
		EVM:   pick(flags.EVM, defaults.EVM),
		EVMWS: pick(flags.EVMWS, defaults.EVMWS),
	}
}

// trimSlashes drops trailing slashes, so base+path never sends "//".
func trimSlashes(e compat.Endpoints) compat.Endpoints {
	t := func(s string) string { return strings.TrimRight(s, "/") }
	return compat.Endpoints{LCD: t(e.LCD), RPC: t(e.RPC), GRPC: t(e.GRPC), EVM: t(e.EVM), EVMWS: t(e.EVMWS)}
}
