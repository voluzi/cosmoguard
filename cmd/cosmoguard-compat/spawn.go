package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"text/template"
	"time"

	"github.com/voluzi/cosmoguard/v5/internal/compat"
)

// spawnConfig allows everything, so no rule hides a difference, and caches
// every rule, as a production config would, so cached answers are
// compared too. Subscriptions are never cached. The cache TTL is kept
// short so the rounds of a differing endpoint (spawnRoundDelay apart) each
// reach the node instead of replaying one cached answer.
var spawnConfig = template.Must(template.New("config").Parse(`host: 127.0.0.1
lcdPort: {{.LCD}}
rpcPort: {{.RPC}}
grpcPort: {{.GRPC}}
enableEvm: {{.EVM}}
evmRpcPort: {{.EVMRPC}}
evmRpcWsPort: {{.EVMWS}}
nodes:
  - name: node
    lcdURL: {{.Node.LCD}}
    rpcURL: {{.Node.RPC}}
    grpcURL: {{.Node.GRPC}}
{{- if .EVM}}
    evmRpcURL: {{.Node.EVM}}
    evmRpcWsURL: {{.EVMWSUpstream}}
{{- end}}
metrics:
  enable: true
  port: {{.Metrics}}
dashboard:
  enable: false
cache:
  ttl: {{.CacheTTL}}
lcd:
  default: allow
  rules:
    - action: allow
      match:
        paths: ["/**"]
      cache:
        enable: true
rpc:
  default: allow
  webSocketEnabled: true
  # One upstream subscription is compared; do not hold the default 40
  # sockets open on the node.
  webSocketConnections: 1
  rules:
    - action: allow
      match:
        paths: ["/**"]
      cache:
        enable: true
  jsonrpc:
    default: allow
    rules:
      - priority: 1
        action: allow
        methods: [subscribe, unsubscribe, unsubscribe_all]
      - priority: 2
        action: allow
        methods: ["*"]
        cache:
          enable: true
grpc:
  default: allow
  rules:
    - action: allow
      methods: ["/**"]
      cache:
        enable: true
{{- if .EVM}}
evm:
  rpc:
    default: allow
    rules:
      - action: allow
        methods: ["*"]
        cache:
          enable: true
  ws:
    default: allow
    webSocketConnections: 1
{{- end}}
`))

// withoutOverrides drops COSMOGUARD_* variables, which would override the
// generated config (e.g. COSMOGUARD_NODE_RPC_URL pointing cosmoguard at
// another node than the one compared).
func withoutOverrides(env []string) []string {
	out := env[:0:0]
	for _, kv := range env {
		if !strings.HasPrefix(kv, "COSMOGUARD_") {
			out = append(out, kv)
		}
	}
	return out
}

// writeSpawnConfig renders spawnConfig for node on ports (LCD, RPC, gRPC,
// EVM RPC, EVM WS, metrics).
func writeSpawnConfig(w io.Writer, node compat.Endpoints, ports []int) error {
	data := struct {
		Node                                   compat.Endpoints
		EVM                                    bool
		EVMWSUpstream                          string
		CacheTTL                               time.Duration
		LCD, RPC, GRPC, EVMRPC, EVMWS, Metrics int
	}{node, node.EVM != "", httpScheme(node.EVMWS), spawnCacheTTL, ports[0], ports[1], ports[2], ports[3], ports[4], ports[5]}
	if data.EVMWSUpstream == "" {
		// cosmoguard always proxies EVM WebSocket with EVM on; point it at
		// the node's host so it never falls back to a local default port.
		data.EVMWSUpstream = node.EVM
	}
	return spawnConfig.Execute(w, data)
}

// httpScheme rewrites a ws:// or wss:// URL to http:// or https://.
// cosmoguard takes its WebSocket upstreams as HTTP URLs and switches the
// scheme itself; the tool's own dialer keeps ws(s).
func httpScheme(url string) string {
	switch {
	case strings.HasPrefix(url, "ws://"):
		return "http://" + strings.TrimPrefix(url, "ws://")
	case strings.HasPrefix(url, "wss://"):
		return "https://" + strings.TrimPrefix(url, "wss://")
	}
	return url
}

const (
	spawnCacheTTL   = 2 * time.Second
	spawnRoundDelay = spawnCacheTTL + time.Second
)

type spawned struct {
	cmd     *exec.Cmd
	done    chan struct{} // closed once the process has exited
	dir     string
	logPath string
}

func (s *spawned) exited() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// spawn starts the cosmoguard binary in front of node and returns the
// endpoints it serves once it reports ready.
func spawn(ctx context.Context, bin string, node compat.Endpoints) (*spawned, compat.Endpoints, error) {
	ports, err := freePorts(6)
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	metrics := ports[5]
	dir, err := os.MkdirTemp("", "cosmoguard-compat-")
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	fail := func(err error) (*spawned, compat.Endpoints, error) {
		_ = os.RemoveAll(dir)
		return nil, compat.Endpoints{}, err
	}
	s := &spawned{dir: dir, logPath: filepath.Join(dir, "cosmoguard.log"), done: make(chan struct{})}
	cfgPath := filepath.Join(dir, "cosmoguard.yaml")
	f, err := os.Create(cfgPath)
	if err != nil {
		return fail(err)
	}
	err = writeSpawnConfig(f, node, ports)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fail(err)
	}
	logFile, err := os.Create(s.logPath)
	if err != nil {
		return fail(err)
	}
	s.cmd = exec.Command(bin, "--config", cfgPath, "--log-level", "warn", "--log-format", "text")
	s.cmd.Env = withoutOverrides(os.Environ())
	s.cmd.Stdout, s.cmd.Stderr = logFile, logFile
	if err := s.cmd.Start(); err != nil {
		logFile.Close()
		return fail(err)
	}
	go func() {
		_ = s.cmd.Wait()
		logFile.Close()
		close(s.done)
	}()

	guard := compat.Endpoints{
		LCD:  fmt.Sprintf("http://127.0.0.1:%d", ports[0]),
		RPC:  fmt.Sprintf("http://127.0.0.1:%d", ports[1]),
		GRPC: fmt.Sprintf("http://127.0.0.1:%d", ports[2]),
	}
	if node.EVM != "" {
		guard.EVM = fmt.Sprintf("http://127.0.0.1:%d", ports[3])
		guard.EVMWS = fmt.Sprintf("http://127.0.0.1:%d", ports[4])
	}
	if err := waitReady(ctx, fmt.Sprintf("http://127.0.0.1:%d/readyz", metrics), s); err != nil {
		s.stop()
		logText, _ := os.ReadFile(s.logPath)
		return fail(fmt.Errorf("%w; cosmoguard log:\n%s", err, logText))
	}
	return s, guard, nil
}

func waitReady(ctx context.Context, url string, s *spawned) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	for {
		if s.exited() {
			return fmt.Errorf("cosmoguard exited during startup")
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("cosmoguard not ready after a minute")
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// stop terminates cosmoguard, giving it 10s to drain. Spawn mode is
// written for Unix: elsewhere SIGTERM fails and stop falls back to Kill.
func (s *spawned) stop() {
	if !s.exited() {
		_ = s.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-s.done:
		case <-time.After(10 * time.Second):
			_ = s.cmd.Process.Kill()
			<-s.done
		}
	}
}

// cleanup removes the config and log, unless keepLog asks to keep the log
// for a run that found problems; it returns the kept log's path.
func (s *spawned) cleanup(keepLog bool) string {
	_ = os.Remove(filepath.Join(s.dir, "cosmoguard.yaml"))
	if keepLog {
		return s.logPath
	}
	_ = os.RemoveAll(s.dir)
	return ""
}

func freePorts(n int) ([]int, error) {
	ports := make([]int, 0, n)
	var ls []net.Listener
	defer func() {
		for _, l := range ls {
			l.Close()
		}
	}()
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		ls = append(ls, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}
