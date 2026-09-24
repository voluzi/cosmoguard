package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"text/template"
	"time"

	"github.com/voluzi/cosmoguard/internal/compat"
)

// spawnConfig allows everything, so no rule hides a difference, and caches
// every rule, as a production config would, so cached answers are
// compared too. Subscriptions are never cached.
var spawnConfig = template.Must(template.New("config").Parse(`host: 127.0.0.1
lcdPort: {{.LCD}}
rpcPort: {{.RPC}}
grpcPort: {{.GRPC}}
enableEvm: {{.EVM}}
evmRpcPort: {{.EVMRPC}}
evmRpcWsPort: {{.EVMWS}}
nodes:
  - name: {{.Chain}}
    lcdURL: {{.Node.LCD}}
    rpcURL: {{.Node.RPC}}
    grpcURL: {{.Node.GRPC}}
{{- if .EVM}}
    evmRpcURL: {{.Node.EVM}}
    evmRpcWsURL: {{.Node.EVMWS}}
{{- end}}
metrics:
  enable: true
  port: {{.Metrics}}
dashboard:
  enable: false
cache:
  ttl: 30s
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
{{- end}}
`))

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
func spawn(ctx context.Context, bin, chain string, node compat.Endpoints) (*spawned, compat.Endpoints, error) {
	ports, err := freePorts(6)
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	data := struct {
		Chain                                  string
		Node                                   compat.Endpoints
		EVM                                    bool
		LCD, RPC, GRPC, EVMRPC, EVMWS, Metrics int
	}{chain, node, node.EVM != "", ports[0], ports[1], ports[2], ports[3], ports[4], ports[5]}

	dir, err := os.MkdirTemp("", "cosmoguard-compat-")
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	s := &spawned{dir: dir, logPath: filepath.Join(dir, "cosmoguard.log"), done: make(chan struct{})}
	cfgPath := filepath.Join(dir, "cosmoguard.yaml")
	f, err := os.Create(cfgPath)
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	err = spawnConfig.Execute(f, data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	logFile, err := os.Create(s.logPath)
	if err != nil {
		return nil, compat.Endpoints{}, err
	}
	s.cmd = exec.Command(bin, "--config", cfgPath, "--log-level", "warn", "--log-format", "text")
	s.cmd.Stdout, s.cmd.Stderr = logFile, logFile
	if err := s.cmd.Start(); err != nil {
		logFile.Close()
		return nil, compat.Endpoints{}, err
	}
	go func() {
		_ = s.cmd.Wait()
		logFile.Close()
		close(s.done)
	}()

	guard := compat.Endpoints{
		LCD:  fmt.Sprintf("http://127.0.0.1:%d", data.LCD),
		RPC:  fmt.Sprintf("http://127.0.0.1:%d", data.RPC),
		GRPC: fmt.Sprintf("http://127.0.0.1:%d", data.GRPC),
	}
	if data.EVM {
		guard.EVM = fmt.Sprintf("http://127.0.0.1:%d", data.EVMRPC)
		guard.EVMWS = fmt.Sprintf("http://127.0.0.1:%d", data.EVMWS)
	}
	if err := waitReady(ctx, fmt.Sprintf("http://127.0.0.1:%d/readyz", data.Metrics), s); err != nil {
		s.stop()
		return nil, compat.Endpoints{}, fmt.Errorf("%w (log: %s)", err, s.logPath)
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

// stop terminates cosmoguard, giving it 10s to drain.
func (s *spawned) stop() {
	if s.exited() {
		return
	}
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.done
	}
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
