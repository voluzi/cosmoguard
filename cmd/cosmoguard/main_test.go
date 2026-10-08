package main

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/pkg/cosmoguard"
	"gopkg.in/yaml.v3"
)

func TestSignalDrainHelper(t *testing.T) {
	if os.Getenv("COSMOGUARD_DRAIN_HELPER") != "1" {
		return
	}
	main()
	os.Exit(0)
}

func drainTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestSignalDrainKeepsServingAndClosesAllListeners(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		_, _ = w.Write([]byte("alive"))
	}))
	defer upstream.Close()
	defer close(release)
	enabled := true
	cfg := cosmoguard.Config{Host: "127.0.0.1", LcdPort: drainTestPort(t), RpcPort: drainTestPort(t), GrpcPort: drainTestPort(t), EvmRpcPort: drainTestPort(t), EvmRpcWsPort: drainTestPort(t),
		Metrics: cosmoguard.MetricsConfig{Enable: &enabled, Port: drainTestPort(t)},
		Nodes:   []cosmoguard.NodeConfig{{Name: "up", LcdURL: upstream.URL, RpcURL: upstream.URL}},
		LCD:     cosmoguard.LcdConfig{Default: cosmoguard.RuleActionAllow},
		RPC:     cosmoguard.RpcConfig{Default: cosmoguard.RuleActionAllow, WebSocketConnections: 1},
	}
	data, err := yaml.Marshal(&cfg)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, data, 0600))
	var logs bytes.Buffer
	cmd := exec.Command(os.Args[0], "-test.run=^TestSignalDrainHelper$", "-config", path)
	cmd.Env = append(os.Environ(), "COSMOGUARD_DRAIN_HELPER=1")
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	waited := false
	go func() { done <- cmd.Wait() }()
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
		t.Log(logs.String())
	}()
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	base := "http://127.0.0.1:" + strconv.Itoa(cfg.Metrics.Port)
	status := func(url string) int {
		r, err := client.Get(url)
		if err != nil {
			return 0
		}
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
		return r.StatusCode
	}
	require.Eventually(t, func() bool { return status(base+"/readyz") == 200 }, 10*time.Second, 10*time.Millisecond)
	signaled := time.Now()
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.Eventually(t, func() bool { return status(base+"/readyz") == 503 }, time.Second, 10*time.Millisecond)
	lcd := "http://127.0.0.1:" + strconv.Itoa(cfg.LcdPort)
	for time.Since(signaled) < 4*time.Second {
		require.Equal(t, 200, status(base+"/healthz"))
		require.Equal(t, 200, status(base+"/info"))
		require.Equal(t, 200, status(lcd+"/"))
		time.Sleep(100 * time.Millisecond)
	}
	slow := make(chan error, 1)
	go func() {
		c := &http.Client{Timeout: 10 * time.Second}
		r, err := c.Get(lcd + "/slow")
		if err == nil {
			defer r.Body.Close()
			body, e := io.ReadAll(r.Body)
			err = e
			if string(body) != "alive" {
				err = io.ErrUnexpectedEOF
			}
		}
		slow <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("hold did not admit the request")
	}
	peer, _, err := websocket.DefaultDialer.Dial("ws://127.0.0.1:"+strconv.Itoa(cfg.RpcPort)+"/websocket", nil)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, peer.SetReadDeadline(signaled.Add(8*time.Second)))
	_, _, err = peer.ReadMessage()
	require.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway), "close: %v", err)
	for _, port := range []int{cfg.LcdPort, cfg.RpcPort, cfg.GrpcPort, cfg.Metrics.Port} {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		require.Error(t, err, "listener %d must stop while the LCD request drains", port)
	}
	release <- struct{}{}
	require.NoError(t, <-slow)
	select {
	case err := <-done:
		waited = true
		require.NoError(t, err, "%s", logs.String())
	case <-time.After(5 * time.Second):
		t.Fatal("signal drain did not finish")
	}
	require.GreaterOrEqual(t, time.Since(signaled), 5*time.Second)
	require.Less(t, time.Since(signaled), 10*time.Second)
}
