package cosmoguard

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func startupHealthConfig(t *testing.T) *Config {
	t.Helper()
	ports := reserveLoopbackPorts(t, 7)
	enabled, disabled := true, false
	return &Config{Host: "127.0.0.1", RpcPort: ports[0], LcdPort: ports[1], GrpcPort: ports[2],
		Metrics: MetricsConfig{Enable: &enabled, Port: ports[3]}, Dashboard: DashboardConfig{Enable: &disabled},
		Nodes: []NodeConfig{{Name: "up", Host: "127.0.0.1"}}, RPC: RpcConfig{WebSocketEnabled: &disabled},
		Cache: CacheGlobalConfig{Cluster: &ClusterConfig{BindAddr: "127.0.0.1", BindPort: ports[4], GossipPort: ports[5], PeerApiPort: ports[6],
			ReplicaCount: 2, Quorum: 2, EncryptionKey: testClusterEncryptionKey,
			Discovery: &ClusterDiscoveryConfig{Mode: "static", Static: &StaticDiscoveryConfig{Peers: []string{fmt.Sprintf("127.0.0.1:%d", ports[5])}}}}}}
}

func TestStartupHealthListensWhileBootstrapWaits(t *testing.T) {
	cfg := startupHealthConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		cg, err := NewContext(ctx, cfg)
		if cg != nil {
			_ = cg.Shutdown(context.Background())
		}
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(50 * time.Second):
			t.Error("constructor did not stop")
		}
	})
	client := &http.Client{Timeout: 100 * time.Millisecond}
	base := fmt.Sprintf("http://127.0.0.1:%d", cfg.Metrics.Port)
	require.Eventually(t, func() bool {
		response, err := client.Get(base + "/healthz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.Cache.Cluster.BindPort), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Second, 10*time.Millisecond)
	// Let Olric finish its started callback before canceling the bootstrap wait.
	time.Sleep(50 * time.Millisecond)
	for _, path := range []string{"/readyz", "/info"} {
		response, err := client.Get(base + path)
		require.NoError(t, err)
		_ = response.Body.Close()
		require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	}
}

func TestReadinessRequiresServingListeners(t *testing.T) {
	cfg := startupHealthConfig(t)
	cfg.Cache.Cluster = nil
	cg, err := New(cfg)
	require.NoError(t, err)
	defer cg.Shutdown(context.Background())
	response := httptest.NewRecorder()
	cg.metricsServer.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}

func TestConstructionCanceledAfterClusterStartsRollsBack(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		_, _ = w.Write([]byte(`{"keys":[{"kty":"oct","kid":"test","alg":"HS256","k":"c2VjcmV0"}]}`))
	}))
	defer func() { unblock(); jwks.Close() }()
	cfg := startupHealthConfig(t)
	cfg.Cache.Cluster = nil
	cfg.Auth = AuthConfig{Enable: true, Methods: []AuthMethodConfig{{Type: "jwt", JwksURL: jwks.URL}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type outcome struct {
		guard *CosmoGuard
		err   error
	}
	done := make(chan outcome, 1)
	go func() { g, err := NewContext(ctx, cfg); done <- outcome{g, err} }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("constructor did not reach JWKS after cluster start")
	}
	cancel()
	unblock()
	select {
	case result := <-done:
		if result.guard != nil {
			defer result.guard.Shutdown(context.Background())
		}
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.guard)
	case <-time.After(5 * time.Second):
		t.Fatal("constructor did not return after cancellation")
	}
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.Metrics.Port), time.Second)
		if err == nil {
			c.Close()
			return false
		}
		return true
	}, 5*time.Second, time.Millisecond)
}
