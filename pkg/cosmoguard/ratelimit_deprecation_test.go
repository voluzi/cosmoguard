package cosmoguard

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type rateLimitLogBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *rateLimitLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *rateLimitLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func TestDeprecatedRateLimitFailureModeStartupAndReload(t *testing.T) {
	up := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(up.Close)
	grpcPort := reserveLoopbackPorts(t, 1)[0]
	raw := func(mode string) string {
		field := ""
		if mode != "" {
			field = ", failureMode: " + mode
		}
		return fmt.Sprintf(`host: 127.0.0.1
grpcPort: %d
nodes: [{name: up, lcdURL: %q, rpcURL: %q, grpcURL: %q}]
rpc: {webSocketEnabled: false}
metrics: {enable: false}
dashboard: {enable: false}
lcd: {rules: [{action: allow, tag: http-limited, rateLimit: {rate: 0.001, burst: 1%s}}]}
grpc: {rules: [{action: allow, tag: grpc-limited, methods: ["*"], rateLimit: {rate: 0.001, burst: 1%s}}]}
`, grpcPort, up.URL, up.URL, up.URL, field, field)
	}
	var logs rateLimitLogBuffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)
	initial := parseRestartConfig(t, raw("fail-open"))
	cg, err := New(initial)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, cg.Shutdown(ctx))
	}()
	const message = "rateLimit.failureMode is deprecated and ignored"
	require.Equal(t, 1, strings.Count(logs.String(), message))
	require.Contains(t, logs.String(), "lcd[0] (http-limited)")
	require.Contains(t, logs.String(), "grpc[0] (grpc-limited)")
	next := parseRestartConfig(t, raw("fail-closed"))
	restart, _ := RequiresRestart(initial, next)
	require.False(t, restart)
	require.Equal(t, fingerprint(t, initial), fingerprint(t, next))
	require.Equal(t, initial.LCD.Rules[0].Fingerprint, next.LCD.Rules[0].Fingerprint)
	require.Equal(t, initial.GRPC.Rules[0].Fingerprint, next.GRPC.Rules[0].Fingerprint)
	cg.applyRules()
	cg.cfgFile = filepath.Join(t.TempDir(), "config.yaml")
	fp := initial.LCD.Rules[0].Fingerprint
	limiter := cg.lcdProxy.limiters[fp]
	for _, mode := range []string{"fail-open", "fail-closed", "", "fail-closed"} {
		reloadTestFile(t, cg, raw(mode))
		require.True(t, cg.dashboard.lastReload.Success)
		require.Same(t, limiter, cg.lcdProxy.limiters[fp], "ignored key must not replace the limiter")
	}
	reloadTestFile(t, cg, strings.Replace(raw("fail-closed"), "lcd: {rules: [", "lcd: {rules: [{priority: 1, action: deny, tag: unrelated}, ", 1))
	require.True(t, cg.dashboard.lastReload.Success)
	require.Equal(t, 2, strings.Count(logs.String(), message), "warn only at startup and when reload introduces the key")
	_, err = ParseConfig([]byte(raw("unsupported")), nil)
	require.ErrorContains(t, err, "rateLimit.failureMode")
}
