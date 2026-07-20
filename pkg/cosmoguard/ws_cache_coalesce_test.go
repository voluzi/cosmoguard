package cosmoguard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/pkg/util"
)

type wsCacheUpstream struct {
	delay time.Duration
	calls atomic.Int32
}

func (u *wsCacheUpstream) Run(*Entry) error { return nil }

func (u *wsCacheUpstream) MakeRequest(request *JsonRpcMsg) (*JsonRpcMsg, error) {
	u.calls.Add(1)
	time.Sleep(u.delay)
	return WithResult(request, map[string]interface{}{"height": "42"}), nil
}

func (u *wsCacheUpstream) HasSubscription(string) bool      { return false }
func (u *wsCacheUpstream) Subscribe(string) (string, error) { return "", nil }
func (u *wsCacheUpstream) Unsubscribe(string) error         { return nil }
func (u *wsCacheUpstream) LocalUnsubscribe(string)          {}
func (u *wsCacheUpstream) IsHealthy() bool                  { return true }
func (u *wsCacheUpstream) Stop()                            {}

func newWSCacheProxy(t *testing.T, coalesce *bool, delay time.Duration) (*JsonRpcWebSocketProxy, *JsonRpcRule, *wsCacheUpstream) {
	t.Helper()

	responseCache, err := newResponseCache[uint64, *JsonRpcMsg](nil, nil, t.Name(), CacheBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, responseCache.Close()) })

	upstream := &wsCacheUpstream{delay: delay}
	constructor := func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager {
		return upstream
	}
	proxy, err := NewJsonRpcWebSocketProxy(
		t.Name(),
		[]string{"ws://upstream.invalid"},
		"/websocket",
		1,
		constructor,
		responseCache,
		false,
		nil,
	)
	require.NoError(t, err)
	proxy.log = log.WithField("test", t.Name())
	proxy.cgDashboard = newDashboardObservability()
	proxy.section = "rpc.jsonrpc"

	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache: &RuleCache{
			Enable:   true,
			TTL:      time.Minute,
			Coalesce: coalesce,
		},
	}
	require.NoError(t, rule.Compile())
	proxy.SetRules([]*JsonRpcRule{rule}, RuleActionDeny, nil)

	return proxy, rule, upstream
}

func newWSCacheClient(t *testing.T) (*JsonRpcWsClient, *websocket.Conn) {
	t.Helper()

	accepted := make(chan *websocket.Conn, 1)
	upgradeErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upgradeErr <- err
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)

	dialURL := "ws" + strings.TrimPrefix(server.URL, "http")
	peer, _, err := websocket.DefaultDialer.Dial(dialURL, nil)
	require.NoError(t, err)

	var serverConn *websocket.Conn
	select {
	case serverConn = <-accepted:
	case err = <-upgradeErr:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for websocket upgrade")
	}

	client := NewJsonRpcWsClient(serverConn)
	t.Cleanup(func() {
		_ = client.Close()
		_ = peer.Close()
	})
	return client, peer
}

func wsCacheRequest(id interface{}) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		ID:      id,
		Method:  "status",
		Params:  []interface{}{"latest"},
	}
}

func runConcurrentWSCacheCalls(t *testing.T, proxy *JsonRpcWebSocketProxy, client *JsonRpcWsClient, peer *websocket.Conn, n int) {
	t.Helper()

	start := make(chan struct{})
	errs := make(chan error, n)
	var ready sync.WaitGroup
	var calls sync.WaitGroup
	ready.Add(n)
	calls.Add(n)
	for i := 0; i < n; i++ {
		id := i + 1
		go func() {
			defer calls.Done()
			ready.Done()
			<-start
			errs <- proxy.handleRequest(client, wsCacheRequest(id), "127.0.0.1", nil)
		}()
	}
	ready.Wait()
	close(start)
	calls.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
	ids := make([]int, 0, n)
	for i := 0; i < n; i++ {
		var response JsonRpcMsg
		require.NoError(t, peer.ReadJSON(&response))
		id, ok := response.ID.(int)
		require.True(t, ok, "response ID has type %T", response.ID)
		ids = append(ids, id)
		require.JSONEq(t, `{"height":"42"}`, string(response.Result))
	}

	wantIDs := make([]int, n)
	for i := range wantIDs {
		wantIDs[i] = i + 1
	}
	require.ElementsMatch(t, wantIDs, ids)
}

func TestWSCacheCoalescesConcurrentColdCalls(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 75*time.Millisecond)
	client, peer := newWSCacheClient(t)

	const requests = 16
	runConcurrentWSCacheCalls(t, proxy, client, peer, requests)

	require.Equal(t, int32(1), upstream.calls.Load())
}

func TestWSCacheCoalescesConcurrentExpiredCalls(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 75*time.Millisecond)
	client, peer := newWSCacheClient(t)

	now := time.Unix(2_000_000, 0).UTC()
	proxy.now = func() time.Time { return now }
	cached := WithResult(wsCacheRequest(0), map[string]interface{}{"height": "stale"})
	cached.StoredAt = now.Add(-2 * rule.Cache.TTL)
	require.NoError(t, proxy.cache.Set(
		context.Background(),
		wsCacheRequest(0).HashWithRule(rule.Fingerprint),
		cached,
		time.Hour,
	))

	const requests = 16
	runConcurrentWSCacheCalls(t, proxy, client, peer, requests)

	require.Equal(t, int32(1), upstream.calls.Load())
}

func TestWSCacheCoalesceDisabledMakesIndependentCalls(t *testing.T) {
	off := false
	proxy, _, upstream := newWSCacheProxy(t, &off, 75*time.Millisecond)
	client, peer := newWSCacheClient(t)

	const requests = 8
	runConcurrentWSCacheCalls(t, proxy, client, peer, requests)

	require.Equal(t, int32(requests), upstream.calls.Load())
}

func TestWSNotificationBypassesPrimedCache(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 0)
	client, peer := newWSCacheClient(t)

	require.NoError(t, proxy.handleRequest(client, wsCacheRequest(1), "127.0.0.1", nil))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	var response JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&response))
	require.Equal(t, int32(1), upstream.calls.Load())

	require.NoError(t, proxy.handleRequest(client, wsCacheRequest(nil), "127.0.0.1", nil))
	require.Equal(t, int32(2), upstream.calls.Load())
}

func TestWSNotificationsAreNotCoalesced(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 75*time.Millisecond)

	const notifications = 8
	start := make(chan struct{})
	errs := make(chan error, notifications)
	var calls sync.WaitGroup
	calls.Add(notifications)
	for i := 0; i < notifications; i++ {
		go func() {
			defer calls.Done()
			<-start
			errs <- proxy.handleRequest(nil, wsCacheRequest(nil), "127.0.0.1", nil)
		}()
	}
	close(start)
	calls.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, int32(notifications), upstream.calls.Load())
}
