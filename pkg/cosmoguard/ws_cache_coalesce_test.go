package cosmoguard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	cachepkg "github.com/voluzi/cosmoguard/pkg/cache"
	"github.com/voluzi/cosmoguard/pkg/util"
)

type wsCacheUpstream struct {
	delay        time.Duration
	calls        atomic.Int32
	makeResponse func(*JsonRpcMsg, int32) (*JsonRpcMsg, error)
}

func (u *wsCacheUpstream) Run(*Entry) error { return nil }

func (u *wsCacheUpstream) MakeRequest(request *JsonRpcMsg) (*JsonRpcMsg, error) {
	call := u.calls.Add(1)
	if u.makeResponse != nil {
		return u.makeResponse(request, call)
	}
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

type wsCacheSetCounter struct {
	cachepkg.Cache[uint64, *JsonRpcMsg]
	sets               atomic.Int32
	blockNextGet       atomic.Bool
	setStarted         chan struct{}
	setDone            chan struct{}
	releaseSet         <-chan struct{}
	secondMissObserved chan struct{}
	releaseSecondGet   <-chan struct{}
	started            sync.Once
	done               sync.Once
	secondMiss         sync.Once
}

func (c *wsCacheSetCounter) Get(ctx context.Context, key uint64) (*JsonRpcMsg, error) {
	value, err := c.Cache.Get(ctx, key)
	if c.blockNextGet.CompareAndSwap(true, false) && c.secondMissObserved != nil {
		c.secondMiss.Do(func() { close(c.secondMissObserved) })
		select {
		case <-c.releaseSecondGet:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return value, err
}

func (c *wsCacheSetCounter) Set(ctx context.Context, key uint64, value *JsonRpcMsg, ttl time.Duration) error {
	c.sets.Add(1)
	if c.secondMissObserved != nil {
		select {
		case <-c.secondMissObserved:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if c.setStarted != nil {
		c.started.Do(func() { close(c.setStarted) })
	}
	if c.releaseSet != nil {
		select {
		case <-c.releaseSet:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := c.Cache.Set(ctx, key, value, ttl)
	if c.setDone != nil {
		c.done.Do(func() { close(c.setDone) })
	}
	return err
}

func TestWSCacheCoalescesConcurrentColdCalls(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 75*time.Millisecond)
	client, peer := newWSCacheClient(t)

	const requests = 16
	runConcurrentWSCacheCalls(t, proxy, client, peer, requests)

	require.Equal(t, int32(1), upstream.calls.Load())
}

func TestWSCacheCoalescedMissStoresOnce(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 75*time.Millisecond)
	setCounter := &wsCacheSetCounter{Cache: proxy.cache, setDone: make(chan struct{})}
	proxy.cache = setCounter
	client, peer := newWSCacheClient(t)

	const requests = 16
	runConcurrentWSCacheCalls(t, proxy, client, peer, requests)

	select {
	case <-setCounter.setDone:
	case <-time.After(time.Second):
		t.Fatal("coalesced cache persistence did not complete")
	}
	require.Equal(t, int32(1), upstream.calls.Load())
	require.Equal(t, int32(1), setCounter.sets.Load())
}

func TestWSCacheCoalescedOwnerSendFailureStillStoresResponse(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
	setCounter := &wsCacheSetCounter{Cache: proxy.cache, setDone: make(chan struct{})}
	proxy.cache = setCounter
	client, _ := newWSCacheClient(t)
	require.NoError(t, client.Close())

	request := wsCacheRequest(1)
	require.ErrorIs(t, proxy.handleRequest(client, request, "127.0.0.1", nil), ErrClosed)

	select {
	case <-setCounter.setDone:
	case <-time.After(time.Second):
		t.Fatal("cache persistence depended on owner delivery")
	}
	cached, err := proxy.cache.Get(context.Background(), request.HashWithRule(rule.Fingerprint))
	require.NoError(t, err)
	require.JSONEq(t, `{"height":"42"}`, string(cached.Result))
	require.Equal(t, int32(1), upstream.calls.Load())
	require.Equal(t, int32(1), setCounter.sets.Load())
}

func TestWSCacheCoalescedPendingPersistenceDoesNotOpenDuplicateMiss(t *testing.T) {
	proxy, _, upstream := newWSCacheProxy(t, nil, 0)
	releaseSet := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSet) }) })
	setCounter := &wsCacheSetCounter{
		Cache:      proxy.cache,
		setStarted: make(chan struct{}),
		releaseSet: releaseSet,
	}
	proxy.cache = setCounter
	client, peer := newWSCacheClient(t)

	firstErr := make(chan error, 1)
	go func() { firstErr <- proxy.handleRequest(client, wsCacheRequest(1), "127.0.0.1", nil) }()
	<-setCounter.setStarted
	secondErr := make(chan error, 1)
	go func() { secondErr <- proxy.handleRequest(client, wsCacheRequest(2), "127.0.0.1", nil) }()

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	for range 2 {
		var response JsonRpcMsg
		require.NoError(t, peer.ReadJSON(&response))
	}
	releaseOnce.Do(func() { close(releaseSet) })
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
	require.Equal(t, int32(1), upstream.calls.Load())
	require.Equal(t, int32(1), setCounter.sets.Load())
}

func TestWSCacheCoalescedPersistenceHandoffDoesNotOpenDuplicateMiss(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
	releaseSecondGet := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSecondGet) }) })
	setCounter := &wsCacheSetCounter{
		Cache:              proxy.cache,
		setDone:            make(chan struct{}),
		secondMissObserved: make(chan struct{}),
		releaseSecondGet:   releaseSecondGet,
	}
	proxy.cache = setCounter
	client, peer := newWSCacheClient(t)

	firstErr := make(chan error, 1)
	go func() { firstErr <- proxy.handleRequest(client, wsCacheRequest(1), "127.0.0.1", nil) }()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	var firstResponse JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&firstResponse))

	setCounter.blockNextGet.Store(true)
	secondErr := make(chan error, 1)
	go func() { secondErr <- proxy.handleRequest(client, wsCacheRequest(2), "127.0.0.1", nil) }()
	<-setCounter.secondMissObserved
	<-setCounter.setDone
	hash := wsCacheRequest(1).HashWithRule(rule.Fingerprint)
	require.Eventually(t, func() bool {
		_, pending := proxy.pendingMisses.Load(hash)
		return !pending
	}, time.Second, time.Millisecond)
	releaseOnce.Do(func() { close(releaseSecondGet) })

	var secondResponse JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&secondResponse))
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
	require.Equal(t, int32(1), upstream.calls.Load())
	require.Equal(t, int32(1), setCounter.sets.Load())
}

func TestWSCacheCoalescedExpiredPendingResponseRefetches(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
	rule.Cache.TTL = time.Second
	upstream.makeResponse = func(request *JsonRpcMsg, call int32) (*JsonRpcMsg, error) {
		return WithResult(request, map[string]interface{}{"height": strconv.Itoa(int(call))}), nil
	}
	base := time.Unix(2_000_000, 0).UTC()
	var nowUnixNano atomic.Int64
	nowUnixNano.Store(base.UnixNano())
	proxy.now = func() time.Time { return time.Unix(0, nowUnixNano.Load()).UTC() }

	releaseSet := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseSet) }) })
	setCounter := &wsCacheSetCounter{
		Cache:      proxy.cache,
		setStarted: make(chan struct{}),
		releaseSet: releaseSet,
	}
	proxy.cache = setCounter
	client, peer := newWSCacheClient(t)

	firstErr := make(chan error, 1)
	go func() { firstErr <- proxy.handleRequest(client, wsCacheRequest(1), "127.0.0.1", nil) }()
	<-setCounter.setStarted
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	var firstResponse JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&firstResponse))
	require.JSONEq(t, `{"height":"1"}`, string(firstResponse.Result))

	nowUnixNano.Store(base.Add(2 * time.Second).UnixNano())
	secondErr := make(chan error, 1)
	go func() { secondErr <- proxy.handleRequest(client, wsCacheRequest(2), "127.0.0.1", nil) }()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	var secondResponse JsonRpcMsg
	require.NoError(t, peer.ReadJSON(&secondResponse))
	require.JSONEq(t, `{"height":"2"}`, string(secondResponse.Result))

	require.Equal(t, int32(2), upstream.calls.Load())
	releaseOnce.Do(func() { close(releaseSet) })
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
	hash := wsCacheRequest(1).HashWithRule(rule.Fingerprint)
	require.Eventually(t, func() bool {
		_, pending := proxy.pendingMisses.Load(hash)
		return !pending
	}, time.Second, time.Millisecond)
	cached, err := proxy.cache.Get(context.Background(), hash)
	require.NoError(t, err)
	require.JSONEq(t, `{"height":"2"}`, string(cached.Result))
}

func TestWSCacheCoalescedWaiterRefetchesUncacheableResponse(t *testing.T) {
	tests := []struct {
		name         string
		makeResponse func(*JsonRpcMsg) *JsonRpcMsg
		check        func(*testing.T, map[int]*JsonRpcMsg)
	}{
		{
			name: "error",
			makeResponse: func(request *JsonRpcMsg) *JsonRpcMsg {
				return ErrorResponse(request, -32000, "request-"+strconv.Itoa(request.ID.(int)), nil)
			},
			check: func(t *testing.T, responses map[int]*JsonRpcMsg) {
				require.Equal(t, "request-1", responses[1].Error.Message)
				require.Equal(t, "request-2", responses[2].Error.Message)
			},
		},
		{
			name: "empty result",
			makeResponse: func(request *JsonRpcMsg) *JsonRpcMsg {
				return WithResult(request, nil)
			},
			check: func(t *testing.T, responses map[int]*JsonRpcMsg) {
				require.True(t, responses[1].IsEmptyResult())
				require.True(t, responses[2].IsEmptyResult())
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
			started := make(chan struct{})
			release := make(chan struct{})
			var startedOnce, releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			upstream.makeResponse = func(request *JsonRpcMsg, call int32) (*JsonRpcMsg, error) {
				if call == 1 {
					startedOnce.Do(func() { close(started) })
					<-release
				}
				return tt.makeResponse(request), nil
			}
			hash := wsCacheRequest(1).HashWithRule(rule.Fingerprint)
			type outcome struct {
				message *JsonRpcMsg
				err     error
			}
			first := make(chan outcome, 1)
			second := make(chan outcome, 1)
			go func() {
				message, err := proxy.coalescedWSRequest(context.Background(), hash, wsCacheRequest(1), rule.Cache, "test")
				first <- outcome{message: message, err: err}
			}()
			<-started
			waiterObserved := make(chan struct{})
			waiterCtx := &observedDoneContext{Context: context.Background(), observed: waiterObserved}
			go func() {
				message, err := proxy.coalescedWSRequest(waiterCtx, hash, wsCacheRequest(2), rule.Cache, "test")
				second <- outcome{message: message, err: err}
			}()
			<-waiterObserved
			releaseOnce.Do(func() { close(release) })
			firstResult := <-first
			secondResult := <-second
			require.NoError(t, firstResult.err)
			require.NoError(t, secondResult.err)
			responses := map[int]*JsonRpcMsg{1: firstResult.message, 2: secondResult.message}
			tt.check(t, responses)
			require.Equal(t, int32(2), upstream.calls.Load())
		})
	}
}

func TestWSCacheCoalescedWaiterCachesRetryAfterUnshareableResponse(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	upstream.makeResponse = func(request *JsonRpcMsg, call int32) (*JsonRpcMsg, error) {
		if call == 1 {
			startedOnce.Do(func() { close(started) })
			<-release
			return ErrorResponse(request, -32000, "transient upstream error", nil), nil
		}
		return WithResult(request, map[string]interface{}{"height": "2"}), nil
	}

	hash := wsCacheRequest(1).HashWithRule(rule.Fingerprint)
	type outcome struct {
		message *JsonRpcMsg
		err     error
	}
	first := make(chan outcome, 1)
	second := make(chan outcome, 1)
	go func() {
		message, err := proxy.coalescedWSRequest(context.Background(), hash, wsCacheRequest(1), rule.Cache, "test")
		first <- outcome{message: message, err: err}
	}()
	<-started
	waiterObserved := make(chan struct{})
	waiterCtx := &observedDoneContext{Context: context.Background(), observed: waiterObserved}
	go func() {
		message, err := proxy.coalescedWSRequest(waiterCtx, hash, wsCacheRequest(2), rule.Cache, "test")
		second <- outcome{message: message, err: err}
	}()
	<-waiterObserved
	releaseOnce.Do(func() { close(release) })

	firstResult := <-first
	secondResult := <-second
	require.NoError(t, firstResult.err)
	require.NotNil(t, firstResult.message.Error)
	require.NoError(t, secondResult.err)
	require.JSONEq(t, `{"height":"2"}`, string(secondResult.message.Result))

	thirdMessage, err := proxy.coalescedWSRequest(context.Background(), hash, wsCacheRequest(3), rule.Cache, "test")
	require.NoError(t, err)
	require.JSONEq(t, `{"height":"2"}`, string(thirdMessage.Result))
	require.Equal(t, int32(2), upstream.calls.Load())
}

func TestWSCacheCoalescedWaiterRefetchesTransportError(t *testing.T) {
	proxy, rule, upstream := newWSCacheProxy(t, nil, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	transportErr := errors.New("upstream transport failed")
	upstream.makeResponse = func(request *JsonRpcMsg, call int32) (*JsonRpcMsg, error) {
		if call == 1 {
			startedOnce.Do(func() { close(started) })
			<-release
			return nil, transportErr
		}
		return WithResult(request, map[string]interface{}{"height": "2"}), nil
	}

	hash := wsCacheRequest(1).HashWithRule(rule.Fingerprint)
	type outcome struct {
		message *JsonRpcMsg
		err     error
	}
	first := make(chan outcome, 1)
	second := make(chan outcome, 1)
	go func() {
		message, err := proxy.coalescedWSRequest(context.Background(), hash, wsCacheRequest(1), rule.Cache, "test")
		first <- outcome{message: message, err: err}
	}()
	<-started
	waiterObserved := make(chan struct{})
	waiterCtx := &observedDoneContext{Context: context.Background(), observed: waiterObserved}
	go func() {
		message, err := proxy.coalescedWSRequest(waiterCtx, hash, wsCacheRequest(2), rule.Cache, "test")
		second <- outcome{message: message, err: err}
	}()
	<-waiterObserved
	releaseOnce.Do(func() { close(release) })

	firstResult := <-first
	secondResult := <-second
	require.ErrorIs(t, firstResult.err, transportErr)
	require.NoError(t, secondResult.err)
	require.JSONEq(t, `{"height":"2"}`, string(secondResult.message.Result))

	thirdMessage, err := proxy.coalescedWSRequest(context.Background(), hash, wsCacheRequest(3), rule.Cache, "test")
	require.NoError(t, err)
	require.JSONEq(t, `{"height":"2"}`, string(thirdMessage.Result))
	require.Equal(t, int32(2), upstream.calls.Load())
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
