package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/pkg/util"
	"gotest.tools/assert"
)

type lifecycleRunBackend struct {
	server   *httptest.Server
	accepted chan *lifecycleRunConn
}

type lifecycleRunConn struct {
	conn     *websocket.Conn
	requests chan *JsonRpcMsg
	writeMu  sync.Mutex
}

func newLifecycleRunBackend(t *testing.T) *lifecycleRunBackend {
	t.Helper()
	backend := &lifecycleRunBackend{accepted: make(chan *lifecycleRunConn, 4)}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	backend.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		peer := &lifecycleRunConn{conn: conn, requests: make(chan *JsonRpcMsg, 8)}
		backend.accepted <- peer
		go func() {
			defer close(peer.requests)
			for {
				var request JsonRpcMsg
				if err := conn.ReadJSON(&request); err != nil {
					return
				}
				peer.requests <- &request
			}
		}()
	}))
	t.Cleanup(backend.server.Close)
	return backend
}

func (b *lifecycleRunBackend) wsURL(t *testing.T) url.URL {
	t.Helper()
	parsed, err := url.Parse("ws" + strings.TrimPrefix(b.server.URL, "http"))
	assert.NilError(t, err)
	return *parsed
}

func (b *lifecycleRunBackend) nextConn(t *testing.T) *lifecycleRunConn {
	t.Helper()
	select {
	case conn := <-b.accepted:
		t.Cleanup(func() { _ = conn.close() })
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream connection")
		return nil
	}
}

func (c *lifecycleRunConn) nextRequest(t *testing.T) *JsonRpcMsg {
	t.Helper()
	select {
	case request, ok := <-c.requests:
		if !ok {
			t.Fatal("upstream connection closed before request")
		}
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for upstream request")
		return nil
	}
}

func (c *lifecycleRunConn) respond(t *testing.T, response *JsonRpcMsg) {
	t.Helper()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	encoded, err := response.Marshal()
	assert.NilError(t, err)
	assert.NilError(t, c.conn.WriteMessage(websocket.TextMessage, encoded))
}

func (c *lifecycleRunConn) close() error {
	return c.conn.Close()
}

func respondLifecycleSubscribe(t *testing.T, protocol wsProtocolCase, conn *lifecycleRunConn, request *JsonRpcMsg, wireID string) {
	t.Helper()
	if protocol.evm {
		conn.respond(t, WithResult(request, wireID))
		return
	}
	conn.respond(t, WithResult(request, map[string]any{}))
}

func setLifecycleRunHooks(manager UpstreamConnManager, beforeUnsubscribeCommit func(), beforeInstall, afterInstall func(*JsonRpcWsClient)) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.beforeUnsubscribeCommit = beforeUnsubscribeCommit
		manager.beforeInstall = beforeInstall
		manager.afterInstall = afterInstall
	case *UpstreamConnManagerEth:
		manager.beforeUnsubscribeCommit = beforeUnsubscribeCommit
		manager.beforeInstall = beforeInstall
		manager.afterInstall = afterInstall
	}
}

func TestRunFinalUnsubscribeSerializesReplacementInstall(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			backend := newLifecycleRunBackend(t)
			manager := protocol.constructor(backend.wsURL(t), &util.UniqueID{}, func(*JsonRpcMsg) {})

			outcomeReached := make(chan struct{})
			releaseOutcome := make(chan struct{})
			installedA := make(chan struct{})
			installedB := make(chan struct{})
			attemptedB := make(chan struct{})
			var installs int
			setLifecycleRunHooks(manager, func() {
				close(outcomeReached)
				<-releaseOutcome
			}, func(*JsonRpcWsClient) {
				if installs == 1 {
					close(attemptedB)
				}
			}, func(*JsonRpcWsClient) {
				installs++
				if installs == 1 {
					close(installedA)
				} else if installs == 2 {
					close(installedB)
				}
			})
			done := make(chan error, 1)
			go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
			t.Cleanup(func() {
				select {
				case <-releaseOutcome:
				default:
					close(releaseOutcome)
				}
				manager.Stop()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("manager Run did not stop")
				}
			})

			connA := backend.nextConn(t)
			<-installedA
			subscribe := startSubscribe(manager, "victim")
			subscribeRequest := connA.nextRequest(t)
			respondLifecycleSubscribe(t, protocol, connA, subscribeRequest, "0x-a")
			created := <-subscribe
			assert.NilError(t, created.err)

			unsubscribe := startUnsubscribe(manager, created.id)
			_ = connA.nextRequest(t)
			assert.NilError(t, connA.close())
			<-outcomeReached
			_ = backend.nextConn(t)
			<-attemptedB
			select {
			case <-installedB:
				t.Fatal("replacement socket installed before final removal committed")
			default:
			}

			close(releaseOutcome)
			assert.Assert(t, isUncertainWSUpstreamOutcome(<-unsubscribe))
			select {
			case <-installedB:
			case <-time.After(2 * time.Second):
				t.Fatal("replacement socket was not installed after removal committed")
			}
		})
	}
}

func TestRunRetireBeforeReplayDoesNotCleanReplacementSocket(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			backend := newLifecycleRunBackend(t)
			manager := protocol.constructor(backend.wsURL(t), &util.UniqueID{}, func(*JsonRpcMsg) {})

			installedB := make(chan struct{})
			installedA := make(chan struct{})
			releaseInstall := make(chan struct{})
			var installs int
			setLifecycleRunHooks(manager, nil, nil, func(*JsonRpcWsClient) {
				installs++
				if installs == 1 {
					close(installedA)
				} else if installs == 2 {
					close(installedB)
					<-releaseInstall
				}
			})
			done := make(chan error, 1)
			go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
			t.Cleanup(func() {
				select {
				case <-releaseInstall:
				default:
					close(releaseInstall)
				}
				manager.Stop()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("manager Run did not stop")
				}
			})

			connA := backend.nextConn(t)
			<-installedA
			subscribe := startSubscribe(manager, "victim")
			subscribeRequest := connA.nextRequest(t)
			respondLifecycleSubscribe(t, protocol, connA, subscribeRequest, "0x-a")
			assert.NilError(t, (<-subscribe).err)

			assert.NilError(t, connA.close())
			connB := backend.nextConn(t)
			<-installedB
			cleanup := manager.LocalUnsubscribe("victim")
			select {
			case request := <-connB.requests:
				t.Fatalf("retirement sent %s to replacement before replay: %+v", request.Method, request)
			case err := <-cleanup:
				assert.NilError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("retirement did not settle from the closed origin")
			}
			close(releaseInstall)
		})
	}
}

func TestRunReconnectPreservesLogicalSubscriptionID(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			backend := newLifecycleRunBackend(t)
			notifications := make(chan *JsonRpcMsg, 1)
			replayed := make(chan struct{}, 2)
			manager := protocol.constructor(backend.wsURL(t), &util.UniqueID{}, func(msg *JsonRpcMsg) {
				notifications <- msg
			})
			switch manager := manager.(type) {
			case *UpstreamConnManagerCosmos:
				manager.afterResubmit = func() { replayed <- struct{}{} }
			case *UpstreamConnManagerEth:
				manager.afterResubmit = func() { replayed <- struct{}{} }
			}
			done := make(chan error, 1)
			go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
			t.Cleanup(func() {
				manager.Stop()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("manager Run did not stop")
				}
			})

			connA := backend.nextConn(t)
			<-replayed
			created := startSubscribe(manager, "victim")
			requestA := connA.nextRequest(t)
			respondLifecycleSubscribe(t, protocol, connA, requestA, "0x-a")
			subscription := <-created
			assert.NilError(t, subscription.err)

			assert.NilError(t, connA.close())
			connB := backend.nextConn(t)
			replay := connB.nextRequest(t)
			respondLifecycleSubscribe(t, protocol, connB, replay, "0x-b")
			<-replayed
			if protocol.evm {
				if handle, ok := managerLifecycle(manager).route(currentManagerClient(manager), "0x-b"); !ok || handle != subscription.id {
					t.Fatalf("replayed EVM binding = %q,%v; want %q,true", handle, ok, subscription.id)
				}
			}

			if protocol.evm {
				connB.respond(t, &JsonRpcMsg{Version: jsonRpcVersion, Params: map[string]any{
					"subscription": "0x-b", "result": map[string]any{"height": 1},
				}})
			} else {
				connB.respond(t, &JsonRpcMsg{Version: jsonRpcVersion, ID: subscription.id, Result: []byte(`{"height":1}`)})
			}
			notification := <-notifications
			assert.Equal(t, notification.ID, subscription.id)
			if protocol.evm {
				params := notification.Params.(map[string]any)
				assert.Equal(t, params["subscription"], subscription.id)
			}

			removed := startUnsubscribe(manager, subscription.id)
			unsubscribe := connB.nextRequest(t)
			if protocol.evm {
				params := unsubscribe.Params.([]any)
				assert.Equal(t, params[0], "0x-b")
				connB.respond(t, WithResult(unsubscribe, true))
			} else {
				connB.respond(t, WithResult(unsubscribe, map[string]any{}))
			}
			assert.NilError(t, <-removed)
		})
	}
}

func TestEVMRunReconnectKeepsLogicalHandlesIndependentFromWireIDs(t *testing.T) {
	backend := newLifecycleRunBackend(t)
	notifications := make(chan *JsonRpcMsg, 2)
	replayed := make(chan struct{}, 2)
	manager := EthUpstreamConnManager(backend.wsURL(t), &util.UniqueID{}, func(msg *JsonRpcMsg) {
		notifications <- msg
	}).(*UpstreamConnManagerEth)
	manager.afterResubmit = func() { replayed <- struct{}{} }
	done := make(chan error, 1)
	go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
	t.Cleanup(func() {
		manager.Stop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("manager Run did not stop")
		}
	})

	connA := backend.nextConn(t)
	<-replayed
	firstCall := startSubscribe(manager, "first")
	firstRequest := connA.nextRequest(t)
	connA.respond(t, WithResult(firstRequest, "0x2"))
	first := <-firstCall
	assert.NilError(t, first.err)

	assert.NilError(t, connA.close())
	connB := backend.nextConn(t)
	replay := connB.nextRequest(t)
	connB.respond(t, WithResult(replay, "replayed-first"))
	<-replayed

	secondCall := startSubscribe(manager, "second")
	secondRequest := connB.nextRequest(t)
	connB.respond(t, WithResult(secondRequest, first.id))
	second := <-secondCall
	assert.NilError(t, second.err)
	assert.Assert(t, first.id != second.id, "logical handles collided at %q", first.id)

	connB.respond(t, &JsonRpcMsg{Version: jsonRpcVersion, Params: map[string]any{
		"subscription": "replayed-first", "result": "first-event",
	}})
	connB.respond(t, &JsonRpcMsg{Version: jsonRpcVersion, Params: map[string]any{
		"subscription": first.id, "result": "second-event",
	}})
	routed := map[any]bool{}
	for range 2 {
		msg := <-notifications
		routed[msg.ID] = true
	}
	assert.Assert(t, routed[first.id])
	assert.Assert(t, routed[second.id])

	firstRemoval := startUnsubscribe(manager, first.id)
	firstUnsubscribe := connB.nextRequest(t)
	assert.DeepEqual(t, firstUnsubscribe.Params, []any{"replayed-first"})
	connB.respond(t, WithResult(firstUnsubscribe, true))
	assert.NilError(t, <-firstRemoval)

	secondRemoval := startUnsubscribe(manager, second.id)
	secondUnsubscribe := connB.nextRequest(t)
	assert.DeepEqual(t, secondUnsubscribe.Params, []any{first.id})
	connB.respond(t, WithResult(secondUnsubscribe, true))
	assert.NilError(t, <-secondRemoval)
}
