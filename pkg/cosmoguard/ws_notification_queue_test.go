package cosmoguard

import (
	"bytes"
	stdjson "encoding/json"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/pkg/util"
)

type notificationTestUpstream struct {
	healthy            atomic.Bool
	unsubscribeStarted chan struct{}
	releaseUnsubscribe <-chan struct{}
	unsubscribeOnce    sync.Once
}

func newNotificationTestUpstream() *notificationTestUpstream {
	upstream := &notificationTestUpstream{unsubscribeStarted: make(chan struct{})}
	upstream.healthy.Store(true)
	return upstream
}

func (*notificationTestUpstream) Run(*Entry) error                             { return nil }
func (*notificationTestUpstream) MakeRequest(*JsonRpcMsg) (*JsonRpcMsg, error) { return nil, nil }
func (*notificationTestUpstream) HasSubscription(string) bool                  { return false }
func (*notificationTestUpstream) Subscribe(string) (string, error)             { return "subscription", nil }
func (u *notificationTestUpstream) Unsubscribe(string) error {
	u.unsubscribeOnce.Do(func() { close(u.unsubscribeStarted) })
	if u.releaseUnsubscribe != nil {
		<-u.releaseUnsubscribe
	}
	return nil
}
func (*notificationTestUpstream) LocalUnsubscribe(string) {}
func (u *notificationTestUpstream) IsHealthy() bool       { return u.healthy.Load() }
func (*notificationTestUpstream) Stop()                   {}

func newNotificationTestBroker(t *testing.T, upstream *notificationTestUpstream) (*Broker, string) {
	t.Helper()
	broker := NewBroker(
		[]string{"ws://upstream.invalid"},
		"/",
		1,
		func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager { return upstream },
	)
	broker.log = log.WithField("test", t.Name())
	subscriptionID, err := broker.pool.Subscribe("tm.event='NewBlock'")
	require.NoError(t, err)
	broker.sm.AddSubscription("tm.event='NewBlock'", subscriptionID)
	return broker, subscriptionID
}

func TestBrokerNotificationFanoutBoundsBlockedWritersPerClient(t *testing.T) {
	client, _ := newWSCacheClient(t)

	broker := &Broker{
		log: log.WithField("test", t.Name()),
		sm:  NewSubscriptionManager(),
	}
	broker.sm.AddSubscription("tm.event='NewBlock'", "subscription")
	broker.sm.SubscribeClient("subscription", client, nil)

	client.writeMux.Lock()
	defer client.writeMux.Unlock()

	const notifications = 64
	for i := 0; i < notifications; i++ {
		broker.onSubscriptionMessage(&JsonRpcMsg{
			Version: "2.0",
			ID:      "subscription",
			Result:  []byte(`{"height":"1"}`),
		})
	}

	maxBlockedWriters := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		stacks := make([]byte, 1<<20)
		n := runtime.Stack(stacks, true)
		blocked := strings.Count(string(stacks[:n]), "(*JsonRpcWsClient).writeMessage")
		if blocked > maxBlockedWriters {
			maxBlockedWriters = blocked
		}
		runtime.Gosched()
	}

	if maxBlockedWriters > 1 {
		t.Fatalf("one client accumulated %d blocked notification writers; want at most one", maxBlockedWriters)
	}
}

func TestBrokerNotificationFanoutPreservesOrderAndClientIDs(t *testing.T) {
	clientA, peerA := newWSCacheClient(t)
	clientB, peerB := newWSCacheClient(t)

	broker := &Broker{
		log: log.WithField("test", t.Name()),
		sm:  NewSubscriptionManager(),
	}
	broker.sm.AddSubscription("tm.event='NewBlock'", "subscription")
	broker.sm.SubscribeClient("subscription", clientA, "client-a")
	broker.sm.SubscribeClient("subscription", clientB, "client-b")

	const notifications = 12
	for i := 0; i < notifications; i++ {
		broker.onSubscriptionMessage(&JsonRpcMsg{
			Version:    "2.0",
			ID:         "subscription",
			Result:     []byte(`{"sequence":` + strconv.Itoa(i) + `}`),
			WireSuffix: []byte("\n"),
		})
	}

	assertNotificationSequence(t, peerA, "client-a", notifications)
	assertNotificationSequence(t, peerB, "client-b", notifications)
}

func TestBrokerNotificationFanoutPreservesEthCanonicalIDAndOmitsEnvelopeID(t *testing.T) {
	client, peer := newWSCacheClient(t)

	broker := &Broker{
		log: log.WithField("test", t.Name()),
		sm:  NewSubscriptionManager(),
	}
	broker.sm.AddSubscription("newHeads", "canonical-id")
	broker.sm.SubscribeClient("canonical-id", client, nil)
	require.True(t, broker.sm.Migrate("canonical-id", "current-upstream-id"))

	broker.onSubscriptionMessage(&JsonRpcMsg{
		Version: "2.0",
		ID:      "current-upstream-id",
		Method:  "eth_subscription",
		Params: map[string]interface{}{
			"subscription": "current-upstream-id",
			"result":       map[string]interface{}{"number": "0x2a"},
		},
		WireSuffix: []byte("\n"),
	})

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	_, raw, err := peer.ReadMessage()
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(string(raw), "\n"), "wire suffix was not preserved: %q", raw)

	var envelope map[string]interface{}
	require.NoError(t, stdjson.Unmarshal(raw, &envelope))
	assert.NotContains(t, envelope, "id")
	params, ok := envelope["params"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "canonical-id", params["subscription"])
}

func TestBrokerNotificationOverflowClosesOnlySlowClient(t *testing.T) {
	slow, _ := newWSCacheClient(t)
	healthy, healthyPeer := newWSCacheClient(t)

	broker := NewBroker([]string{"ws://upstream.invalid"}, "/", 1, fakeConstructor)
	broker.log = log.WithField("test", t.Name())
	subscriptionID, err := broker.pool.Subscribe("tm.event='NewBlock'")
	require.NoError(t, err)
	broker.sm.AddSubscription("tm.event='NewBlock'", subscriptionID)
	broker.sm.SubscribeClient(subscriptionID, slow, "slow")
	broker.sm.SubscribeClient(subscriptionID, healthy, "healthy")
	slow.SetOnDisconnectCallback(broker.onClientDisconnect)
	require.NoError(t, healthyPeer.SetReadDeadline(time.Now().Add(3*time.Second)))
	healthyFrames := readNotificationFrames(healthyPeer, wsNotificationQueueMessages+1)

	slow.writeMux.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			slow.writeMux.Unlock()
		}
	}()

	const notifications = wsNotificationQueueMessages + 1
	received := 0
	for i := 0; i < notifications; i++ {
		broker.onSubscriptionMessage(&JsonRpcMsg{
			Version: "2.0",
			ID:      subscriptionID,
			Result:  []byte(`{"sequence":` + strconv.Itoa(i) + `}`),
		})
		if (i+1)%8 == 0 {
			for received < i+1 {
				requireNotificationFrame(t, healthyFrames, "healthy", received)
				received++
			}
		}
	}

	require.Eventually(t, slow.IsClosed, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return !broker.sm.ClientSubscribed(subscriptionID, slow)
	}, time.Second, time.Millisecond)
	assert.False(t, healthy.IsClosed())
	for received < notifications {
		requireNotificationFrame(t, healthyFrames, "healthy", received)
		received++
	}

	slow.writeMux.Unlock()
	unlocked = true
}

func TestNotificationQueueBudgetExceedsAcceptedUpstreamFrame(t *testing.T) {
	const envelopeHeadroom = 1 << 20
	require.GreaterOrEqual(t,
		uint64(wsNotificationQueueBytes),
		uint64(upstreamWSReadLimit)+uint64(envelopeHeadroom),
	)
}

func TestNotificationQueueDeliversLargeAcceptedNotification(t *testing.T) {
	client, peer := newWSCacheClient(t)

	const payloadSize = 5 << 20
	result := make([]byte, payloadSize+2)
	result[0] = '"'
	copy(result[1:], bytes.Repeat([]byte{'x'}, payloadSize))
	result[len(result)-1] = '"'

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	frame := readNotificationFrames(peer, 1)
	require.NoError(t, client.enqueueNotification(&JsonRpcMsg{Version: "2.0", Result: result}))

	received := <-frame
	require.NoError(t, received.err)
	assert.Greater(t, len(received.raw), payloadSize)
	assert.False(t, client.IsClosed())
}

func TestNotificationWorkerExitsWhenIdleAndRestarts(t *testing.T) {
	client, peer := newWSCacheClient(t)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))

	require.NoError(t, client.enqueueNotification(sequenceNotification(0)))
	client.notificationMux.Lock()
	firstDone := client.notificationDone
	client.notificationMux.Unlock()
	requireNotificationFrame(t, readNotificationFrames(peer, 1), nil, 0)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("notification worker remained parked after draining the queue")
	}
	assert.False(t, client.IsClosed())

	frames := readNotificationFrames(peer, 2)
	require.NoError(t, client.enqueueNotification(sequenceNotification(1)))
	require.NoError(t, client.enqueueNotification(sequenceNotification(2)))
	client.notificationMux.Lock()
	secondDone := client.notificationDone
	client.notificationMux.Unlock()
	require.NotEqual(t, firstDone, secondDone)
	requireNotificationFrame(t, frames, nil, 1)
	requireNotificationFrame(t, frames, nil, 2)
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("restarted notification worker remained parked after draining the queue")
	}
}

func TestLastSubscriberOverflowReturnsBeforeUpstreamCleanup(t *testing.T) {
	releaseUnsubscribe := make(chan struct{})
	upstream := newNotificationTestUpstream()
	upstream.releaseUnsubscribe = releaseUnsubscribe
	broker, subscriptionID := newNotificationTestBroker(t, upstream)
	client, _ := newWSCacheClient(t)
	broker.sm.SubscribeClient(subscriptionID, client, "client")
	client.SetOnDisconnectCallback(broker.onClientDisconnect)

	client.writeMux.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			client.writeMux.Unlock()
		}
	}()
	for i := 0; i < wsNotificationQueueMessages; i++ {
		broker.onSubscriptionMessage(sequenceNotificationForSubscription(subscriptionID, i))
	}

	broadcastDone := make(chan struct{})
	go func() {
		broker.onSubscriptionMessage(sequenceNotificationForSubscription(subscriptionID, wsNotificationQueueMessages))
		close(broadcastDone)
	}()
	select {
	case <-broadcastDone:
	case <-time.After(time.Second):
		t.Fatal("overflow broadcast waited for upstream unsubscribe")
	}
	select {
	case <-upstream.unsubscribeStarted:
	case <-time.After(time.Second):
		t.Fatal("disconnect cleanup did not start upstream unsubscribe")
	}
	assert.True(t, broker.sm.SubscriptionEmpty(subscriptionID))
	_, subscriptionExists := broker.sm.GetSubscriptionID("tm.event='NewBlock'")
	assert.True(t, subscriptionExists, "upstream cleanup should still be blocked")

	close(releaseUnsubscribe)
	require.Eventually(t, func() bool {
		_, exists := broker.sm.GetSubscriptionID("tm.event='NewBlock'")
		return !exists
	}, time.Second, time.Millisecond)
	client.writeMux.Unlock()
	unlocked = true
}

func TestNotificationWriteFailureClosesAndRemovesClient(t *testing.T) {
	upstream := newNotificationTestUpstream()
	broker, subscriptionID := newNotificationTestBroker(t, upstream)
	client, _ := newWSCacheClient(t)
	broker.sm.SubscribeClient(subscriptionID, client, "client")
	client.SetOnDisconnectCallback(broker.onClientDisconnect)
	require.NoError(t, client.conn.Close())

	broker.onSubscriptionMessage(sequenceNotificationForSubscription(subscriptionID, 0))

	require.Eventually(t, client.IsClosed, time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		_, exists := broker.sm.GetSubscriptionID("tm.event='NewBlock'")
		return !exists
	}, time.Second, time.Millisecond)
	select {
	case <-upstream.unsubscribeStarted:
	case <-time.After(time.Second):
		t.Fatal("write failure did not unsubscribe the upstream subscription")
	}
}

func TestQueuedNotificationsStayOrderedAcrossCanonicalMigration(t *testing.T) {
	client, peer := newWSCacheClient(t)
	broker := &Broker{log: log.WithField("test", t.Name()), sm: NewSubscriptionManager()}
	broker.sm.AddSubscription("newHeads", "canonical-id")
	broker.sm.SubscribeClient("canonical-id", client, nil)

	client.writeMux.Lock()
	broker.onSubscriptionMessage(ethSequenceNotification("canonical-id", 0))
	require.True(t, broker.sm.Migrate("canonical-id", "current-upstream-id"))
	broker.onSubscriptionMessage(ethSequenceNotification("current-upstream-id", 1))
	client.writeMux.Unlock()

	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
	for sequence := 0; sequence < 2; sequence++ {
		_, raw, err := peer.ReadMessage()
		require.NoError(t, err)
		var envelope map[string]interface{}
		require.NoError(t, stdjson.Unmarshal(raw, &envelope))
		params := envelope["params"].(map[string]interface{})
		assert.Equal(t, "canonical-id", params["subscription"])
		assert.Equal(t, float64(sequence), params["sequence"])
	}
}

func TestNotificationQueueRejectsOversizedMessage(t *testing.T) {
	client, _ := newWSCacheClient(t)

	err := client.enqueueNotification(&JsonRpcMsg{
		Version: "2.0",
		Result:  make([]byte, wsNotificationQueueBytes),
	})
	require.ErrorIs(t, err, errNotificationQueueFull)
	require.Eventually(t, client.IsClosed, time.Second, time.Millisecond)

	count, bytes := notificationQueueStats(client)
	assert.Zero(t, count)
	assert.Zero(t, bytes)
}

func TestNotificationQueueRejectsCumulativeByteOverflow(t *testing.T) {
	client, _ := newWSCacheClient(t)

	client.writeMux.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			client.writeMux.Unlock()
		}
	}()

	payload := make([]byte, wsNotificationQueueBytes/2)
	require.NoError(t, client.enqueueNotification(&JsonRpcMsg{Version: "2.0", Result: payload}))
	require.ErrorIs(t,
		client.enqueueNotification(&JsonRpcMsg{Version: "2.0", Result: payload}),
		errNotificationQueueFull,
	)
	require.Eventually(t, client.IsClosed, time.Second, time.Millisecond)

	client.writeMux.Unlock()
	unlocked = true
	require.Eventually(t, func() bool {
		count, bytes := notificationQueueStats(client)
		return count == 0 && bytes == 0
	}, time.Second, time.Millisecond)
}

func TestNotificationQueueCloseIsPromptAndReleasesAccounting(t *testing.T) {
	client, _ := newWSCacheClient(t)

	var callbacks atomic.Int32
	client.SetOnDisconnectCallback(func(*JsonRpcWsClient) {
		callbacks.Add(1)
	})

	client.writeMux.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			client.writeMux.Unlock()
		}
	}()

	for i := 0; i < 3; i++ {
		require.NoError(t, client.enqueueNotification(&JsonRpcMsg{
			Version: "2.0",
			Result:  []byte(`{"sequence":` + strconv.Itoa(i) + `}`),
		}))
	}
	require.Eventually(t, func() bool {
		count, bytes := notificationQueueStats(client)
		return count == 3 && bytes > 0
	}, time.Second, time.Millisecond)
	client.notificationMux.Lock()
	workerDone := client.notificationDone
	client.notificationMux.Unlock()
	require.NotNil(t, workerDone)

	closeDone := make(chan error, 1)
	go func() { closeDone <- client.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for the blocked writer")
	}
	select {
	case <-client.Closed():
	default:
		t.Fatal("Close returned before publishing closed state")
	}

	client.writeMux.Unlock()
	unlocked = true
	require.Eventually(t, func() bool {
		count, bytes := notificationQueueStats(client)
		return count == 0 && bytes == 0
	}, time.Second, time.Millisecond)
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("notification worker did not exit after Close")
	}
	require.Eventually(t, func() bool { return callbacks.Load() == 1 }, time.Second, time.Millisecond)
}

func TestCloseAndCallbackRegistrationInvokeCallbackOnce(t *testing.T) {
	client, _ := newWSCacheClient(t)

	require.NoError(t, client.Close())

	var callbacks atomic.Int32
	callback := func(*JsonRpcWsClient) { callbacks.Add(1) }
	var setters sync.WaitGroup
	setters.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer setters.Done()
			client.SetOnDisconnectCallback(callback)
		}()
	}
	setters.Wait()

	require.Eventually(t, func() bool { return callbacks.Load() == 1 }, time.Second, time.Millisecond)
	assert.ErrorIs(t, client.Close(), ErrClosed)
}

func TestConcurrentCloseAndCallbackRegistrationInvokeCallbackOnce(t *testing.T) {
	client, _ := newWSCacheClient(t)

	var callbacks atomic.Int32
	callback := func(*JsonRpcWsClient) { callbacks.Add(1) }
	start := make(chan struct{})
	closeResult := make(chan error, 1)
	go func() {
		<-start
		closeResult <- client.Close()
	}()

	var setters sync.WaitGroup
	setters.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer setters.Done()
			<-start
			client.SetOnDisconnectCallback(callback)
		}()
	}
	close(start)
	setters.Wait()
	require.NoError(t, <-closeResult)

	require.Eventually(t, func() bool { return callbacks.Load() == 1 }, time.Second, time.Millisecond)
}

func assertNotificationSequence(t *testing.T, peer *websocket.Conn, wantID interface{}, count int) {
	t.Helper()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
	for i := 0; i < count; i++ {
		_, raw, err := peer.ReadMessage()
		require.NoError(t, err)

		var msg JsonRpcMsg
		require.NoError(t, stdjson.Unmarshal(raw, &msg))
		assert.Equal(t, wantID, msg.ID)
		assert.JSONEq(t, `{"sequence":`+strconv.Itoa(i)+`}`, string(msg.Result))
	}
}

type notificationFrame struct {
	raw []byte
	err error
}

func readNotificationFrames(peer *websocket.Conn, count int) <-chan notificationFrame {
	frames := make(chan notificationFrame, count)
	go func() {
		defer close(frames)
		for i := 0; i < count; i++ {
			_, raw, err := peer.ReadMessage()
			frames <- notificationFrame{raw: raw, err: err}
			if err != nil {
				return
			}
		}
	}()
	return frames
}

func requireNotificationFrame(t *testing.T, frames <-chan notificationFrame, wantID interface{}, sequence int) {
	t.Helper()
	frame, ok := <-frames
	require.True(t, ok, "notification reader exited before sequence %d", sequence)
	require.NoError(t, frame.err)

	var msg JsonRpcMsg
	require.NoError(t, stdjson.Unmarshal(frame.raw, &msg))
	assert.Equal(t, wantID, msg.ID)
	assert.JSONEq(t, `{"sequence":`+strconv.Itoa(sequence)+`}`, string(msg.Result))
}

func sequenceNotification(sequence int) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		Result:  []byte(`{"sequence":` + strconv.Itoa(sequence) + `}`),
	}
}

func sequenceNotificationForSubscription(subscriptionID string, sequence int) *JsonRpcMsg {
	msg := sequenceNotification(sequence)
	msg.ID = subscriptionID
	return msg
}

func ethSequenceNotification(subscriptionID string, sequence int) *JsonRpcMsg {
	return &JsonRpcMsg{
		Version: "2.0",
		ID:      subscriptionID,
		Method:  "eth_subscription",
		Params: map[string]interface{}{
			"subscription": subscriptionID,
			"sequence":     sequence,
		},
	}
}

func notificationQueueStats(client *JsonRpcWsClient) (int, uint64) {
	client.notificationMux.Lock()
	defer client.notificationMux.Unlock()
	return client.notificationCount, client.notificationBytes
}
