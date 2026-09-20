package cosmoguard

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrClosed                = errors.New("websocket client closed")
	errNotificationQueueFull = errors.New("websocket notification queue full")
	// ErrBadMessage marks a frame that failed to decode as JSON-RPC on an
	// otherwise-healthy connection (empty frame, malformed JSON). Unlike a
	// read error, the socket is still usable, so callers should reply with a
	// JSON-RPC parse error (-32700) and keep the connection open rather than
	// disconnecting the client (and dropping its subscriptions).
	ErrBadMessage = errors.New("bad json rpc message")
	// ErrInvalidRequest marks a frame that IS valid JSON but is not a
	// supported single JSON-RPC request (e.g. a batch array). It maps to
	// -32600 Invalid Request rather than -32700 Parse Error.
	ErrInvalidRequest = errors.New("invalid json rpc request")
)

const (
	// A subscriber can absorb short bursts without letting sustained lag
	// retain unbounded messages or payload bytes.
	wsNotificationQueueMessages = 64
	// The byte budget tracks decoded values retained by a client's queue, not
	// their escaped wire encoding. Two upstream frame limits leave headroom
	// for decoded map/slice storage and the client-specific envelope.
	wsNotificationQueueBytes uint64 = uint64(upstreamWSReadLimit) * 2
	wsClientWriteDeadline           = 10 * time.Second
)

const wsNotificationCostLimit = wsNotificationQueueBytes + 1

type queuedNotification struct {
	msg  *JsonRpcMsg
	cost uint64
}

type JsonRpcWsClient struct {
	conn     *websocket.Conn
	closed   atomic.Bool
	closeMux sync.Mutex
	writeMux sync.Mutex
	readMux  sync.Mutex

	// closeCh wakes requests waiting for a response as soon as the socket
	// closes instead of leaving them parked until responseTimeout.
	closeCh   chan struct{}
	closeOnce sync.Once

	notificationMux       sync.Mutex
	notificationQueue     []queuedNotification
	notificationCount     int
	notificationBytes     uint64
	notificationRunning   bool
	notificationStopped   bool
	notificationDone      chan struct{}
	notificationCloseOnce sync.Once

	// onDisconnect is written by SetOnDisconnectCallback (called from
	// the broker's HandleSubscription goroutine on first subscribe)
	// and read by Close() (which can fire from the reader goroutine
	// or any caller observing a dead socket). Guard with cbMu so the
	// race detector stays quiet and the read sees a consistent value.
	cbMu                 sync.RWMutex
	onDisconnect         func(client *JsonRpcWsClient)
	callbackScheduleOnce sync.Once
}

func (c *JsonRpcWsClient) String() string {
	return c.conn.RemoteAddr().String()
}

func NewJsonRpcWsClient(conn *websocket.Conn) *JsonRpcWsClient {
	return &JsonRpcWsClient{
		conn:    conn,
		closeCh: make(chan struct{}),
	}
}

// Closed returns a channel that is closed when the client is closed.
// Use it in a select to fail fast on disconnect.
func (c *JsonRpcWsClient) Closed() <-chan struct{} {
	return c.closeCh
}

func (c *JsonRpcWsClient) readMessage() (int, []byte, error) {
	c.readMux.Lock()
	defer c.readMux.Unlock()
	return c.conn.ReadMessage()
}

func (c *JsonRpcWsClient) writeMessage(messageType int, data []byte) error {
	c.writeMux.Lock()
	defer c.writeMux.Unlock()
	// Cap the write attempt so a stuck TCP send doesn't pin the
	// caller's goroutine forever — see wsClientWriteDeadline.
	if err := c.conn.SetWriteDeadline(time.Now().Add(wsClientWriteDeadline)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return c.conn.WriteMessage(messageType, data)
}

func (c *JsonRpcWsClient) IsClosed() bool {
	if c == nil {
		return true
	}
	return c.closed.Load()
}

func (c *JsonRpcWsClient) ReceiveMsg() (*JsonRpcMsg, error) {
	if c.IsClosed() {
		return nil, ErrClosed
	}

	_, message, err := c.readMessage()

	if err != nil {
		// Any read error means the WS conn is no longer usable —
		// gorilla doesn't recover a connection from a read failure.
		// Close it so the caller's HandleConnection loop observes
		// IsClosed==true on the next iteration and breaks; without
		// this Close on the IsUnexpectedCloseError branch, the loop
		// continues forever on a dead conn, burning CPU and leaking
		// the goroutine + the underlying socket FD for the life of
		// the process.
		_ = c.Close()
		if websocket.IsUnexpectedCloseError(err,
			websocket.CloseNormalClosure,
			websocket.CloseGoingAway,
			websocket.CloseAbnormalClosure,
			websocket.CloseNoStatusReceived,
		) {
			return nil, err
		}
		return nil, ErrClosed
	}

	// The three cases below are malformed frames on a still-usable socket
	// (ErrBadMessage), NOT dead connections — the caller keeps the conn
	// open and replies with a JSON-RPC parse error instead of tearing down
	// the client and dropping all its subscriptions over one bad frame.
	if string(message) == "" {
		return nil, fmt.Errorf("%w: received empty message", ErrBadMessage)
	}

	msg, batch, err := ParseJsonRpcMessage(message)
	if err != nil {
		if errors.Is(err, ErrInvalidRequest) {
			return nil, fmt.Errorf("parse message: %w", err)
		}
		return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}

	if msg == nil {
		// Parsed as valid JSON but not a single request. A batch array is
		// well-formed but unsupported on the WS path → Invalid Request
		// (-32600), distinct from a Parse Error (-32700).
		if batch != nil {
			return nil, fmt.Errorf("%w: batch requests are not supported over websocket", ErrInvalidRequest)
		}
		return nil, fmt.Errorf("%w: not a single request: %s", ErrInvalidRequest, string(message))
	}

	return msg, nil
}

func (c *JsonRpcWsClient) SendMsg(msg *JsonRpcMsg) error {
	if c.IsClosed() {
		return ErrClosed
	}

	b, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("error encoding msg: %v", err)
	}

	return c.writeMessage(websocket.TextMessage, b)
}

func (c *JsonRpcWsClient) Close() error {
	c.closeMux.Lock()
	if c.closed.Load() {
		c.closeMux.Unlock()
		return ErrClosed
	}
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		if c.closeCh != nil {
			close(c.closeCh)
		}
	})
	c.stopNotificationQueue()

	var err error
	if c.conn != nil {
		err = c.conn.Close()
	}
	c.closeMux.Unlock()

	// Subscription cleanup can round-trip through the upstream connection.
	// Keep it off both websocket reader and notification writer goroutines.
	c.scheduleDisconnectCallback()
	return err
}

func (c *JsonRpcWsClient) scheduleDisconnectCallback() {
	c.cbMu.RLock()
	cb := c.onDisconnect
	c.cbMu.RUnlock()
	if cb == nil {
		return
	}
	c.callbackScheduleOnce.Do(func() {
		go cb(c)
	})
}

func wsNotificationSharedCost(msg *JsonRpcMsg) uint64 {
	cost := addWSNotificationCost(jsonRpcMsgOverheadBytes, uint64(len(msg.Result)))
	cost = addWSNotificationCost(cost, uint64(len(msg.WireSuffix)))
	cost = addWSNotificationCost(cost, uint64(len(msg.Method)))
	cost = addWSNotificationCost(cost, wsRetainedJSONCost(msg.Params))
	if msg.Error != nil {
		cost = addWSNotificationCost(cost, uint64(len(msg.Error.Message)))
		cost = addWSNotificationCost(cost, wsRetainedJSONCost(msg.Error.Data))
	}
	return cost
}

func wsNotificationCostWithID(sharedCost uint64, id interface{}) uint64 {
	return addWSNotificationCost(sharedCost, wsRetainedJSONCost(id))
}

// wsRetainedJSONCost estimates allocations reachable from decoded JSON values.
// Unknown shapes saturate instead of falling back to per-client serialization.
func wsRetainedJSONCost(v interface{}) uint64 {
	switch value := v.(type) {
	case nil, explicitNullIDType:
		return 0
	case string:
		return addWSNotificationCost(16, uint64(len(value)))
	case []byte:
		return addWSNotificationCost(24, uint64(len(value)))
	case bool, float64, float32,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return 16
	case []interface{}:
		cost := addWSNotificationCost(24, uint64(len(value))*16)
		for _, item := range value {
			cost = addWSNotificationCost(cost, wsRetainedJSONCost(item))
			if cost == wsNotificationCostLimit {
				return cost
			}
		}
		return cost
	case map[string]interface{}:
		cost := uint64(48)
		for key, item := range value {
			cost = addWSNotificationCost(cost, 32)
			cost = addWSNotificationCost(cost, uint64(len(key)))
			cost = addWSNotificationCost(cost, wsRetainedJSONCost(item))
			if cost == wsNotificationCostLimit {
				return cost
			}
		}
		return cost
	default:
		return wsNotificationCostLimit
	}
}

func addWSNotificationCost(cost, part uint64) uint64 {
	if cost >= wsNotificationCostLimit || part >= wsNotificationCostLimit || part > wsNotificationCostLimit-cost {
		return wsNotificationCostLimit
	}
	return cost + part
}

func (c *JsonRpcWsClient) enqueueNotification(msg *JsonRpcMsg, cost uint64) error {
	if c.IsClosed() {
		return ErrClosed
	}

	if cost > wsNotificationQueueBytes {
		c.closeForNotificationFailure()
		return errNotificationQueueFull
	}

	c.notificationMux.Lock()
	if c.notificationStopped || c.IsClosed() {
		c.notificationMux.Unlock()
		return ErrClosed
	}
	if c.notificationCount >= wsNotificationQueueMessages || c.notificationBytes+cost > wsNotificationQueueBytes {
		c.notificationMux.Unlock()
		c.closeForNotificationFailure()
		return errNotificationQueueFull
	}
	c.notificationQueue = append(c.notificationQueue, queuedNotification{msg: msg, cost: cost})
	c.notificationCount++
	c.notificationBytes += cost
	c.startNotificationWorkerLocked()
	c.notificationMux.Unlock()
	return nil
}

func (c *JsonRpcWsClient) startNotificationWorkerLocked() {
	if c.notificationRunning {
		return
	}
	c.notificationRunning = true
	c.notificationDone = make(chan struct{})
	go c.runNotificationQueue(c.notificationDone)
}

func (c *JsonRpcWsClient) runNotificationQueue(done chan struct{}) {
	for {
		c.notificationMux.Lock()
		if c.notificationStopped || len(c.notificationQueue) == 0 {
			c.notificationRunning = false
			close(done)
			c.notificationMux.Unlock()
			return
		}
		notification := c.notificationQueue[0]
		c.notificationQueue[0] = queuedNotification{}
		c.notificationQueue = c.notificationQueue[1:]
		c.notificationMux.Unlock()

		err := c.SendMsg(notification.msg)

		c.notificationMux.Lock()
		c.notificationCount--
		c.notificationBytes -= notification.cost
		if err != nil {
			c.notificationStopped = true
			c.notificationRunning = false
			close(done)
			c.notificationMux.Unlock()
			_ = c.Close()
			return
		}
		if c.notificationStopped || len(c.notificationQueue) == 0 {
			c.notificationRunning = false
			close(done)
			c.notificationMux.Unlock()
			return
		}
		c.notificationMux.Unlock()
	}
}

func (c *JsonRpcWsClient) closeForNotificationFailure() {
	c.notificationCloseOnce.Do(func() {
		go func() { _ = c.Close() }()
	})
}

func (c *JsonRpcWsClient) stopNotificationQueue() {
	c.notificationMux.Lock()
	c.notificationStopped = true
	for i := range c.notificationQueue {
		c.notificationBytes -= c.notificationQueue[i].cost
		c.notificationQueue[i] = queuedNotification{}
		c.notificationCount--
	}
	c.notificationQueue = nil
	c.notificationMux.Unlock()
}

func (c *JsonRpcWsClient) SetOnDisconnectCallback(f func(client *JsonRpcWsClient)) {
	c.cbMu.Lock()
	c.onDisconnect = f
	c.cbMu.Unlock()
	if c.IsClosed() {
		c.scheduleDisconnectCallback()
	}
}
