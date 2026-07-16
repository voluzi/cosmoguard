package cosmoguard

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	ErrClosed = errors.New("websocket client closed")
	// ErrBadMessage marks a frame that failed to decode as JSON-RPC on an
	// otherwise-healthy connection (empty frame, malformed JSON, nil
	// message). Unlike a read error, the socket is still usable, so callers
	// should reply with a JSON-RPC parse error and keep the connection open
	// rather than disconnecting the client (and dropping its subscriptions).
	ErrBadMessage = errors.New("bad json rpc message")
)

// wsClientWriteDeadline caps how long a single WriteMessage can park
// on a slow / stuck WS client. Without it, the broker's per-message
// fan-out goroutines (ws_broker.go:onSubscriptionMessage) accumulate
// indefinitely against a single misbehaving subscriber — every
// upstream message spawns a goroutine that queues behind writeMux
// and never returns. 10s is a generous upper bound for a normal
// TCP write; anything longer is a sign the client is gone or its
// kernel buffer is wedged, and the broker should reclaim the
// goroutine rather than wait.
const wsClientWriteDeadline = 10 * time.Second

type JsonRpcWsClient struct {
	conn   *websocket.Conn
	closed bool

	closedMux sync.RWMutex
	writeMux  sync.Mutex
	readMux   sync.Mutex

	// closeCh is closed exactly once by Close() so callers parked in
	// a select (e.g. makeRequestWithIDOnClient waiting for a response)
	// can wake up immediately on disconnect instead of stalling until
	// responseTimeout. closeOnce guards the close().
	closeCh   chan struct{}
	closeOnce sync.Once

	// onDisconnect is written by SetOnDisconnectCallback (called from
	// the broker's HandleSubscription goroutine on first subscribe)
	// and read by Close() (which can fire from the reader goroutine
	// or any caller observing a dead socket). Guard with cbMu so the
	// race detector stays quiet and the read sees a consistent value.
	cbMu         sync.RWMutex
	onDisconnect func(client *JsonRpcWsClient)
}

func (c *JsonRpcWsClient) String() string {
	return c.conn.RemoteAddr().String()
}

func NewJsonRpcWsClient(conn *websocket.Conn) *JsonRpcWsClient {
	return &JsonRpcWsClient{
		conn:    conn,
		closed:  false,
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

func (c *JsonRpcWsClient) setClosed() {
	c.closedMux.Lock()
	defer c.closedMux.Unlock()
	c.closed = true
}

func (c *JsonRpcWsClient) IsClosed() bool {
	if c == nil {
		return true
	}
	c.closedMux.RLock()
	defer c.closedMux.RUnlock()
	return c.closed
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

	msg, _, err := ParseJsonRpcMessage(message)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadMessage, err)
	}

	if msg == nil {
		return nil, fmt.Errorf("%w: not a single request: %s", ErrBadMessage, string(message))
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
	if c.IsClosed() {
		return ErrClosed
	}

	c.cbMu.RLock()
	cb := c.onDisconnect
	c.cbMu.RUnlock()
	if cb != nil {
		cb(c)
	}

	c.writeMux.Lock()
	defer c.writeMux.Unlock()

	// conn.Close errors (e.g. broken pipe on an already-half-dead
	// socket) must NOT short-circuit the closed-state bookkeeping:
	// callers parked on Closed() rely on the channel firing on every
	// disconnect, and IsClosed() must reflect reality once Close()
	// returns regardless of what the underlying conn reported.
	err := c.conn.Close()
	c.setClosed()
	c.closeOnce.Do(func() { close(c.closeCh) })
	return err
}

func (c *JsonRpcWsClient) SetOnDisconnectCallback(f func(client *JsonRpcWsClient)) {
	c.cbMu.Lock()
	c.onDisconnect = f
	c.cbMu.Unlock()
}
