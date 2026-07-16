package cosmoguard

import (
	"errors"
	"net/url"
	"time"

	"github.com/voluzi/cosmoguard/pkg/util"
)

const (
	jsonRpcVersion     = "2.0"
	connectTimeout     = 10 * time.Second
	connectRetryPeriod = 5 * time.Second
	responseTimeout    = 10 * time.Second

	// upstreamWSReadLimit caps any single inbound frame from an upstream
	// WS endpoint. gorilla/websocket defaults to "no limit", so without
	// this an upstream sending a multi-GB frame (misbehaving node,
	// compromised endpoint, buggy custom build) would balloon the heap
	// with one allocation. 16 MiB is generous for every realistic
	// subscription payload — Cosmos NewBlock messages on busy mainnets
	// run a few MB, EVM eth_subscribe logs returns are typically <1 MB —
	// while leaving an order-of-magnitude headroom over normal traffic
	// and bounding worst-case abuse at a value the proxy can absorb.
	upstreamWSReadLimit int64 = 16 << 20
)

var (
	ErrSubscriptionExists = errors.New("subscription already exists")
)

type UpstreamConnManagerConstructor func(url.URL, *util.UniqueID, func(msg *JsonRpcMsg)) UpstreamConnManager

type UpstreamConnManager interface {
	Run(*Entry) error
	MakeRequest(*JsonRpcMsg) (*JsonRpcMsg, error)
	HasSubscription(string) bool
	Subscribe(string) (string, error)
	Unsubscribe(string) error
	// LocalUnsubscribe forgets a subscription param from this manager's
	// own bookkeeping WITHOUT a network round-trip. Called by the pool's
	// migrator after a subscription is re-established on a healthy
	// upstream: the source connection is (by definition) unhealthy, so a
	// networked Unsubscribe would fail, and leaving the param in its local
	// maps would make it re-subscribe on reconnect — a permanent duplicate
	// upstream subscription. Idempotent; unknown params are a no-op.
	LocalUnsubscribe(param string)
	// IsHealthy reports whether the underlying WS connection is in a
	// usable state. Returns false when the connection is closed, nil,
	// or stuck in reconnect backoff. Used by the pool's subscription
	// migrator to detect dead backends so it can re-route their
	// subscriptions onto a survivor connection.
	IsHealthy() bool
	// Stop ends the Run goroutine and closes the live WS connection,
	// if any. Idempotent. Called from UpstreamPool.Stop during
	// CosmoGuard.Shutdown so we don't leak the Run goroutines.
	Stop()
}
