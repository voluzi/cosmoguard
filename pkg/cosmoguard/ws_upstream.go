package cosmoguard

import (
	"errors"
	"fmt"
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

// uncertainWSUpstreamOutcomeError means a request may have reached the
// upstream even though its caller did not receive a usable acknowledgement.
// Unwrap preserves errors.Is/errors.As checks for the original cause.
type uncertainWSUpstreamOutcomeError struct {
	cause error
}

func (e *uncertainWSUpstreamOutcomeError) Error() string { return e.cause.Error() }
func (e *uncertainWSUpstreamOutcomeError) Unwrap() error { return e.cause }

func uncertainWSUpstreamOutcome(err error) error {
	if err == nil || isUncertainWSUpstreamOutcome(err) {
		return err
	}
	return &uncertainWSUpstreamOutcomeError{cause: err}
}

func isUncertainWSUpstreamOutcome(err error) bool {
	var uncertain *uncertainWSUpstreamOutcomeError
	return errors.As(err, &uncertain)
}

func validateWSJSONRPCResponse(method string, response *JsonRpcMsg) error {
	if response == nil {
		return fmt.Errorf("upstream %s returned no response", method)
	}
	if response.Error != nil {
		return fmt.Errorf("upstream %s rejected request with code %d: %s", method, response.Error.Code, response.Error.Message)
	}
	return nil
}

type UpstreamConnManagerConstructor func(url.URL, *util.UniqueID, func(msg *JsonRpcMsg)) UpstreamConnManager

type UpstreamConnManager interface {
	Run(*Entry) error
	MakeRequest(*JsonRpcMsg) (*JsonRpcMsg, error)
	HasSubscription(string) bool
	Subscribe(string) (string, error)
	Unsubscribe(string) error
	// LocalUnsubscribe forgets and tombstones a migrated subscription. A nil
	// result means no live socket cleanup remains; otherwise the channel yields
	// the cleanup outcome exactly once without blocking migration routing.
	LocalUnsubscribe(param string) <-chan error
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
