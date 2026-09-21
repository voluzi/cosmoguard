package cosmoguard

import (
	"errors"
	"fmt"
	"net/url"
	"sync/atomic"
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

type wsResponseKey struct {
	client *JsonRpcWsClient
	id     string
}

type wsInternalRequestIDs struct {
	sequence atomic.Uint64
}

// next never reuses completed or timed-out IDs, so a late response cannot be
// mistaken for a newer request and no per-timeout reservation needs a waiter.
func (ids *wsInternalRequestIDs) next() string {
	return fmt.Sprintf("cosmoguard-request-%d", ids.sequence.Add(1))
}

func effectiveWSResponseTimeout(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return responseTimeout
}

// uncertainWSUpstreamOutcomeError means a request may have reached the
// upstream even though its caller did not receive a usable acknowledgement.
// Unwrap preserves errors.Is/errors.As checks for the original cause.
type uncertainWSUpstreamOutcomeError struct {
	cause   error
	settled <-chan struct{}
}

func (e *uncertainWSUpstreamOutcomeError) Error() string { return e.cause.Error() }
func (e *uncertainWSUpstreamOutcomeError) Unwrap() error { return e.cause }

func uncertainWSUpstreamOutcome(err error) error {
	return uncertainWSUpstreamOutcomeUntil(err, nil)
}

func uncertainWSUpstreamOutcomeUntil(err error, settled <-chan struct{}) error {
	if err == nil || isUncertainWSUpstreamOutcome(err) {
		return err
	}
	return &uncertainWSUpstreamOutcomeError{cause: err, settled: settled}
}

func isUncertainWSUpstreamOutcome(err error) bool {
	var uncertain *uncertainWSUpstreamOutcomeError
	return errors.As(err, &uncertain)
}

func uncertainWSUpstreamOutcomeSettlement(err error) <-chan struct{} {
	var uncertain *uncertainWSUpstreamOutcomeError
	if !errors.As(err, &uncertain) {
		return nil
	}
	return uncertain.settled
}

func validateWSJSONRPCResponse(method string, response *JsonRpcMsg) error {
	if response == nil {
		return fmt.Errorf("upstream %s returned no response", method)
	}
	if response.Error != nil {
		return fmt.Errorf("upstream %s rejected request with code %d: %s", method, response.Error.Code, response.Error.Message)
	}
	if response.IsEmptyResult() {
		return fmt.Errorf("upstream %s returned a missing result", method)
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
	// LocalUnsubscribe retires a migrated subscription. A nil result means no
	// physical cleanup remains; otherwise the channel closes after the exact
	// subscription record has settled.
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
