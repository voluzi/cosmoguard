package cosmoguard

import (
	"errors"
	"fmt"
	"sync"
)

const (
	defaultMaxSubscriptionsPerClient             = 32
	defaultMaxSubscriptionsPerIdentity           = 128
	defaultMaxSubscriptionsPerUpstreamConnection = 10
	defaultMaxConnectionsPerIP                   = 16

	wsLimitScopeClient             = "client"
	wsLimitScopeIdentity           = "identity"
	wsLimitScopeUpstreamConnection = "upstream_connection"
	wsLimitScopeSourceIP           = "source_ip"
)

// WebSocketLimits is the effective, process-local WebSocket admission policy.
// A zero field disables that limit.
type WebSocketLimits struct {
	MaxSubscriptionsPerClient             int `json:"max_subscriptions_per_client"`
	MaxSubscriptionsPerIdentity           int `json:"max_subscriptions_per_identity"`
	MaxSubscriptionsPerUpstreamConnection int `json:"max_subscriptions_per_upstream_connection"`
	MaxConnectionsPerIP                   int `json:"max_connections_per_ip"`
}

// WSResourceExhaustedError identifies a bounded admission scope without
// exposing the current usage or authenticated identity.
type WSResourceExhaustedError struct {
	Scope string
	Limit int
}

func (e *WSResourceExhaustedError) Error() string {
	return fmt.Sprintf("websocket resource exhausted: %s limit %d", e.Scope, e.Limit)
}

func wsResourceExhaustedData(err error) map[string]any {
	var exhausted *WSResourceExhaustedError
	if !errors.As(err, &exhausted) {
		return nil
	}
	return map[string]any{"scope": exhausted.Scope, "limit": exhausted.Limit}
}

type wsAdmissionController struct {
	mu sync.Mutex

	limits WebSocketLimits

	connectionsByIP map[string]int
	clientSubs      map[*JsonRpcWsClient]int
	identitySubs    map[string]int
	clientIdentity  map[*JsonRpcWsClient]string
}

func newWSAdmissionController(limits WebSocketLimits) *wsAdmissionController {
	return &wsAdmissionController{
		limits:          limits,
		connectionsByIP: make(map[string]int),
		clientSubs:      make(map[*JsonRpcWsClient]int),
		identitySubs:    make(map[string]int),
		clientIdentity:  make(map[*JsonRpcWsClient]string),
	}
}

func (a *wsAdmissionController) Limits() WebSocketLimits {
	if a == nil {
		return WebSocketLimits{}
	}
	return a.limits
}

func (a *wsAdmissionController) reserveConnection(sourceIP string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	limit := a.limits.MaxConnectionsPerIP
	if limit > 0 && a.connectionsByIP[sourceIP] >= limit {
		return &WSResourceExhaustedError{Scope: wsLimitScopeSourceIP, Limit: limit}
	}
	a.connectionsByIP[sourceIP]++
	return nil
}

func (a *wsAdmissionController) releaseConnection(sourceIP string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.connectionsByIP[sourceIP] <= 1 {
		delete(a.connectionsByIP, sourceIP)
		return
	}
	a.connectionsByIP[sourceIP]--
}

func (a *wsAdmissionController) reserveSubscription(client *JsonRpcWsClient, identity string) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if limit := a.limits.MaxSubscriptionsPerClient; limit > 0 && a.clientSubs[client] >= limit {
		return &WSResourceExhaustedError{Scope: wsLimitScopeClient, Limit: limit}
	}
	if limit := a.limits.MaxSubscriptionsPerIdentity; identity != "" && limit > 0 && a.identitySubs[identity] >= limit {
		return &WSResourceExhaustedError{Scope: wsLimitScopeIdentity, Limit: limit}
	}
	a.clientSubs[client]++
	a.clientIdentity[client] = identity
	if identity != "" {
		a.identitySubs[identity]++
	}
	return nil
}

func (a *wsAdmissionController) releaseSubscription(client *JsonRpcWsClient) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	count := a.clientSubs[client]
	if count == 0 {
		return
	}
	identity := a.clientIdentity[client]
	if count == 1 {
		delete(a.clientSubs, client)
		delete(a.clientIdentity, client)
	} else {
		a.clientSubs[client] = count - 1
	}
	if identity == "" {
		return
	}
	if a.identitySubs[identity] <= 1 {
		delete(a.identitySubs, identity)
	} else {
		a.identitySubs[identity]--
	}
}
