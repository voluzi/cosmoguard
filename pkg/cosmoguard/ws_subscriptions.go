package cosmoguard

import (
	"maps"
	"sync"
)

type SubscriptionManager struct {
	subscriptionMux sync.RWMutex
	paramToID       map[string]string
	idToParam       map[string]string

	// The SubscriptionManager is keyed throughout by a STABLE "canonical"
	// subscription id (the id the client was first handed). The underlying
	// upstream subscription id can change when a subscription is migrated
	// off a dead upstream onto a healthy one (the new upstream mints a
	// different id). These two maps translate between the canonical id and
	// the current upstream id so:
	//   - inbound notifications (which carry the CURRENT upstream id) route
	//     to the right client set (upstreamToCanonical), and
	//   - the broker can call pool.Unsubscribe with the CURRENT upstream id
	//     (canonicalToUpstream).
	// For EVM subscriptions the canonical id is also embedded verbatim in
	// each notification's params.subscription, so keeping it stable is what
	// stops web3 clients from silently dropping events after a migration.
	upstreamToCanonical map[string]string
	canonicalToUpstream map[string]string

	clientsMux          sync.RWMutex
	clientSubscriptions map[string]map[*JsonRpcWsClient]interface{}
}

func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		paramToID:           make(map[string]string),
		idToParam:           make(map[string]string),
		upstreamToCanonical: make(map[string]string),
		canonicalToUpstream: make(map[string]string),
		clientSubscriptions: make(map[string]map[*JsonRpcWsClient]interface{}),
	}
}

// AddSubscription registers a new upstream subscription. `id` is both the
// canonical (stable, client-facing) id and the initial upstream id — they
// only diverge later if Migrate moves the subscription to another upstream.
func (s *SubscriptionManager) AddSubscription(param string, id string) {
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	s.paramToID[param] = id
	s.idToParam[id] = param
	s.upstreamToCanonical[id] = id
	s.canonicalToUpstream[id] = id

	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	s.clientSubscriptions[id] = make(map[*JsonRpcWsClient]interface{})
}

// RemoveSubscription drops a subscription by its canonical id, including
// the canonical↔upstream translation entries.
func (s *SubscriptionManager) RemoveSubscription(id string) {
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	delete(s.paramToID, s.idToParam[id])
	delete(s.idToParam, id)
	if upstream, ok := s.canonicalToUpstream[id]; ok {
		delete(s.upstreamToCanonical, upstream)
		delete(s.canonicalToUpstream, id)
	}

	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	delete(s.clientSubscriptions, id)
}

// CanonicalID maps a current upstream subscription id to its stable
// canonical id. Returns (upstreamID, true) unchanged when there is no
// translation entry, so callers can use the result directly.
func (s *SubscriptionManager) CanonicalID(upstreamID string) (string, bool) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	if canonical, ok := s.upstreamToCanonical[upstreamID]; ok {
		return canonical, true
	}
	return upstreamID, false
}

// UpstreamID maps a canonical subscription id to its current upstream id
// (what pool.Subscribe/Unsubscribe key on). Returns (canonicalID, false)
// when unknown so callers can fall back to the canonical id.
func (s *SubscriptionManager) UpstreamID(canonicalID string) (string, bool) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	if upstream, ok := s.canonicalToUpstream[canonicalID]; ok {
		return upstream, true
	}
	return canonicalID, false
}

func (s *SubscriptionManager) SubscriptionEmpty(id string) bool {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()
	return len(s.clientSubscriptions[id]) == 0
}

func (s *SubscriptionManager) SubscribeClient(id string, client *JsonRpcWsClient, clientSubID interface{}) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	s.clientSubscriptions[id][client] = clientSubID
}

func (s *SubscriptionManager) UnsubscribeClient(id string, client *JsonRpcWsClient) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	delete(s.clientSubscriptions[id], client)
}

func (s *SubscriptionManager) ClientSubscribed(id string, client *JsonRpcWsClient) bool {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()
	_, ok := s.clientSubscriptions[id][client]
	return ok
}

func (s *SubscriptionManager) GetSubscriptions(client *JsonRpcWsClient) []string {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()

	list := make([]string, 0)
	for id, m := range s.clientSubscriptions {
		if _, ok := m[client]; ok {
			list = append(list, id)
		}
	}
	return list
}

func (s *SubscriptionManager) GetSubscriptionID(param string) (string, bool) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	id, ok := s.paramToID[param]
	return id, ok
}

func (s *SubscriptionManager) GetSubscriptionParam(id string) (string, bool) {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	q, ok := s.idToParam[id]
	return q, ok
}

func (s *SubscriptionManager) GetSubscriptionClients(id string) map[*JsonRpcWsClient]interface{} {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()
	return maps.Clone(s.clientSubscriptions[id])
}

// SubStat is one upstream subscription's dashboard view: the
// subscription param (e.g. tm.event='NewBlock' / newHeads), the
// upstream id it was minted as, and how many clients are fanned out
// from it.
type SubStat struct {
	ID          string `json:"-"`
	Param       string `json:"param"`
	Subscribers int    `json:"subscribers"`
}

// Snapshot returns one SubStat per active upstream subscription. The
// subscriber count is the number of clients fanned out from each
// upstream subscription — the dedup ratio between client-facing and
// upstream-facing subscriptions is the panel's headline number.
func (s *SubscriptionManager) Snapshot() []SubStat {
	s.subscriptionMux.RLock()
	defer s.subscriptionMux.RUnlock()
	s.clientsMux.RLock()
	defer s.clientsMux.RUnlock()
	out := make([]SubStat, 0, len(s.idToParam))
	for id, param := range s.idToParam {
		out = append(out, SubStat{ID: id, Param: param, Subscribers: len(s.clientSubscriptions[id])})
	}
	return out
}

// Migrate repoints a subscription from an old upstream id to a new one
// after the pool re-subscribed it on a healthy upstream. Crucially it
// leaves the canonical (client-facing) id, the param→canonical mapping,
// and the client set UNCHANGED — only the canonical↔upstream translation
// is updated. This is what keeps the client-facing subscription id stable
// across a migration: EVM clients match notifications on
// params.subscription (the canonical id), and a client's
// eth_unsubscribe(canonicalID) keeps resolving.
//
// oldUpstreamID / newUpstreamID are the pool's subscription ids before and
// after the re-subscribe. Returns true when the swap happened; false when
// oldUpstreamID was unknown (caller raced with a client unsubscribe).
func (s *SubscriptionManager) Migrate(oldUpstreamID, newUpstreamID string) bool {
	if oldUpstreamID == newUpstreamID {
		return true
	}
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	canonical, ok := s.upstreamToCanonical[oldUpstreamID]
	if !ok {
		return false
	}
	delete(s.upstreamToCanonical, oldUpstreamID)
	s.upstreamToCanonical[newUpstreamID] = canonical
	s.canonicalToUpstream[canonical] = newUpstreamID
	return true
}
