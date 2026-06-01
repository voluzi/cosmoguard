package cosmoguard

import (
	"maps"
	"sync"
)

type SubscriptionManager struct {
	subscriptionMux sync.RWMutex
	paramToID       map[string]string
	idToParam       map[string]string

	clientsMux          sync.RWMutex
	clientSubscriptions map[string]map[*JsonRpcWsClient]interface{}
}

func NewSubscriptionManager() *SubscriptionManager {
	return &SubscriptionManager{
		paramToID:           make(map[string]string),
		idToParam:           make(map[string]string),
		clientSubscriptions: make(map[string]map[*JsonRpcWsClient]interface{}),
	}
}

func (s *SubscriptionManager) AddSubscription(param string, id string) {
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	s.paramToID[param] = id
	s.idToParam[id] = param

	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	s.clientSubscriptions[id] = make(map[*JsonRpcWsClient]interface{})
}

func (s *SubscriptionManager) RemoveSubscription(id string) {
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	delete(s.paramToID, s.idToParam[id])
	delete(s.idToParam, id)

	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	delete(s.clientSubscriptions, id)
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

// ReplaceSubscriptionID rewires the subscription registry from oldID
// to newID while preserving the existing client set + the param→id
// mapping. Used by the pool's subscription migrator when an upstream
// dies and its subscriptions are re-issued on a healthy upstream
// (which returns a different ID).
//
// Returns true when the swap happened; false when oldID was unknown
// (caller raced with a client unsubscribe).
func (s *SubscriptionManager) ReplaceSubscriptionID(oldID, newID string) bool {
	if oldID == newID {
		return true
	}
	s.subscriptionMux.Lock()
	defer s.subscriptionMux.Unlock()
	param, ok := s.idToParam[oldID]
	if !ok {
		return false
	}
	delete(s.idToParam, oldID)
	s.idToParam[newID] = param
	s.paramToID[param] = newID

	s.clientsMux.Lock()
	defer s.clientsMux.Unlock()
	if clients, ok := s.clientSubscriptions[oldID]; ok {
		s.clientSubscriptions[newID] = clients
		delete(s.clientSubscriptions, oldID)
	}
	return true
}
