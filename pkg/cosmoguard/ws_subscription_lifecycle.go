package cosmoguard

import (
	"errors"
	"fmt"
	"sync"
)

type wsSubscriptionState uint8

const (
	wsSubscriptionCreating wsSubscriptionState = iota
	wsSubscriptionActive
	wsSubscriptionRemoving
	wsSubscriptionDraining
	wsSubscriptionRetired
)

type wsSubscriptionBinding struct {
	client *JsonRpcWsClient
	wireID string
}

type wsSubscriptionRecord struct {
	param       string
	handle      string
	reservation string
	state       wsSubscriptionState
	desired     bool
	binding     *wsSubscriptionBinding
	// early routes events that arrive before subscribeOn returns: the
	// upstream may send one right behind its acknowledgement.
	early   *wsSubscriptionBindingKey
	settled chan struct{}
}

type wsSubscriptionExchange interface {
	subscribeOn(*JsonRpcWsClient, string, string, bool) (string, error)
	unsubscribeOn(wsSubscriptionBinding, string) error
	stableHandle(string) string
	releaseHandle(string)
	// knownWireID reports the wire id a subscribe sent with request id id
	// will carry, when the protocol fixes it in advance.
	knownWireID(id string) (string, bool)
}

type wsSubscriptionLifecycle struct {
	opMu sync.Mutex
	mu   sync.RWMutex

	current  *JsonRpcWsClient
	stopped  bool
	byParam  map[string]*wsSubscriptionRecord
	byHandle map[string]*wsSubscriptionRecord
	byWire   map[wsSubscriptionBindingKey]*wsSubscriptionRecord
}

type wsSubscriptionBindingKey struct {
	client *JsonRpcWsClient
	wireID string
}

func newWSSubscriptionLifecycle() *wsSubscriptionLifecycle {
	return &wsSubscriptionLifecycle{
		byParam:  make(map[string]*wsSubscriptionRecord),
		byHandle: make(map[string]*wsSubscriptionRecord),
		byWire:   make(map[wsSubscriptionBindingKey]*wsSubscriptionRecord),
	}
}

func (l *wsSubscriptionLifecycle) install(client *JsonRpcWsClient) bool {
	l.opMu.Lock()
	defer l.opMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	l.current = client
	return true
}

func (l *wsSubscriptionLifecycle) currentClient() *JsonRpcWsClient {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.current
}

func (l *wsSubscriptionLifecycle) hasParam(param string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record := l.byParam[param]
	return record != nil && record.desired
}

func (l *wsSubscriptionLifecycle) lookupParam(param string) *wsSubscriptionRecord {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.byParam[param]
}

func (l *wsSubscriptionLifecycle) reservationForHandle(handle string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record := l.byHandle[handle]
	if record == nil || !record.desired {
		return "", false
	}
	return record.reservation, true
}

func (l *wsSubscriptionLifecycle) route(client *JsonRpcWsClient, wireID string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	record := l.byWire[wsSubscriptionBindingKey{client: client, wireID: wireID}]
	if record == nil || !record.desired || (record.state != wsSubscriptionActive && record.state != wsSubscriptionCreating) {
		return "", false
	}
	return record.handle, true
}

// bindEarly routes wireID on client to the record for param before the
// subscribe that produced wireID has returned.
func (l *wsSubscriptionLifecycle) bindEarly(client *JsonRpcWsClient, param, wireID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if record := l.byParam[param]; record != nil && record.desired {
		l.bindEarlyLocked(record, client, wireID)
	}
}

func (l *wsSubscriptionLifecycle) bindEarlyLocked(record *wsSubscriptionRecord, client *JsonRpcWsClient, wireID string) {
	l.unbindEarlyLocked(record)
	key := wsSubscriptionBindingKey{client: client, wireID: wireID}
	l.byWire[key] = record
	record.early = &key
}

func (l *wsSubscriptionLifecycle) unbindEarlyLocked(record *wsSubscriptionRecord) {
	if record.early == nil {
		return
	}
	if l.byWire[*record.early] == record {
		delete(l.byWire, *record.early)
	}
	record.early = nil
}

// subscribe creates the subscription for param. created, when set, receives
// the subscription's handle before the subscribe request is sent, so the
// caller can route events that follow the acknowledgement immediately.
func (l *wsSubscriptionLifecycle) subscribe(param, provisional string, client *JsonRpcWsClient, exchange wsSubscriptionExchange, created func(string)) (string, error) {
	l.opMu.Lock()
	defer l.opMu.Unlock()

	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		exchange.releaseHandle(provisional)
		return "", ErrClosed
	}
	if existing := l.byParam[param]; existing != nil && existing.desired {
		l.mu.Unlock()
		exchange.releaseHandle(provisional)
		return "", ErrSubscriptionExists
	}
	handle := exchange.stableHandle(provisional)
	record := &wsSubscriptionRecord{
		param: param, handle: handle, reservation: provisional, state: wsSubscriptionCreating,
		desired: true, settled: make(chan struct{}),
	}
	l.byParam[param] = record
	l.byHandle[handle] = record
	if wireID, ok := exchange.knownWireID(provisional); ok {
		l.bindEarlyLocked(record, client, wireID)
	}
	l.mu.Unlock()

	if created != nil {
		created(handle)
	}
	wireID, err := exchange.subscribeOn(client, provisional, param, false)
	if err != nil {
		l.mu.Lock()
		l.unbindEarlyLocked(record)
		if record.state == wsSubscriptionRetired {
			l.mu.Unlock()
			return "", err
		}
		if l.byParam[param] == record {
			delete(l.byParam, param)
		}
		if l.byHandle[handle] == record {
			delete(l.byHandle, handle)
		}
		record.desired = false
		if isUncertainWSUpstreamOutcome(err) {
			record.state = wsSubscriptionDraining
			record.binding = &wsSubscriptionBinding{client: client, wireID: wireID}
			binding := record.binding
			settled := record.settled
			l.mu.Unlock()
			l.watchDrain(record, binding, true, exchange)
			return "", uncertainWSUpstreamOutcomeWithSettlement(err, settled)
		}
		record.state = wsSubscriptionRetired
		close(record.settled)
		l.mu.Unlock()
		exchange.releaseHandle(provisional)
		return "", err
	}

	l.mu.Lock()
	l.unbindEarlyLocked(record)
	if record.state == wsSubscriptionRetired {
		l.mu.Unlock()
		return "", ErrClosed
	}
	if l.stopped {
		record.desired = false
		record.state = wsSubscriptionDraining
		record.binding = &wsSubscriptionBinding{client: client, wireID: wireID}
		binding := record.binding
		delete(l.byParam, param)
		delete(l.byHandle, handle)
		l.mu.Unlock()
		l.watchDrain(record, binding, true, exchange)
		return "", ErrClosed
	}
	record.state = wsSubscriptionActive
	record.binding = &wsSubscriptionBinding{client: client, wireID: wireID}
	l.byWire[wsSubscriptionBindingKey{client: client, wireID: wireID}] = record
	l.mu.Unlock()
	return handle, nil
}

func (l *wsSubscriptionLifecycle) unsubscribe(handle string, exchange wsSubscriptionExchange) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()

	l.mu.Lock()
	record := l.byHandle[handle]
	if record == nil || !record.desired {
		l.mu.Unlock()
		return fmt.Errorf("subscription with ID %s not found", handle)
	}
	record.state = wsSubscriptionRemoving
	binding := record.binding
	l.mu.Unlock()

	if binding == nil || binding.client.IsClosed() {
		l.settle(record, true, exchange)
		return nil
	}
	err := exchange.unsubscribeOn(*binding, record.param)
	if err == nil {
		l.settle(record, true, exchange)
		return nil
	}

	l.mu.Lock()
	if record.state == wsSubscriptionRetired {
		settled := record.settled
		l.mu.Unlock()
		return uncertainWSUpstreamOutcomeWithSettlement(err, settled)
	}
	if !isUncertainWSUpstreamOutcome(err) && !binding.client.IsClosed() {
		record.state = wsSubscriptionActive
		l.mu.Unlock()
		return err
	}
	record.desired = false
	record.state = wsSubscriptionDraining
	if l.byParam[record.param] == record {
		delete(l.byParam, record.param)
	}
	if l.byHandle[record.handle] == record {
		delete(l.byHandle, record.handle)
	}
	delete(l.byWire, wsSubscriptionBindingKey{client: binding.client, wireID: binding.wireID})
	l.mu.Unlock()
	l.watchDrain(record, binding, true, exchange)
	return uncertainWSUpstreamOutcomeWithSettlement(err, record.settled)
}

func (l *wsSubscriptionLifecycle) retire(record *wsSubscriptionRecord, exchange wsSubscriptionExchange) <-chan error {
	return l.retireWithHandleRelease(record, true, exchange)
}

func (l *wsSubscriptionLifecycle) retirePreservingHandle(record *wsSubscriptionRecord, exchange wsSubscriptionExchange) <-chan error {
	return l.retireWithHandleRelease(record, false, exchange)
}

func (l *wsSubscriptionLifecycle) retireWithHandleRelease(record *wsSubscriptionRecord, releaseHandle bool, exchange wsSubscriptionExchange) <-chan error {
	if record == nil {
		return nil
	}
	result := make(chan error, 1)
	go func() {
		l.opMu.Lock()
		l.mu.Lock()
		if l.byParam[record.param] != record || !record.desired {
			settled := record.settled
			l.mu.Unlock()
			l.opMu.Unlock()
			<-settled
			result <- nil
			close(result)
			return
		}
		record.desired = false
		record.state = wsSubscriptionRemoving
		delete(l.byParam, record.param)
		delete(l.byHandle, record.handle)
		binding := record.binding
		if binding != nil {
			delete(l.byWire, wsSubscriptionBindingKey{client: binding.client, wireID: binding.wireID})
		}
		l.mu.Unlock()

		if binding == nil || binding.client.IsClosed() {
			l.settle(record, releaseHandle, exchange)
			l.opMu.Unlock()
			result <- nil
			close(result)
			return
		}
		err := exchange.unsubscribeOn(*binding, record.param)
		if err == nil {
			l.settle(record, releaseHandle, exchange)
			l.opMu.Unlock()
			result <- nil
			close(result)
			return
		}
		l.mu.Lock()
		if record.state != wsSubscriptionRetired {
			record.state = wsSubscriptionDraining
		}
		l.mu.Unlock()
		l.watchDrain(record, binding, releaseHandle, exchange)
		settled := record.settled
		l.opMu.Unlock()
		<-settled
		result <- nil
		close(result)
	}()
	return result
}

func (l *wsSubscriptionLifecycle) resubmit(client *JsonRpcWsClient, exchange wsSubscriptionExchange) error {
	l.mu.RLock()
	records := make([]*wsSubscriptionRecord, 0, len(l.byParam))
	for _, record := range l.byParam {
		records = append(records, record)
	}
	l.mu.RUnlock()

	for _, record := range records {
		if err := l.resubmitRecord(client, record, exchange); err != nil {
			return err
		}
	}
	return nil
}

func (l *wsSubscriptionLifecycle) resubmitRecord(client *JsonRpcWsClient, record *wsSubscriptionRecord, exchange wsSubscriptionExchange) error {
	l.opMu.Lock()
	defer l.opMu.Unlock()

	l.mu.Lock()
	if l.stopped || l.current != client || l.byParam[record.param] != record || !record.desired || record.state != wsSubscriptionActive {
		l.mu.Unlock()
		return nil
	}
	if record.binding != nil && record.binding.client == client && !client.IsClosed() {
		l.mu.Unlock()
		return nil
	}
	oldBinding := record.binding
	if oldBinding != nil {
		delete(l.byWire, wsSubscriptionBindingKey{client: oldBinding.client, wireID: oldBinding.wireID})
	}
	record.binding = nil
	if wireID, ok := exchange.knownWireID(record.handle); ok {
		l.bindEarlyLocked(record, client, wireID)
	}
	l.mu.Unlock()

	wireID, err := exchange.subscribeOn(client, record.handle, record.param, true)
	if err != nil {
		l.mu.Lock()
		l.unbindEarlyLocked(record)
		if l.byParam[record.param] == record && record.desired {
			if isUncertainWSUpstreamOutcome(err) {
				record.binding = &wsSubscriptionBinding{client: client, wireID: wireID}
			} else {
				record.binding = oldBinding
			}
		}
		l.mu.Unlock()
		return err
	}

	l.mu.Lock()
	l.unbindEarlyLocked(record)
	if l.stopped || l.current != client || l.byParam[record.param] != record || !record.desired {
		l.mu.Unlock()
		binding := wsSubscriptionBinding{client: client, wireID: wireID}
		if err := exchange.unsubscribeOn(binding, record.param); err != nil && !binding.client.IsClosed() {
			return err
		}
		return nil
	}
	record.binding = &wsSubscriptionBinding{client: client, wireID: wireID}
	l.byWire[wsSubscriptionBindingKey{client: client, wireID: wireID}] = record
	l.mu.Unlock()
	return nil
}

func (l *wsSubscriptionLifecycle) settle(record *wsSubscriptionRecord, releaseHandle bool, exchange wsSubscriptionExchange) {
	l.mu.Lock()
	if record.state == wsSubscriptionRetired {
		l.mu.Unlock()
		return
	}
	if l.byParam[record.param] == record {
		delete(l.byParam, record.param)
	}
	if l.byHandle[record.handle] == record {
		delete(l.byHandle, record.handle)
	}
	if record.binding != nil {
		delete(l.byWire, wsSubscriptionBindingKey{client: record.binding.client, wireID: record.binding.wireID})
	}
	l.unbindEarlyLocked(record)
	record.desired = false
	record.state = wsSubscriptionRetired
	record.binding = nil
	close(record.settled)
	reservation := record.reservation
	l.mu.Unlock()
	if releaseHandle {
		exchange.releaseHandle(reservation)
	}
}

func (l *wsSubscriptionLifecycle) watchDrain(record *wsSubscriptionRecord, binding *wsSubscriptionBinding, releaseHandle bool, exchange wsSubscriptionExchange) {
	if binding == nil || binding.client == nil {
		l.settle(record, releaseHandle, exchange)
		return
	}
	select {
	case <-binding.client.Closed():
		l.settle(record, releaseHandle, exchange)
	default:
		go func() {
			<-binding.client.Closed()
			l.opMu.Lock()
			l.mu.RLock()
			same := record.state == wsSubscriptionDraining && record.binding == binding
			l.mu.RUnlock()
			if same {
				l.settle(record, releaseHandle, exchange)
			}
			l.opMu.Unlock()
		}()
	}
}

func (l *wsSubscriptionLifecycle) stop(exchange wsSubscriptionExchange) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	client := l.current
	l.current = nil
	records := make([]*wsSubscriptionRecord, 0, len(l.byParam)+len(l.byHandle))
	seen := make(map[*wsSubscriptionRecord]struct{})
	for _, record := range l.byParam {
		if _, ok := seen[record]; !ok {
			seen[record] = struct{}{}
			records = append(records, record)
		}
	}
	for _, record := range l.byHandle {
		if _, ok := seen[record]; !ok {
			seen[record] = struct{}{}
			records = append(records, record)
		}
	}
	l.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	for _, record := range records {
		l.settle(record, true, exchange)
	}
}

func uncertainWSUpstreamOutcomeWithSettlement(err error, settled <-chan struct{}) error {
	var uncertain *uncertainWSUpstreamOutcomeError
	if !errors.As(err, &uncertain) {
		return uncertainWSUpstreamOutcomeUntil(err, settled)
	}
	return &uncertainWSUpstreamOutcomeError{cause: uncertain.cause, settled: settled}
}
