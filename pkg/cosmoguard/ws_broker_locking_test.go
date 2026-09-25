package cosmoguard

import (
	"net/url"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/v5/pkg/util"
	"gotest.tools/assert"
)

// A hung upstream round trip for one param must not stall subscription
// changes for other params.
func TestBrokerSlowUpstreamOnlyBlocksItsParam(t *testing.T) {
	for _, hung := range []string{"unsubscribe", "subscribe"} {
		t.Run(hung, func(t *testing.T) {
			upstream := newLimitingUpstream()
			broker := NewBroker([]string{"ws://upstream.test"}, "/", 1, limitingConstructor(upstream))
			broker.log = log.WithField("test", t.Name())
			subscribe := func(client *JsonRpcWsClient, param string) <-chan brokerCallResult {
				return startBrokerCall(broker, client, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{param},
				}, "alice")
			}
			slow := NewJsonRpcWsClient(nil)
			other := NewJsonRpcWsClient(nil)
			assert.NilError(t, mustRecv(t, subscribe(other, "other"), "other subscribe").err)

			var stalled <-chan brokerCallResult
			if hung == "unsubscribe" {
				assert.NilError(t, mustRecv(t, subscribe(slow, "slow"), "slow subscribe").err)
				upstream.unsubscribeStarted = make(chan struct{}, 1)
				upstream.unsubscribeRelease = make(chan struct{})
				stalled = startBrokerCall(broker, slow, &JsonRpcMsg{
					Version: jsonRpcVersion, ID: 2, Method: methodUnsubscribeCosmos, Params: []any{"slow"},
				}, "alice")
				<-upstream.unsubscribeStarted
				// Unblock only the pending call; later unsubscribes pass.
				upstream.unsubscribeStarted = nil
				defer close(upstream.unsubscribeRelease)
			} else {
				upstream.subscribeStarted = make(chan struct{})
				upstream.subscribeRelease = make(chan struct{})
				stalled = subscribe(slow, "slow")
				<-upstream.subscribeStarted
				defer close(upstream.subscribeRelease)
			}

			// limitingUpstream holds every Subscribe while subscribeStarted
			// is set, so a second subscribe is only possible in the
			// unsubscribe case.
			if hung == "unsubscribe" {
				result := mustRecv(t, subscribe(other, "third"), "subscribe of another param")
				assert.NilError(t, result.err)
				assert.Assert(t, result.response.Error == nil)
			}
			unsubscribed := mustRecv(t, startBrokerCall(broker, other, &JsonRpcMsg{
				Version: jsonRpcVersion, ID: 3, Method: methodUnsubscribeCosmos, Params: []any{"other"},
			}, "alice"), "unsubscribe of another param")
			assert.NilError(t, unsubscribed.err)
			assert.Assert(t, unsubscribed.response.Error == nil, "%+v", unsubscribed.response.Error)

			select {
			case <-stalled:
				t.Fatal("slow call finished before its upstream answered")
			default:
			}
		})
	}
}

// Migration re-subscribes and commits under the param lock, so an
// unsubscribe arriving mid-migration targets the migrated subscription.
func TestBrokerUnsubscribeWaitsForMigrationOfItsParam(t *testing.T) {
	dead, alive := newLimitingUpstream(), newLimitingUpstream()
	alive.subscribeCalls.Store(10)
	alive.healthy.Store(false)
	upstreams := []*limitingUpstream{dead, alive}
	built := 0
	broker := NewBroker([]string{"ws://upstream.test"}, "/", 2, func(url.URL, *util.UniqueID, func(*JsonRpcMsg)) UpstreamConnManager {
		built++
		return upstreams[built-1]
	})
	broker.log = log.WithField("test", t.Name())
	client := NewJsonRpcWsClient(nil)
	_, err := broker.addSubscription(client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 1, Method: methodSubscribeCosmos, Params: []any{"p"},
	})
	assert.NilError(t, err)

	dead.healthy.Store(false)
	alive.healthy.Store(true)
	alive.subscribeStarted = make(chan struct{})
	alive.subscribeRelease = make(chan struct{})
	migrated := make(chan struct{})
	go func() { broker.runMigration(); close(migrated) }()
	mustWait(t, alive.subscribeStarted, "migration re-subscribe")

	unsubscribed := startBrokerCall(broker, client, &JsonRpcMsg{
		Version: jsonRpcVersion, ID: 2, Method: methodUnsubscribeCosmos, Params: []any{"p"},
	}, "")
	select {
	case <-unsubscribed:
		t.Fatal("unsubscribe ran while its param was migrating")
	case <-time.After(100 * time.Millisecond):
	}
	close(alive.subscribeRelease)
	mustWait(t, migrated, "migration")
	result := mustRecv(t, unsubscribed, "unsubscribe")
	assert.NilError(t, result.err)
	assert.Assert(t, result.response.Error == nil, "%+v", result.response.Error)
	assert.Equal(t, dead.unsubscribeCalls.Load(), int32(0))
	assert.Equal(t, alive.unsubscribeCalls.Load(), int32(1))
}
