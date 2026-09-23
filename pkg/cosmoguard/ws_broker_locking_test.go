package cosmoguard

import (
	"testing"

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
