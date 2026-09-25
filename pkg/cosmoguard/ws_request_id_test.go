package cosmoguard

import (
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/v5/pkg/util"
	"gotest.tools/assert"
)

func TestOrdinaryRPCTimeoutsDoNotRetainRequestIDsOrWaiters(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			const requestCount = 8
			var uniqueIDCalls atomic.Int32
			idGen := util.NewUniqueID(func() string {
				uniqueIDCalls.Add(1)
				return "subscription-id"
			})

			client, peer := newWSCacheClient(t)
			manager := protocol.constructor(url.URL{}, idGen, func(*JsonRpcMsg) {})
			setManagerClient(manager, client)
			setManagerLog(manager, log.WithField("test", t.Name()))
			setManagerRequestTimeout(manager, 100*time.Millisecond)

			readerDone := make(chan struct{})
			go func() {
				defer close(readerDone)
				for {
					msg, err := client.ReceiveMsg()
					if err != nil {
						return
					}
					dispatchManagerMessage(manager, msg)
				}
			}()
			baselineGoroutines := runtime.NumGoroutine()

			requests := make([]*JsonRpcMsg, 0, requestCount)
			requestIDs := make(map[string]struct{}, requestCount)
			for i := 0; i < requestCount; i++ {
				result := make(chan error, 1)
				go func() {
					_, err := manager.MakeRequest(&JsonRpcMsg{Version: jsonRpcVersion, Method: "status"})
					result <- err
				}()
				var request JsonRpcMsg
				assert.NilError(t, peer.ReadJSON(&request))
				requestID, ok := request.ID.(string)
				assert.Assert(t, ok)
				assert.Assert(t, strings.HasPrefix(requestID, "cosmoguard-request-"))
				_, duplicate := requestIDs[requestID]
				assert.Assert(t, !duplicate, "ordinary RPC reused request ID %q", requestID)
				requestIDs[requestID] = struct{}{}
				requests = append(requests, &request)
				assert.Assert(t, isUncertainWSUpstreamOutcome(mustRecv(t, result, "ordinary RPC timeout")))
				assert.Equal(t, managerResponseCount(manager), 0)
			}
			assert.Equal(t, uniqueIDCalls.Load(), int32(0), "ordinary RPC must not consume subscription IDs")

			for _, request := range requests {
				encoded, err := WithResult(request, map[string]any{"late": true}).Marshal()
				assert.NilError(t, err)
				assert.NilError(t, peer.WriteMessage(websocket.TextMessage, encoded))
			}
			finalResult := make(chan error, 1)
			go func() {
				_, err := manager.MakeRequest(&JsonRpcMsg{Version: jsonRpcVersion, Method: "status"})
				finalResult <- err
			}()
			var finalRequest JsonRpcMsg
			assert.NilError(t, peer.ReadJSON(&finalRequest))
			encoded, err := WithResult(&finalRequest, map[string]any{"current": true}).Marshal()
			assert.NilError(t, err)
			assert.NilError(t, peer.WriteMessage(websocket.TextMessage, encoded))
			assert.NilError(t, mustRecv(t, finalResult, "final ordinary RPC result"))
			assert.Equal(t, managerResponseCount(manager), 0)
			runtime.Gosched()
			assert.Assert(t, runtime.NumGoroutine() <= baselineGoroutines+2)

			id := idGen.ID()
			assert.Equal(t, id, "subscription-id")
			assert.Equal(t, uniqueIDCalls.Load(), int32(1))
			idGen.Release(id)

			assert.NilError(t, client.Close())
			mustWait(t, readerDone, "ordinary RPC reader shutdown")
		})
	}
}

func setManagerRequestTimeout(manager UpstreamConnManager, timeout time.Duration) {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.requestTimeout = timeout
	case *UpstreamConnManagerEth:
		manager.requestTimeout = timeout
	}
}

func managerResponseCount(manager UpstreamConnManager) int {
	switch manager := manager.(type) {
	case *UpstreamConnManagerCosmos:
		manager.respMux.Lock()
		defer manager.respMux.Unlock()
		return len(manager.respMap)
	case *UpstreamConnManagerEth:
		manager.respMux.Lock()
		defer manager.respMux.Unlock()
		return len(manager.respMap)
	default:
		return -1
	}
}
