package cosmoguard

import (
	"fmt"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/pkg/util"
	"gotest.tools/assert"
)

func TestOrdinaryRPCTimeoutsDoNotRetainRequestIDsOrWaiters(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		t.Run(protocol.name, func(t *testing.T) {
			const requestCount = 8
			candidates := make([]string, requestCount)
			for i := range candidates {
				candidates[i] = fmt.Sprintf("shared-%d", i)
			}
			candidateCalls := make(chan string, requestCount*4)
			var candidate atomic.Uint32
			idGen := util.NewUniqueID(func() string {
				id := candidates[(candidate.Add(1)-1)%requestCount]
				candidateCalls <- id
				return id
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
			for i := 0; i < requestCount; i++ {
				result := make(chan error, 1)
				go func() {
					_, err := manager.MakeRequest(&JsonRpcMsg{Version: jsonRpcVersion, Method: "status"})
					result <- err
				}()
				var request JsonRpcMsg
				assert.NilError(t, peer.ReadJSON(&request))
				requests = append(requests, &request)
				assert.Assert(t, isUncertainWSUpstreamOutcome(<-result))
				assert.Equal(t, managerResponseCount(manager), 0)
			}

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
			assert.NilError(t, <-finalResult)
			assert.Equal(t, managerResponseCount(manager), 0)
			runtime.Gosched()
			assert.Assert(t, runtime.NumGoroutine() <= baselineGoroutines+2)

			reused := make(chan string, 1)
			go func() { reused <- idGen.ID() }()
			firstCandidate := <-candidateCalls
			select {
			case id := <-reused:
				assert.Equal(t, id, firstCandidate)
				idGen.Release(id)
			case secondCandidate := <-candidateCalls:
				t.Fatalf("ordinary RPC retained request IDs %q and %q", firstCandidate, secondCandidate)
			}

			assert.NilError(t, client.Close())
			<-readerDone
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
