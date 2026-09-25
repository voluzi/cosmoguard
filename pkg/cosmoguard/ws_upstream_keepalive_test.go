package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/voluzi/cosmoguard/v5/pkg/util"
	"gotest.tools/assert"
)

func TestUpstreamKeepaliveDropsSilentUpstream(t *testing.T) {
	for _, protocol := range wsProtocolCases() {
		for _, answersPings := range []bool{true, false} {
			name := protocol.name + "/answers pings"
			if !answersPings {
				name = protocol.name + "/silent"
			}
			t.Run(name, func(t *testing.T) {
				var connections atomic.Int32
				upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					connections.Add(1)
					if !answersPings {
						conn.SetPingHandler(func(string) error { return nil })
					}
					for {
						if _, _, err := conn.ReadMessage(); err != nil {
							return
						}
					}
				}))
				t.Cleanup(server.Close)
				target, err := url.Parse("ws" + strings.TrimPrefix(server.URL, "http"))
				assert.NilError(t, err)

				// A longer period for the answering case keeps a scheduling
				// stall on a loaded runner from reading as a lost pong.
				period := 50 * time.Millisecond
				if answersPings {
					period = 200 * time.Millisecond
				}
				manager := protocol.constructor(*target, &util.UniqueID{}, func(*JsonRpcMsg) {})
				switch m := manager.(type) {
				case *UpstreamConnManagerCosmos:
					m.pingPeriod = period
				case *UpstreamConnManagerEth:
					m.pingPeriod = period
				}
				go func() { _ = manager.Run(log.WithField("test", t.Name())) }()
				t.Cleanup(manager.Stop)

				// Three silent ping periods close the socket; Run redials.
				if answersPings {
					time.Sleep(1500 * time.Millisecond)
					assert.Equal(t, connections.Load(), int32(1))
					return
				}
				deadline := time.Now().Add(2 * time.Second)
				for connections.Load() < 2 {
					assert.Assert(t, time.Now().Before(deadline), "silent upstream was never dropped")
					time.Sleep(10 * time.Millisecond)
				}
			})
		}
	}
}
