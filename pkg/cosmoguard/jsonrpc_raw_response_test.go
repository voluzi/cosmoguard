package cosmoguard

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJSONRPCCoalescedOwnerPreservesRawResponse(t *testing.T) {
	rule := &JsonRpcRule{
		Action:  RuleActionAllow,
		Methods: []string{"status"},
		Cache:   &RuleCache{Enable: true, TTL: time.Minute},
	}
	h := newJSONCacheHandler(t, rule)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	raw := []byte("{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 1,\n  \"result\": \"ok\",\n  \"trace\": \"owner-only-extension\"\n}\n\n")
	next := func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-release
		_, _ = w.Write(raw)
	}

	ownerResult := make(chan *httptest.ResponseRecorder, 1)
	waiterResult := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 1, Method: "status"}, rec, req, next, time.Now())
		ownerResult <- rec
	}()
	<-started
	go func() {
		rec := httptest.NewRecorder()
		req, _ := jsonRequestContext()
		h.handleHttpSingle(&JsonRpcMsg{Version: "2.0", ID: 2, Method: "status"}, rec, req, next, time.Now())
		waiterResult <- rec
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)

	owner := <-ownerResult
	waiter := <-waiterResult
	require.True(t, bytes.Equal(raw, owner.Body.Bytes()), "owner body = %q", owner.Body.Bytes())
	require.JSONEq(t, `{"jsonrpc":"2.0","id":2,"result":"ok"}`, waiter.Body.String())
}
