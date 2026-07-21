package cosmoguard

import (
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type countingReadCloser struct {
	reader io.Reader
	reads  atomic.Int32
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.reader.Read(p)
}

func (*countingReadCloser) Close() error { return nil }

func TestCoalescedFetchFactoriesDeferRequestBodyReads(t *testing.T) {
	tests := []struct {
		name  string
		build func(*countingReadCloser)
	}{
		{
			name: "http foreground",
			build: func(body *countingReadCloser) {
				req := httptest.NewRequest("POST", "/status", nil)
				req.Body = body
				_ = (&HttpProxy{}).foregroundFetchFn(req, "key", &RuleCache{}, "rule", &responseOwner{})
			},
		},
		{
			name: "http refresh",
			build: func(body *countingReadCloser) {
				req := httptest.NewRequest("POST", "/status", nil)
				req.Body = body
				_ = (&HttpProxy{}).backgroundRefreshFn(req, "key", &RuleCache{}, "rule")
			},
		},
		{
			name: "json-rpc foreground",
			build: func(body *countingReadCloser) {
				req := httptest.NewRequest("POST", "/", nil)
				req.Body = body
				_ = (&JsonRpcHandler{}).singleForegroundFetchFn(req, nil, 1, &RuleCache{}, "rule", "status", &jsonRpcResponseOwner{})
			},
		},
		{
			name: "json-rpc refresh",
			build: func(body *countingReadCloser) {
				req := httptest.NewRequest("POST", "/", nil)
				req.Body = body
				_ = (&JsonRpcHandler{}).singleBackgroundRefreshFn(req, nil, 1, &RuleCache{}, "rule", "status")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := &countingReadCloser{reader: strings.NewReader("request body")}
			tt.build(body)
			require.Zero(t, body.reads.Load())
		})
	}
}
