package cosmoguard

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var peerAuthTestNow = time.Unix(1_800_000_000, 0).UTC()

func TestDerivePeerAPIKey(t *testing.T) {
	decoded, err := base64.StdEncoding.DecodeString(testClusterEncryptionKey)
	require.NoError(t, err)

	mac := hmac.New(sha256.New, decoded)
	_, err = mac.Write([]byte("cosmoguard/peer-api/key/v1"))
	require.NoError(t, err)

	require.Equal(t, mac.Sum(nil), derivePeerAPIKey(decoded))
	require.NotEqual(t, decoded, derivePeerAPIKey(decoded))
}

func TestAuthenticatePeerRequest_KeyValidation(t *testing.T) {
	key := []byte("peer-api-test-key")
	req := newSignedPeerRequest(t, key, peerAuthTestNow, "http://peer.internal/api/v1/metrics?window=5m")

	require.NoError(t, authenticatePeerRequest(req, key, peerAuthTestNow))
	require.Error(t, authenticatePeerRequest(req, []byte("wrong-key"), peerAuthTestNow))
	require.Error(t, authenticatePeerRequest(req, nil, peerAuthTestNow))
}

func TestAuthenticatePeerRequest_RejectsMalformedHeaders(t *testing.T) {
	key := []byte("peer-api-test-key")
	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{
			name: "missing timestamp",
			mutate: func(r *http.Request) {
				r.Header.Del(peerTimestampHeader)
			},
		},
		{
			name: "missing signature",
			mutate: func(r *http.Request) {
				r.Header.Del(peerSignatureHeader)
			},
		},
		{
			name: "duplicate timestamp",
			mutate: func(r *http.Request) {
				r.Header.Add(peerTimestampHeader, r.Header.Get(peerTimestampHeader))
			},
		},
		{
			name: "duplicate signature",
			mutate: func(r *http.Request) {
				r.Header.Add(peerSignatureHeader, r.Header.Get(peerSignatureHeader))
			},
		},
		{
			name: "noncanonical timestamp",
			mutate: func(r *http.Request) {
				r.Header.Set(peerTimestampHeader, "01800000000")
			},
		},
		{
			name: "malformed timestamp",
			mutate: func(r *http.Request) {
				r.Header.Set(peerTimestampHeader, "not-a-timestamp")
			},
		},
		{
			name: "uppercase signature",
			mutate: func(r *http.Request) {
				r.Header.Set(peerSignatureHeader, strings.ToUpper(r.Header.Get(peerSignatureHeader)))
			},
		},
		{
			name: "malformed signature",
			mutate: func(r *http.Request) {
				r.Header.Set(peerSignatureHeader, "xyz")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedPeerRequest(t, key, peerAuthTestNow, "http://peer.internal/api/v1/metrics")
			tt.mutate(req)
			require.Error(t, authenticatePeerRequest(req, key, peerAuthTestNow))
		})
	}
}

func TestAuthenticatePeerRequest_TimestampWindow(t *testing.T) {
	key := []byte("peer-api-test-key")
	tests := []struct {
		name    string
		signed  time.Time
		wantErr bool
	}{
		{name: "past boundary accepted", signed: peerAuthTestNow.Add(-30 * time.Second)},
		{name: "future boundary accepted", signed: peerAuthTestNow.Add(30 * time.Second)},
		{name: "stale rejected", signed: peerAuthTestNow.Add(-31 * time.Second), wantErr: true},
		{name: "future rejected", signed: peerAuthTestNow.Add(31 * time.Second), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedPeerRequest(t, key, tt.signed, "http://peer.internal/api/v1/metrics")
			err := authenticatePeerRequest(req, key, peerAuthTestNow)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAuthenticatePeerRequest_RejectsTampering(t *testing.T) {
	key := []byte("peer-api-test-key")
	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "method", mutate: func(r *http.Request) { r.Method = http.MethodPost }},
		{name: "host", mutate: func(r *http.Request) { r.Host = "other.internal" }},
		{name: "path", mutate: func(r *http.Request) { r.URL.Path = "/api/v1/denied" }},
		{name: "query", mutate: func(r *http.Request) { r.URL.RawQuery = "window=10m" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedPeerRequest(t, key, peerAuthTestNow, "http://peer.internal/api/v1/metrics?window=5m")
			tt.mutate(req)
			require.Error(t, authenticatePeerRequest(req, key, peerAuthTestNow))
		})
	}
}

func TestAuthenticatePeerRequest_RejectsRequestBody(t *testing.T) {
	key := []byte("peer-api-test-key")
	req := newSignedPeerRequest(t, key, peerAuthTestNow, "http://peer.internal/api/v1/metrics")
	req.Body = io.NopCloser(strings.NewReader("body"))
	req.ContentLength = 4

	require.Error(t, authenticatePeerRequest(req, key, peerAuthTestNow))
}

func TestAuthenticatePeerRequest_ReplayExpiresWithWindow(t *testing.T) {
	key := []byte("peer-api-test-key")
	req := newSignedPeerRequest(t, key, peerAuthTestNow, "http://peer.internal/api/v1/metrics")

	require.NoError(t, authenticatePeerRequest(req, key, peerAuthTestNow))
	require.NoError(t, authenticatePeerRequest(req, key, peerAuthTestNow.Add(30*time.Second)))
	require.Error(t, authenticatePeerRequest(req, key, peerAuthTestNow.Add(31*time.Second)))
}

func TestPeerAuthGate_UnsignedLoopbackInstalledHandlerIsUnauthorized(t *testing.T) {
	key := []byte("peer-api-test-key")
	cg := &CosmoGuard{
		cfg: &Config{Cache: CacheGlobalConfig{Cluster: &ClusterConfig{
			BindAddr: "127.0.0.1",
			BindPort: 3320,
		}}},
		cluster: &clusterRuntime{peerAPIKey: key},
	}
	srv := installPeerAPIServer(cg)
	require.NotNil(t, srv)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "http://peer.internal/api/v1/", nil)
	req.RemoteAddr = "127.0.0.1:12345"
	srv.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "unauthorized\n", rec.Body.String())
	require.NotContains(t, rec.Body.String(), "unknown api path")
}

func TestPeerAuthGate_SignedLoopbackInstalledHandlerSucceeds(t *testing.T) {
	clusterKey, err := DecodeClusterEncryptionKey(testClusterEncryptionKey)
	require.NoError(t, err)
	peerAPIKey := derivePeerAPIKey(clusterKey)
	cg := &CosmoGuard{
		cfg: &Config{Cache: CacheGlobalConfig{Cluster: &ClusterConfig{
			BindAddr: "127.0.0.1",
			BindPort: 3320,
		}}},
		cluster: &clusterRuntime{peerAPIKey: peerAPIKey},
	}
	srv := installPeerAPIServer(cg)
	require.NotNil(t, srv)

	rec := httptest.NewRecorder()
	req := newSignedPeerRequest(t, peerAPIKey, time.Now(), "http://peer.internal/api/v1/rules")
	req.RemoteAddr = "127.0.0.1:12345"
	srv.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"rules":[]}`, rec.Body.String())
}

func TestPeerAuthGate_ValidNonMemberRemainsForbidden(t *testing.T) {
	key := []byte("peer-api-test-key")
	h := peerAuthGate(key)(peerMembershipGate(&CosmoGuard{})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("non-member request reached handler")
	})))
	req := newSignedPeerRequest(t, key, time.Now(), "http://peer.internal/api/v1/metrics")
	req.RemoteAddr = "203.0.113.5:54321"
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestFanoutGet_AuthenticatedRoundTrip(t *testing.T) {
	key := []byte("peer-api-test-key")
	srv := httptest.NewServer(peerAuthGate(key)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"history":[]}`))
	})))
	t.Cleanup(srv.Close)

	body, err := fanoutGet(t.Context(), srv.Client(), srv.URL+"/api/v1/metrics", key)
	require.NoError(t, err)
	require.JSONEq(t, `{"history":[]}`, string(body))
}

func TestFanoutGet_MissingKeyFailsLocally(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	_, err := fanoutGet(t.Context(), srv.Client(), srv.URL, nil)
	require.Error(t, err)
	require.Zero(t, hits.Load())
}

func TestFanoutClient_DoesNotUseProxy(t *testing.T) {
	transport, ok := fanoutClient.Transport.(*http.Transport)
	require.True(t, ok)
	require.Nil(t, transport.Proxy)
}

func TestFanoutClient_RefusesRedirects(t *testing.T) {
	key := []byte("peer-api-test-key")
	var redirectedHits atomic.Int32
	redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(redirected.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirected.URL, http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	_, err := fanoutGet(t.Context(), fanoutClient, redirector.URL, key)
	require.Error(t, err)
	require.Zero(t, redirectedHits.Load())
}

func newSignedPeerRequest(t *testing.T, key []byte, now time.Time, target string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	require.NoError(t, signPeerRequest(req, key, now))
	require.Len(t, req.Header.Values(peerTimestampHeader), 1)
	require.Len(t, req.Header.Values(peerSignatureHeader), 1)
	_, err := hex.DecodeString(req.Header.Get(peerSignatureHeader))
	require.NoError(t, err)
	return req
}
