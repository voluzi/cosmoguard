package cosmoguard

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v5/internal/boundedcall"
)

func TestJWTReplayBoundPreservesVerifiedIdentityAndRecovers(t *testing.T) {
	release, unblock := boundedTestRelease(t)
	dm := &stalledDMap{release: release, stage: "put"}
	store := &olricReplayStore{dm: dm, operationGate: boundedcall.NewWaiting(1, replayOperationBudget, func(outcome string) { recordBackendOperationFailure("replay", outcome) })}
	secret := "local-test-signing-secret"
	method, err := buildJWTMethod(AuthMethodConfig{Secret: secret}, &Authenticator{replay: store})
	require.NoError(t, err)
	defer method.Close()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "verified-user", "jti": "token-id", "exp": time.Now().Add(time.Minute).Unix()})
	signed, err := token.SignedString([]byte(secret))
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+signed)
	beforeTimeout := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("replay", "timeout"))
	beforeRejected := testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("replay", "rejected"))
	type result struct {
		identity *Identity
		err      error
	}
	resolve := func() {
		done := make(chan result, 1)
		go func() { id, err := method.Resolve(request); done <- result{id, err} }()
		select {
		case res := <-done:
			require.NoError(t, res.err)
			require.NotNil(t, res.identity)
			require.Equal(t, "verified-user", res.identity.Name)
		case <-time.After(5 * time.Second):
			t.Fatal("JWT request waited for stalled replay store")
		}
	}
	resolve()
	resolve()
	require.Equal(t, int32(1), dm.puts.Load(), "waiting calls must not reach the stalled store")
	require.Equal(t, beforeTimeout+2, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("replay", "timeout")))
	require.Equal(t, beforeRejected, testutil.ToFloat64(backendOperationFailuresCounter.WithLabelValues("replay", "rejected")))
	unblock()
	require.Eventually(t, func() bool {
		seen, err := store.SeenOrStore(t.Context(), "recovered", time.Minute)
		return !seen && err == nil
	}, 5*time.Second, time.Millisecond)
}
