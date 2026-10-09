package cosmoguard

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWebSocketDrainClosesRegistryAndLateUpgrades(t *testing.T) {
	p, _, _ := newWSCacheProxy(t, nil, 0)
	client, peer := newWSCacheClient(t)
	p.registerConn(client, "127.0.0.1", nil, time.Now())
	require.Equal(t, 1, p.StatsSnapshot().Connections)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	p.drainConnections(ctx)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err := peer.ReadMessage()
	require.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway), "close: %v", err)
	require.True(t, client.IsClosed())
	require.Zero(t, p.StatsSnapshot().Connections)
	late, latePeer := newWSCacheClient(t)
	p.registerConn(late, "127.0.0.1", nil, time.Now())
	require.NoError(t, latePeer.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err = latePeer.ReadMessage()
	require.True(t, websocket.IsCloseError(err, websocket.CloseGoingAway), "late upgrade close: %v", err)
	require.True(t, late.IsClosed())
	require.Zero(t, p.StatsSnapshot().Connections)
}
