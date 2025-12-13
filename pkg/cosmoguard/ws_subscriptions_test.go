package cosmoguard

import (
	"sync"
	"testing"

	"gotest.tools/assert"
)

func TestSubscriptionManager_AddAndRemoveSubscription(t *testing.T) {
	sm := NewSubscriptionManager()

	// Add subscription
	sm.AddSubscription("param1", "id1")

	// Verify it was added
	id, ok := sm.GetSubscriptionID("param1")
	assert.Assert(t, ok)
	assert.Equal(t, id, "id1")

	param, ok := sm.GetSubscriptionParam("id1")
	assert.Assert(t, ok)
	assert.Equal(t, param, "param1")

	// Remove subscription
	sm.RemoveSubscription("id1")

	// Verify it was removed
	_, ok = sm.GetSubscriptionID("param1")
	assert.Assert(t, !ok)

	_, ok = sm.GetSubscriptionParam("id1")
	assert.Assert(t, !ok)
}

func TestSubscriptionManager_ClientSubscriptions(t *testing.T) {
	sm := NewSubscriptionManager()

	// Create mock clients (we just need pointers, content doesn't matter for this test)
	client1 := &JsonRpcWsClient{}
	client2 := &JsonRpcWsClient{}

	// Add subscription
	sm.AddSubscription("param1", "id1")

	// Initially subscription should be empty
	assert.Assert(t, sm.SubscriptionEmpty("id1"))

	// Subscribe clients
	sm.SubscribeClient("id1", client1, "client1-sub-id")
	sm.SubscribeClient("id1", client2, "client2-sub-id")

	// Subscription should not be empty now
	assert.Assert(t, !sm.SubscriptionEmpty("id1"))

	// Check client subscribed
	assert.Assert(t, sm.ClientSubscribed("id1", client1))
	assert.Assert(t, sm.ClientSubscribed("id1", client2))

	// Get subscriptions for client1
	subs := sm.GetSubscriptions(client1)
	assert.Equal(t, len(subs), 1)
	assert.Equal(t, subs[0], "id1")

	// Unsubscribe client1
	sm.UnsubscribeClient("id1", client1)

	// Verify client1 is no longer subscribed
	assert.Assert(t, !sm.ClientSubscribed("id1", client1))
	assert.Assert(t, sm.ClientSubscribed("id1", client2))

	// Get subscription clients
	clients := sm.GetSubscriptionClients("id1")
	assert.Equal(t, len(clients), 1)
	_, ok := clients[client2]
	assert.Assert(t, ok)
}

func TestSubscriptionManager_MultipleSubscriptions(t *testing.T) {
	sm := NewSubscriptionManager()

	client := &JsonRpcWsClient{}

	// Add multiple subscriptions
	sm.AddSubscription("param1", "id1")
	sm.AddSubscription("param2", "id2")
	sm.AddSubscription("param3", "id3")

	// Subscribe client to all
	sm.SubscribeClient("id1", client, "sub1")
	sm.SubscribeClient("id2", client, "sub2")
	sm.SubscribeClient("id3", client, "sub3")

	// Get all subscriptions for client
	subs := sm.GetSubscriptions(client)
	assert.Equal(t, len(subs), 3)

	// Verify all IDs are present
	subMap := make(map[string]bool)
	for _, s := range subs {
		subMap[s] = true
	}
	assert.Assert(t, subMap["id1"])
	assert.Assert(t, subMap["id2"])
	assert.Assert(t, subMap["id3"])
}

func TestSubscriptionManager_GetSubscriptionClients_ReturnsCopy(t *testing.T) {
	sm := NewSubscriptionManager()

	client := &JsonRpcWsClient{}

	sm.AddSubscription("param1", "id1")
	sm.SubscribeClient("id1", client, "sub1")

	// Get clients
	clients1 := sm.GetSubscriptionClients("id1")
	clients2 := sm.GetSubscriptionClients("id1")

	// Modify one - shouldn't affect the other or original
	delete(clients1, client)

	// clients2 should still have the client
	_, ok := clients2[client]
	assert.Assert(t, ok)

	// Original should still have the client
	assert.Assert(t, sm.ClientSubscribed("id1", client))
}

func TestSubscriptionManager_NonExistentSubscription(t *testing.T) {
	sm := NewSubscriptionManager()

	// Try to get non-existent subscription
	_, ok := sm.GetSubscriptionID("nonexistent")
	assert.Assert(t, !ok)

	_, ok = sm.GetSubscriptionParam("nonexistent")
	assert.Assert(t, !ok)

	// These should not panic
	clients := sm.GetSubscriptionClients("nonexistent")
	assert.Equal(t, len(clients), 0)
}

func TestSubscriptionManager_Concurrent(t *testing.T) {
	sm := NewSubscriptionManager()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Concurrent adds
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				param := "param"
				subID := "id"
				sm.AddSubscription(param, subID)
				sm.GetSubscriptionID(param)
				sm.GetSubscriptionParam(subID)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent client operations
	sm.AddSubscription("shared-param", "shared-id")

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			client := &JsonRpcWsClient{}
			for j := 0; j < numOperations; j++ {
				sm.SubscribeClient("shared-id", client, id)
				sm.ClientSubscribed("shared-id", client)
				sm.GetSubscriptions(client)
				sm.GetSubscriptionClients("shared-id")
				sm.SubscriptionEmpty("shared-id")
				sm.UnsubscribeClient("shared-id", client)
			}
		}(i)
	}
	wg.Wait()
}

func TestSubscriptionManager_SubscriptionEmpty(t *testing.T) {
	sm := NewSubscriptionManager()

	// Non-existent subscription
	assert.Assert(t, sm.SubscriptionEmpty("nonexistent"))

	// Add empty subscription
	sm.AddSubscription("param1", "id1")
	assert.Assert(t, sm.SubscriptionEmpty("id1"))

	// Add client
	client := &JsonRpcWsClient{}
	sm.SubscribeClient("id1", client, "sub1")
	assert.Assert(t, !sm.SubscriptionEmpty("id1"))

	// Remove client
	sm.UnsubscribeClient("id1", client)
	assert.Assert(t, sm.SubscriptionEmpty("id1"))
}

func TestNewSubscriptionManager(t *testing.T) {
	sm := NewSubscriptionManager()

	assert.Assert(t, sm != nil)
	assert.Assert(t, sm.paramToID != nil)
	assert.Assert(t, sm.idToParam != nil)
	assert.Assert(t, sm.clientSubscriptions != nil)
}

func BenchmarkSubscriptionManager_AddRemove(b *testing.B) {
	sm := NewSubscriptionManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.AddSubscription("param", "id")
		sm.RemoveSubscription("id")
	}
}

func BenchmarkSubscriptionManager_ClientOperations(b *testing.B) {
	sm := NewSubscriptionManager()
	sm.AddSubscription("param", "id")
	client := &JsonRpcWsClient{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.SubscribeClient("id", client, "sub")
		sm.ClientSubscribed("id", client)
		sm.UnsubscribeClient("id", client)
	}
}
