package cosmoguard

import (
	"errors"
	"testing"

	"gotest.tools/assert"
)

func TestUpstreamManagerZeroValueInitializesLifecycleOnce(t *testing.T) {
	type observation struct {
		healthy         bool
		hasSubscription bool
		lifecycle       *wsSubscriptionLifecycle
	}
	managers := []UpstreamConnManager{
		&UpstreamConnManagerCosmos{},
		&UpstreamConnManagerEth{},
	}
	for _, manager := range managers {
		observations := make(chan observation, 16)
		for range 16 {
			go func() {
				observations <- observation{
					healthy:         manager.IsHealthy(),
					hasSubscription: manager.HasSubscription("missing"),
					lifecycle:       managerLifecycle(manager),
				}
			}()
		}
		var initialized *wsSubscriptionLifecycle
		for range 16 {
			result := mustRecv(t, observations, "zero-value manager observation")
			assert.Assert(t, !result.healthy)
			assert.Assert(t, !result.hasSubscription)
			assert.Assert(t, result.lifecycle != nil)
			if initialized == nil {
				initialized = result.lifecycle
			}
			assert.Assert(t, result.lifecycle == initialized, "concurrent access created multiple lifecycles")
		}

		_, err := manager.Subscribe("victim")
		assert.Assert(t, errors.Is(err, ErrClosed))
		assert.Assert(t, manager.LocalUnsubscribe("missing") == nil)
		manager.Stop()
		manager.Stop()
	}

	for _, manager := range []UpstreamConnManager{
		&UpstreamConnManagerCosmos{},
		&UpstreamConnManagerEth{},
	} {
		done := make(chan error, 1)
		go func() { done <- manager.Run(log.WithField("test", t.Name())) }()
		manager.Stop()
		assert.NilError(t, mustRecv(t, done, "zero-value manager shutdown"))
	}
}
