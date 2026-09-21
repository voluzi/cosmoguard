package cosmoguard

import (
	"errors"
	"sync"
	"testing"

	"gotest.tools/assert"
)

func TestUpstreamManagerZeroValueInitializesLifecycleOnce(t *testing.T) {
	managers := []UpstreamConnManager{
		&UpstreamConnManagerCosmos{},
		&UpstreamConnManagerEth{},
	}
	for _, manager := range managers {
		var wg sync.WaitGroup
		lifecycles := make(chan *wsSubscriptionLifecycle, 16)
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				assert.Assert(t, !manager.IsHealthy())
				assert.Assert(t, !manager.HasSubscription("missing"))
				lifecycles <- managerLifecycle(manager)
			}()
		}
		wg.Wait()
		close(lifecycles)
		var initialized *wsSubscriptionLifecycle
		for lifecycle := range lifecycles {
			if initialized == nil {
				initialized = lifecycle
			}
			assert.Assert(t, lifecycle == initialized, "concurrent access created multiple lifecycles")
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
