//go:build integration

package cosmoguard

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/olric/config"
)

func TestMixedEngineJanitorRetainsAcknowledgedWrites(t *testing.T) {
	for _, engine := range []struct {
		name    string
		bounded bool
	}{{"native", false}, {"custom", true}} {
		t.Run(engine.name, func(t *testing.T) {
			for _, lifecycle := range []struct {
				name    string
				destroy bool
			}{{"empty", false}, {"recreated", true}} {
				t.Run(lifecycle.name, func(t *testing.T) {
					node := startMixedNodeConfigured(t, engine.bounded, "", 32<<20, func(c *config.Config, _ *mixedNode) {
						c.DMaps.CheckEmptyFragmentsInterval = time.Microsecond
					})
					var wg sync.WaitGroup
					failures := make(chan error, 3*8*200)
					for worker := range 8 {
						wg.Go(func() {
							dm, err := node.db.NewEmbeddedClient().NewDMap(fmt.Sprint("janitor-", worker))
							if err != nil {
								failures <- err
								return
							}
							key := fmt.Sprint("writer-", worker)
							for i := range 200 {
								value := []byte(fmt.Sprint(i))
								if err := dm.Put(t.Context(), key, value); err != nil {
									failures <- fmt.Errorf("put: %w", err)
									continue
								}
								response, err := dm.Get(t.Context(), key)
								if err != nil {
									failures <- fmt.Errorf("acknowledged put disappeared: %w", err)
								} else {
									got, err := response.Byte()
									if err != nil || !bytes.Equal(got, value) {
										failures <- fmt.Errorf("got %q, want %q: %v", got, value, err)
									}
								}
								if _, err := dm.Delete(t.Context(), key); err != nil {
									failures <- fmt.Errorf("delete: %w", err)
								}
								if lifecycle.destroy {
									if err := dm.Destroy(t.Context()); err != nil {
										failures <- fmt.Errorf("destroy: %w", err)
									}
								}
							}
						})
					}
					wg.Wait()
					close(failures)
					for err := range failures {
						require.NoError(t, err)
					}
				})
			}
		})
	}
}
