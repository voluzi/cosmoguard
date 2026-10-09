//go:build integration

package cosmoguard

import (
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"
	"github.com/voluzi/olric/pkg/storage"
)

func TestMixedEngineExpiredImportPreservesNativeVersionMerge(t *testing.T) {
	for _, bounded := range []bool{false, true} {
		for _, newer := range []bool{false, true} {
			t.Run(fmt.Sprintf("custom=%v/newer=%v", bounded, newer), func(t *testing.T) {
				receiver := startMixedNode(t, bounded, "", 8<<20)
				const name, key = "version-merge", "key"
				dm, err := receiver.db.NewEmbeddedClient().NewDMap(name)
				require.NoError(t, err)
				require.NoError(t, dm.Put(t.Context(), key, []byte("survivor")))
				c := config.NewEngine()
				require.NoError(t, c.Sanitize())
				source, err := c.Implementation.Fork(storage.NewConfig(c.Config))
				require.NoError(t, err)
				require.NoError(t, source.Start())
				t.Cleanup(func() { _ = source.Close(); _ = source.Destroy() })
				entry := source.NewEntry()
				entry.SetKey(key)
				entry.SetValue([]byte("expired"))
				entry.SetTTL(time.Now().Add(-time.Hour).UnixMilli())
				timestamp := time.Now().Add(-time.Hour)
				if newer {
					timestamp = time.Now().Add(time.Hour)
				}
				entry.SetTimestamp(timestamp.UnixNano())
				hash := xxhash.Sum64String(name + key)
				require.NoError(t, source.Put(hash, entry))
				iterator := source.TransferIterator()
				require.True(t, iterator.Next())
				payload, _, err := iterator.Export()
				require.NoError(t, err)
				packet, err := msgpack.Marshal(struct {
					PartID  uint64
					Kind    int
					Name    string
					Payload []byte
				}{hash % 271, 1, name, payload})
				require.NoError(t, err)
				client := mixedReplicaClients(t, receiver)[0]
				ack, err := client.Do(t.Context(), "internal.node.movefragment", packet).Text()
				require.NoError(t, err)
				require.Equal(t, "OK", ack)
				got, err := dm.Get(t.Context(), key)
				if newer {
					require.ErrorIs(t, err, olric.ErrKeyNotFound, "newer expired version must displace older live value")
				} else {
					require.NoError(t, err)
					value, err := got.Byte()
					require.NoError(t, err)
					require.Equal(t, []byte("survivor"), value)
				}
			})
		}
	}
}
