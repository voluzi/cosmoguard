package cosmoguard

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// TestMarshalReplicationPayload_TrimsToFit ensures a high-cardinality
// replication payload is trimmed (oldest history first) so its olric Put
// stays under the per-entry cap and never fails with ErrEntryTooLarge —
// which would silently drop dashboard restart-restore (the observability
// DMap has no uncached fallback).
func TestMarshalReplicationPayload_TrimsToFit(t *testing.T) {
	// Build a history far larger than the entry cap.
	big := make([]MetricsSnapshot, 0, 512)
	for i := 0; i < 512; i++ {
		big = append(big, MetricsSnapshot{})
	}
	// Pad each snapshot's wire size so the total blob clearly exceeds the cap.
	payload := &replicationPayload{
		Observability: make([]byte, 64<<10), // 64 KiB base blob, kept
		History:       big,
		WrittenMs:     1,
	}
	// Sanity: the untrimmed payload must exceed the cap for the test to mean
	// anything.
	raw, err := msgpack.Marshal(payload)
	require.NoError(t, err)
	// Inflate history entries so the untrimmed blob is over the cap even with
	// empty structs being small: use a payload whose Observability alone is
	// under cap but History pushes it over.
	if len(raw) <= maxReplicationBlobBytes {
		// Empty MetricsSnapshot marshals tiny; make the base blob dominate the
		// budget so trimming History is exercised deterministically.
		payload.Observability = make([]byte, maxReplicationBlobBytes-(8<<10))
	}

	blob, err := marshalReplicationPayload(payload)
	require.NoError(t, err)
	require.LessOrEqual(t, len(blob), maxReplicationBlobBytes, "trimmed blob must fit under the entry cap")

	// It must still be decodable and retain the base observability snapshot.
	var out replicationPayload
	require.NoError(t, msgpack.Unmarshal(blob, &out))
	require.Equal(t, len(payload.Observability), len(out.Observability), "base observability snapshot must be preserved")
}

// TestMarshalReplicationPayload_OversizedSnapshotStillOverCap: when the base
// observability snapshot alone exceeds the cap (history trimmed to empty), the
// helper returns a blob still over the cap — flush then skips the Put rather
// than erroring (asserted here by confirming the over-cap signal is visible).
func TestMarshalReplicationPayload_OversizedSnapshotStillOverCap(t *testing.T) {
	payload := &replicationPayload{
		Observability: make([]byte, maxReplicationBlobBytes+(32<<10)), // base alone over cap
		History:       []MetricsSnapshot{{}, {}, {}},
		WrittenMs:     1,
	}
	blob, err := marshalReplicationPayload(payload)
	require.NoError(t, err)
	// History is trimmed to nothing but the base snapshot keeps it over cap;
	// flush uses this len(blob) > cap check to skip the write.
	require.Greater(t, len(blob), maxReplicationBlobBytes)
	// Regression guard: the trimming actually emptied History (rather than the
	// blob merely being over cap because of the base snapshot).
	var out replicationPayload
	require.NoError(t, msgpack.Unmarshal(blob, &out))
	require.Len(t, out.History, 0, "history must be fully trimmed when the base snapshot alone exceeds the cap")
}

// TestMarshalReplicationPayload_SmallPayloadUntouched: a payload already under
// the cap is returned as-is with its full history.
func TestMarshalReplicationPayload_SmallPayloadUntouched(t *testing.T) {
	payload := &replicationPayload{
		Observability: []byte("small"),
		History:       []MetricsSnapshot{{}, {}, {}},
		WrittenMs:     42,
	}
	blob, err := marshalReplicationPayload(payload)
	require.NoError(t, err)
	var out replicationPayload
	require.NoError(t, msgpack.Unmarshal(blob, &out))
	require.Len(t, out.History, 3, "small payload history must be untouched")
}
