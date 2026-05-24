package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olric-data/olric"
)

// OlricCache implements Cache[K, V] backed by an olric DMap. The DMap name
// is the cache namespace, so two cache instances created with different
// namespaces never collide on keys even if they live in the same olric
// daemon.
//
// Hot-path note: when V is []byte (e.g. cached HTTP response payloads) we
// skip the msgpack round-trip and pass the bytes through to olric directly.
// Saves ~1 alloc + the encode/decode CPU per call. The non-[]byte path
// preserves the same msgpack semantics as RedisCache for cross-backend
// behavioural parity.
type OlricCache[K comparable, V any] struct {
	dm        olric.DMap
	cfg       *Options
	namespace string
}

// NewOlricCache constructs a cache backed by an olric DMap. The caller owns
// the *olric.EmbeddedClient lifetime — the cache will not close it.
//
// Returns the concrete *OlricCache type (rather than Cache[K, V]) so the
// tiered-cache wiring can call GetWithExpiry directly without a type
// assertion. *OlricCache still satisfies Cache[K, V] structurally, so
// callers that want the interface can assign it without a cast.
func NewOlricCache[K comparable, V any](
	client *olric.EmbeddedClient,
	namespace string,
	opts ...Option,
) (*OlricCache[K, V], error) {
	if client == nil {
		return nil, errors.New("olric cache: client must not be nil")
	}
	if namespace == "" {
		namespace = DefaultNamespace
	}

	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	dm, err := client.NewDMap(namespace)
	if err != nil {
		return nil, fmt.Errorf("olric cache: open dmap %q: %w", namespace, err)
	}

	return &OlricCache[K, V]{
		dm:        dm,
		cfg:       options,
		namespace: namespace,
	}, nil
}

func (c *OlricCache[K, V]) Set(ctx context.Context, key K, value V, ttl time.Duration) error {
	itemTTL := ttl
	if itemTTL == 0 {
		itemTTL = c.cfg.TTL
	}

	payload, err := marshalForOlric(value)
	if err != nil {
		return err
	}

	return c.dm.Put(ctx, c.keyStr(key), payload, olric.EX(itemTTL))
}

func (c *OlricCache[K, V]) Get(ctx context.Context, key K) (V, error) {
	v, _, err := c.getWithExpiry(ctx, key)
	return v, err
}

// GetWithExpiry returns the value alongside its absolute wall-clock
// expiry deadline in Unix milliseconds (from olric.GetResponse.TTL()).
// Used by TieredCache so an L1 entry can be sized to the remaining time
// of the underlying L2 entry — both L1 and L2 expire at the same wall-
// clock instant on every pod, eliminating any staleness window opened
// by the L1 indirection.
//
// expiryMs == 0 when the underlying entry has no TTL (matches olric's
// convention). Callers should treat that as "do not cache in L1" so an
// untyped key can't pin memory forever.
func (c *OlricCache[K, V]) GetWithExpiry(ctx context.Context, key K) (V, int64, error) {
	return c.getWithExpiry(ctx, key)
}

func (c *OlricCache[K, V]) getWithExpiry(ctx context.Context, key K) (V, int64, error) {
	var zero V

	resp, err := c.dm.Get(ctx, c.keyStr(key))
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return zero, 0, ErrNotFound
		}
		return zero, 0, err
	}

	raw, err := resp.Byte()
	if err != nil {
		return zero, 0, err
	}

	v, err := unmarshalFromOlric[V](raw)
	return v, resp.TTL(), err
}

func (c *OlricCache[K, V]) Has(ctx context.Context, key K) (bool, error) {
	_, err := c.dm.Get(ctx, c.keyStr(key))
	if err != nil {
		if errors.Is(err, olric.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Close is a no-op: the underlying *olric.EmbeddedClient is owned by the
// caller and shared across cache + rate-limiter + observability snapshots.
// Tearing it down here would yank the rug out from under those consumers.
func (c *OlricCache[K, V]) Close() error { return nil }

func (c *OlricCache[K, V]) keyStr(key K) string {
	return fmt.Sprintf("%v", key)
}

// marshalForOlric encodes a value for storage. []byte payloads are passed
// through directly; everything else goes through msgpack.
func marshalForOlric(value any) ([]byte, error) {
	if b, ok := value.([]byte); ok {
		// Defensive copy: olric writes msgpack-wrapped bytes for non-byte
		// values, so we mirror the "value is fully owned by olric after
		// Put" contract by copying. If the caller mutates the slice after
		// Set, the cached entry stays intact.
		cp := make([]byte, len(b))
		copy(cp, b)
		return cp, nil
	}
	return EncodeValue(value)
}

// unmarshalFromOlric is the inverse of marshalForOlric. When V is []byte we
// hand back a defensive copy of the payload; otherwise we msgpack-decode
// into V.
//
// The copy matters for embedded-mode reads: olric's GetResponse.Byte() walks
// through resp.Scan, which for *[]byte aliases the entry's internal buffer
// (`*v = b` in olric/internal/resp/scan.go) and Entry.Value() returns its
// stored slice directly. For keys whose partition owner is this pod, the
// returned []byte therefore shares memory with the cache's in-memory store
// — a caller that mutates the slice would corrupt the cached value for
// every subsequent reader. Symmetric to marshalForOlric, which copies on
// the way in.
func unmarshalFromOlric[V any](raw []byte) (V, error) {
	var zero V
	if _, isBytes := any(zero).([]byte); isBytes {
		cp := make([]byte, len(raw))
		copy(cp, raw)
		// V == []byte. Return the copy via the any-cast — the type
		// parameter forces the result type to match V at compile time.
		return any(cp).(V), nil
	}
	return DecodeValue[V](raw)
}
