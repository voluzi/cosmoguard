package cosmoguard

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/olric-data/olric"
)

// ReplayStore tracks seen JWT identifiers (`jti` claim) to block
// token replay within the expiration window. olric-backed when a
// cluster runtime is available (so replicas share the seen-set),
// per-pod memory fallback otherwise.
type ReplayStore interface {
	// SeenOrStore atomically reports whether key was already in the
	// store and, if not, inserts it with the given TTL. Returns true
	// when key was already present (= replay) and false when this
	// call was the first.
	SeenOrStore(ctx context.Context, key string, ttl time.Duration) (seen bool, err error)
	Close() error
}

// NewReplayStore picks the olric-backed store when an embedded client
// is supplied; otherwise the per-pod memory fallback. keyspace
// prefixes keys so co-tenant cosmoguards on one olric don't collide.
func NewReplayStore(olricClient *olric.EmbeddedClient, keyspace string) (ReplayStore, error) {
	if olricClient != nil {
		dm, err := olricClient.NewDMap(keyspace)
		if err != nil {
			return nil, err
		}
		return &olricReplayStore{dm: dm, keyspace: keyspace}, nil
	}
	store := &memoryReplayStore{seen: map[string]time.Time{}, stopCh: make(chan struct{})}
	go store.gcLoop()
	return store, nil
}

// memoryReplayStore is a single-replica in-process replay cache. Each
// entry expires at insertion-time + TTL; a periodic GC sweeps expired
// entries so memory stays bounded.
type memoryReplayStore struct {
	mu     sync.Mutex
	seen   map[string]time.Time // key → expires-at
	stopCh chan struct{}
	once   sync.Once
}

func (s *memoryReplayStore) SeenOrStore(_ context.Context, key string, ttl time.Duration) (bool, error) {
	// Non-positive TTL — caller wants to skip enforcement (e.g.
	// already-expired token); refuse to store so we don't pollute
	// the map and don't accidentally block on a subsequent retry.
	if ttl <= 0 {
		return false, nil
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if exp, ok := s.seen[key]; ok && exp.After(now) {
		return true, nil
	}
	s.seen[key] = now.Add(ttl)
	return false, nil
}

func (s *memoryReplayStore) Close() error {
	s.once.Do(func() { close(s.stopCh) })
	return nil
}

func (s *memoryReplayStore) gcLoop() {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-t.C:
			s.mu.Lock()
			for k, exp := range s.seen {
				if exp.Before(now) {
					delete(s.seen, k)
				}
			}
			s.mu.Unlock()
		}
	}
}

// olricReplayStore uses olric.Put(NX, EX): the partition owner
// serialises concurrent inserts, making the "seen or store" check
// atomic across the cluster.
type olricReplayStore struct {
	dm       olric.DMap
	keyspace string
}

func (s *olricReplayStore) SeenOrStore(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	// Non-positive TTL: skip the store. olric.EX(0) would pin the jti
	// forever, which is the wrong default for an already-expired token.
	if ttl <= 0 {
		return false, nil
	}
	err := s.dm.Put(ctx, key, []byte{1}, olric.NX(), olric.EX(ttl))
	if err == nil {
		return false, nil
	}
	if errors.Is(err, olric.ErrKeyFound) {
		return true, nil
	}
	return false, err
}

// Close is a no-op: the embedded olric client is owned by clusterRuntime.
func (s *olricReplayStore) Close() error { return nil }

// ErrReplay is returned by JWT verification when the token's jti
// matches a previously-seen one within its TTL.
var ErrReplay = errors.New("jwt replay detected")

// ErrInvalidCredential is returned by an AuthMethod when the request
// presented a credential of that method's shape (e.g. a JWT in the
// Authorization header) but it failed verification. The Authenticator
// short-circuits the method chain on this error so a forged JWT
// cannot be accepted by a downstream api-key / external-validator
// method reading the same header value. The pipeline maps it to a
// 401 Unauthorized response.
var ErrInvalidCredential = errors.New("invalid credential")

// ErrAuthUnavailable is returned by a fail-closed AuthMethod (external
// validator / introspection with `failureMode: fail-closed`) when its
// backend is unreachable. Both the HTTP pipeline and the gRPC path must
// DENY on it — a fail-closed method's whole point is that an outage rejects
// rather than admits. Distinct from ErrInvalidCredential (which means "a
// bad credential was presented") and from a generic transient error (which
// is treated as anonymous / fail-open for methods that opted into that).
var ErrAuthUnavailable = errors.New("authentication backend unavailable")
