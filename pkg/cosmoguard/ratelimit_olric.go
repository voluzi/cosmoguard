package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/olric-data/olric"
	"github.com/vmihailenco/msgpack/v5"
)

// olricRateLimiter is the cluster-aware token-bucket implementation: the
// per-bucket state lives in an olric DMap, replicated to peers at the
// configured ReplicaCount. Every Allow() round-trips the bucket through
// the partition owner, so two cosmoguard pods rate-limiting the same key
// share a single token budget — that's the whole point of running olric.
//
// Algorithm: standard GCRA-style token bucket, identical math to
// redisRateLimiter. Atomicity comes from an olric distributed Lock held
// across read → compute → write. Lock takes ~one extra RTT vs Redis's
// EVAL-driven single-shot, but olric's local-partition fast-path makes
// it free for keys hashing to this node, and we don't need scripting
// machinery to extend the limiter logic later.
//
// Contention handling: if Lock times out (another pod is mid-update for
// the same key), we treat that as transient backpressure and deny the
// request rather than block the proxy hot path. The next request from
// the same key retries; under steady-state load this is rare because
// each Allow holds the lock for sub-millisecond.
type olricRateLimiter struct {
	dm        olric.DMap
	locks     olric.DMap    // separate DMap for distributed locks — see comment below
	rate      float64       // tokens per second
	burst     float64       // max bucket size
	keyspace  string        // prefix so multiple cosmoguards don't collide
	refillExp time.Duration // bucket TTL, so idle keys fall out of memory
}

// bucketState is the per-key serialised state — msgpack-encoded and
// stored as the DMap value. Keep this small: it's read and written on
// every Allow() call.
type bucketState struct {
	Tokens float64 `msgpack:"t"`
	Last   int64   `msgpack:"l"` // unix millis at last update
}

// newOlricRateLimiter wires up the limiter against an existing olric
// embedded client. The DMap is acquired up front so we don't pay the
// (cheap, but non-zero) discovery cost on every request.
func newOlricRateLimiter(client *olric.EmbeddedClient, cfg RateLimitConfig, keyspace string) (*olricRateLimiter, error) {
	if client == nil {
		return nil, errors.New("rate limiter: olric client is nil")
	}
	if math.IsNaN(cfg.Rate.PerSecond) || math.IsInf(cfg.Rate.PerSecond, 0) {
		return nil, fmt.Errorf("rate limit: rate must be a finite number (got %v)", cfg.Rate.PerSecond)
	}
	if cfg.Rate.PerSecond <= 0 {
		return nil, errors.New("rate limit: non-positive rate")
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = int(cfg.Rate.PerSecond)
		if burst < 1 {
			burst = 1
		}
	}

	// DMap name must be stable across pods so all members converge on
	// the same partition map. The keyspace lives inside the key, not
	// the DMap name, so a single DMap holds every rule's buckets — fewer
	// olric internal structures than one DMap per rule.
	dm, err := client.NewDMap("ratelimit")
	if err != nil {
		return nil, fmt.Errorf("rate limiter: dmap: %w", err)
	}

	// Locks live in a SEPARATE DMap so the lock-token keyspace and the
	// bucket-value keyspace can't intersect regardless of caller input.
	// Olric's Lock stores its random token under the supplied key in the
	// same DMap the call was made on (see comment in Allow); parking it
	// in the bucket DMap was a latent footgun — a caller-supplied
	// identity ending in ":lock" would let one user's bucket key land on
	// another user's lock key, silently resetting that bucket on every
	// Allow and breaking Unlock for the holder. With locks isolated, no
	// key collision is structurally possible regardless of what identity
	// names an external validator returns.
	locks, err := client.NewDMap("ratelimit-locks")
	if err != nil {
		return nil, fmt.Errorf("rate limiter: locks dmap: %w", err)
	}

	// Mirror the upstream-Redis refillExp cap: 24h ceiling guards against
	// an operator who configures burst:1e9 rate:0.001 ("trickle one a day")
	// from blowing past int64 when the multiplication overflows.
	refill := 2*float64(time.Second)*float64(burst)/cfg.Rate.PerSecond + float64(time.Minute)
	if refill > float64(24*time.Hour) || math.IsInf(refill, 0) {
		refill = float64(24 * time.Hour)
	}

	return &olricRateLimiter{
		dm:        dm,
		locks:     locks,
		rate:      cfg.Rate.PerSecond,
		burst:     float64(burst),
		keyspace:  keyspace,
		refillExp: time.Duration(refill),
	}, nil
}

// Allow implements RateLimiter. The whole critical section runs under
// an olric distributed lock; if the lock can't be acquired in 250 ms we
// surface the request as denied (with no retry-after) so the proxy
// doesn't stall — the next request retries the bucket.
func (l *olricRateLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	fullKey := l.keyspace + ":" + key
	// Olric's Lock stores its random token at the supplied key in the
	// DMap it was called on. We deliberately use a SEPARATE DMap for
	// locks so the lock token can never end up at a key that happens to
	// match another caller's bucket key (which would silently reset the
	// bucket on the next Get and break Unlock for the lock holder). With
	// the split, the lock key is just fullKey verbatim — same string
	// across all pods so they converge on the same lock object.

	// LockWithTimeout(deadline=250ms, lease=2s):
	//   - deadline bounds how long Allow() will wait for the lock.
	//   - lease is the maximum hold time before olric auto-releases.
	//     If we crash mid-critical-section, the lease ensures the
	//     bucket isn't stuck locked forever.
	lock, err := l.locks.LockWithTimeout(ctx, fullKey, 2*time.Second, 250*time.Millisecond)
	if err != nil {
		if errors.Is(err, olric.ErrLockNotAcquired) {
			// Contention is itself a signal of load. Deny rather
			// than queue — keeps the proxy hot path bounded.
			return false, 0, nil
		}
		return false, 0, err
	}
	// Unlock must succeed even when the request context has been
	// cancelled (client hangup mid-Allow). Reusing the request ctx here
	// silently no-ops Unlock on cancellation; the bucket then stays
	// locked for the 2 s lease, denying every concurrent request on the
	// same key as "contention" for that whole window. Detach the ctx so
	// Unlock actually runs, capped at a short deadline so a half-dead
	// peer can't pin the deferred call.
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
		defer cancel()
		_ = lock.Unlock(uctx)
	}()

	nowMs := time.Now().UnixMilli()

	// Read existing bucket state. ErrKeyNotFound is the cold-start path.
	var st bucketState
	resp, err := l.dm.Get(ctx, fullKey)
	switch {
	case err == nil:
		blob, berr := resp.Byte()
		if berr != nil {
			return false, 0, fmt.Errorf("rate limiter: decode response: %w", berr)
		}
		if uerr := msgpack.Unmarshal(blob, &st); uerr != nil {
			// Corrupt blob (older format, partial write under crash).
			// Reset to a fresh bucket rather than fail the request.
			st = bucketState{}
		}
	case errors.Is(err, olric.ErrKeyNotFound):
		// fresh bucket
	default:
		return false, 0, fmt.Errorf("rate limiter: get: %w", err)
	}

	if st.Last == 0 {
		st.Tokens = l.burst
		st.Last = nowMs
	}

	// Refill since last update. Clock skew between pods is bounded by
	// NTP (which the operator runs anyway for olric to work at all), so
	// we treat nowMs - st.Last conservatively: any negative interval (a
	// pod with a backwards clock) is dropped to zero — no spurious
	// tokens minted by a misbehaving peer.
	elapsed := float64(nowMs-st.Last) / 1000.0
	if elapsed > 0 {
		st.Tokens = math.Min(l.burst, st.Tokens+elapsed*l.rate)
	}

	allowed := false
	var retryAfter time.Duration
	if st.Tokens >= 1 {
		st.Tokens -= 1
		allowed = true
	} else {
		retryAfter = time.Duration(math.Ceil((1-st.Tokens)/l.rate*1000)) * time.Millisecond
	}
	st.Last = nowMs

	blob, err := msgpack.Marshal(&st)
	if err != nil {
		return false, 0, fmt.Errorf("rate limiter: encode: %w", err)
	}
	if err := l.dm.Put(ctx, fullKey, blob, olric.EX(l.refillExp)); err != nil {
		return false, 0, fmt.Errorf("rate limiter: put: %w", err)
	}
	return allowed, retryAfter, nil
}

// Close is a no-op: the olric client is owned by the surrounding
// clusterRuntime, not by the limiter. Closing it here would yank the
// daemon out from under the cache + observability replication too.
func (l *olricRateLimiter) Close() error { return nil }
