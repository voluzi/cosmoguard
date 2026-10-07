package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/olric-data/olric"
	"github.com/olric-data/olric/config"
)

// Non-cache olric DMap names that must NEVER be subject to the response
// cache's LRU eviction. Evicting rate-limit buckets, their lock tokens, the
// JWT replay set, or the observability snapshots under memory pressure would
// be a correctness/security regression (e.g. a replayed JWT, a reset rate
// limiter). The response-cache DMaps opt INTO eviction via the global LRU
// default; these are pinned back to EvictionPolicy=NONE via DMaps.Custom.
// Referenced by their NewDMap call sites so a rename can't silently drop the
// exemption. (replicationDMap = "observability" is defined alongside the
// observability replicator.)
const (
	rateLimitDMap      = "ratelimit"
	rateLimitLocksDMap = "ratelimit-locks"
	replayJTIDMap      = "cosmoguard:jti"
)

// evictionExemptDMaps is the set of DMaps kept free of LRU eviction. Kept in
// one place so the wiring and its regression test share a single source.
var evictionExemptDMaps = []string{
	rateLimitDMap,
	rateLimitLocksDMap,
	replayJTIDMap,
	replicationDMap,
}

// olricLRUSamples is the sample size for olric's approximate (Redis-style)
// LRU. Deliberately 2× olric's own default of 5 (config.DefaultLRUSamples)
// for tighter eviction accuracy — cheap given the cache's short TTL.
const olricLRUSamples = 10

// embeddedPartitionCount reduces the single-node per-DMap storage floor.
// With the current 1 MiB engine table size, 16 partitions still mean a 16 MiB
// floor per DMap; the aggregate can exceed small pods' approximate L2 budget.
// Clustered members retain the default count because all peers must agree.
const embeddedPartitionCount = 16

// olricTableSizeBytes is the fallback table size when the engine has none.
// config.New currently supplies a 1 MiB table size, which is preserved. Native
// oversized entries are served uncached; clustered response-cache workers also
// reject payloads above the engine default table size.
const olricTableSizeBytes uint64 = 256 << 10 // 256 KiB

// l2AssumedEntryOverheadBytes is the assumed per-key heap cost in olric
// (hashed key, index/access-log slab entry, storage bookkeeping) beyond the
// value bytes. Used to derive a MaxKeys ceiling alongside the byte cap: a
// flood of tiny values can exhaust heap in per-key structures while the
// byte-based Inuse counter stays low, so we also bound the key count to
// ~budget/overhead. Mirrors the L1 entryOverheadBytes intent for L2.
const l2AssumedEntryOverheadBytes uint64 = 512

// applyL2EvictionConfig bounds the olric L2 working set (issue #15). LRU is
// set as the GLOBAL default so every response-cache DMap inherits it; the
// non-cache DMaps (rate-limit buckets/locks, JWT replay set, observability
// snapshots) are pinned back to no-eviction via Custom so memory pressure
// can never evict security/correctness state. l2MaxBytesPerNode == 0 leaves
// eviction disabled (unlimited). Split out so it is unit-testable without
// standing up a real olric daemon.
//
// olric's LRU cap only governs PRIMARY-partition writes; backup (replica)
// writes bypass it (putOnReplicaFragment → PutRaw). So a node with
// replicaFactor copies resident holds ~replicaFactor × MaxInuse. To keep the
// node's actual in-use bytes within l2MaxBytesPerNode we set MaxInuse to
// l2MaxBytesPerNode / replicaFactor. replicaFactor is 1 in embedded/single-
// pod mode (no peers → no backups).
func applyL2EvictionConfig(dmaps *config.DMaps, l2MaxBytesPerNode uint64, replicaFactor int) {
	if dmaps == nil || l2MaxBytesPerNode == 0 {
		return
	}
	if replicaFactor < 1 {
		replicaFactor = 1
	}
	maxInuse := l2MaxBytesPerNode / uint64(replicaFactor)
	if maxInuse == 0 {
		maxInuse = 1
	}
	dmaps.EvictionPolicy = config.LRUEviction
	dmaps.MaxInuse = int(maxInuse)
	// Complement the byte cap with a key-count cap so a high-cardinality flood
	// of tiny values can't exhaust heap in per-key index structures before the
	// byte-based Inuse counter trips. olric honors MaxInuse and MaxKeys
	// together (whichever binds first). Derived from the same per-node byte
	// budget and an assumed per-entry overhead.
	if maxKeys := maxInuse / l2AssumedEntryOverheadBytes; maxKeys > 0 {
		dmaps.MaxKeys = int(maxKeys)
	}
	dmaps.LRUSamples = olricLRUSamples
	if dmaps.Custom == nil {
		dmaps.Custom = map[string]config.DMap{}
	}
	for _, name := range evictionExemptDMaps {
		dmaps.Custom[name] = config.DMap{EvictionPolicy: config.EvictionPolicy("NONE")}
	}
}

// clusterRuntime owns the in-process olric daemon. It is always running, even
// in the zero-config single-instance deployment, so the consumers (cache,
// rate-limiter, observability replication) get a single uniform interface
// regardless of whether the operator has flipped cluster mode on.
//
// In embedded mode (the default) the daemon binds the redis-protocol port to
// 127.0.0.1:<ephemeral> and the memberlist port to 127.0.0.1:<ephemeral>.
// Nothing externally addressable is opened. In cluster mode (cache.cluster.
// enable=true) the daemon binds the configured BindAddr:BindPort + GossipPort
// and joins peers advertised by the configured discovery plugin.
type clusterRuntime struct {
	db         *olric.Olric
	client     *olric.EmbeddedClient
	discovery  *clusterServiceDiscovery // non-nil only in cluster mode
	peerAPIKey []byte
}

// clusterRuntimeOptions configures the runtime.
//
// A nil ClusterConfig (or one with Enable=false) yields embedded-only
// behaviour: loopback ephemeral ports, no peers, no replication. A
// ClusterConfig with Enable=true switches to networked mode.
type clusterRuntimeOptions struct {
	// Cluster is the operator-facing cluster config. nil → embedded-only.
	Cluster *ClusterConfig
	// LogOutput receives olric's own logs. Defaults to io.Discard because
	// olric's default DEBUG verbosity drowns the cosmoguard log otherwise.
	LogOutput io.Writer
	// StartTimeout bounds discovery, daemon start and bootstrap together.
	// The default leaves margin within the operator's 60s startup probe.
	StartTimeout time.Duration
	// Lookup is the DNS resolver used by the discovery plugin. nil →
	// defaultLookup. Plumbed for tests so 2-node cluster integration tests
	// don't depend on the host's resolver.
	Lookup LookupFunc
	// L2MaxBytesPerNode caps the olric L2's per-node in-use bytes for each
	// response-cache DMap (LRU eviction above the cap). 0 disables L2
	// eviction (unlimited). The non-cache DMaps (evictionExemptDMaps) are
	// always kept exempt regardless of this value.
	L2MaxBytesPerNode uint64
}

func newClusterRuntime(opts clusterRuntimeOptions) (*clusterRuntime, error) {
	if opts.LogOutput == nil {
		opts.LogOutput = io.Discard
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 45 * time.Second
	}

	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), opts.StartTimeout)
	defer cancel()

	// The presence of a Cluster block is the operator's signal that
	// they want networked cluster mode. Omit it for embedded loopback
	// (the default for single-pod installs and tests).
	clustered := opts.Cluster != nil

	c := config.New("local")

	// In embedded/single-pod mode this node owns every partition, so the
	// olric LRU cap (which can't evict a partition below one entry) has an
	// effective floor of PartitionCount × maxEntrySize per DMap. olric's
	// default of 271 partitions would floor a small pod's L2 well above its
	// budget (271 × 1 MiB = 271 MiB/DMap) and reintroduce the OOM risk this
	// guards. A smaller count lowers that floor (~16 MiB/DMap) so MaxInuse
	// actually binds at realistic budgets. Only safe to change in embedded
	// mode — in a real cluster every peer must agree on PartitionCount, so
	// there we keep olric's default.
	if !clustered {
		c.PartitionCount = embeddedPartitionCount
	}

	mc := memberlist.DefaultLocalConfig()
	// memberlist refuses to start if both LogOutput and Logger are set,
	// and olric installs its own Logger onto MemberlistConfig at startup
	// (olric/internal/discovery/discovery.go), so we explicitly clear
	// LogOutput here.
	mc.LogOutput = nil

	var peerAPIKey []byte
	if clustered {
		bindAddr := opts.Cluster.BindAddr
		if bindAddr == "" {
			bindAddr = "0.0.0.0"
		}
		c.BindAddr = bindAddr
		c.BindPort = opts.Cluster.BindPort

		mc.BindAddr = bindAddr
		mc.BindPort = opts.Cluster.GossipPort
		mc.AdvertisePort = opts.Cluster.GossipPort

		// Enable memberlist gossip encryption + authentication. The key is
		// validated at config load (validateCacheBackend), but decode again
		// here so a runtime constructed directly (tests) still fails closed
		// rather than starting an unencrypted cluster.
		key, err := DecodeClusterEncryptionKey(opts.Cluster.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("cluster runtime: encryption key: %w", err)
		}
		mc.SecretKey = key
		peerAPIKey = derivePeerAPIKey(key)
		// SecretKey only protects the memberlist GOSSIP plane. The olric RESP
		// DATA port (BindPort) is a separate listener over which peers (and
		// the embedded client) read/write the shared DMaps — without auth,
		// any host reaching that port could manipulate rate-limit buckets,
		// the cache, and the JWT replay set. Turn on olric's password auth so
		// the data plane requires the same shared secret; olric wires the
		// embedded client's credentials from the same setting automatically.
		c.Authentication = &config.Authentication{Password: opts.Cluster.EncryptionKey}
	} else {
		// Embedded-only: ephemeral loopback ports for both surfaces.
		mc.BindAddr = "127.0.0.1"
		mc.BindPort = 0

		port, err := pickLoopbackPort()
		if err != nil {
			return nil, fmt.Errorf("cluster runtime: %w", err)
		}
		c.BindAddr = "127.0.0.1"
		c.BindPort = port
	}
	c.MemberlistConfig = mc
	c.MemberlistConfig.Name = net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort))

	// olric rejects configs that set both LogOutput and Logger ("Cannot
	// specify both" — see olric.go cluster-join code). Pick Logger and
	// route everything there.
	c.LogOutput = nil
	c.Logger = stdlog.New(opts.LogOutput, "olric: ", stdlog.LstdFlags)
	c.LogLevel = config.LogLevelError
	c.LogVerbosity = 1

	// Leaving the cluster fast on shutdown — we never reuse this daemon
	// after Close() so there's no need to give peers a polite goodbye.
	c.LeaveTimeout = 500 * time.Millisecond

	var discovery *clusterServiceDiscovery
	if clustered {
		c.ReplicaCount = opts.Cluster.ReplicaCount
		c.MemberCountQuorum = int32(opts.Cluster.Quorum)
		c.ReadQuorum = opts.Cluster.Quorum
		c.WriteQuorum = opts.Cluster.Quorum

		// Build the discovery plugin first so we can hand olric an
		// initial peer set. Olric re-queries discovery during join attempts;
		//
		// The self identifier is the memberlist gossip address (BindAddr
		// + GossipPort), because that's the form DiscoverPeers returns
		// for peers. Using c.MemberlistConfig.Name (which we set to
		// BindAddr:BindPort for olric's own bookkeeping) would never
		// match a peer entry — different port surface.
		gossipSelf := net.JoinHostPort(c.BindAddr, strconv.Itoa(opts.Cluster.GossipPort))
		self := []string{gossipSelf}
		d, err := newClusterServiceDiscovery(opts.Cluster, self, opts.Lookup)
		if err != nil {
			return nil, fmt.Errorf("cluster runtime: discovery: %w", err)
		}
		discovery = d

		// Wire the plugin into olric's ServiceDiscovery map. Olric reads
		// this map after Initialize/SetConfig/SetLogger; ServiceDiscovery
		// is the official plugin shape. The "provider" key is convention
		// (the consul/k8s/nats plugins use it), but olric doesn't read it
		// for its own logic — it just iterates the map values.
		c.ServiceDiscovery = map[string]interface{}{
			"plugin": discovery,
		}

		// Pre-populate Peers with whatever the discovery plugin currently
		// knows. An empty list is valid; join attempts can refresh it.
		// Log a cold-start DNS failure so operators can diagnose failed joins.
		initialPeers := discovery.DiscoverPeers
		if discovery.mode == "dns" {
			initialPeers = func() ([]string, error) { return discovery.discoverDNS(ctx) }
		}
		if peers, err := initialPeers(); err == nil {
			c.Peers = peers
		} else {
			slog.Warn("cluster discovery: initial peer lookup failed (will retry)", "error", err, "mode", opts.Cluster.Discovery.Mode)
		}
	}

	// Preserve the engine's table size, supplying a fallback only when absent.
	// Entries above the native table limit are served uncached.
	if c.DMaps == nil {
		c.DMaps = &config.DMaps{}
	}
	if c.DMaps.Engine == nil {
		c.DMaps.Engine = config.NewEngine()
	}
	if c.DMaps.Engine.Config == nil {
		c.DMaps.Engine.Config = map[string]interface{}{}
	}
	if _, set := c.DMaps.Engine.Config["tableSize"]; !set {
		c.DMaps.Engine.Config["tableSize"] = uint64(olricTableSizeBytes)
	}

	// Bound the L2 (olric) working set so a high-cardinality query load can't
	// grow the shared store until the pod is OOMKilled (issue #15). Embedded/
	// single-pod mode stores no backups (replicaFactor 1); clustered mode
	// holds replicaCount copies per node, so the cap is divided accordingly.
	replicaFactor := 1
	if clustered && opts.Cluster.ReplicaCount > 0 {
		replicaFactor = opts.Cluster.ReplicaCount
	}
	applyL2EvictionConfig(c.DMaps, opts.L2MaxBytesPerNode, replicaFactor)

	if err := c.Sanitize(); err != nil {
		return nil, fmt.Errorf("cluster runtime: sanitize: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("cluster runtime: validate: %w", err)
	}

	if err := ctx.Err(); err != nil {
		if discovery != nil {
			_ = discovery.Close()
		}
		return nil, fmt.Errorf("cluster runtime: before start: %w", err)
	}
	ready := make(chan struct{})
	c.Started = func() { close(ready) }

	db, err := olric.New(c)
	if err != nil {
		return nil, fmt.Errorf("cluster runtime: new: %w", err)
	}

	startErr := make(chan error, 1)
	go func() {
		if err := db.Start(); err != nil {
			startErr <- err
		}
	}()

	select {
	case <-ready:
		// daemon is up
	case err := <-startErr:
		// Defensive teardown: olric.Start may have spawned partial
		// internal state (memberlist, partition runner, etc.) before
		// returning the error, and we own the only handle to it. The
		// timeout branch already does this; mirror it here so a failed
		// start doesn't leak goroutines or bound sockets into the
		// remainder of the process.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = db.Shutdown(ctx)
		cancel()
		if discovery != nil {
			_ = discovery.Close()
		}
		return nil, fmt.Errorf("cluster runtime: start: %w", err)
	case <-ctx.Done():
		startCause := ctx.Err()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = db.Shutdown(ctx)
		cancel()
		if discovery != nil {
			_ = discovery.Close()
		}
		return nil, fmt.Errorf("cluster runtime: timed out after %s waiting for olric to start: %w", opts.StartTimeout, startCause)
	}

	client := db.NewEmbeddedClient()
	bootstrapAt := time.Now()
	if err := waitClusterBootstrap(ctx, client); err != nil {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		_ = db.Shutdown(shutdownCtx)
		stop()
		if discovery != nil {
			_ = discovery.Close()
		}
		return nil, fmt.Errorf("cluster runtime: bootstrap after %s: %w", time.Since(startedAt), err)
	}
	slog.Info("olric bootstrap ready", "bootstrap_wait", time.Since(bootstrapAt), "startup_elapsed", time.Since(startedAt))

	return &clusterRuntime{
		db:         db,
		client:     client,
		discovery:  discovery,
		peerAPIKey: peerAPIKey,
	}, nil
}

// One worker retries on the same daemon. NewDMap ignores context, so the
// caller's deadline is enforced separately; an active native check can finish
// after shutdown (olric bounds it to 10s). The buffered result never blocks it.
func waitClusterBootstrap(ctx context.Context, client interface {
	NewDMap(string, ...olric.DMapOption) (olric.DMap, error)
}) error {
	result := make(chan error, 1)
	var mu sync.Mutex
	var last error
	deadlineError := func() error {
		mu.Lock()
		defer mu.Unlock()
		if last != nil {
			return fmt.Errorf("%w (last bootstrap error: %w)", ctx.Err(), last)
		}
		return ctx.Err()
	}
	go func() {
		logged := false
		for {
			if ctx.Err() != nil {
				result <- ctx.Err()
				return
			}
			// Opening this stable internal DMap leaves an empty map for the daemon's lifetime.
			_, err := client.NewDMap("cosmoguard:bootstrap")
			if err == nil || (!errors.Is(err, olric.ErrOperationTimeout) && !errors.Is(err, olric.ErrClusterQuorum)) {
				result <- err
				return
			}
			mu.Lock()
			last = err
			mu.Unlock()
			if !logged {
				slog.Info("waiting for olric bootstrap", "error", err)
				logged = true
			}
			select {
			case <-ctx.Done():
				result <- ctx.Err()
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	select {
	case <-ctx.Done():
		return deadlineError()
	case err := <-result:
		if ctx.Err() != nil {
			return deadlineError()
		}
		return err
	}
}

// Client returns the in-process client used by cache, rate-limiter, and
// observability replication. Returns nil if the runtime is nil so call sites
// in tests can guard against it without panicking.
func (cr *clusterRuntime) Client() *olric.EmbeddedClient {
	if cr == nil {
		return nil
	}
	return cr.client
}

// Close stops the daemon. The provided context bounds the shutdown wait; if
// it expires, olric returns a context error and we surface it.
func (cr *clusterRuntime) Close(ctx context.Context) error {
	if cr == nil || cr.db == nil {
		return nil
	}
	if cr.discovery != nil {
		_ = cr.discovery.Close()
	}
	return cr.db.Shutdown(ctx)
}

// pickLoopbackPort asks the kernel for a free TCP port on 127.0.0.1 and
// returns it immediately after closing the probe listener. There's a tiny
// TOCTOU window between Close and olric's bind, but TCP port reuse on
// loopback within a single process is reliable in practice and the
// alternative (BindPort: 0) isn't accepted by olric's config validator.
func pickLoopbackPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}
