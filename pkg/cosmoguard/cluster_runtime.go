package cosmoguard

import (
	"context"
	"fmt"
	"io"
	stdlog "log"
	"log/slog"
	"net"
	"strconv"
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
// LRU. 10 matches olric's own default and is plenty for a cache whose
// hot set turns over within the (short) response TTL.
const olricLRUSamples = 10

// applyL2EvictionConfig bounds the olric L2 working set (issue #15). LRU is
// set as the GLOBAL default so every response-cache DMap inherits it; the
// non-cache DMaps (rate-limit buckets/locks, JWT replay set, observability
// snapshots) are pinned back to no-eviction via Custom so memory pressure
// can never evict security/correctness state. l2MaxBytesPerNode == 0 leaves
// eviction disabled (unlimited). Split out so it is unit-testable without
// standing up a real olric daemon.
func applyL2EvictionConfig(dmaps *config.DMaps, l2MaxBytesPerNode uint64) {
	if dmaps == nil || l2MaxBytesPerNode == 0 {
		return
	}
	dmaps.EvictionPolicy = config.LRUEviction
	dmaps.MaxInuse = int(l2MaxBytesPerNode)
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
	db        *olric.Olric
	client    *olric.EmbeddedClient
	discovery *clusterServiceDiscovery // non-nil only in cluster mode
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
	// StartTimeout caps how long we wait for the daemon to call its Started
	// callback. Embedded-only startup is sub-second on every machine I've
	// measured; the 15s ceiling is for cluster-mode where peer convergence
	// has to happen before Started fires.
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
		opts.StartTimeout = 15 * time.Second
	}

	// The presence of a Cluster block is the operator's signal that
	// they want networked cluster mode. Omit it for embedded loopback
	// (the default for single-pod installs and tests).
	clustered := opts.Cluster != nil

	c := config.New("local")

	mc := memberlist.DefaultLocalConfig()
	// memberlist refuses to start if both LogOutput and Logger are set,
	// and olric installs its own Logger onto MemberlistConfig at startup
	// (olric/internal/discovery/discovery.go), so we explicitly clear
	// LogOutput here.
	mc.LogOutput = nil

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
		// initial peer set. olric's discovery hook re-queries it on a
		// timer, so this initial list is just a fast-path; the periodic
		// DiscoverPeers calls handle steady-state churn.
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
		// knows. Olric won't crash on an empty list (it just waits for
		// the next periodic discovery tick), but giving it a head start
		// shortens cluster-form time on fresh starts. A startup-time
		// discovery failure (commonly: DNS not yet resolvable on cold
		// boot) is soft-failed — olric's periodic retry compensates —
		// but it MUST be logged so an operator staring at "why won't
		// my cluster form" has a breadcrumb to follow.
		if peers, err := discovery.DiscoverPeers(); err == nil {
			c.Peers = peers
		} else {
			slog.Warn("cluster discovery: initial peer lookup failed (will retry)", "error", err, "mode", opts.Cluster.Discovery.Mode)
		}
	}

	// Cap olric's per-fragment table allocation at 256 KiB. Olric's
	// default (1 MiB per (partition, dmap) fragment, allocated upfront
	// regardless of contents) puts an idle 271-partition × 3-dmap cluster
	// well past 500 MiB. 256 KiB covers typical cosmoguard cache entries;
	// oversized entries return ErrEntryTooLarge and the proxy serves
	// them uncached.
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
		c.DMaps.Engine.Config["tableSize"] = uint64(256 << 10) // 256 KiB
	}

	// Bound the L2 (olric) working set so a high-cardinality query load can't
	// grow the shared store until the pod is OOMKilled (issue #15).
	applyL2EvictionConfig(c.DMaps, opts.L2MaxBytesPerNode)

	if err := c.Sanitize(); err != nil {
		return nil, fmt.Errorf("cluster runtime: sanitize: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("cluster runtime: validate: %w", err)
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
		return nil, fmt.Errorf("cluster runtime: start: %w", err)
	case <-time.After(opts.StartTimeout):
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = db.Shutdown(ctx)
		cancel()
		return nil, fmt.Errorf("cluster runtime: timed out after %s waiting for olric to start", opts.StartTimeout)
	}

	return &clusterRuntime{
		db:        db,
		client:    db.NewEmbeddedClient(),
		discovery: discovery,
	}, nil
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
