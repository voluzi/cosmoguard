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
