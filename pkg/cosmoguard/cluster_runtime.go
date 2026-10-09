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
	"github.com/redis/go-redis/v9"
	"github.com/voluzi/olric"
	"github.com/voluzi/olric/config"

	"github.com/voluzi/cosmoguard/v6/internal/boundedcall"
	"github.com/voluzi/cosmoguard/v6/internal/olricstore"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
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

// olricLRUSamples bounds Olric's eviction candidates; the slab engine supplies
// them in recency order, starting with the oldest.
const olricLRUSamples = 10

const bootstrapMaxWait = 10 * time.Minute

// Standalone members have no clustered peers. Clustered members keep 271.
const embeddedPartitionCount = 16

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
// MaxInuse uses the total response budget divided by RF as a soft working-set
// threshold. Olric divides it among owned primary partitions; backups bypass
// native LRU. The shared allocator, not these per-DMap thresholds, bounds backing.
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
	// together (both may evict). Derived from the same per-node byte
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
	responseOperations                  cache.Option
	memoryPool                          *cache.MemoryPool
	limiterOperations, replayOperations *boundedcall.Gate
	db                                  *olric.Olric
	client                              *olric.EmbeddedClient
	discovery                           *clusterServiceDiscovery // non-nil only in cluster mode
	peerAPIKey                          []byte
	responsePool, securityPool          *olricstore.Pool
	removeMetrics                       func()
}

// clusterRuntimeOptions configures the runtime.
//
// A nil ClusterConfig (or one with Enable=false) yields embedded-only
// behaviour: loopback ephemeral ports, no peers, no replication. A
// ClusterConfig with Enable=true switches to networked mode.
type clusterRuntimeOptions struct {
	Context context.Context
	// Cluster is the operator-facing cluster config. nil → embedded-only.
	Cluster *ClusterConfig
	// LogOutput receives olric's own logs. Defaults to io.Discard because
	// olric's default DEBUG verbosity drowns the cosmoguard log otherwise.
	LogOutput io.Writer
	// StartTimeout bounds startup and loss of coordinator reachability.
	StartTimeout     time.Duration
	BootstrapTimeout time.Duration
	// Lookup is the DNS resolver used by the discovery plugin. nil →
	// defaultLookup. Plumbed for tests so 2-node cluster integration tests
	// don't depend on the host's resolver.
	Lookup                  LookupFunc
	L1MaxBytes, L1MaxItems  uint64
	ResponsePoolBytes       uint64
	ResponseLRUBytesPerDMap uint64
	L2WorkBytes             uint64
}

func newClusterRuntime(opts clusterRuntimeOptions) (*clusterRuntime, error) {
	if opts.LogOutput == nil {
		opts.LogOutput = io.Discard
	}
	if opts.StartTimeout == 0 {
		opts.StartTimeout = 45 * time.Second
	}

	parent := opts.Context
	if parent == nil {
		parent = context.Background()
	}
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(parent, opts.StartTimeout)
	defer cancel()

	// The presence of a Cluster block is the operator's signal that
	// they want networked cluster mode. Omit it for embedded loopback
	// (the default for single-pod installs and tests).
	clustered := opts.Cluster != nil

	c := config.New("local")

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

	// Bound the graceful leave broadcast within the process shutdown budget.
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

	memoryPool := cache.NewMemoryPool(opts.L1MaxBytes, opts.L1MaxItems)
	responsePool := olricstore.NewPool(opts.ResponsePoolBytes, olricstore.Response, recordL2StorageRejection)
	securityPool := olricstore.NewPool(0, olricstore.Security, nil)
	success := false
	defer func() {
		if !success {
			_ = memoryPool.Close()
			_ = responsePool.Close(context.Background())
			_ = securityPool.Close(context.Background())
		}
	}()
	// Compaction holds the fragment lock while expiring primary and backup records.
	c.DMaps.TriggerCompactionInterval = time.Second
	c.DMaps.Engine = &config.Engine{Implementation: olricstore.NewEngine(responsePool)}
	replicaFactor := 1
	if clustered && opts.Cluster.ReplicaCount > 0 {
		replicaFactor = opts.Cluster.ReplicaCount
	}
	applyL2EvictionConfig(c.DMaps, opts.ResponseLRUBytesPerDMap, replicaFactor)
	if c.DMaps.Custom == nil {
		c.DMaps.Custom = map[string]config.DMap{}
	}
	for _, name := range evictionExemptDMaps {
		c.DMaps.Custom[name] = config.DMap{EvictionPolicy: config.EvictionPolicy("NONE"), Engine: &config.Engine{Implementation: olricstore.NewEngine(securityPool)}}
	}

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
	bootstrapBudget := opts.BootstrapTimeout
	if bootstrapBudget == 0 {
		bootstrapBudget = opts.StartTimeout
	}
	quorum := 1
	password := ""
	if clustered {
		quorum, password = opts.Cluster.Quorum, opts.Cluster.EncryptionKey
	}
	if err := waitClusterBootstrapProgress(parent, client, bootstrapBudget, startedAt.Add(bootstrapMaxWait), func(ctx context.Context) bool {
		return bootstrapCoordinatorReachable(ctx, client, password, quorum)
	}); err != nil {
		shutdownCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
		_ = db.Shutdown(shutdownCtx)
		stop()
		if discovery != nil {
			_ = discovery.Close()
		}
		return nil, fmt.Errorf("cluster runtime: bootstrap after %s: %w", time.Since(startedAt), err)
	}
	slog.Info("olric bootstrap ready", "bootstrap_wait", time.Since(bootstrapAt), "startup_elapsed", time.Since(startedAt))

	success = true
	cr := &clusterRuntime{
		db:           db,
		memoryPool:   memoryPool,
		client:       client,
		discovery:    discovery,
		peerAPIKey:   peerAPIKey,
		responsePool: responsePool, securityPool: securityPool,
	}
	workBytes := opts.L2WorkBytes
	if workBytes == 0 {
		workBytes = responseWorkBytes()
	}
	cr.responseOperations = newResponseOperations(workBytes)
	cr.limiterOperations = newLimiterOperations()
	cr.replayOperations = newReplayOperations()
	cr.removeMetrics = addL2Metrics(cr)
	return cr, nil
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
			return fmt.Errorf("%w (last bootstrap error: %w)", context.Cause(ctx), last)
		}
		return context.Cause(ctx)
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

// A reachable coordinator may be scanning old owners before its first routing push.
// Keep one native bootstrap worker, but bound waiting without live cluster evidence.
func waitClusterBootstrapProgress(ctx context.Context, client interface {
	NewDMap(string, ...olric.DMapOption) (olric.DMap, error)
}, budget time.Duration, deadline time.Time, reachable func(context.Context) bool) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	interval := min(time.Second, budget/4)
	progress := make(chan bool, 1)
	go func() {
		for ctx.Err() == nil {
			probeCtx, stop := context.WithTimeout(ctx, interval)
			live := reachable(probeCtx)
			stop()
			select {
			case progress <- live:
			case <-ctx.Done():
				return
			}
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() { done <- waitClusterBootstrap(ctx, client) }()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	hardLimit := time.NewTimer(time.Until(deadline))
	defer hardLimit.Stop()
	wasReachable := false
	for {
		select {
		case live := <-progress:
			if live {
				wasReachable = true
				timer.Reset(budget)
			}
		case <-hardLimit.C:
			cause := fmt.Errorf("bootstrap exceeded the 10-minute constructor startup limit: %w", context.DeadlineExceeded)
			if wasReachable {
				cause = fmt.Errorf("coordinator was reachable but no routing table arrived: %w", cause)
			}
			cancel(cause)
		case <-timer.C:
			cancel(context.DeadlineExceeded)
		case err := <-done:
			return err
		}
	}
}

func bootstrapCoordinatorReachable(ctx context.Context, client *olric.EmbeddedClient, password string, quorum int) bool {
	members, err := client.Members(ctx)
	if err != nil || len(members) < quorum {
		return false
	}
	for _, member := range members {
		if !member.Coordinator {
			continue
		}
		// A separate bounded connection avoids the data pool's routing/replica backlog.
		probe := redis.NewClient(&redis.Options{Addr: member.Name, Password: password,
			MaxRetries: -1, PoolSize: 1, ContextTimeoutEnabled: true,
			DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
		defer probe.Close()
		return probe.Ping(ctx).Err() == nil
	}
	return false
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
	cr.responseOperations.CloseOperations()
	_ = cr.memoryPool.Close()
	cr.limiterOperations.Close()
	cr.replayOperations.Close()
	err := cr.db.Shutdown(ctx)
	if cr.removeMetrics != nil {
		cr.removeMetrics()
	}
	_ = cr.responsePool.Close(context.Background())
	_ = cr.securityPool.Close(context.Background())
	return err
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

func (cr *clusterRuntime) ResponseOperations() cache.Option {
	if cr == nil {
		return nil
	}
	return cr.responseOperations
}
