package cosmoguard

import (
	"context"
	"errors"
	"fmt"
	stdlog "log"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

// clusterServiceDiscovery is the olric ServiceDiscovery adapter cosmoguard
// installs when the cluster: block is present. Olric calls DiscoverPeers()
// periodically (and on demand during join) and expects host:port strings
// it can hand to memberlist. Initialize / SetConfig / SetLogger / Register
// / Deregister / Close are no-ops or trivial book-keeping for our modes.
//
// The plugin is constructed by newClusterServiceDiscovery from a config
// snapshot and a default port (cluster.gossipPort) used when a peer entry
// omits its port. Self-references (matching the local advertise address)
// are filtered out at runtime so the same config can be deployed to every
// pod without each pod treating itself as a peer.
type clusterServiceDiscovery struct {
	mode        string
	defaultPort int
	selfAddrs   map[string]struct{}

	// dns-mode fields
	host            string
	port            int
	refreshInterval time.Duration
	lookup          LookupFunc
	ctx             context.Context
	cancel          context.CancelFunc

	// static-mode field
	staticPeers []string

	logger *stdlog.Logger
	mu     sync.Mutex
}

// newClusterServiceDiscovery wires a discovery instance from a validated
// ClusterDiscoveryConfig snapshot. The selfAddrs argument is the set of
// addresses the local node advertises (host:port) so DiscoverPeers can
// filter them out of the returned list. A nil/empty selfAddrs is fine —
// it just means no filtering happens (caller's responsibility to verify
// olric handles the self-membership case gracefully, which it does).
func newClusterServiceDiscovery(
	cfg *ClusterConfig,
	selfAddrs []string,
	lookup LookupFunc,
) (*clusterServiceDiscovery, error) {
	if cfg == nil || cfg.Discovery == nil {
		return nil, errors.New("cluster discovery: cluster config or discovery unset")
	}
	if lookup == nil {
		lookup = defaultLookup
	}

	d := &clusterServiceDiscovery{
		mode:        cfg.Discovery.Mode,
		defaultPort: cfg.GossipPort,
		selfAddrs:   make(map[string]struct{}, len(selfAddrs)),
		lookup:      lookup,
	}
	for _, s := range selfAddrs {
		d.selfAddrs[s] = struct{}{}
	}

	switch cfg.Discovery.Mode {
	case "dns":
		if cfg.Discovery.DNS == nil {
			return nil, errors.New("cluster discovery: dns mode requires discovery.dns")
		}
		d.host = cfg.Discovery.DNS.Host
		d.port = cfg.Discovery.DNS.Port
		if d.port == 0 {
			d.port = cfg.GossipPort
		}
		d.refreshInterval = cfg.Discovery.DNS.RefreshInterval
		if d.refreshInterval <= 0 {
			d.refreshInterval = 10 * time.Second
		}
	case "static":
		if cfg.Discovery.Static == nil {
			return nil, errors.New("cluster discovery: static mode requires discovery.static")
		}
		d.staticPeers = append([]string(nil), cfg.Discovery.Static.Peers...)
	case "kubernetes", "mdns":
		// Experimental — accepted at config load but blocked at runtime
		// until v4 Step 7 finishes manual validation. Surfacing a clear
		// error here (rather than at config load) lets operators try
		// these modes in a feature branch without a global rebuild.
		return nil, fmt.Errorf("cluster discovery: mode %q is experimental and not yet implemented in this build (track v4 Step 7)", cfg.Discovery.Mode)
	default:
		return nil, fmt.Errorf("cluster discovery: unknown mode %q", cfg.Discovery.Mode)
	}

	return d, nil
}

// Initialize satisfies olric.ServiceDiscovery. Olric calls it once after
// SetConfig + SetLogger. We use it to install the lookup context so a
// later Close cancels in-flight DNS work.
func (d *clusterServiceDiscovery) Initialize() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ctx, d.cancel = context.WithCancel(context.Background())
	return nil
}

// SetConfig is a no-op: we already received our config via the
// constructor (cosmoguard owns the plugin lifecycle directly rather than
// going through olric's generic map[string]interface{} surface).
func (d *clusterServiceDiscovery) SetConfig(_ map[string]interface{}) error { return nil }

// SetLogger captures olric's logger for plugin-internal messages.
func (d *clusterServiceDiscovery) SetLogger(l *stdlog.Logger) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.logger = l
}

// Register is a no-op: registration in our supported modes (DNS, static)
// is the operator's responsibility — they declare the peer set externally
// (DNS records or static config). The other modes (kubernetes, mdns) will
// implement Register when Step 7 lands.
func (d *clusterServiceDiscovery) Register() error { return nil }

// Deregister is a no-op for the same reason as Register.
func (d *clusterServiceDiscovery) Deregister() error { return nil }

// DiscoverPeers returns the current set of peer host:port addresses. For
// DNS mode this issues a fresh resolver lookup; for static it returns the
// configured list. Self-addresses (the local node's own advertise pairs)
// are filtered out so olric doesn't try to join itself.
func (d *clusterServiceDiscovery) DiscoverPeers() ([]string, error) {
	d.mu.Lock()
	ctx := d.ctx
	d.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	switch d.mode {
	case "dns":
		return d.discoverDNS(ctx)
	case "static":
		return d.filterSelf(d.staticPeers), nil
	default:
		return nil, fmt.Errorf("cluster discovery: mode %q has no DiscoverPeers implementation", d.mode)
	}
}

// Close cancels any in-flight discovery work. Safe to call multiple times.
func (d *clusterServiceDiscovery) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.cancel != nil {
		d.cancel()
		d.cancel = nil
	}
	return nil
}

// discoverDNS resolves the configured host to a sorted, deduplicated list
// of host:port peer addresses. Lookup errors are returned to the caller —
// olric retries periodically so a transient DNS hiccup doesn't crash the
// cluster join.
func (d *clusterServiceDiscovery) discoverDNS(ctx context.Context) ([]string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, d.refreshInterval)
	defer cancel()
	ips, err := d.lookup(lookupCtx, d.host)
	if err != nil {
		return nil, fmt.Errorf("cluster discovery: dns lookup %q: %w", d.host, err)
	}
	sort.Strings(ips)
	out := make([]string, 0, len(ips))
	portStr := strconv.Itoa(d.port)
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, portStr))
	}
	return d.filterSelf(out), nil
}

// filterSelf removes any peer address that matches one of the local
// node's advertised addresses. Critical for the "single config across
// every pod" deployment model — without this filtering each pod would
// see itself in DiscoverPeers and olric would attempt to join itself.
//
// Peer entries may arrive as "host:port" or as a bare host (static-peers
// short-hand: the configured cluster.gossipPort is implied). The "has
// port?" detection uses net.SplitHostPort rather than a colon-search
// because IPv6 literals contain colons in the host portion: a bare
// "::1" or "[::1]" would otherwise be treated as already-having-a-port
// and never get normalised, so the local node's "[::1]:<gossipPort>"
// in selfAddrs would never match — the pod would self-join.
func (d *clusterServiceDiscovery) filterSelf(in []string) []string {
	if len(d.selfAddrs) == 0 {
		return append([]string(nil), in...)
	}
	out := make([]string, 0, len(in))
	for _, p := range in {
		if _, self := d.selfAddrs[p]; self {
			continue
		}
		// Normalised match: both endpoints expressed as host:port.
		// SplitHostPort errors on a bare host (no port, no brackets)
		// AND on a bracketed-but-portless "[::1]" — both are the
		// short-hand we want to expand. Successful parse means the
		// entry already carries a port and is left alone.
		if _, _, err := net.SplitHostPort(p); err != nil {
			// Strip any outer brackets before re-joining: net.JoinHostPort
			// wraps anything containing ':' in brackets, so feeding it a
			// pre-bracketed "[::1]" would yield "[[::1]]:port" and break
			// the selfAddrs equality check (and confuse downstream
			// memberlist parsing). The brackets-around-bare-host shape is
			// only valid as short-hand here; portless brackets carry no
			// other meaning so this strip is unambiguous.
			host := p
			if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
				host = host[1 : len(host)-1]
			}
			p = net.JoinHostPort(host, strconv.Itoa(d.defaultPort))
		}
		if _, self := d.selfAddrs[p]; self {
			continue
		}
		out = append(out, p)
	}
	return out
}
