package cosmoguard

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// LookupFunc resolves a DNS host to a sorted, deduplicated list of IPs.
// Pulled out as a type so tests can inject a deterministic resolver
// without touching the system stub.
//
// Both A and AAAA records are returned (Go's net.DefaultResolver does
// both transparently). The order is stabilized by Discoverer to make
// reconcile diffs reproducible.
//
// The ctx is the caller's lifecycle: Discoverer.Stop cancels every
// in-flight reconcile via this context, so a hung DNS server can't
// pin a reconciler goroutine past Stop. Custom implementations MUST
// honour ctx so the same invariant holds — wrap any blocking call in
// a select on ctx.Done() or pass ctx into the underlying resolver.
type LookupFunc func(ctx context.Context, host string) ([]string, error)

// defaultLookup is the net.DefaultResolver-backed lookup. We use
// LookupHost (not LookupIP) because the resolver applies the cluster's
// search domains to LookupHost — the relevant behavior for short
// names like `<svc>.<ns>` that Kubernetes pods rely on.
//
// We honour the caller's ctx AND impose a 5s per-lookup ceiling so a
// hung resolver can't pin the reconcile goroutine indefinitely even
// if the caller forgot to bound their ctx. 5s is comfortably longer
// than any reasonable cluster-DNS roundtrip and short enough that a
// stuck resolver doesn't pin the reconciler for an entire interval.
func defaultLookup(ctx context.Context, host string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return net.DefaultResolver.LookupHost(ctx, host)
}

// DiscoveryTemplate bundles a NodeConfig template with the set of
// IPs it expanded to at boot. The Discoverer uses the seed map to
// initialize its `current` state so the first runtime reconcile
// doesn't perceive boot-time upstreams as "new" and emit
// "discovery added upstream" logs for them.
type DiscoveryTemplate struct {
	Template NodeConfig
	// SeedIPs is the set of IPs that resolved at boot. Stored as a
	// map for O(1) membership tests inside the reconciler. Empty
	// when the boot-time lookup returned zero records or errored.
	SeedIPs map[string]struct{}
}

// expandDiscoveryNodes walks cfg.Nodes and replaces each entry with
// Discovery set by one synthetic NodeConfig per resolved IP. Static
// entries (Discovery == nil) pass through unchanged.
//
// The returned templates slice captures the original template entries
// plus the IPs each one resolved to at boot, so the runtime
// reconciler can seed its `current` state and not re-add boot
// upstreams on its first tick. A template that resolves to zero IPs
// at boot is still recorded in templates — the reconcile loop will
// pick up pods as they appear — but it does NOT contribute concrete
// upstreams to cfg.Nodes for the initial pool construction.
//
// Lookup errors are soft-failed: a transient cluster-DNS hiccup
// during cosmoguard startup must not crash-loop an autoscaled
// deployment. The error is logged via slog so operators can see
// what happened.
//
// Caller must distinguish an empty config from pending discovery. The
// latter starts with empty pools that the reconciler fills in.
func expandDiscoveryNodes(cfg *Config, lookup LookupFunc) ([]DiscoveryTemplate, error) {
	if lookup == nil {
		lookup = defaultLookup
	}
	// Validate template Names are unique BEFORE expansion. Two
	// templates with the same Name would synthesize duplicate
	// upstream names per IP — AddUpstream would overwrite the
	// earlier entry and metric series for the first template's
	// upstreams would leak (no DeleteLabelValues fires because
	// RemoveUpstream is never called on the overwritten entry).
	seenTemplate := map[string]struct{}{}
	for _, n := range cfg.Nodes {
		if n.Discovery == nil {
			continue
		}
		key := n.Name
		if key == "" {
			// Auto-assigned names happen later; for templates we
			// require operator-supplied to keep the failure mode
			// clean. PrepareConfig assigns "node-N" so an empty
			// Name here means the operator left it blank AND no
			// auto-assignment has run, which shouldn't happen.
			continue
		}
		if _, dup := seenTemplate[key]; dup {
			return nil, fmt.Errorf("discovery template names must be unique: %q appears more than once", key)
		}
		seenTemplate[key] = struct{}{}
	}
	// Boot-time lookups inherit a single bounded ctx — total wall-time
	// for discovery expansion shouldn't exceed a few seconds even
	// across many templates, otherwise startup latency snowballs.
	bootCtx, cancelBoot := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelBoot()
	expanded := make([]NodeConfig, 0, len(cfg.Nodes))
	templates := make([]DiscoveryTemplate, 0)
	for _, n := range cfg.Nodes {
		if n.Discovery == nil {
			expanded = append(expanded, n)
			continue
		}
		seed := map[string]struct{}{}
		ips, err := lookup(bootCtx, n.Discovery.Host)
		if err != nil {
			// Soft-fail: log with the template's identifiers so an
			// operator can find the bad host quickly, and continue.
			// The reconcile loop will pick the template up as soon
			// as DNS recovers.
			slog.Warn("discovery: boot-time DNS lookup failed; will retry from the reconcile loop",
				"template", n.Name,
				"host", n.Discovery.Host,
				"error", err.Error())
			templates = append(templates, DiscoveryTemplate{Template: n, SeedIPs: seed})
			continue
		}
		sort.Strings(ips)
		for _, ip := range ips {
			expanded = append(expanded, synthesizeDiscoveryNode(n, ip))
			seed[ip] = struct{}{}
		}
		templates = append(templates, DiscoveryTemplate{Template: n, SeedIPs: seed})
	}
	cfg.Nodes = expanded
	return templates, nil
}

// synthesizeDiscoveryNode produces a concrete NodeConfig from a
// template + resolved IP. The discovered IP becomes Host, the
// template's name + a sanitized IP form the upstream name (stable
// per pod IP so /metrics labels don't churn for the same pod), and
// all other template fields (ports, healthcheck, circuit-breaker,
// weight, TLS) are copied verbatim. Discovery is cleared on the
// synthetic so PrepareConfig's validator doesn't object on
// re-validation, and pool constructors don't try to expand it again.
func synthesizeDiscoveryNode(tmpl NodeConfig, ip string) NodeConfig {
	n := tmpl
	n.Discovery = nil
	n.Host = ip
	n.Name = discoveryUpstreamName(tmpl.Name, ip)
	return n
}

// discoveryUpstreamName produces a stable, label-safe upstream name
// from a template name + IP. We replace the colons in IPv6 literals
// with dashes — Prometheus label values accept anything, but
// dashboards typically display "name=2001-db8--1" more cleanly than
// "name=2001:db8::1" (and a few downstream tools tokenize on `:`).
func discoveryUpstreamName(templateName, ip string) string {
	clean := strings.ReplaceAll(ip, ":", "-")
	if templateName == "" {
		return clean
	}
	return templateName + "-" + clean
}

// Discoverer is the runtime DNS reconciler. It owns one goroutine per
// discovery template; each goroutine periodically re-resolves the
// template's Discovery.Host and reconciles the result against the
// pools it manages, calling AddUpstream / RemoveUpstream to converge
// pool membership on the live set of pods.
//
// One Discoverer instance serves every pool of every service for a
// given CosmoGuard — the same template populates the LCD, RPC,
// gRPC, and EVM pools, so reconciling once at the Discoverer level
// fans out to all of them.
type Discoverer struct {
	log       *Entry
	templates []DiscoveryTemplate
	lookup    LookupFunc

	// Pools to reconcile. HTTP pools and the gRPC pool both expose
	// AddUpstream / RemoveUpstream, but their upstream types differ
	// so we hold them in separately-typed slices rather than behind
	// a shared interface (an interface here would force everything
	// onto interface dispatch on the hot path, which the picker
	// can't afford).
	httpPools []*HttpUpstreamPool
	grpcPool  *GrpcUpstreamPool

	// cancel / wg drive the goroutine lifecycle. Start spawns one
	// goroutine per template under ctx; Stop cancels and waits.
	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// dash is the optional observability sink for refresh events.
	// nil-safe; set by SetDashboard.
	dash *dashboardObservability
}

// SetDashboard wires the dashboard observability sink so refresh
// events (ticks, adds, removes, errors) feed the dashboard's
// discovery-log endpoint.
func (d *Discoverer) SetDashboard(o *dashboardObservability) {
	d.mu.Lock()
	d.dash = o
	d.mu.Unlock()
}

// NewDiscoverer builds the runtime reconciler. templates is the slice
// returned by expandDiscoveryNodes; lookup is the resolver (use nil
// to pick up the default system resolver).
func NewDiscoverer(log *Entry, templates []DiscoveryTemplate, lookup LookupFunc) *Discoverer {
	if lookup == nil {
		lookup = defaultLookup
	}
	return &Discoverer{
		log:       log,
		templates: templates,
		lookup:    lookup,
	}
}

// RegisterHttpPool adds an HTTP pool to the set the Discoverer
// reconciles. Pools registered before Start receive the first
// reconcile tick. After Start, the registered pools are mutated
// by AddUpstream / RemoveUpstream as the resolved IP set drifts.
func (d *Discoverer) RegisterHttpPool(p *HttpUpstreamPool) {
	if p == nil {
		return
	}
	d.mu.Lock()
	d.httpPools = append(d.httpPools, p)
	d.mu.Unlock()
}

// RegisterGrpcPool registers the gRPC pool with the reconciler. Only
// one gRPC pool exists per CosmoGuard, so this is a single setter
// rather than an append.
func (d *Discoverer) RegisterGrpcPool(p *GrpcUpstreamPool) {
	d.mu.Lock()
	d.grpcPool = p
	d.mu.Unlock()
}

// Start kicks off one reconcile goroutine per template. No-op when
// there are no templates (the all-static-nodes case). Idempotent —
// calling Start a second time without Stop is a no-op.
func (d *Discoverer) Start() {
	d.mu.Lock()
	if d.cancel != nil || len(d.templates) == 0 {
		d.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	templates := append([]DiscoveryTemplate(nil), d.templates...) // snapshot under lock
	d.mu.Unlock()

	for i := range templates {
		t := templates[i]
		interval := t.Template.Discovery.RefreshInterval
		if interval <= 0 {
			interval = 15 * time.Second
		}
		d.wg.Add(1)
		go d.reconcileLoop(ctx, t.Template, t.SeedIPs, interval)
	}
}

// Stop cancels all reconcile goroutines and waits for them to drain.
// Safe to call multiple times and safe under concurrent calls.
func (d *Discoverer) Stop() {
	d.mu.Lock()
	cancel := d.cancel
	d.cancel = nil
	d.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	d.wg.Wait()
}

// reconcileLoop runs one template's reconcile until ctx is canceled.
// current maintains the set of IPs this loop has installed in the
// pools so the next iteration's diff stays cheap (no need to walk
// pool membership to recover state). seedIPs is the boot-time set
// captured by expandDiscoveryNodes; passing it in by value sidesteps
// any "walk a pool to recover state" coupling and works the same
// whether the registered pools are HTTP, gRPC, or any mix.
func (d *Discoverer) reconcileLoop(ctx context.Context, t NodeConfig, seedIPs map[string]struct{}, interval time.Duration) {
	defer d.wg.Done()
	current := make(map[string]struct{}, len(seedIPs))
	for ip := range seedIPs {
		current[ip] = struct{}{}
	}

	// Reconcile once immediately so a slow first refresh doesn't
	// strand the cluster's first pod-IP shift behind the full
	// interval (matters most when interval is large).
	d.reconcileOnce(ctx, t, current)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.reconcileOnce(ctx, t, current)
		}
	}
}

// reconcileOnce resolves the template's host and converges pool
// membership onto the result. Mutates current. The ctx is the
// reconcile loop's lifecycle so a Stop() during a hung DNS lookup
// unblocks immediately instead of waiting for the lookup's own
// timeout to fire.
func (d *Discoverer) reconcileOnce(ctx context.Context, t NodeConfig, current map[string]struct{}) {
	ips, err := d.lookup(ctx, t.Discovery.Host)
	if err != nil {
		if d.log != nil {
			d.log.WithFields(Fields{
				"template": t.Name,
				"host":     t.Discovery.Host,
				"error":    err.Error(),
			}).Warn("discovery lookup failed; keeping previous pool membership")
		}
		d.dash.RecordDiscovery(DiscoveryEvent{
			Template: t.Name,
			Type:     "error",
			Error:    err.Error(),
		})
		return
	}
	sort.Strings(ips)
	desired := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		desired[ip] = struct{}{}
	}
	// Push a tick event with the resolved set so an operator can
	// see "the resolver was healthy at T, returned these IPs". Adds
	// and removes are emitted below as their own events.
	d.dash.RecordDiscovery(DiscoveryEvent{
		Template: t.Name,
		Type:     "tick",
		Resolved: append([]string(nil), ips...),
	})

	d.mu.Lock()
	httpPools := append([]*HttpUpstreamPool(nil), d.httpPools...)
	grpcPool := d.grpcPool
	d.mu.Unlock()

	// Build sorted add/remove lists so log lines are reproducible
	// across reconciles (map iteration is random in Go, which makes
	// post-mortem log diffs noisy without a stable order).
	var toRemove []string
	for ip := range current {
		if _, keep := desired[ip]; !keep {
			toRemove = append(toRemove, ip)
		}
	}
	sort.Strings(toRemove)

	var toAdd []string
	for ip := range desired {
		if _, have := current[ip]; !have {
			toAdd = append(toAdd, ip)
		}
	}
	sort.Strings(toAdd)

	// Removals first so a deleted pod's gauge series is freed
	// before its successor (same IP very unlikely, but no reason
	// to leak the series even briefly).
	for _, ip := range toRemove {
		name := discoveryUpstreamName(t.Name, ip)
		for _, p := range httpPools {
			p.RemoveUpstream(name)
		}
		if grpcPool != nil {
			grpcPool.RemoveUpstream(name)
		}
		delete(current, ip)
		if d.log != nil {
			d.log.WithFields(Fields{
				"template": t.Name,
				"upstream": name,
				"ip":       ip,
			}).Info("discovery removed upstream")
		}
		d.dash.RecordDiscovery(DiscoveryEvent{
			Template: t.Name,
			Type:     "remove",
			Upstream: name,
			IP:       ip,
		})
	}

	for _, ip := range toAdd {
		node := synthesizeDiscoveryNode(t, ip)
		for _, p := range httpPools {
			if err := p.AddUpstream(node); err != nil && d.log != nil {
				d.log.WithFields(Fields{
					"template": t.Name,
					"upstream": node.Name,
					"ip":       ip,
					"pool":     p.name,
					"error":    err.Error(),
				}).Warn("discovery: failed to add http upstream")
			}
		}
		if grpcPool != nil {
			if err := grpcPool.AddUpstream(node); err != nil && d.log != nil {
				d.log.WithFields(Fields{
					"template": t.Name,
					"upstream": node.Name,
					"ip":       ip,
					"error":    err.Error(),
				}).Warn("discovery: failed to add grpc upstream")
			}
		}
		current[ip] = struct{}{}
		if d.log != nil {
			d.log.WithFields(Fields{
				"template": t.Name,
				"upstream": node.Name,
				"ip":       ip,
			}).Info("discovery added upstream")
		}
		d.dash.RecordDiscovery(DiscoveryEvent{
			Template: t.Name,
			Type:     "add",
			Upstream: node.Name,
			IP:       ip,
		})
	}
}
