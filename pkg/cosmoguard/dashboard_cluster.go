package cosmoguard

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/olric-data/olric"
)

// dashboard_cluster.go implements the cluster-aware dashboard surface
// the operator opens when "show me all pods" is the question. The
// model is:
//
//   - Every pod runs a small internal peer-API HTTP listener on
//     cluster.PeerApiPort (defaulting to cluster.BindPort + 1). It
//     mounts the same local /api/v1/<resource> handlers as the public
//     dashboard but skips auth — access is restricted at the network
//     layer to the memberlist member set.
//   - The PUBLIC dashboard adds /api/v1/cluster/<resource> handlers
//     that fan-out concurrently to every peer's internal API, merge
//     the responses with per-resource aggregators, and return one
//     cluster-wide view.
//
// Why fan-out instead of pulling state from the olric DMap directly?
// Two reasons documented in the v4 plan:
//
//   - The DMap snapshot is a 30 s rolling write per pod (hot-path
//     discipline pinned in observability_replication.go); querying it
//     on dashboard polls would render data up to 30 s stale.
//   - Some endpoints (cardinality, metrics) compose state that never
//     enters the DMap at all — they're derived from live process
//     counters at request time. Fan-out is the only path that returns
//     the freshest possible view.
//
// Per-peer failures (timeout, refused connection, non-200) are
// captured as soft errors in the response envelope. The dashboard
// renders the partial result with a marker on the unreachable peer —
// matches the "show what you can see" posture of the rest of the
// dashboard.

// fanoutTimeout is the per-peer GET deadline for cluster fan-outs.
// 2 s is long enough to absorb a brief routing-table reshuffle on a
// freshly-restarted peer, short enough that a wedged pod doesn't
// stall the whole dashboard refresh (which polls on a 5 s cadence
// from the client).
const fanoutTimeout = 2 * time.Second

// fanoutClient is the shared HTTP client every cluster fan-out call
// reuses. The previous shape allocated a fresh &http.Client per
// clusterFanout invocation, so every dashboard refresh paid a TCP
// handshake per (peer, resource) pair — at ~7 cluster endpoints
// polled every 5 s by the front-end, that's ~4 fresh dials/sec per
// peer per browsing pod under steady-state operator use. A shared
// client with a tuned transport keeps idle keep-alive connections
// in a pool so subsequent fan-outs to the same peer reuse the
// already-open TCP socket.
//
// Transport tuning rationale:
//   - MaxIdleConnsPerHost: dashboard polls every panel in parallel
//     on each refresh tick, so each peer can see ~7 simultaneous
//     in-flight requests. 16 leaves headroom for an operator with
//     two browser tabs open + the React-strict double-render dev
//     idiosyncrasy, without bloating the idle pool.
//   - MaxConnsPerHost: hard cap so a single wedged peer can't pin
//     unbounded sockets. 32 is double the per-host idle pool, which
//     covers brief bursts while still backstopping a runaway loop.
//   - IdleConnTimeout: olric peer-API ListenAndServe sets a
//     60 s IdleTimeout on the server side (installPeerAPIServer);
//     45 s on the client side keeps the dance asymmetric so we
//     close before the server does, avoiding the "connection reset
//     by peer" surface area on retries.
//   - DialContext.Timeout: bounded so a peer with a dead listener
//     fails fast rather than waiting for the full per-request
//     fanoutTimeout. 1 s is generous for intra-cluster RTT.
//   - ResponseHeaderTimeout: a peer that accepted the conn but is
//     hung in the handler should be cut loose well before the full
//     fanoutTimeout; the dashboard then surfaces the peer as soft
//     error rather than stalling the whole panel.
//
// Timeout is set on the client itself so every fan-out request
// inherits the 2 s total deadline — preserved verbatim from the
// per-call allocation it replaced.
var fanoutClient = &http.Client{
	Timeout: fanoutTimeout,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   16,
		MaxConnsPerHost:       32,
		IdleConnTimeout:       45 * time.Second,
		TLSHandshakeTimeout:   1 * time.Second,
		ExpectContinueTimeout: 0,
		ResponseHeaderTimeout: fanoutTimeout,
		ForceAttemptHTTP2:     false, // peer API is HTTP/1.1 only; skip the negotiation cost.
	},
}

// peerResponse is one peer's slice of a cluster fan-out. PodID is the
// olric member name (host:port of the RESP socket); Addr is the URL
// the fan-out targeted; Err is non-empty when the peer was
// unreachable. Body holds the raw JSON the peer returned.
type peerResponse struct {
	PodID string
	Addr  string
	Body  stdjson.RawMessage
	Err   string
}

// peerInfo is the JSON shape returned by /api/v1/cluster/peers — one
// row per known cluster member. PartitionsOwned counts the routing
// table entries this member owns as primary; Replicas counts the
// entries it backs as a replica. AgeSeconds derives from olric's
// member birthdate so the dashboard can flag a newly-joined pod
// distinct from a long-running one.
type peerInfo struct {
	PodID            string `json:"pod_id"`
	Addr             string `json:"addr"`
	Coordinator      bool   `json:"coordinator"`
	AgeSeconds       int64  `json:"age_seconds"`
	PartitionsOwned  int    `json:"partitions_owned"`
	PartitionsBacked int    `json:"partitions_backed"`
}

// clusterPeerApiPort returns the port the peer-API listener binds on
// this pod. PeerApiPort=0 (the default) maps to BindPort+1 — the same
// rule documented on ClusterConfig.PeerApiPort. Returns 0 when the
// cluster runtime isn't networked, signalling "no peer API to listen
// on".
func clusterPeerApiPort(cfg *ClusterConfig) int {
	if cfg == nil {
		return 0
	}
	if cfg.PeerApiPort != 0 {
		return cfg.PeerApiPort
	}
	return cfg.BindPort + 1
}

// clusterMembers returns the olric member list for this pod, or nil
// when the cluster runtime is absent. Used by fan-out + the peers
// endpoint. ctx scopes the underlying olric RPC so a cancelled
// request doesn't leave the member lookup running past the caller's
// deadline; an additional 500ms cap is layered on top so the lookup
// fails fast even when ctx itself is unbounded (e.g. peerMembership-
// Gate paths called before the handler installs its own deadline).
func clusterMembers(ctx context.Context, cg *CosmoGuard) []olric.Member {
	if cg == nil || cg.cluster == nil || cg.cluster.Client() == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lookupCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	members, err := cg.cluster.Client().Members(lookupCtx)
	if err != nil {
		return nil
	}
	return members
}

// clusterFanout interrogates every peer's /api/v1/<resource> endpoint
// concurrently. Returns one peerResponse per known member in member
// order. Resource is the trailing path segment ("unmatched",
// "denied", "metrics", …). Always returns a slice the caller can
// iterate without nil-checking, even when fan-out cannot happen
// (single-instance mode).
//
// The peer-API port is a per-pod config knob, but in a homogeneous
// cosmoguard cluster (the only supported deployment shape) every
// peer runs the same image with the same config — so the local
// PeerApiPort is the right port to target on every peer too. Operators
// running heterogeneous clusters are out of scope for v4.
func clusterFanout(ctx context.Context, cg *CosmoGuard, resource string, peerApiPort int) []peerResponse {
	members := clusterMembers(ctx, cg)
	if len(members) == 0 {
		return nil
	}
	out := make([]peerResponse, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(i int, m olric.Member) {
			defer wg.Done()
			host, _, err := net.SplitHostPort(m.Name)
			if err != nil {
				// Member name is the BindAddr:BindPort olric advertises;
				// a parse failure here is a "shouldn't happen". Fall
				// back to using the name verbatim so we at least surface
				// it as a soft error rather than silently dropping the
				// peer.
				host = m.Name
			}
			addr := net.JoinHostPort(host, strconv.Itoa(peerApiPort))
			out[i] = peerResponse{PodID: m.Name, Addr: addr}
			body, err := fanoutGet(ctx, fanoutClient, "http://"+addr+"/api/v1/"+resource)
			if err != nil {
				out[i].Err = err.Error()
				return
			}
			out[i].Body = body
		}(i, m)
	}
	wg.Wait()
	return out
}

// fanoutGet issues a GET with the shared client and returns the
// response body when status is 2xx. Body is bounded at 4 MiB —
// large enough for the biggest legitimate dashboard payload
// (denied ring buffer with full strings) and small enough that a
// runaway peer can't pin gigabytes in the aggregator.
//
// Truncation is reported as an error rather than being returned
// silently: a 4.5 MiB peer body would otherwise return invalid JSON
// to the aggregator, which would drop the peer's data while leaving
// peerResponse.Err empty — the dashboard would then show the peer
// as healthy with mysteriously missing rows. Reading cap+1 bytes
// lets us distinguish "exactly at the cap" (valid) from "over the
// cap" (truncated, surface as soft error).
func fanoutGet(ctx context.Context, client *http.Client, url string) (stdjson.RawMessage, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	const maxBodyBytes = 4 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	if int64(len(body)) > maxBodyBytes {
		return nil, fmt.Errorf("peer body exceeded %d-byte cap", maxBodyBytes)
	}
	return body, nil
}

// installPeerAPIServer builds the internal peer-API HTTP server. nil
// when cluster mode is off — the public dashboard's cluster endpoints
// detect that case and short-circuit to a single-pod (local-only)
// envelope.
//
// The listener is bound to BindAddr (typically the pod IP) on
// PeerApiPort. Auth is by network — every request is checked against
// an IP allowlist built from the current memberlist roster. The
// allowlist refreshes on each request, so a newly-joined peer is
// reachable without a restart, and a peer that's left the cluster
// stops being trusted within one membership tick.
func installPeerAPIServer(cg *CosmoGuard) *http.Server {
	if cg == nil || cg.cluster == nil {
		return nil
	}
	// Single snapshot for both port + bindAddr — a concurrent reload
	// between two snapshotConfig() calls would race the cluster
	// pointer and could yield mismatched port/bind values.
	cfg := cg.snapshotConfig()
	if cfg == nil || cfg.Cache.Cluster == nil {
		return nil
	}
	port := clusterPeerApiPort(cfg.Cache.Cluster)
	if port == 0 {
		return nil
	}
	bindAddr := cfg.Cache.Cluster.BindAddr
	if bindAddr == "" {
		bindAddr = "0.0.0.0"
	}
	mux := http.NewServeMux()
	// Mount only the local /api/v1/<resource> handlers — no static
	// UI, no /api/v1/cluster/* routes (which would let a peer
	// fan-out call recursively into another peer's fan-out). The
	// outer membership gate handles auth; the inner gate passed to
	// installLocalAPIRoutes is a passthrough.
	installLocalAPIRoutes(mux, cg, basicAuthGate("", ""), "/api/v1")
	return &http.Server{
		Addr:              net.JoinHostPort(bindAddr, strconv.Itoa(port)),
		Handler:           peerMembershipGate(cg)(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// peerMembershipGate restricts the peer-API to the IPs in the current
// memberlist roster + loopback (so health probes and tests can hit
// the listener without joining the cluster). Refreshes the allowlist
// on every request so churn is picked up within one membership tick.
//
// Returning a closure lets installPeerAPIServer compose the gate with
// the same mux installDashboardRoutes builds.
func peerMembershipGate(cg *CosmoGuard) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			ip := net.ParseIP(host)
			if ip == nil || (!ip.IsLoopback() && !peerIPAllowed(r.Context(), cg, ip)) {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
}

// peerIPAllowed checks ip against the current memberlist roster. A
// member name is "host:port" where host can be an IP literal or a
// hostname (the K8s headless-service case). Hostnames are resolved
// best-effort with a per-lookup 250 ms cap so a degraded DNS never
// wedges the gate. Loopback is allowed unconditionally by the caller.
//
// Each LookupIP gets its own deadline rather than sharing one across
// the loop. The shared-budget shape made acceptance asymmetric across
// members: member 1 had the full 250 ms, member N could be left with
// near-zero remaining and would be denied solely because olric's
// member ordering happened to put it late in the roster. Per-lookup
// budgets give every member the same fair chance and bound worst-case
// total work to 250ms × N, which is still safely below any caller's
// timeout (peerMembershipGate is hit from fan-out paths with at least
// a 2 s budget).
func peerIPAllowed(ctx context.Context, cg *CosmoGuard, ip net.IP) bool {
	members := clusterMembers(ctx, cg)
	if ctx == nil {
		ctx = context.Background()
	}
	var resolver net.Resolver
	for _, m := range members {
		host, _, err := net.SplitHostPort(m.Name)
		if err != nil {
			host = m.Name
		}
		if memberIP := net.ParseIP(host); memberIP != nil {
			if memberIP.Equal(ip) {
				return true
			}
			continue
		}
		// Hostname: resolve and compare each address. Per-lookup
		// deadline — see function-level comment for the rationale.
		lookupCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		ips, err := resolver.LookupIP(lookupCtx, "ip", host)
		cancel()
		if err != nil {
			continue
		}
		for _, candidate := range ips {
			if candidate.Equal(ip) {
				return true
			}
		}
	}
	return false
}

// ---------------------------------------------------------------------
// Aggregators
// ---------------------------------------------------------------------

// clusterEnvelope wraps a cluster fan-out response: Data is the
// merged payload (shape depends on the resource); Peers carries one
// row per cluster member with the per-peer status so the UI can flag
// unreachable pods even when the aggregate is otherwise valid.
type clusterEnvelope struct {
	Data  any            `json:"data"`
	Peers []peerEnvelope `json:"peers"`
}

type peerEnvelope struct {
	PodID string `json:"pod_id"`
	Addr  string `json:"addr"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func toPeerEnvelopes(responses []peerResponse) []peerEnvelope {
	out := make([]peerEnvelope, 0, len(responses))
	for _, r := range responses {
		out = append(out, peerEnvelope{
			PodID: r.PodID,
			Addr:  r.Addr,
			OK:    r.Err == "",
			Error: r.Err,
		})
	}
	return out
}

// listClusterPeers returns one row per cluster member with partition
// counts pulled from the live routing table. The routing-table call
// is best-effort: a transient olric error degrades to zero-valued
// counts rather than failing the whole endpoint, because the panel's
// primary value is "is this peer reachable" not "exact partition
// distribution".
func listClusterPeers(cg *CosmoGuard) map[string]any {
	out := []peerInfo{}
	if cg == nil || cg.cluster == nil || cg.cluster.Client() == nil {
		return map[string]any{"peers": out}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	members, err := cg.cluster.Client().Members(ctx)
	if err != nil {
		return map[string]any{"peers": out}
	}
	rt, _ := cg.cluster.Client().RoutingTable(ctx)
	owned := map[string]int{}
	backed := map[string]int{}
	for _, route := range rt {
		for _, o := range route.PrimaryOwners {
			owned[o]++
		}
		for _, r := range route.ReplicaOwners {
			backed[r]++
		}
	}
	// Snapshot config once for the whole loop — a concurrent reload
	// during peer iteration could otherwise yield rows that mix peer
	// API ports from before and after the reload.
	cfgSnap := cg.snapshotConfig()
	var clusterCfg *ClusterConfig
	if cfgSnap != nil {
		clusterCfg = cfgSnap.Cache.Cluster
	}
	peerPort := clusterPeerApiPort(clusterCfg)
	now := time.Now()
	for _, m := range members {
		host, _, err := net.SplitHostPort(m.Name)
		if err != nil {
			host = m.Name
		}
		addr := net.JoinHostPort(host, strconv.Itoa(peerPort))
		age := int64(now.Sub(time.Unix(0, m.Birthdate)).Seconds())
		if age < 0 {
			age = 0
		}
		out = append(out, peerInfo{
			PodID:            m.Name,
			Addr:             addr,
			Coordinator:      m.Coordinator,
			AgeSeconds:       age,
			PartitionsOwned:  owned[m.Name],
			PartitionsBacked: backed[m.Name],
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PodID < out[j].PodID })
	return map[string]any{"peers": out}
}

// aggregateUnmatched merges per-peer /api/v1/unmatched payloads by
// (section, method, path), summing counts and taking min(FirstSeen) /
// max(LastSeen) — the union semantics an operator wants when asking
// "across the cluster, which endpoints have fallen through to the
// default action and how often."
func aggregateUnmatched(responses []peerResponse) map[string][]UnmatchedEntry {
	type key struct{ section, method, path string }
	agg := map[key]*UnmatchedEntry{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Sections map[string][]UnmatchedEntry `json:"sections"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		for section, entries := range p.Sections {
			for _, e := range entries {
				k := key{section, e.Method, e.Path}
				existing, ok := agg[k]
				if !ok {
					copyE := e
					copyE.Section = section
					agg[k] = &copyE
					continue
				}
				existing.Count += e.Count
				if e.FirstSeen > 0 && (existing.FirstSeen == 0 || e.FirstSeen < existing.FirstSeen) {
					existing.FirstSeen = e.FirstSeen
				}
				if e.LastSeen > existing.LastSeen {
					existing.LastSeen = e.LastSeen
				}
			}
		}
	}
	out := map[string][]UnmatchedEntry{}
	for k, v := range agg {
		out[k.section] = append(out[k.section], *v)
	}
	for s, list := range out {
		sort.Slice(list, func(i, j int) bool { return list[i].Count > list[j].Count })
		out[s] = list
	}
	return out
}

// aggregateDenied merges per-peer denied ring buffers and returns the
// top maxDeniedAcrossCluster entries sorted by descending timestamp.
// The bound prevents an N-pod cluster from returning N× the local
// ring-buffer size in a single response. Always returns a non-nil
// slice so the JSON shape is "denied": [] when no peer responded,
// matching the single-pod endpoint contract — clients iterating the
// payload would otherwise have to special-case `null`.
const maxDeniedAcrossCluster = 200

func aggregateDenied(responses []peerResponse) []DenyRecord {
	all := []DenyRecord{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Denied []DenyRecord `json:"denied"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		all = append(all, p.Denied...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].TimestampMs > all[j].TimestampMs })
	if len(all) > maxDeniedAcrossCluster {
		all = all[:maxDeniedAcrossCluster]
	}
	return all
}

// aggregateRequests merges per-peer time-windowed request slices by
// descending timestamp. enabled is true iff at least one peer
// reported the log as enabled. The cap sits well above the per-pod
// hard cap × replica count so a fully-windowed three-pod deployment
// isn't silently truncated.
const maxRequestsAcrossCluster = 5000

func aggregateRequests(responses []peerResponse) (entries []RequestLogEntry, enabled bool) {
	entries = []RequestLogEntry{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Requests []RequestLogEntry `json:"requests"`
			Enabled  bool              `json:"enabled"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		if p.Enabled {
			enabled = true
		}
		entries = append(entries, p.Requests...)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TimestampMs > entries[j].TimestampMs })
	if len(entries) > maxRequestsAcrossCluster {
		entries = entries[:maxRequestsAcrossCluster]
	}
	return entries, enabled
}

// aggregateDiscoveryLog merges per-peer DNS discovery refresh logs by
// descending timestamp. Same bound as aggregateDenied for the same
// reason — operators care about the recent past, not every event from
// every pod since boot. Always returns a non-nil slice (see
// aggregateDenied for the rationale).
const maxDiscoveryAcrossCluster = 200

func aggregateDiscoveryLog(responses []peerResponse) []DiscoveryEvent {
	all := []DiscoveryEvent{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Events []DiscoveryEvent `json:"events"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		all = append(all, p.Events...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].TimestampMs > all[j].TimestampMs })
	if len(all) > maxDiscoveryAcrossCluster {
		all = all[:maxDiscoveryAcrossCluster]
	}
	return all
}

// aggregateCardinality sums distinct-key counts per (section,
// rule_tag) across peers. Documented as an UPPER BOUND because two
// pods may have observed overlapping keys for the same rule — without
// a cross-pod set-merge of the underlying TopNCounter contents (which
// we deliberately don't ship to keep the snapshot blob small), an
// exact union would require shipping the full key set on every peer.
// Hot samples are unioned (keeping the first maxCardinalityHotSamples
// per rule) so the operator still sees concrete example keys.
func aggregateCardinality(responses []peerResponse) map[string][]CardinalityRule {
	type key struct{ section, tag string }
	agg := map[key]*CardinalityRule{}
	// sampleSeen dedupes HotSamples per (section, rule_tag) across the
	// fan-out. Two peers both observing the same hot key — extremely
	// common for sticky cache hotspots — would otherwise surface that
	// key twice in the operator UI's hot-sample chips, burning a chip
	// slot for no information gain and obscuring the next-hottest key.
	// Per-rule cap of maxCardinalityHotSamples still applies after dedup.
	sampleSeen := map[key]map[string]struct{}{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Sections map[string][]CardinalityRule `json:"sections"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		for section, rules := range p.Sections {
			for _, rule := range rules {
				k := key{section, rule.RuleTag}
				existing, ok := agg[k]
				if !ok {
					// First time we see this rule: copy the row without
					// HotSamples — the dedup loop below populates them
					// through the seen-set so the same code path handles
					// the first-peer and subsequent-peer cases. Avoids
					// the defensive-slice-copy dance and removes the
					// chance of a duplicate slipping in via a peer
					// reporting the same key twice in its own list.
					copyR := rule
					copyR.HotSamples = nil
					agg[k] = &copyR
					existing = agg[k]
					sampleSeen[k] = map[string]struct{}{}
				} else {
					existing.DistinctKeys += rule.DistinctKeys
				}
				for _, s := range rule.HotSamples {
					if len(existing.HotSamples) >= maxCardinalityHotSamples {
						break
					}
					if _, dup := sampleSeen[k][s]; dup {
						continue
					}
					sampleSeen[k][s] = struct{}{}
					existing.HotSamples = append(existing.HotSamples, s)
				}
			}
		}
	}
	out := map[string][]CardinalityRule{}
	for k, v := range agg {
		out[k.section] = append(out[k.section], *v)
	}
	for s, list := range out {
		sort.Slice(list, func(i, j int) bool { return list[i].DistinctKeys > list[j].DistinctKeys })
		out[s] = list
	}
	return out
}

// aggregateWebSocket merges per-peer /api/v1/websocket payloads by
// section. Connection counts, client subscriptions, and upstream pool
// sizes sum across pods (each pod owns its own client connections and
// upstream pool). Subscription params are merged by (section, param)
// with subscriber counts summed — the same logical subscription is
// served independently on every pod, so the cluster-wide subscriber
// count is the sum. Per-connection and per-upstream detail rows are
// concatenated across pods so the operator sees every client and every
// backend connection in the cluster.
func aggregateWebSocket(responses []peerResponse) []WSSectionStats {
	type subKey struct{ section, param string }
	order := []string{}
	bySection := map[string]*WSSectionStats{}
	subAgg := map[subKey]*WSSubInfo{}
	subOrder := map[string][]subKey{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			Sections []WSSectionStats `json:"sections"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		for _, s := range p.Sections {
			agg, ok := bySection[s.Section]
			if !ok {
				agg = &WSSectionStats{
					Section:   s.Section,
					Path:      s.Path,
					Conns:     []WSConnInfo{},
					Subs:      []WSSubInfo{},
					Upstreams: []ConnStat{},
				}
				bySection[s.Section] = agg
				order = append(order, s.Section)
			}
			agg.Connections += s.Connections
			agg.ClientSubscriptions += s.ClientSubscriptions
			agg.UpstreamSubscriptions += s.UpstreamSubscriptions
			agg.UpstreamConnsHealthy += s.UpstreamConnsHealthy
			agg.UpstreamConnsTotal += s.UpstreamConnsTotal
			agg.Conns = append(agg.Conns, s.Conns...)
			agg.Upstreams = append(agg.Upstreams, s.Upstreams...)
			for _, sub := range s.Subs {
				k := subKey{s.Section, sub.Param}
				existing, ok := subAgg[k]
				if !ok {
					copySub := sub
					subAgg[k] = &copySub
					subOrder[s.Section] = append(subOrder[s.Section], k)
					continue
				}
				existing.Subscribers += sub.Subscribers
			}
		}
	}
	out := make([]WSSectionStats, 0, len(order))
	for _, section := range order {
		agg := bySection[section]
		for _, k := range subOrder[section] {
			agg.Subs = append(agg.Subs, *subAgg[k])
		}
		out = append(out, *agg)
	}
	return out
}

// perPeerReload is the JSON row returned by /api/v1/cluster/reload-
// status — unlike the other aggregators we DON'T merge across peers,
// because the operator's question for reload is "did every pod pick
// up the new config" which requires seeing each pod's status
// separately.
type perPeerReload struct {
	PodID  string       `json:"pod_id"`
	Reload ReloadStatus `json:"reload"`
}

func collectReloadStatus(responses []peerResponse) []perPeerReload {
	out := make([]perPeerReload, 0, len(responses))
	for _, r := range responses {
		row := perPeerReload{PodID: r.PodID, Reload: ReloadStatus{Sections: map[string]ReloadSection{}}}
		if r.Err != "" || len(r.Body) == 0 {
			out = append(out, row)
			continue
		}
		var p struct {
			Reload ReloadStatus `json:"reload"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			out = append(out, row)
			continue
		}
		row.Reload = p.Reload
		if row.Reload.Sections == nil {
			row.Reload.Sections = map[string]ReloadSection{}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PodID < out[j].PodID })
	return out
}

// aggregateMetrics sums histograms + per-label counts across every
// peer's latest MetricsSnapshot, producing one cluster-wide snapshot
// the client can chart with the same rate-from-deltas math it uses
// for single-pod. TimestampMs comes from this aggregator (not from
// the peers) so successive cluster-poll snapshots have a coherent dt.
func aggregateMetrics(responses []peerResponse) *MetricsSnapshot {
	out := &MetricsSnapshot{
		TimestampMs: time.Now().UnixMilli(),
		Protocols:   map[string]ProtocolMetrics{},
		Batches:     map[string]BatchMetrics{},
		Upstreams:   []UpstreamHealth{},
	}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var snap MetricsSnapshot
		if err := stdjson.Unmarshal(r.Body, &snap); err != nil {
			continue
		}
		for slug, pm := range snap.Protocols {
			existing, ok := out.Protocols[slug]
			if !ok {
				existing = ProtocolMetrics{
					ByCache:             map[string]uint64{},
					ByAction:            map[string]uint64{},
					ByStatus:            map[string]uint64{},
					ByUpstream:          map[string]uint64{},
					ByRule:              map[string]uint64{},
					ByMethod:            map[string]uint64{},
					ByUpstreamHistogram: map[string]HistogramView{},
				}
			}
			existing.Histogram = mergeHistogram(existing.Histogram, pm.Histogram)
			existing.ByCache = mergeUint64Map(existing.ByCache, pm.ByCache)
			existing.ByAction = mergeUint64Map(existing.ByAction, pm.ByAction)
			existing.ByStatus = mergeUint64Map(existing.ByStatus, pm.ByStatus)
			existing.ByUpstream = mergeUint64Map(existing.ByUpstream, pm.ByUpstream)
			existing.ByRule = mergeUint64Map(existing.ByRule, pm.ByRule)
			existing.ByMethod = mergeUint64Map(existing.ByMethod, pm.ByMethod)
			for u, h := range pm.ByUpstreamHistogram {
				existing.ByUpstreamHistogram[u] = mergeHistogram(existing.ByUpstreamHistogram[u], h)
			}
			out.Protocols[slug] = existing
		}
		for slug, bm := range snap.Batches {
			existing, ok := out.Batches[slug]
			if !ok {
				existing = BatchMetrics{BySizeClass: map[string]uint64{}}
			}
			existing.Histogram = mergeHistogram(existing.Histogram, bm.Histogram)
			existing.BySizeClass = mergeUint64Map(existing.BySizeClass, bm.BySizeClass)
			out.Batches[slug] = existing
		}
		// Upstream health is per-(pool, upstream) and identical across
		// peers when the health checker is co-located; just take the
		// max so a single healthy pod reports the upstream as healthy.
		for _, uh := range snap.Upstreams {
			merged := false
			for i, existing := range out.Upstreams {
				if existing.Pool == uh.Pool && existing.Upstream == uh.Upstream {
					if uh.Healthy > existing.Healthy {
						out.Upstreams[i].Healthy = uh.Healthy
					}
					merged = true
					break
				}
			}
			if !merged {
				out.Upstreams = append(out.Upstreams, uh)
			}
		}
	}
	return out
}

func mergeHistogram(a, b HistogramView) HistogramView {
	bucketAgg := map[float64]uint64{}
	for _, bk := range a.Buckets {
		bucketAgg[bk.Le] += bk.Count
	}
	for _, bk := range b.Buckets {
		bucketAgg[bk.Le] += bk.Count
	}
	bounds := make([]float64, 0, len(bucketAgg))
	for le := range bucketAgg {
		bounds = append(bounds, le)
	}
	sort.Float64s(bounds)
	out := HistogramView{
		Sum:     a.Sum + b.Sum,
		Count:   a.Count + b.Count,
		Buckets: make([]HistBucketV, 0, len(bounds)),
	}
	for _, le := range bounds {
		out.Buckets = append(out.Buckets, HistBucketV{Le: le, Count: bucketAgg[le]})
	}
	return out
}

func mergeUint64Map(a, b map[string]uint64) map[string]uint64 {
	if a == nil {
		a = map[string]uint64{}
	}
	for k, v := range b {
		a[k] += v
	}
	return a
}

// aggregateMetricsHistory merges per-peer ring buffers by 5 s bucket.
// Same shape as a single-pod /metrics/history response: ordered
// oldest-first so the client can append live polls onto the tail.
// Buckets where no peer reported are dropped — we don't synthesise
// zero-valued snapshots, because the client interprets the gap as
// "no data" which is the truth.
const metricsHistoryBucketMs = 5 * 1000

func aggregateMetricsHistory(responses []peerResponse) []MetricsSnapshot {
	buckets := map[int64][]MetricsSnapshot{}
	for _, r := range responses {
		if r.Err != "" || len(r.Body) == 0 {
			continue
		}
		var p struct {
			History []MetricsSnapshot `json:"history"`
		}
		if err := stdjson.Unmarshal(r.Body, &p); err != nil {
			continue
		}
		for _, s := range p.History {
			bucket := (s.TimestampMs / metricsHistoryBucketMs) * metricsHistoryBucketMs
			buckets[bucket] = append(buckets[bucket], s)
		}
	}
	keys := make([]int64, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]MetricsSnapshot, 0, len(keys))
	for _, k := range keys {
		merged := aggregateMetrics(responsesFromSnaps(buckets[k]))
		merged.TimestampMs = k
		out = append(out, *merged)
	}
	return out
}

// responsesFromSnaps wraps a slice of MetricsSnapshot into the
// peerResponse shape aggregateMetrics expects. Used by the metrics-
// history bucket-merge so it can reuse the per-snapshot summation
// logic instead of duplicating it.
func responsesFromSnaps(snaps []MetricsSnapshot) []peerResponse {
	out := make([]peerResponse, 0, len(snaps))
	for _, s := range snaps {
		body, err := stdjson.Marshal(s)
		if err != nil {
			continue
		}
		out = append(out, peerResponse{Body: body})
	}
	return out
}

// ---------------------------------------------------------------------
// HTTP handlers — wired into the public dashboard mux.
// ---------------------------------------------------------------------

// installClusterRoutes mounts the /api/v1/cluster/<resource> endpoints
// on mux. apiBase is the same base path installDashboardRoutes uses
// ("/api/v1" for the standalone listener, "/admin/api/v1" for the
// legacy co-mount). gate is the auth middleware already applied to
// the public-facing routes — the cluster fan-out endpoints reuse it.
//
// The peer-API port is resolved per request from the live config
// snapshot so a hot-reload that bumps cluster.peerApiPort takes effect
// on the next fan-out instead of being pinned to whatever value was
// captured at registration. A resolved port of 0 (cluster off or
// embedded-only) short-circuits the fan-out to a "no cluster" envelope
// so the embedded-only / single-instance deployment doesn't 5xx the
// new routes.
func installClusterRoutes(mux *http.ServeMux, cg *CosmoGuard, gate func(http.Handler) http.Handler, apiBase string) {
	base := apiBase + "/cluster"

	mux.Handle(base+"/peers", gate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, listClusterPeers(cg))
	})))

	resolvePeerPort := func() int {
		cfg := cg.snapshotConfig()
		if cfg == nil {
			return 0
		}
		return clusterPeerApiPort(cfg.Cache.Cluster)
	}

	fanoutHandler := func(resource string, build func([]peerResponse) any) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), fanoutTimeout+1*time.Second)
			defer cancel()
			responses := clusterFanout(ctx, cg, resource, resolvePeerPort())
			env := clusterEnvelope{Data: build(responses), Peers: toPeerEnvelopes(responses)}
			writeJSON(w, env)
		})
	}

	mux.Handle(base+"/unmatched", gate(fanoutHandler("unmatched", func(rs []peerResponse) any {
		return map[string]any{"sections": aggregateUnmatched(rs)}
	})))
	mux.Handle(base+"/denied", gate(fanoutHandler("denied", func(rs []peerResponse) any {
		return map[string]any{"denied": aggregateDenied(rs)}
	})))
	mux.Handle(base+"/discovery-log", gate(fanoutHandler("discovery-log", func(rs []peerResponse) any {
		return map[string]any{"events": aggregateDiscoveryLog(rs)}
	})))
	mux.Handle(base+"/cache-cardinality", gate(fanoutHandler("cache-cardinality", func(rs []peerResponse) any {
		return map[string]any{"sections": aggregateCardinality(rs)}
	})))
	mux.Handle(base+"/reload-status", gate(fanoutHandler("reload-status", func(rs []peerResponse) any {
		return map[string]any{"reload": collectReloadStatus(rs)}
	})))
	mux.Handle(base+"/metrics", gate(fanoutHandler("metrics", func(rs []peerResponse) any {
		return aggregateMetrics(rs)
	})))
	mux.Handle(base+"/metrics/history", gate(fanoutHandler("metrics/history", func(rs []peerResponse) any {
		return map[string]any{"history": aggregateMetricsHistory(rs)}
	})))
	mux.Handle(base+"/requests/recent", gate(fanoutHandler("requests/recent", func(rs []peerResponse) any {
		entries, enabled := aggregateRequests(rs)
		return map[string]any{"requests": entries, "enabled": enabled}
	})))
	mux.Handle(base+"/websocket", gate(fanoutHandler("websocket", func(rs []peerResponse) any {
		return map[string]any{"sections": aggregateWebSocket(rs), "enabled": true}
	})))
}
