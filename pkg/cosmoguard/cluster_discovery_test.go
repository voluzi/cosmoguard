package cosmoguard

import (
	"context"
	"errors"
	"net"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestClusterServiceDiscovery_StaticFilterSelf_IPv6ShortHand guards the
// filterSelf colon-vs-SplitHostPort decision. The bug it catches: a
// previous implementation used strings.Contains(p, ":") to decide
// whether a static-peer entry already carried a port. For an IPv6
// literal like "::1" or "[::1]" that test is always true, so the entry
// was left un-normalised, never matched the local node's normalised
// "[::1]:<gossipPort>" entry in selfAddrs, and the pod self-joined.
//
// Each table row exercises one short-hand input shape; the expectation
// is that filterSelf normalises it to the bracketed/ported form, sees
// it matches selfAddrs, and drops it from the peer list.
func TestClusterServiceDiscovery_StaticFilterSelf_IPv6ShortHand(t *testing.T) {
	const gossipPort = 3322
	self := net.JoinHostPort("::1", "3322") // "[::1]:3322"

	cases := []struct {
		name string
		peer string
	}{
		{"bare ipv6 literal", "::1"},
		{"bracketed ipv6 literal without port", "[::1]"},
		{"already-formed bracketed host:port", "[::1]:3322"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ClusterConfig{
				GossipPort: gossipPort,
				Discovery: &ClusterDiscoveryConfig{
					Mode: "static",
					Static: &StaticDiscoveryConfig{
						Peers: []string{tc.peer},
					},
				},
			}
			d, err := newClusterServiceDiscovery(cfg, []string{self}, nil)
			require.NoError(t, err)
			peers, err := d.DiscoverPeers()
			require.NoError(t, err)
			require.Empty(t, peers,
				"self-reference %q in short-hand form was not filtered — pod would self-join", tc.peer)
		})
	}
}

// TestClusterServiceDiscovery_StaticFilterSelf_MixedPeersKeptOnce
// asserts the IPv6 fix didn't regress the ipv4 / mixed-shorthand path:
// non-self entries pass through, and equivalent-form duplicates of self
// collapse to a single filter hit so the resulting peer list contains
// exactly the legitimate non-self peers, in input order.
func TestClusterServiceDiscovery_StaticFilterSelf_MixedPeersKeptOnce(t *testing.T) {
	const gossipPort = 3322
	selfV6 := net.JoinHostPort("::1", "3322")
	selfV4 := net.JoinHostPort("10.0.0.1", "3322")

	cfg := &ClusterConfig{
		GossipPort: gossipPort,
		Discovery: &ClusterDiscoveryConfig{
			Mode: "static",
			Static: &StaticDiscoveryConfig{
				Peers: []string{
					"::1",            // self (bare)
					"[::1]",          // self (bracketed, portless)
					"10.0.0.1",       // self (bare ipv4)
					"10.0.0.2",       // peer (bare ipv4 → expanded)
					"10.0.0.3:3322",  // peer (already ported)
					"[fe80::2]:3322", // peer (already ported ipv6)
				},
			},
		},
	}
	d, err := newClusterServiceDiscovery(cfg, []string{selfV6, selfV4}, nil)
	require.NoError(t, err)
	peers, err := d.DiscoverPeers()
	require.NoError(t, err)

	got := append([]string(nil), peers...)
	sort.Strings(got)
	want := []string{
		"10.0.0.2:3322",
		"10.0.0.3:3322",
		"[fe80::2]:3322",
	}
	sort.Strings(want)
	require.Equal(t, want, got)
}

// TestClusterServiceDiscovery_DNSFilterSelf checks the DNS path produces
// host:port pairs from the resolver output, sorts them deterministically,
// and removes the local advertise address from the result. Uses a fake
// LookupFunc to keep the test deterministic.
func TestClusterServiceDiscovery_DNSFilterSelf(t *testing.T) {
	const gossipPort = 3322
	self := net.JoinHostPort("10.0.0.1", "3322")

	lookup := func(_ context.Context, _ string) ([]string, error) {
		return []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, nil
	}
	cfg := &ClusterConfig{
		GossipPort: gossipPort,
		Discovery: &ClusterDiscoveryConfig{
			Mode: "dns",
			DNS: &DNSDiscoveryConfig{
				Host: "cosmoguard-peers.default.svc.cluster.local",
			},
		},
	}
	d, err := newClusterServiceDiscovery(cfg, []string{self}, lookup)
	require.NoError(t, err)
	require.NoError(t, d.Initialize())
	defer d.Close()

	peers, err := d.DiscoverPeers()
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.2:3322", "10.0.0.3:3322"}, peers,
		"self entry must be filtered and remaining peers returned sorted")
}

// TestClusterServiceDiscovery_DNSLookupErrorPropagates makes sure a
// resolver failure surfaces to the olric caller (which retries on its
// own ticker) rather than being silently swallowed into an empty peer
// list — an empty list with a swallowed error would let a pod boot into
// a "membership of one" state on a transient DNS hiccup.
func TestClusterServiceDiscovery_DNSLookupErrorPropagates(t *testing.T) {
	wantErr := errors.New("simulated dns failure")
	lookup := func(_ context.Context, _ string) ([]string, error) {
		return nil, wantErr
	}
	cfg := &ClusterConfig{
		GossipPort: 3322,
		Discovery: &ClusterDiscoveryConfig{
			Mode: "dns",
			DNS:  &DNSDiscoveryConfig{Host: "missing.example"},
		},
	}
	d, err := newClusterServiceDiscovery(cfg, nil, lookup)
	require.NoError(t, err)
	require.NoError(t, d.Initialize())
	defer d.Close()

	_, err = d.DiscoverPeers()
	require.ErrorIs(t, err, wantErr)
}

// TestClusterServiceDiscovery_ExperimentalModesRejected pins the
// "config accepts kubernetes/mdns but runtime refuses to build them"
// contract documented in cluster_discovery.go — a v4 reviewer should
// see a clean error message rather than a silent fallthrough or a
// half-built plugin.
func TestClusterServiceDiscovery_ExperimentalModesRejected(t *testing.T) {
	for _, mode := range []string{"kubernetes", "mdns"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &ClusterConfig{
				GossipPort: 3322,
				Discovery:  &ClusterDiscoveryConfig{Mode: mode},
			}
			_, err := newClusterServiceDiscovery(cfg, nil, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "experimental")
		})
	}
}
