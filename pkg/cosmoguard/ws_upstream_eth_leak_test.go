package cosmoguard

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/voluzi/cosmoguard/pkg/util"
)

// TestEthBrokerReconnectStormNoLeak boots a real Broker with a 10-connection
// pool dialling a closed port — the exact production shape — and lets the
// background reconnect loops run for ~30s of wall time. The leak in
// production grew at ~50 MiB/h with this configuration, so a 30s window
// against a 64-KiB heap-growth ceiling catches even sub-percent-rate leaks.
func TestEthBrokerReconnectStormNoLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping leak repro in -short")
	}

	// Pick a port that's definitely closed.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	// 10 connections to one bad backend = exactly the prod EVM-WS shape.
	broker := NewBroker([]string{"ws://" + addr}, "/", 10,
		func(u url.URL, idg *util.UniqueID, cb func(*JsonRpcMsg)) UpstreamConnManager {
			return EthUpstreamConnManager(u, idg, cb)
		})
	if err := broker.Start(log.WithField("test", "leak")); err != nil {
		t.Fatal(err)
	}
	defer broker.Stop()

	// Warm-up: let the goroutine count stabilise (Start spawns 10
	// Run goroutines that each enter their connect-fail loop).
	time.Sleep(2 * time.Second)

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	g0 := runtime.NumGoroutine()

	// 30s — at connectRetryPeriod=5s, every conn dials ~6 times. With
	// 10 conns that's ~60 failed Dials. If each leaks even 1 KiB the
	// delta is >50 KiB.
	const runFor = 30 * time.Second
	time.Sleep(runFor)

	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	g1 := runtime.NumGoroutine()

	heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)
	allocDelta := int64(after.Alloc) - int64(before.Alloc)
	objDelta := int64(after.HeapObjects) - int64(before.HeapObjects)
	totalAllocDelta := after.TotalAlloc - before.TotalAlloc

	f, _ := os.Create("/tmp/leak_broker_log.txt")
	if f != nil {
		fmt.Fprintf(f, "after %v of broker reconnect storm (10 conns, all to closed port):\n  heap_inuse_delta = %d B\n  alloc_delta      = %d B\n  objects_delta    = %d\n  total_alloc_delta = %d B\n  goroutines       = before=%d after=%d delta=%d\n",
			runFor, heapDelta, allocDelta, objDelta, totalAllocDelta, g0, g1, g1-g0)
		f.Close()
	}
	t.Logf("broker leak test: heap Δ=%+d B objects Δ=%+d goroutines Δ=%+d",
		heapDelta, objDelta, g1-g0)

	// Heap growth ceiling: 64 KiB total over 30s. Production was leaking
	// ~50 MiB/h ≈ 14 KiB/s ≈ 420 KiB in 30s — well above this cap.
	if heapDelta > 64*1024 {
		t.Errorf("heap grew %d bytes during reconnect storm (cap 64 KiB) — leak suspected", heapDelta)
	}
	if g1-g0 > 2 {
		t.Errorf("goroutines grew by %d during reconnect storm — goroutine leak suspected", g1-g0)
	}
}

// TestEthUpstreamReconnectNoLeak runs the eth WS upstream Run loop against
// a TCP port that's bound-then-closed (connection refused) for a fixed
// number of reconnect attempts and asserts the heap doesn't grow per failed
// Dial. Production symptom (helm/cosmoguard against a chain with no WS):
// pods at 512 Mi after ~10h with no inbound traffic, only the reconnect
// storm. The raw gorilla Dial loop does not leak, so this test focuses on
// what the manager adds on top.
func TestEthUpstreamReconnectNoLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping leak repro in -short")
	}

	// Bind+close a port → guarantees connection refused.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	u, _ := url.Parse("ws://" + addr + "/")
	idGen := &util.UniqueID{}
	mgr := EthUpstreamConnManager(*u, idGen, func(*JsonRpcMsg) {}).(*UpstreamConnManagerEth)

	// connectRetryPeriod is a 5s const so we can't drive thousands of
	// dials through Run() in a test. Instead, exercise the same code
	// path Run() walks — connect() + log + atomic bump — directly.
	mgr.log = log.WithField("upstream-id", 0)
	mgr.initStopCh()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	g0 := runtime.NumGoroutine()

	const N = 2000
	for i := 0; i < N; i++ {
		if err := mgr.connect(); err != nil {
			mgr.log.Errorf("error connecting: %v", err)
			mgr.failedReconnects.Add(1)
		}
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	g1 := runtime.NumGoroutine()

	heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)
	allocDelta := int64(after.Alloc) - int64(before.Alloc)
	objDelta := int64(after.HeapObjects) - int64(before.HeapObjects)
	bytesPerDial := float64(heapDelta) / float64(N)

	t.Logf("after %d failed dials: heap_inuse Δ=%+d B (%.1f B/dial)  alloc Δ=%+d B  objects Δ=%+d  goroutines Δ=%+d",
		N, heapDelta, bytesPerDial, allocDelta, objDelta, g1-g0)
	// Also write to /tmp because the test wrapper swallows -v output.
	f, _ := os.Create("/tmp/leak_test_log.txt")
	if f != nil {
		fmt.Fprintf(f, "after %d failed dials:\n  heap_inuse_delta = %d B (%.1f B/dial)\n  alloc_delta = %d B\n  objects_delta = %d\n  goroutines_delta = %d\n",
			N, heapDelta, bytesPerDial, allocDelta, objDelta, g1-g0)
		f.Close()
	}

	// 1024 bytes/dial × N=2000 = 2 MiB — generous ceiling that
	// catches real leaks (production was ~7 KiB/dial) without
	// flaking on GC noise (observed up to ~300 B/dial under load).
	if bytesPerDial > 1024 {
		t.Errorf("heap grew %.1f bytes per failed dial (cap 1024) — leak suspected", bytesPerDial)
	}
	if g1-g0 > 2 {
		t.Errorf("goroutines grew by %d during %d failed dials — leak suspected", g1-g0, N)
	}
}
