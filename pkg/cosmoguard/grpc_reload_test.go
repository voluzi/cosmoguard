package cosmoguard

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func writeReloadProtoset(t *testing.T, syntax string) string {
	t.Helper()
	file := &descriptorpb.FileDescriptorProto{
		Name: proto.String("reload.proto"), Package: proto.String("cosmoguard.test"), Syntax: proto.String(syntax),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Sample"), Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("a"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: proto.String("b"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		}}},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Cache"), Method: []*descriptorpb.MethodDescriptorProto{{
			Name: proto.String("Query"), InputType: proto.String(".cosmoguard.test.Sample"), OutputType: proto.String(".cosmoguard.test.Sample"),
		}}}},
	}
	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{file}})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), syntax+".protoset")
	require.NoError(t, os.WriteFile(path, raw, 0600))
	return path
}

func grpcReloadConfig(path, action string) string {
	paths := "[]"
	if path != "" {
		paths = fmt.Sprintf("[%q]", path)
	}
	return fmt.Sprintf("lcd: {rules: [{paths: [/old], action: allow}]}\nrpc: {jsonrpc: {default: allow, maxBatchSize: 2}}\ngrpc: {protosets: %s, rules: [{methods: [%q], action: %s, cache: {enable: true, keyMode: canonical, ttl: 1m, coalesce: false}}]}", paths, grpcCacheTestMethod, action)
}

func newGRPCReloadTestGuard(t *testing.T) (*CosmoGuard, string, string) {
	t.Helper()
	proto3, proto2 := writeReloadProtoset(t, "proto3"), writeReloadProtoset(t, "proto2")
	cg := newHotReloadTestGuard(t, grpcReloadConfig(proto3, "allow"))
	var invokes atomic.Int32
	conn := newRawGRPCTestUpstream(t, func(_ any, stream grpc.ServerStream) error {
		var request rawFrame
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		return stream.SendMsg(&rawFrame{Payload: []byte(fmt.Sprintf("upstream-%d", invokes.Add(1)))})
	})
	p, _ := newGRPCCacheTestProxy(t, conn, cg.cfg.GRPC.Rules[0].Cache)
	registry, err := LoadCanonicalRegistry(cg.cfg.GRPC.Protosets)
	require.NoError(t, err)
	p.canonical = registry
	cg.grpcProxy = p
	cg.applyRulesLocked()
	registerSharedMetrics()
	return cg, proto3, proto2
}

func assertGRPCReloadCache(t *testing.T, cg *CosmoGuard, payload []byte, state, response string) {
	t.Helper()
	stream := newGRPCCacheTestStream(payload)
	require.NoError(t, grpcCacheTestHandler(cg.grpcProxy)(nil, stream))
	gotState, gotResponse := stream.result()
	require.Equal(t, state, gotState)
	require.Equal(t, response, string(gotResponse))
}

func TestTryReloadGRPCProtosets(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	cg, proto3, proto2 := newGRPCReloadTestGuard(t)
	absent, explicitZero := []byte{0x10, 0x01}, []byte{0x08, 0x00, 0x10, 0x01}
	assertGRPCReloadCache(t, cg, absent, cacheMiss, "upstream-1")
	assertGRPCReloadCache(t, cg, explicitZero, cacheHit, "upstream-1")
	reloadTestFile(t, cg, grpcReloadConfig(proto2, "allow"))
	require.True(t, cg.dashboard.lastReload.Success)
	// The absent-field payload has identical canonical bytes in both schemas;
	// descriptor changes must still stop finding the old schema's cache entries.
	assertGRPCReloadCache(t, cg, absent, cacheMiss, "upstream-2")
	assertGRPCReloadCache(t, cg, explicitZero, cacheMiss, "upstream-3")
	assertGRPCReloadCache(t, cg, explicitZero, cacheHit, "upstream-3")

	require.NoError(t, os.Remove(proto2))
	reloadTestFile(t, cg, strings.Replace(grpcReloadConfig(proto2, "allow"), "/old", "/new", 1))
	require.True(t, cg.dashboard.lastReload.Success, "an unchanged protoset list must not reopen files")
	assertGRPCReloadCache(t, cg, absent, cacheHit, "upstream-2")
	assertGRPCReloadCache(t, cg, explicitZero, cacheHit, "upstream-3")

	reloadTestFile(t, cg, grpcReloadConfig("", "allow"))
	require.True(t, cg.dashboard.lastReload.Success)
	assertGRPCReloadCache(t, cg, absent, cacheMiss, "upstream-4")
	assertGRPCReloadCache(t, cg, explicitZero, cacheMiss, "upstream-5")
	reloadTestFile(t, cg, grpcReloadConfig(proto3, "allow"))
	require.True(t, cg.dashboard.lastReload.Success)
	assertGRPCReloadCache(t, cg, explicitZero, cacheHit, "upstream-1")

	proto2 = writeReloadProtoset(t, "proto2")
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			for range 100 {
				stream := newGRPCCacheTestStream(explicitZero)
				if err := grpcCacheTestHandler(cg.grpcProxy)(nil, stream); err != nil {
					t.Errorf("concurrent canonical request: %v", err)
					return
				}
			}
		})
	}
	for i := 0; i < 10; i++ {
		path := proto3
		if i%2 == 0 {
			path = proto2
		}
		reloadTestFile(t, cg, grpcReloadConfig(path, "allow"))
		require.True(t, cg.dashboard.lastReload.Success)
	}
	readers.Wait()
}

func TestTryReloadInvalidGRPCProtosets(t *testing.T) {
	if restartTestProcess(t) {
		return
	}
	cg, proto3, _ := newGRPCReloadTestGuard(t)
	absent, explicitZero := []byte{0x10, 0x01}, []byte{0x08, 0x00, 0x10, 0x01}
	assertGRPCReloadCache(t, cg, absent, cacheMiss, "upstream-1")
	invalid := filepath.Join(t.TempDir(), "invalid.protoset")
	require.NoError(t, os.WriteFile(invalid, []byte{0xff}, 0600))
	missing := filepath.Join(t.TempDir(), "missing.protoset")
	unresolved := filepath.Join(t.TempDir(), "unresolved.protoset")
	raw, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String("unresolved.proto"), Syntax: proto.String("proto3"), Dependency: []string{"missing.proto"},
	}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(unresolved, raw, 0600))
	wantInvalid := testutil.ToFloat64(configReloadsCounter.WithLabelValues("invalid"))
	applied := testutil.ToFloat64(configReloadsCounter.WithLabelValues("applied"))
	restart := testutil.ToFloat64(configReloadsCounter.WithLabelValues("restart_required"))
	for _, path := range []string{missing, invalid, unresolved} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			next := strings.ReplaceAll(grpcReloadConfig(path, "deny"), "/old", "/rejected")
			next = strings.Replace(next, "maxBatchSize: 2", "maxBatchSize: 1", 1)
			parseRestartConfig(t, next)
			reloadTestFile(t, cg, next)
			require.False(t, cg.dashboard.lastReload.Success)
			require.Contains(t, cg.dashboard.lastReload.Error, "grpc canonical registry")
			require.Equal(t, []string{proto3}, cg.cfg.GRPC.Protosets)
			require.Equal(t, "/old", cg.lcdProxy.rules[0].Paths[0])
			assertGRPCReloadCache(t, cg, explicitZero, cacheHit, "upstream-1")
			assertReloadBatch(t, cg.jsonRpcHandler, 2, http.StatusOK)
			wantInvalid++
			require.Equal(t, wantInvalid, testutil.ToFloat64(configReloadsCounter.WithLabelValues("invalid")))
			require.Equal(t, applied, testutil.ToFloat64(configReloadsCounter.WithLabelValues("applied")))
			require.Equal(t, restart, testutil.ToFloat64(configReloadsCounter.WithLabelValues("restart_required")))
		})
	}
}
