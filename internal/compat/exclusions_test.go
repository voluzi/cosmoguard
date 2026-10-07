package compat

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"gotest.tools/assert"
)

type exclusionTestServices struct{}

func (exclusionTestServices) GetServiceInfo() map[string]grpc.ServiceInfo {
	return map[string]grpc.ServiceInfo{"eth.evm.v1.Query": {}, "cosmos.distribution.v1beta1.Query": {}}
}

func TestRunMethodExclusions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		patterns  []string
		unsafe    bool
		skipTrace bool
	}{
		{name: "default", skipTrace: true},
		{name: "exact", patterns: []string{"eth.evm.v1.Query/TraceCall"}, unsafe: true, skipTrace: true},
		{name: "prefix", patterns: []string{"/eth.evm.v1.Query/Trace*"}, unsafe: true, skipTrace: true},
		{name: "unsafe opt in", unsafe: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := map[string]int{}
			count := func(key string) { mu.Lock(); calls[key]++; mu.Unlock() }
			endpoints := func(side string) Endpoints {
				files := new(protoregistry.Files)
				for _, pkg := range []string{"eth.evm.v1", "cosmos.distribution.v1beta1"} {
					names := []string{"TraceCall", "Params"}
					if pkg != "eth.evm.v1" {
						names = []string{"CommunityPool"}
					}
					fd := &descriptorpb.FileDescriptorProto{Name: proto.String(pkg + ".proto"), Package: proto.String(pkg), Syntax: proto.String("proto3"),
						MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Empty")}},
						Service:     []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Query")}},
					}
					for _, name := range names {
						opts := &descriptorpb.MethodOptions{}
						path := "/trace"
						if name == "CommunityPool" {
							path = crossHeightLCD
						}
						if name != "Params" {
							proto.SetExtension(opts, annotations.E_Http, &annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: path}})
						}
						fd.Service[0].Method = append(fd.Service[0].Method, &descriptorpb.MethodDescriptorProto{Name: proto.String(name), InputType: proto.String("." + pkg + ".Empty"), OutputType: proto.String("." + pkg + ".Empty"), Options: opts})
					}
					desc, err := protodesc.NewFile(fd, protoregistry.GlobalFiles)
					assert.NilError(t, err)
					assert.NilError(t, files.RegisterFile(desc))
				}
				lis, err := net.Listen("tcp", "127.0.0.1:0")
				assert.NilError(t, err)
				srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
					method, _ := grpc.MethodFromServerStream(stream)
					count(side + method)
					var req descriptorpb.DescriptorProto
					if err := stream.RecvMsg(&req); err != nil {
						return err
					}
					return stream.SendMsg(&descriptorpb.DescriptorProto{})
				}))
				for _, pkg := range []string{"eth.evm.v1", "cosmos.distribution.v1beta1"} {
					srv.RegisterService(&grpc.ServiceDesc{ServiceName: pkg + ".Query", HandlerType: (*interface{})(nil)}, struct{}{})
				}
				reflectionv1.RegisterServerReflectionServer(srv, reflection.NewServerV1(reflection.ServerOptions{Services: exclusionTestServices{}, DescriptorResolver: files}))
				go func() { _ = srv.Serve(lis) }()
				t.Cleanup(srv.Stop)
				httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					count(side + r.URL.Path)
					if r.URL.Path == "/status" {
						fmt.Fprint(w, `{"result":{"node_info":{"network":"test"},"sync_info":{"latest_block_height":"8"}}}`)
						return
					}
					fmt.Fprint(w, `{}`)
				}))
				t.Cleanup(httpSrv.Close)
				return Endpoints{GRPC: "http://" + lis.Addr().String(), LCD: httpSrv.URL, RPC: httpSrv.URL}
			}
			node, guard := endpoints("node"), endpoints("guard")
			var progress bytes.Buffer
			patterns := append(append([]string{}, tc.patterns...), "cosmos.distribution.v1beta1.Query/CommunityPool", "/missing.v1.Query/Nope")
			rep, err := Run(t.Context(), Options{Node: node, Guard: guard, Height: 3, Timeout: time.Second, Concurrency: 2,
				Protocols: map[string]bool{ProtoGRPC: true, ProtoLCD: true, ProtoRPC: true}, Log: &progress, ExcludeMethods: patterns, AllowUnsafeMethods: tc.unsafe})
			assert.NilError(t, err)
			mu.Lock()
			defer mu.Unlock()
			for _, side := range []string{"node", "guard"} {
				assert.Assert(t, calls[side+"/eth.evm.v1.Query/Params"] > 0, "ordinary method must run")
				assert.Equal(t, calls[side+crossHeightGRPC], 0)
				assert.Equal(t, calls[side+crossHeightLCD], 0)
				if tc.skipTrace {
					assert.Equal(t, calls[side+"/eth.evm.v1.Query/TraceCall"], 0)
					assert.Equal(t, calls[side+"/trace"], 0)
				} else {
					assert.Assert(t, calls[side+"/eth.evm.v1.Query/TraceCall"] > 0)
				}
			}
			skips := 0
			for _, r := range rep.Results {
				if strings.Contains(r.Name, "CommunityPool") || strings.Contains(r.Name, crossHeightLCD) || (tc.skipTrace && (strings.Contains(r.Name, "TraceCall") || r.Name == "/trace")) {
					assert.Equal(t, r.Class, Skipped)
					assert.Assert(t, strings.Contains(r.Detail, "excluded by "), r.Detail)
					skips++
				}
			}
			assert.Assert(t, skips >= 5, "gRPC, LCD and all three cross-height probes must be recorded")
			assert.Assert(t, strings.Contains(progress.String(), "unmatched exclusion /missing.v1.Query/Nope"), progress.String())
		})
	}
}

func TestInvalidMethodExclusionsDoNotContactNode(t *testing.T) {
	for _, pattern := range []string{"", "*", "/Query/TraceCall", "/x.Query/", "/x.Query/Tr*ace", "/x.Query/Trace**", "//x.Query/Trace", "/x.Query/Trace/Call", "/x.Query/Trace?", "/x..Query/Trace"} {
		t.Run(pattern, func(t *testing.T) {
			contacted := false
			srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacted = true }))
			defer srv.Close()
			_, err := Run(context.Background(), Options{Node: Endpoints{RPC: srv.URL}, ExcludeMethods: []string{pattern}})
			assert.ErrorContains(t, err, "exclude-method")
			assert.Assert(t, !contacted)
		})
	}
}
