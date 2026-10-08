// l2testupstream serves deterministic responses for the coordinator's guard soaks.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type request struct {
	Key   string `json:"key"`
	Size  int    `json:"size"`
	Delay int    `json:"delay_ms"`
}

type upstream struct{ requests atomic.Uint64 }

func payload(r request) ([]byte, error) {
	if r.Size == 0 {
		r.Size = 16 << 10
	}
	if r.Size < 0 || r.Size > 2<<20 || r.Delay < 0 || r.Delay > 10000 {
		return nil, errors.New("invalid test response size or delay")
	}
	h := sha256.Sum256([]byte("42:" + r.Key))
	pattern := hex.EncodeToString(h[:])
	return []byte(strings.Repeat(pattern, (r.Size+len(pattern)-1)/len(pattern))[:r.Size]), nil
}

func (u *upstream) response(ctx context.Context, r request) ([]byte, error) {
	value, err := payload(r)
	if err != nil {
		return nil, err
	}
	timer := time.NewTimer(time.Duration(r.Delay) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	u.requests.Add(1)
	return value, nil
}

func (u *upstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetReadLimit(2 << 20)
		for {
			var rpc struct {
				ID     json.RawMessage `json:"id"`
				Params request         `json:"params"`
			}
			if err := conn.ReadJSON(&rpc); err != nil {
				return
			}
			value, err := u.response(r.Context(), rpc.Params)
			if err != nil {
				return
			}
			if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": rpc.ID, "result": string(value)}); err != nil {
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/probe/stats" {
		_ = json.NewEncoder(w).Encode(map[string]any{"requests": u.requests.Load(), "seed": 42})
		return
	}
	var q request
	var id json.RawMessage
	if r.Method == http.MethodPost {
		var rpc struct {
			ID     json.RawMessage `json:"id"`
			Params request         `json:"params"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&rpc); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		q, id = rpc.Params, rpc.ID
	} else {
		q.Key = r.URL.Path + "?" + r.URL.Query().Get("key")
		q.Size, _ = strconv.Atoi(r.URL.Query().Get("size"))
		q.Delay, _ = strconv.Atoi(r.URL.Query().Get("delay_ms"))
	}
	value, err := u.response(r.Context(), q)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h := sha256.Sum256(value)
	w.Header().Set("X-Probe-Hash", hex.EncodeToString(h[:]))
	if r.Method == http.MethodPost {
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": string(value)})
	} else {
		_ = json.NewEncoder(w).Encode(map[string]any{"value": string(value)})
	}
}

type echo interface {
	Query(context.Context, *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error)
}

func (u *upstream) Query(ctx context.Context, value *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
	var r request
	if err := json.Unmarshal(value.Value, &r); err != nil {
		return nil, err
	}
	b, err := u.response(ctx, r)
	return wrapperspb.Bytes(b), err
}

func registerEcho(s *grpc.Server, u *upstream) error {
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("bounded_l2_probe.proto"), Package: proto.String("cosmoguard.probe"), Syntax: proto.String("proto3"),
		Dependency: []string{"google/protobuf/wrappers.proto"},
		Service:    []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Echo"), Method: []*descriptorpb.MethodDescriptorProto{{Name: proto.String("Query"), InputType: proto.String(".google.protobuf.BytesValue"), OutputType: proto.String(".google.protobuf.BytesValue")}}}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		return err
	}
	if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
		return err
	}
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "cosmoguard.probe.Echo", HandlerType: (*echo)(nil), Metadata: "bounded_l2_probe.proto",
		Methods: []grpc.MethodDesc{{MethodName: "Query", Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.BytesValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			handler := func(ctx context.Context, req any) (any, error) {
				return srv.(echo).Query(ctx, req.(*wrapperspb.BytesValue))
			}
			if interceptor == nil {
				return handler(ctx, in)
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/cosmoguard.probe.Echo/Query"}, handler)
		}}},
	}, u)
	reflection.Register(s)
	return nil
}

func run() error {
	lcd := flag.String("lcd", ":1317", "LCD listen address")
	rpc := flag.String("rpc", ":26657", "JSON-RPC listen address")
	grpcAddr := flag.String("grpc", ":9090", "gRPC listen address")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	u := new(upstream)
	group, ctx := errgroup.WithContext(ctx)
	var servers []*http.Server
	for _, addr := range []string{*lcd, *rpc} {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		defer listener.Close()
		s := &http.Server{Handler: u, ReadHeaderTimeout: 5 * time.Second}
		defer s.Close()
		servers = append(servers, s)
		group.Go(func() error {
			err := s.Serve(listener)
			if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		})
	}
	listener, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	s := grpc.NewServer(grpc.MaxRecvMsgSize(2<<20), grpc.MaxSendMsgSize(4<<20))
	defer s.Stop()
	if err := registerEcho(s, u); err != nil {
		return err
	}
	group.Go(func() error { return s.Serve(listener) })
	fmt.Fprintln(os.Stderr, "bounded L2 upstream ready; seed=42")
	<-ctx.Done()
	for _, server := range servers {
		_ = server.Close()
	}
	s.Stop()
	return group.Wait()
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
