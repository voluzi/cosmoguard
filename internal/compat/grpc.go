package compat

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Method is one unary query method the node serves.
type Method struct {
	// FullName is the gRPC path, "/pkg.Service/Method".
	FullName string
	Desc     protoreflect.MethodDescriptor
	// GETs are the google.api.http GET templates the LCD serves it on.
	GETs []string
	// Types resolves the reflected messages, including those packed in
	// google.protobuf.Any fields.
	Types *dynamicpb.Types
}

// volatileMethods answer about the latest state whatever height is
// requested, or about the particular node that answered, so only their
// shape is compared.
var volatileMethods = map[string]bool{
	"/cosmos.base.tendermint.v1beta1.Service/GetLatestBlock":        true,
	"/cosmos.base.tendermint.v1beta1.Service/GetLatestValidatorSet": true,
	"/cosmos.base.tendermint.v1beta1.Service/GetSyncing":            true,
	"/cosmos.base.tendermint.v1beta1.Service/GetNodeInfo":           true,
	"/cosmos.base.node.v1beta1.Service/Status":                      true,
	"/cosmos.base.node.v1beta1.Service/Config":                      true,
}

// reflectionMethods are tried in order. Many Cosmos nodes serve only
// v1alpha, whose messages are wire-identical to v1's, so v1 types are used
// for both.
var reflectionMethods = []string{
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo",
}

// dialGRPC connects to an https:// target over TLS and to an http:// or
// bare host:port target in plaintext.
func dialGRPC(target string) (*grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	addr := target
	if u, err := url.Parse(target); err == nil && u.Host != "" {
		addr = u.Host
		if u.Scheme == "https" {
			creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
			if u.Port() == "" {
				addr += ":443"
			}
		}
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxBody)))
}

// discoverMethods lists the node's unary query methods through server
// reflection. Tx broadcast/simulate and Msg services are left out.
func discoverMethods(ctx context.Context, conn *grpc.ClientConn) ([]Method, error) {
	var stream grpc.ClientStream
	ask := func(req *rpb.ServerReflectionRequest) (*rpb.ServerReflectionResponse, error) {
		if err := stream.SendMsg(req); err != nil {
			return nil, err
		}
		resp := &rpb.ServerReflectionResponse{}
		return resp, stream.RecvMsg(resp)
	}
	var resp *rpb.ServerReflectionResponse
	listReq := &rpb.ServerReflectionRequest{MessageRequest: &rpb.ServerReflectionRequest_ListServices{ListServices: ""}}
	for _, method := range reflectionMethods {
		var err error
		stream, err = conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, method)
		if err != nil {
			return nil, fmt.Errorf("reflection: %w", err)
		}
		resp, err = ask(listReq)
		if status.Code(err) == codes.Unimplemented {
			_ = stream.CloseSend()
			resp = nil
			continue
		}
		defer func() { _ = stream.CloseSend() }()
		if err != nil {
			return nil, fmt.Errorf("reflection list: %w", err)
		}
		break
	}
	if resp == nil {
		return nil, errors.New("the node serves no gRPC server reflection")
	}
	var services []string
	for _, s := range resp.GetListServicesResponse().GetService() {
		services = append(services, s.GetName())
	}

	fdps := map[string]*descriptorpb.FileDescriptorProto{}
	addFiles := func(r *rpb.ServerReflectionResponse) error {
		if e := r.GetErrorResponse(); e != nil {
			return errors.New(e.GetErrorMessage())
		}
		for _, b := range r.GetFileDescriptorResponse().GetFileDescriptorProto() {
			fdp := &descriptorpb.FileDescriptorProto{}
			if err := proto.Unmarshal(b, fdp); err != nil {
				return err
			}
			fdps[fdp.GetName()] = fdp
		}
		return nil
	}
	for _, s := range services {
		r, err := ask(&rpb.ServerReflectionRequest{MessageRequest: &rpb.ServerReflectionRequest_FileContainingSymbol{FileContainingSymbol: s}})
		if err != nil {
			return nil, fmt.Errorf("reflection %s: %w", s, err)
		}
		if err := addFiles(r); err != nil {
			return nil, fmt.Errorf("reflection %s: %w", s, err)
		}
	}
	// Servers may omit dependencies they assume the client has.
	for pending := true; pending; {
		pending = false
		for _, fdp := range fdps {
			for _, dep := range fdp.GetDependency() {
				if _, ok := fdps[dep]; ok {
					continue
				}
				r, err := ask(&rpb.ServerReflectionRequest{MessageRequest: &rpb.ServerReflectionRequest_FileByFilename{FileByFilename: dep}})
				if err != nil {
					return nil, fmt.Errorf("reflection %s: %w", dep, err)
				}
				if err := addFiles(r); err != nil || fdps[dep] == nil {
					// Leave it unresolved; building the file tolerates that.
					fdps[dep] = &descriptorpb.FileDescriptorProto{Name: proto.String(dep)}
				}
				pending = true
			}
		}
	}

	files := buildFiles(fdps)
	types := dynamicpb.NewTypes(files)
	var methods []Method
	for _, s := range services {
		d, err := files.FindDescriptorByName(protoreflect.FullName(s))
		if err != nil {
			continue
		}
		sd, ok := d.(protoreflect.ServiceDescriptor)
		if !ok || !queryService(s) {
			continue
		}
		for i := 0; i < sd.Methods().Len(); i++ {
			md := sd.Methods().Get(i)
			if md.IsStreamingClient() || md.IsStreamingServer() || !queryMethod(string(md.Name())) {
				continue
			}
			methods = append(methods, Method{
				FullName: fmt.Sprintf("/%s/%s", s, md.Name()),
				Desc:     md,
				GETs:     httpGETs(md),
				Types:    types,
			})
		}
	}
	sort.Slice(methods, func(i, j int) bool { return methods[i].FullName < methods[j].FullName })
	return methods, nil
}

func queryService(name string) bool {
	return !strings.HasSuffix(name, ".Msg") && !strings.HasPrefix(name, "grpc.reflection.")
}

func queryMethod(name string) bool {
	return !strings.HasPrefix(name, "Broadcast") && !strings.HasPrefix(name, "Simulate")
}

// buildFiles registers descriptors in dependency order. Unresolvable
// references (e.g. gogoproto options) are tolerated; a file that still
// fails is dropped and its services are simply not found.
func buildFiles(fdps map[string]*descriptorpb.FileDescriptorProto) *protoregistry.Files {
	files := new(protoregistry.Files)
	opts := protodesc.FileOptions{AllowUnresolvable: true}
	done := map[string]bool{}
	var add func(name string)
	add = func(name string) {
		if done[name] {
			return
		}
		done[name] = true
		fdp := fdps[name]
		if fdp == nil {
			return
		}
		for _, dep := range fdp.GetDependency() {
			add(dep)
		}
		if fd, err := opts.New(fdp, files); err == nil {
			_ = files.RegisterFile(fd)
		}
	}
	names := make([]string, 0, len(fdps))
	for n := range fdps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		add(n)
	}
	return files
}

// httpGETs returns the GET templates of a method's google.api.http
// option, including additional bindings.
func httpGETs(md protoreflect.MethodDescriptor) []string {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil || !proto.HasExtension(opts, annotations.E_Http) {
		return nil
	}
	rule, ok := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule)
	if !ok {
		return nil
	}
	var out []string
	for _, r := range append([]*annotations.HttpRule{rule}, rule.GetAdditionalBindings()...) {
		if g := r.GetGet(); g != "" {
			out = append(out, g)
		}
	}
	return out
}

// buildRequest fills every top-level request field that has a live value.
func buildRequest(md protoreflect.MethodDescriptor, p Params) ([]byte, error) {
	msg := dynamicpb.NewMessage(md.Input())
	fields := md.Input().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.IsList() || f.IsMap() {
			continue
		}
		v, ok := p.Lookup(string(f.Name()))
		if !ok {
			continue
		}
		switch f.Kind() {
		case protoreflect.StringKind:
			msg.Set(f, protoreflect.ValueOfString(v))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			if n, err := strconv.ParseUint(v, 10, 64); err == nil {
				msg.Set(f, protoreflect.ValueOfUint64(n))
			}
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				msg.Set(f, protoreflect.ValueOfInt64(n))
			}
		}
	}
	return proto.Marshal(msg)
}

// rawCodec passes already-encoded protobuf bytes through unchanged, so
// responses are compared exactly as they came off the wire.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: unexpected %T", v)
	}
	return b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: unexpected %T", v)
	}
	*p = append((*p)[:0], data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// invoke calls one method pinned to height. An error status is an answer;
// only Unavailable, DeadlineExceeded and Canceled mean no answer arrived.
func invoke(ctx context.Context, conn *grpc.ClientConn, method string, req []byte, height int64) Response {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-cosmos-block-height", strconv.FormatInt(height, 10))
	var out []byte
	var hdr metadata.MD
	err := conn.Invoke(ctx, method, req, &out, grpc.ForceCodec(rawCodec{}), grpc.Header(&hdr))
	var h int64
	if v := hdr.Get("x-cosmos-block-height"); len(v) > 0 {
		h, _ = strconv.ParseInt(v[0], 10, 64)
	}
	if err == nil {
		return Response{Status: int(codes.OK), Body: out, Height: h}
	}
	st := status.Convert(err)
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return Response{Err: err}
	}
	return Response{
		Status: int(st.Code()),
		Body:   []byte(st.Message()),
		Height: h,
		// cosmoguard refuses with Unauthenticated; PermissionDenied is
		// kept for other gateways.
		Denied: st.Code() == codes.Unauthenticated || st.Code() == codes.PermissionDenied,
		// ResourceExhausted is also how an oversized message is refused,
		// which is a deterministic answer, not a rate limit.
		Throttled: st.Code() == codes.ResourceExhausted && !strings.Contains(st.Message(), "larger than max"),
	}
}

// asJSON renders a successful response as JSON, for shape comparison. A
// response that cannot be rendered (an Any whose type reflection did not
// return) is marked, and the endpoint skipped, rather than falling back to
// comparing bytes that change every block. Its LCD route is still
// compared, as the node renders that JSON itself.
func asJSON(m Method, r Response) Response {
	if r.Err != nil || r.Status != int(codes.OK) {
		return r
	}
	msg := dynamicpb.NewMessage(m.Desc.Output())
	err := proto.Unmarshal(r.Body, msg)
	if err == nil {
		var b []byte
		b, err = protojson.MarshalOptions{Resolver: m.Types}.Marshal(msg)
		r.Body = b
	}
	if err != nil {
		r.Unrenderable = "cannot render the answer for shape comparison: " + err.Error()
	}
	return r
}
