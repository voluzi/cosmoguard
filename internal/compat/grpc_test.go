package compat

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"gotest.tools/assert"
)

// testService builds a descriptor like the ones reflection returns: a
// Query service whose Balance method carries a google.api.http GET with
// an additional binding, and whose Params method has none.
func testService(t *testing.T) protoreflect.ServiceDescriptor {
	t.Helper()
	opts := &descriptorpb.MethodOptions{}
	proto.SetExtension(opts, annotations.E_Http, &annotations.HttpRule{
		Pattern:            &annotations.HttpRule_Get{Get: "/bank/balances/{address}/by_denom"},
		AdditionalBindings: []*annotations.HttpRule{{Pattern: &annotations.HttpRule_Get{Get: "/bank/v2/{address}"}}},
	})
	// Round-trip through bytes, as reflection delivers descriptors.
	raw, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("test/query.proto"),
		Package: proto.String("test.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("BalanceRequest"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("address"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("proposal_id"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("height"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
				{Name: proto.String("unknown"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			}},
			{Name: proto.String("Empty")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Query"),
			Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("Balance"), InputType: proto.String(".test.v1.BalanceRequest"), OutputType: proto.String(".test.v1.Empty"), Options: opts},
				{Name: proto.String("Params"), InputType: proto.String(".test.v1.Empty"), OutputType: proto.String(".test.v1.Empty")},
			},
		}},
	})
	assert.NilError(t, err)
	fdp := &descriptorpb.FileDescriptorProto{}
	assert.NilError(t, proto.Unmarshal(raw, fdp))
	files := buildFiles(map[string]*descriptorpb.FileDescriptorProto{fdp.GetName(): fdp})
	d, err := files.FindDescriptorByName("test.v1.Query")
	assert.NilError(t, err)
	return d.(protoreflect.ServiceDescriptor)
}

func TestHTTPGETs(t *testing.T) {
	sd := testService(t)
	assert.DeepEqual(t, httpGETs(sd.Methods().ByName("Balance")), []string{"/bank/balances/{address}/by_denom", "/bank/v2/{address}"})
	assert.Assert(t, httpGETs(sd.Methods().ByName("Params")) == nil)
}

func TestBuildRequest(t *testing.T) {
	md := testService(t).Methods().ByName("Balance")
	raw, err := buildRequest(md, Params{"account": "acc1", "proposal_id": "42", "height": "7"})
	assert.NilError(t, err)
	msg := dynamicpb.NewMessage(md.Input())
	assert.NilError(t, proto.Unmarshal(raw, msg))
	f := md.Input().Fields()
	assert.Equal(t, msg.Get(f.ByName("address")).String(), "acc1")
	assert.Equal(t, msg.Get(f.ByName("proposal_id")).Uint(), uint64(42))
	assert.Equal(t, msg.Get(f.ByName("height")).Int(), int64(7))
	assert.Assert(t, !msg.Has(f.ByName("unknown")))
}

func TestQueryFilters(t *testing.T) {
	assert.Assert(t, queryService("cosmos.bank.v1beta1.Query"))
	assert.Assert(t, queryService("cosmos.base.tendermint.v1beta1.Service"))
	assert.Assert(t, !queryService("cosmos.bank.v1beta1.Msg"))
	assert.Assert(t, !queryService("grpc.reflection.v1alpha.ServerReflection"))
	assert.Assert(t, queryMethod("GetTx"))
	assert.Assert(t, !queryMethod("BroadcastTx"))
	assert.Assert(t, !queryMethod("Simulate"))
}

func TestRawCodec(t *testing.T) {
	c := rawCodec{}
	b, err := c.Marshal([]byte{1, 2})
	assert.NilError(t, err)
	var out []byte
	assert.NilError(t, c.Unmarshal(b, &out))
	assert.DeepEqual(t, out, []byte{1, 2})
	_, err = c.Marshal("not bytes")
	assert.ErrorContains(t, err, "unexpected string")
}

func TestCometCallEncoding(t *testing.T) {
	calls := cometCalls(100, "ABCD", "")
	byName := map[string]rpcCall{}
	for _, c := range calls {
		byName[c.method] = c
	}
	// URI form: strings quoted, bytes as 0x-hex. JSON-RPC form: bytes base64.
	assert.Equal(t, byName["tx_search"].uri(), `/tx_search?query=%22tx.height%3D100%22&per_page=5`)
	assert.Equal(t, byName["block_by_hash"].uri(), `/block_by_hash?hash=0xABCD`)
	assert.DeepEqual(t, byName["block_by_hash"].body(1)["params"], map[string]any{"hash": "q80="})
	assert.Equal(t, byName["tx"].skip, "no live tx hash")
	assert.Equal(t, byName["block_by_hash"].skip, "")
	for _, c := range calls {
		assert.Assert(t, c.method != "broadcast_tx_sync" && c.method != "genesis", c.method)
	}
}

// statusServer answers every method with the status in the request's
// "want" metadata.
func statusServer(t *testing.T) *grpc.ClientConn {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NilError(t, err)
	srv := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		_ = stream.SetHeader(metadata.Pairs("x-cosmos-block-height", "7"))
		switch md.Get("want")[0] {
		case "ok":
			return stream.SendMsg([]byte{})
		case "unauthenticated":
			return status.Error(codes.Unauthenticated, "denied")
		case "throttled":
			return status.Error(codes.ResourceExhausted, "rate limited")
		case "too-large":
			return status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5 vs. 4)")
		case "not-found":
			return status.Error(codes.NotFound, "nope")
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := dialGRPC("http://" + lis.Addr().String())
	assert.NilError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestInvokeStatusMapping(t *testing.T) {
	conn := statusServer(t)
	call := func(ctx context.Context, want string) Response {
		ctx = metadata.AppendToOutgoingContext(ctx, "want", want)
		return invoke(ctx, conn, "/x.Query/M", []byte{}, 7)
	}
	r := call(t.Context(), "ok")
	assert.NilError(t, r.Err)
	assert.Equal(t, r.Status, 0)
	assert.Equal(t, r.Height, int64(7))

	r = call(t.Context(), "unauthenticated")
	assert.Assert(t, r.Denied, "cosmoguard refuses with Unauthenticated")

	assert.Assert(t, call(t.Context(), "throttled").Throttled)
	assert.Assert(t, !call(t.Context(), "too-large").Throttled, "an oversized message is an answer, not a rate limit")

	r = call(t.Context(), "not-found")
	assert.NilError(t, r.Err, "an error status is an answer")
	assert.Equal(t, r.Status, int(codes.NotFound))

	ctx, cancel := context.WithCancel(t.Context())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	assert.Assert(t, call(ctx, "hang").Err != nil, "a cancelled call is no answer")
}

func TestAsJSONResolvesAny(t *testing.T) {
	// A response message holding an Any whose type only the reflected
	// files know, as GetLatestValidatorSet's pub_key does.
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("test/any.proto"),
		Package:    proto.String("test.v1"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/any.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("PubKey"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("key"), JsonName: proto.String("key"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			}},
			{Name: proto.String("Resp"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("pub_key"), JsonName: proto.String("pubKey"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".google.protobuf.Any"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			}},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name:   proto.String("Query"),
			Method: []*descriptorpb.MethodDescriptorProto{{Name: proto.String("Get"), InputType: proto.String(".test.v1.Resp"), OutputType: proto.String(".test.v1.Resp")}},
		}},
	}
	anyFile := protodesc.ToFileDescriptorProto(anypb.File_google_protobuf_any_proto)
	files := buildFiles(map[string]*descriptorpb.FileDescriptorProto{fdp.GetName(): fdp, anyFile.GetName(): anyFile})
	types := dynamicpb.NewTypes(files)
	d, err := files.FindDescriptorByName("test.v1.Query")
	assert.NilError(t, err)
	m := Method{FullName: "/test.v1.Query/Get", Desc: d.(protoreflect.ServiceDescriptor).Methods().Get(0), Types: types}

	pubKey := dynamicpb.NewMessage(m.Desc.Output().ParentFile().Messages().ByName("PubKey"))
	pubKey.Set(pubKey.Descriptor().Fields().ByName("key"), protoreflect.ValueOfBytes([]byte{1}))
	packed, err := proto.Marshal(pubKey)
	assert.NilError(t, err)
	resp := dynamicpb.NewMessage(m.Desc.Output())
	resp.Set(resp.Descriptor().Fields().ByName("pub_key"), protoreflect.ValueOfMessage((&anypb.Any{TypeUrl: "/test.v1.PubKey", Value: packed}).ProtoReflect()))
	body, err := proto.Marshal(resp)
	assert.NilError(t, err)

	r := asJSON(m, Response{Status: 0, Body: body})
	assert.NilError(t, r.Err)
	var got any
	assert.NilError(t, json.Unmarshal(r.Body, &got)) // protojson randomises whitespace
	assert.DeepEqual(t, got, map[string]any{"pubKey": map[string]any{"@type": "/test.v1.PubKey", "key": "AQ=="}})

	r = asJSON(Method{FullName: m.FullName, Desc: m.Desc, Types: dynamicpb.NewTypes(new(protoregistry.Files))}, Response{Body: body})
	assert.Assert(t, r.Unrenderable != "", "an unrenderable answer must not fall back to comparing bytes")
	c, _ := Classify(r, []Response{r}, true)
	assert.Equal(t, c, Skipped)
}

func TestScalarValue(t *testing.T) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name: proto.String("test/kinds.proto"), Package: proto.String("test.v1"), Syntax: proto.String("proto3"),
		EnumType: []*descriptorpb.EnumDescriptorProto{{Name: proto.String("Status"), Value: []*descriptorpb.EnumValueDescriptorProto{
			{Name: proto.String("STATUS_UNSPECIFIED"), Number: proto.Int32(0)},
			{Name: proto.String("STATUS_BONDED"), Number: proto.Int32(3)},
		}}},
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Req"), Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("flag"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: proto.String("small"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: proto.String("status"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(), TypeName: proto.String(".test.v1.Status"), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
			{Name: proto.String("key"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()},
		}}},
	}
	files := buildFiles(map[string]*descriptorpb.FileDescriptorProto{fdp.GetName(): fdp})
	d, err := files.FindDescriptorByName("test.v1.Req")
	assert.NilError(t, err)
	fields := d.(protoreflect.MessageDescriptor).Fields()

	v, err := scalarValue(fields.ByName("flag"), "true")
	assert.NilError(t, err)
	assert.Equal(t, v.Bool(), true)
	v, err = scalarValue(fields.ByName("small"), "-7")
	assert.NilError(t, err)
	assert.Equal(t, v.Int(), int64(-7))
	v, err = scalarValue(fields.ByName("status"), "STATUS_BONDED")
	assert.NilError(t, err)
	assert.Equal(t, v.Enum(), protoreflect.EnumNumber(3))
	v, err = scalarValue(fields.ByName("status"), "3")
	assert.NilError(t, err)
	assert.Equal(t, v.Enum(), protoreflect.EnumNumber(3))

	_, err = scalarValue(fields.ByName("status"), "BONDED")
	assert.ErrorContains(t, err, "is not a value of test.v1.Status")
	_, err = scalarValue(fields.ByName("small"), "abc")
	assert.Assert(t, err != nil, "a malformed value must not become a default")
	v, err = scalarValue(fields.ByName("key"), "x")
	assert.NilError(t, err)
	assert.Assert(t, !v.IsValid(), "bytes fields are not filled from parameters")
}
