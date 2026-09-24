package compat

import (
	"testing"

	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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
