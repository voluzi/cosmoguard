package cosmoguard

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// TestGrpcCacheKey_MetadataDistinguishesHeight is the #6 regression: two
// otherwise-identical requests that differ only by the x-cosmos-block-height
// metadata must produce different cache keys (both raw and method-only),
// and the block-height key must be included by default.
func TestGrpcCacheKey_MetadataDistinguishesHeight(t *testing.T) {
	keys := (&RuleCache{}).EffectiveKeyMetadata()
	ctx100 := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-cosmos-block-height", "100"))
	ctx200 := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-cosmos-block-height", "200"))
	mp100 := grpcCacheKeyMetaPart(ctx100, keys)
	mp200 := grpcCacheKeyMetaPart(ctx200, keys)
	if mp100 == mp200 {
		t.Fatal("metadata part should differ by block height")
	}

	m := "/cosmos.bank.v1beta1.Query/Balance"
	if grpcCacheKey(0xabc, m, []byte("addr=A"), "", nil, mp100) ==
		grpcCacheKey(0xabc, m, []byte("addr=A"), "", nil, mp200) {
		t.Fatal("raw key must differ when block height differs")
	}
	if grpcCacheKey(0xabc, m, []byte("addr=A"), "method-only", nil, mp100) ==
		grpcCacheKey(0xabc, m, []byte("addr=A"), "method-only", nil, mp200) {
		t.Fatal("method-only key must still differ when block height differs")
	}

	// Explicit empty list opts out (keys collapse regardless of height).
	empty := (&RuleCache{KeyMetadata: []string{}}).EffectiveKeyMetadata()
	if grpcCacheKeyMetaPart(ctx100, empty) != "" {
		t.Fatal("explicit empty KeyMetadata should fold in no metadata")
	}
}

// TestGrpcCacheKey_RawIncludesPayload: different payloads under the same
// rule + method must produce different keys.
func TestGrpcCacheKey_RawIncludesPayload(t *testing.T) {
	a := grpcCacheKey(0xdead, "/cosmos.bank.v1beta1.Query/Balance", []byte("addr=A"), "", nil, "")
	b := grpcCacheKey(0xdead, "/cosmos.bank.v1beta1.Query/Balance", []byte("addr=B"), "", nil, "")
	if a == b {
		t.Fatalf("raw key should differ when payload differs; got %s == %s", a, b)
	}
}

// TestGrpcCacheKey_MethodOnlyIgnoresPayload: the same rule + method with
// different payloads must collapse to one key.
func TestGrpcCacheKey_MethodOnlyIgnoresPayload(t *testing.T) {
	a := grpcCacheKey(0xdead, "/cosmos.bank.v1beta1.Query/Params", []byte("anything"), "method-only", nil, "")
	b := grpcCacheKey(0xdead, "/cosmos.bank.v1beta1.Query/Params", []byte("else"), "method-only", nil, "")
	if a != b {
		t.Fatalf("method-only should ignore payload; got %s != %s", a, b)
	}
}

// TestGrpcCacheKey_RuleNamespaceHoldsAcrossKeyModes: changing the rule
// fingerprint must produce a fresh key even in method-only mode (per-rule
// namespacing applies in both modes).
func TestGrpcCacheKey_RuleNamespaceHoldsAcrossKeyModes(t *testing.T) {
	a := grpcCacheKey(0x1, "/m", []byte("x"), "method-only", nil, "")
	b := grpcCacheKey(0x2, "/m", []byte("x"), "method-only", nil, "")
	if a == b {
		t.Fatalf("different rule fingerprints must produce different keys")
	}
}

// TestGrpcCacheKey_CanonicalCollapses: two byte-level serializations of
// the same logical message produce the same key when keyMode=canonical
// and the method is in the registry. Protobuf permits arbitrary field
// order on the wire, so a naive raw hash would miss the collision.
func TestGrpcCacheKey_CanonicalCollapses(t *testing.T) {
	reg := buildCanonicalTestRegistry(t)

	method := "/cosmoguard.test.Svc/Echo"
	abOrder := encodeSample(7, 9, true)  // field a then b on the wire
	baOrder := encodeSample(7, 9, false) // field b then a

	if string(abOrder) == string(baOrder) {
		t.Fatalf("test setup: the two encodings should be byte-different")
	}

	k1 := grpcCacheKey(0xabc, method, abOrder, "canonical", reg, "")
	k2 := grpcCacheKey(0xabc, method, baOrder, "canonical", reg, "")
	if k1 != k2 {
		t.Fatalf("canonical should collapse byte differences; got %s != %s", k1, k2)
	}
}

// TestGrpcCacheKey_CanonicalUnknownMethodDegrades: a method not in the
// registry falls back to raw — different bytes still produce different
// keys.
func TestGrpcCacheKey_CanonicalUnknownMethodDegrades(t *testing.T) {
	reg := buildCanonicalTestRegistry(t)

	k1 := grpcCacheKey(0xabc, "/unknown.Svc/Method", []byte("aaa"), "canonical", reg, "")
	k2 := grpcCacheKey(0xabc, "/unknown.Svc/Method", []byte("bbb"), "canonical", reg, "")
	if k1 == k2 {
		t.Fatalf("unknown methods should degrade to raw; different payloads should differ")
	}
}

// buildCanonicalTestRegistry builds a CanonicalRegistry in memory with
// a single Sample{int32 a=1; int32 b=2} message and Svc/Echo method —
// avoids needing protoc / .protoset files for the test.
func buildCanonicalTestRegistry(t *testing.T) *CanonicalRegistry {
	t.Helper()
	int32Type := descriptorpb.FieldDescriptorProto_TYPE_INT32
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	one := int32(1)
	two := int32(2)
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("cosmoguard.test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Sample"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("a"), Number: &one, Type: &int32Type, Label: &optional, JsonName: proto.String("a")},
					{Name: proto.String("b"), Number: &two, Type: &int32Type, Label: &optional, JsonName: proto.String("b")},
				},
			},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{
				Name: proto.String("Svc"),
				Method: []*descriptorpb.MethodDescriptorProto{
					{
						Name:       proto.String("Echo"),
						InputType:  proto.String(".cosmoguard.test.Sample"),
						OutputType: proto.String(".cosmoguard.test.Sample"),
					},
				},
			},
		},
	}
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{fileDesc}}

	reg := &CanonicalRegistry{methods: map[string]protoreflect.MethodDescriptor{}}
	if err := reg.indexSet(set); err != nil {
		t.Fatalf("indexSet: %v", err)
	}
	if reg.MethodCount() != 1 {
		t.Fatalf("expected 1 method, got %d", reg.MethodCount())
	}
	return reg
}

// encodeSample writes a Sample{a, b} on the wire either with field a
// before b (aFirst=true) or vice versa. Both are valid encodings for
// the same logical message. The two-byte (tag + value) form works
// because a and b are small positive int32s.
func encodeSample(a, b int32, aFirst bool) []byte {
	// Field 1 tag = (1<<3)|0 = 0x08; field 2 tag = (2<<3)|0 = 0x10.
	encA := []byte{0x08, byte(a)}
	encB := []byte{0x10, byte(b)}
	if aFirst {
		return append(append([]byte{}, encA...), encB...)
	}
	return append(append([]byte{}, encB...), encA...)
}
