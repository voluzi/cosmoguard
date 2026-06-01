package cosmoguard

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// CanonicalRegistry holds protobuf method descriptors loaded from
// operator-supplied protoset bundles. Used by the gRPC cache layer to
// canonicalize request payloads before hashing — different gRPC
// clients can serialize the same logical request with different byte
// representations (field order, default-vs-absent, varint width).
// Canonicalization decodes the bytes into a dynamic message and
// re-encodes them with proto.MarshalOptions{Deterministic: true}, so
// the cache key collapses across clients.
//
// A protoset is a binary FileDescriptorSet — the format produced by
// `protoc --descriptor_set_out=foo.protoset` and accepted by
// `grpcurl -protoset foo.protoset`. Operators ship one bundle covering
// every method they want canonicalized.
type CanonicalRegistry struct {
	mu      sync.RWMutex
	methods map[string]protoreflect.MethodDescriptor // "/pkg.Svc/Method" → MethodDescriptor
	files   *protoregistry.Files
}

// LoadCanonicalRegistry reads the listed protoset files and indexes
// every method by its full gRPC path ("/pkg.Svc/Method"). nil-safe:
// an empty paths slice returns an empty registry (canonicalization is
// then a no-op).
//
// All protoset files are aggregated into a single FileDescriptorSet
// before resolution so cross-file imports (e.g. cosmos-sdk's
// Coin message used by ibc-go) resolve correctly. Per-file indexSet
// calls would leave protodesc unable to find imports defined in a
// sibling file, AND the registry's `files` pointer would only hold
// the LAST processed set — method descriptors from earlier files
// would point into objects no longer reachable from the registry.
//
// Loading is best-effort: a malformed protoset returns an error so
// the operator notices at startup rather than silently degrading.
func LoadCanonicalRegistry(paths []string) (*CanonicalRegistry, error) {
	reg := &CanonicalRegistry{
		methods: map[string]protoreflect.MethodDescriptor{},
		files:   &protoregistry.Files{},
	}
	combined := &descriptorpb.FileDescriptorSet{}
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("protoset %s: %w", p, err)
		}
		var set descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(raw, &set); err != nil {
			return nil, fmt.Errorf("protoset %s: parse FileDescriptorSet: %w", p, err)
		}
		combined.File = append(combined.File, set.File...)
	}
	if len(combined.File) > 0 {
		if err := reg.indexSet(combined); err != nil {
			return nil, fmt.Errorf("protosets: %w", err)
		}
	}
	return reg, nil
}

func (r *CanonicalRegistry) indexSet(set *descriptorpb.FileDescriptorSet) error {
	// Build file descriptors in dependency order. protodesc.NewFiles
	// handles this when given the full set; it returns an error on
	// missing imports.
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return fmt.Errorf("build file descriptors: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := 0; j < methods.Len(); j++ {
				m := methods.Get(j)
				path := "/" + string(svc.FullName()) + "/" + string(m.Name())
				r.methods[path] = m
			}
		}
		return true
	})
	r.files = files
	return nil
}

// Method returns the MethodDescriptor for a gRPC method path, or nil
// if the registry doesn't know about it.
func (r *CanonicalRegistry) Method(path string) protoreflect.MethodDescriptor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.methods[path]
}

// Canonicalize decodes payload against the method's input descriptor
// and re-encodes it deterministically. When the registry is nil
// (operator set keyMode: canonical but never wired protosets), when
// the registry doesn't know about the method, or when decoding fails,
// the original payload is returned unchanged — canonicalization is
// best-effort, not a hard requirement.
//
// "Deterministic" here means proto.MarshalOptions{Deterministic:true}:
// map keys sorted, unknown fields preserved in their original order,
// no nondeterministic field reordering. Sufficient for cache-key
// stability across clients that emit the same logical message.
func (r *CanonicalRegistry) Canonicalize(methodPath string, payload []byte) []byte {
	if r == nil {
		return payload
	}
	desc := r.Method(methodPath)
	if desc == nil {
		return payload
	}
	msg := dynamicpb.NewMessage(desc.Input())
	if err := proto.Unmarshal(payload, msg); err != nil {
		return payload
	}
	out, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return payload
	}
	return out
}

// MethodCount returns how many gRPC methods the registry indexed.
// Exposed so the startup log can confirm protoset loading worked.
func (r *CanonicalRegistry) MethodCount() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.methods)
}

// Methods returns the sorted list of gRPC method paths in the
// registry. Used by the web UI / debug endpoints; not on the hot path.
func (r *CanonicalRegistry) Methods() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.methods))
	for p := range r.methods {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
