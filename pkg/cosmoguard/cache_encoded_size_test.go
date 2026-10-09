package cosmoguard

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/voluzi/cosmoguard/v6/pkg/cache"
	"google.golang.org/grpc/metadata"
)

func TestResponseEncodedSizeBoundsMessagePack(t *testing.T) {
	headers := make(map[string]string)
	md := metadata.MD{}
	for i := range 1000 {
		key := fmt.Sprintf("x-%d", i)
		headers[key] = ""
		md[key] = []string{"", "", ""}
	}
	for _, value := range []cache.CacheEncodedSizer{
		CachedResponse{},
		CachedResponse{Data: bytes.Repeat([]byte("x"), 2900), StatusCode: 200, Headers: map[string]string{"Content-Type": "application/json"}, StoredAt: time.Now(), UpstreamAge: 99999},
		CachedResponse{Data: bytes.Repeat([]byte("x"), 900<<10), StatusCode: 299, Headers: headers, StoredAt: time.Now()},
		grpcCachedResponse{},
		grpcCachedResponse{Payload: bytes.Repeat([]byte("x"), 900<<10), StoredAt: time.Now(), Header: md, Trailer: metadata.MD{"non-ascii": {strings.Repeat("λ", 1000)}}},
	} {
		encoded, err := cache.EncodeValue(value)
		require.NoError(t, err)
		require.LessOrEqual(t, uint64(len(encoded)), value.CacheEncodedSize(), "%T encoded bound must cover all fields", value)
	}
}
