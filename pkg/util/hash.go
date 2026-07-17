package util

import (
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// XXHash64Hex returns a fixed-width 16-char hex string of
// xxhash64(content). Used as the HTTP cache key — much cheaper than
// sha256+hex on the hot path, and the 64-bit space is more than wide
// enough for cosmoguard's per-rule cache namespace (rule fingerprint
// is mixed in by the caller).
//
// %016x guarantees zero-padding so every key is exactly 16 chars; the
// previous strconv.FormatUint shape returned 1-16 chars depending on
// the leading-zero count of the hash, which would have produced
// shorter keys for low-entropy inputs and broken any consumer relying
// on fixed-width slicing or lexicographic comparison.
func XXHash64Hex(content string) string {
	return fmt.Sprintf("%016x", xxhash.Sum64String(content))
}
