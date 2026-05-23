package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// Sha256 returns the hex-encoded SHA-256 of the input. Retained for any
// callers needing a strong cryptographic hash. Cache-key hashing moved to
// XXHash64Hex — 5-10× faster, and the cache layer doesn't need
// cryptographic strength.
func Sha256(content string) string {
	h := sha256.New()
	h.Write([]byte(content))
	return hex.EncodeToString(h.Sum(nil))
}

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
