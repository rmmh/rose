package storage

import "crypto/sha256"

// ContentHash is the content address within a persisted deduplication domain.
// Empty domains support bare storage callers; file vlogs use a fixed 32-byte
// bucket/policy domain. The same function validates recovered plaintext.
func ContentHash(domain, payload []byte) [32]byte {
	if len(domain) == 0 {
		return sha256.Sum256(payload)
	}
	h := sha256.New()
	h.Write([]byte("rose scoped chunk v1\x00"))
	h.Write(domain)
	h.Write(payload)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
