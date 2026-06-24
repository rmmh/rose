// Package uid provides a compact 120-bit identifier rendered as Crockford
// base32. 120 bits is a multiple of 5, so the textual form is exactly 24
// characters with no padding, and Crockford's alphabet drops the visually
// ambiguous letters (I, L, O, U) for readability in logs, marker files, and on
// the command line. UIDs identify clusters, disks, vlogs, and plogs; on disk and
// on the wire a UID is the raw 15 bytes, never the text form.
package uid

import (
	"crypto/rand"
	"fmt"
	"strings"
)

// Size is the length of a UID in bytes (120 bits).
const Size = 15

// EncodedLen is the length of a UID's Crockford base32 text form.
const EncodedLen = 24

// crockford is the Crockford base32 alphabet: digits then uppercase letters
// excluding I, L, O, and U.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// UID is a 120-bit identifier.
type UID [Size]byte

// decode maps an (upper-cased, substituted) Crockford symbol to its 5-bit value,
// or -1 if the byte is not a valid symbol.
var decode [256]int8

func init() {
	for i := range decode {
		decode[i] = -1
	}
	for v, c := range crockford {
		decode[c] = int8(v)
		if c >= 'A' && c <= 'Z' {
			decode[c+('a'-'A')] = int8(v) // lower-case form (letters only)
		}
	}
	// Crockford treats these as the digits they resemble.
	decode['O'], decode['o'] = 0, 0
	decode['I'], decode['i'] = 1, 1
	decode['L'], decode['l'] = 1, 1
}

// New returns a fresh random UID.
func New() UID {
	var u UID
	if _, err := rand.Read(u[:]); err != nil {
		// crypto/rand.Read never returns an error on supported platforms; a
		// failure here means the OS RNG is unavailable and the process cannot
		// safely continue minting identifiers.
		panic(fmt.Sprintf("uid: crypto/rand failed: %v", err))
	}
	return u
}

// IsZero reports whether u is the all-zero UID (the unset value).
func (u UID) IsZero() bool {
	return u == UID{}
}

// String renders u as 24 Crockford base32 characters.
func (u UID) String() string {
	var out [EncodedLen]byte
	// Encode 120 bits as 24 groups of 5, most-significant first.
	var acc uint16 // holds up to 12 buffered bits
	bits := 0
	bi := 0
	for i := 0; i < EncodedLen; i++ {
		for bits < 5 {
			acc = acc<<8 | uint16(u[bi])
			bi++
			bits += 8
		}
		bits -= 5
		out[i] = crockford[(acc>>uint(bits))&0x1f]
	}
	return string(out[:])
}

// Parse decodes a Crockford base32 UID. It is case-insensitive and accepts the
// I/L→1 and O→0 substitutions; hyphens are ignored.
func Parse(s string) (UID, error) {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != EncodedLen {
		return UID{}, fmt.Errorf("uid: invalid length %d, want %d", len(s), EncodedLen)
	}
	var u UID
	var acc uint16
	bits := 0
	bi := 0
	for i := 0; i < EncodedLen; i++ {
		v := decode[s[i]]
		if v < 0 {
			return UID{}, fmt.Errorf("uid: invalid character %q", s[i])
		}
		acc = acc<<5 | uint16(v)
		bits += 5
		if bits >= 8 {
			bits -= 8
			u[bi] = byte(acc >> uint(bits))
			bi++
		}
	}
	return u, nil
}
