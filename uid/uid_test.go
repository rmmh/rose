package uid

import "testing"

func TestNewDistinct(t *testing.T) {
	seen := make(map[UID]bool)
	for i := 0; i < 1000; i++ {
		u := New()
		if u.IsZero() {
			t.Fatal("New returned the zero UID")
		}
		if seen[u] {
			t.Fatalf("New returned a duplicate UID: %s", u)
		}
		seen[u] = true
	}
}

func TestStringLength(t *testing.T) {
	u := New()
	s := u.String()
	if len(s) != EncodedLen {
		t.Fatalf("String() length = %d, want %d", len(s), EncodedLen)
	}
	for _, c := range s {
		if strFind(crockford, byte(c)) < 0 {
			t.Fatalf("String() produced non-Crockford char %q in %s", c, s)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	for i := 0; i < 1000; i++ {
		u := New()
		got, err := Parse(u.String())
		if err != nil {
			t.Fatalf("Parse(%s): %v", u.String(), err)
		}
		if got != u {
			t.Fatalf("round trip mismatch: %x != %x", got, u)
		}
	}
}

func TestParseSubstitutions(t *testing.T) {
	u := New()
	s := u.String()
	// Lower-casing and the I/L→1, O→0 substitutions must decode identically.
	variants := []string{
		toLower(s),
		replaceAll(s, '0', 'O'),
		replaceAll(s, '1', 'I'),
		replaceAll(toLower(s), '1', 'l'),
	}
	for _, v := range variants {
		got, err := Parse(v)
		if err != nil {
			t.Fatalf("Parse(%q): %v", v, err)
		}
		if got != u {
			t.Fatalf("Parse(%q) = %x, want %x", v, got, u)
		}
	}
}

func TestParseRejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"tooshort",
		"0000000000000000000000000",  // 25 chars
		"00000000000000000000000",    // 23 chars
		"00000000000000000000000U",   // U is not in the alphabet
		"!0000000000000000000000",    // wrong length and bad char
	}
	for _, c := range cases {
		if _, err := Parse(c); err == nil {
			t.Fatalf("Parse(%q) succeeded, want error", c)
		}
	}
}

// small helpers to avoid importing strings in the test for trivial ops
func strFind(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func toLower(s string) string {
	out := []byte(s)
	for i, c := range out {
		if c >= 'A' && c <= 'Z' {
			out[i] = c + ('a' - 'A')
		}
	}
	return string(out)
}

func replaceAll(s string, old, new byte) string {
	out := []byte(s)
	for i, c := range out {
		if c == old {
			out[i] = new
		}
	}
	return string(out)
}
