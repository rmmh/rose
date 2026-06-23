package server

import (
	"bytes"
	"testing"
)

func TestWriteCacheTailAppendReusesSpanBuffer(t *testing.T) {
	c := &writeCache{
		spans:  []span{{start: 0, data: append(make([]byte, 0, 16), []byte("abcd")...)}},
		length: 4,
	}
	before := &c.spans[0].data[0]

	c.writeLocked(4, []byte("efgh"))

	if len(c.spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(c.spans))
	}
	if got := c.spans[0].data; !bytes.Equal(got, []byte("abcdefgh")) {
		t.Fatalf("span data = %q, want %q", got, []byte("abcdefgh"))
	}
	if after := &c.spans[0].data[0]; after != before {
		t.Fatal("tail append replaced the span backing buffer")
	}
}

func TestWriteCacheTailOverwriteThenExtendReusesSpanBuffer(t *testing.T) {
	c := &writeCache{
		spans:  []span{{start: 0, data: append(make([]byte, 0, 16), []byte("abcdef")...)}},
		length: 6,
	}
	before := &c.spans[0].data[0]

	c.writeLocked(4, []byte("WXYZ"))

	if len(c.spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(c.spans))
	}
	if got := c.spans[0].data; !bytes.Equal(got, []byte("abcdWXYZ")) {
		t.Fatalf("span data = %q, want %q", got, []byte("abcdWXYZ"))
	}
	if after := &c.spans[0].data[0]; after != before {
		t.Fatal("tail extension replaced the span backing buffer")
	}
}

func TestWriteCacheAdjacentNextSpanStillMerges(t *testing.T) {
	c := &writeCache{
		spans: []span{
			{start: 0, data: []byte("aaaa")},
			{start: 8, data: []byte("bbbb")},
		},
		length: 12,
	}

	c.writeLocked(4, []byte("cccc"))

	if len(c.spans) != 1 {
		t.Fatalf("span count = %d, want 1", len(c.spans))
	}
	if c.spans[0].start != 0 {
		t.Fatalf("span start = %d, want 0", c.spans[0].start)
	}
	if got := c.spans[0].data; !bytes.Equal(got, []byte("aaaaccccbbbb")) {
		t.Fatalf("span data = %q, want %q", got, []byte("aaaaccccbbbb"))
	}
}
