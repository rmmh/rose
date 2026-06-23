package server

import (
	"context"
	"sort"
	"sync"

	"github.com/rmmh/rose/meta"
)

// spillThreshold is how large the contiguous dirty prefix above settledLen may
// grow before a write spills it to durable chunks, bounding the per-handle cache
// memory for a large sequential append (e.g. cp of a big file).
const spillThreshold = 4 << 20 // 4 MiB

// spillCarry is the tail of the dirty prefix a spill leaves in memory. Keeping at
// least one max-size FastCDC chunk's worth of bytes unspilled means the next
// appended bytes join a coherent region, so content-defined boundaries land in
// the same place they would for a single pass -- spills do not perturb dedup.
const spillCarry = 256 << 10 // 256 KiB (> FastCDC max chunk size)

// span is a run of pending (un-spilled) dirty bytes at a logical offset. The
// cache keeps spans sorted by start and non-overlapping; a write coalesces into
// them last-writer-wins.
type span struct {
	start int64
	data  []byte
}

func (s span) end() int64 { return s.start + int64(len(s.data)) }

// chunkReader reads a logical byte range out of an ordered placement list (the
// committed bytes the cache overlays). It is the server's readChunksAt bound to
// the cache so ReadAt can fall through to base/settled content.
type chunkReader func(ctx context.Context, chunks []meta.ChunkPlacement, off, length int64) ([]byte, error)

// writeCache holds one write handle's pending modifications over the file version
// open at Open time. It coalesces the kernel's split, out-of-order, overlapping
// writes into a coherent overlay, serves read-your-writes from that overlay, and
// at Close produces the spliced placement list (re-chunking only modified
// windows). See server/api.go for the flush/splice driver.
//
// Content priority for any logical offset, highest first: a covering span, then
// the settled prefix [0,settledLen) (durable chunks spilled during writes), then
// the base version [settledLen,baseLen), then a zero hole up to length, then EOF.
type writeCache struct {
	mu sync.Mutex

	// base is the committed version open at Open time (empty for a new path);
	// baseLen is its logical size. Reused verbatim where untouched.
	base    []meta.ChunkPlacement
	baseLen int64

	// settled is the durable chunk prefix produced by spills, covering exactly
	// [0,settledLen). It shadows base over that range.
	settled    []meta.ChunkPlacement
	settledLen int64

	// length is the logical file size: max of baseLen, the highest written
	// offset, and any explicit truncate.
	length int64

	// spans is the sorted, non-overlapping un-spilled dirty overlay.
	spans []span

	read chunkReader
}

func newWriteCache(base []meta.ChunkPlacement, read chunkReader) *writeCache {
	var baseLen int64
	for _, p := range base {
		baseLen += int64(p.LogicalLen)
	}
	return &writeCache{
		base:    base,
		baseLen: baseLen,
		length:  baseLen,
		read:    read,
	}
}

// WriteAt records a write at any offset, in any order, possibly overlapping
// earlier writes (last-writer-wins). It only mutates the in-memory overlay; the
// caller serializes writes per handle and triggers spilling separately.
func (c *writeCache) WriteAt(off int64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeLocked(off, append([]byte(nil), data...))
}

func (c *writeCache) writeLocked(off int64, data []byte) {
	newStart := off
	newEnd := off + int64(len(data))

	// Fast path: when the write stays within or extends the tail of a single
	// existing span, mutate that span in place and let append grow its backing
	// array geometrically. This keeps sequential appends O(n) instead of
	// rebuilding the full dirty prefix on every 128 KiB write.
	for i := range c.spans {
		sp := &c.spans[i]
		if sp.end() < newStart {
			continue
		}
		if sp.start > newStart {
			break
		}
		nextStart := int64(1<<63 - 1)
		if i+1 < len(c.spans) {
			nextStart = c.spans[i+1].start
		}
		if nextStart <= newEnd {
			break
		}
		if newEnd <= sp.end() {
			copy(sp.data[newStart-sp.start:], data)
		} else {
			overlap := sp.end() - newStart
			if overlap > 0 {
				copy(sp.data[newStart-sp.start:], data[:overlap])
				data = data[overlap:]
			}
			sp.data = append(sp.data, data...)
		}
		if newEnd > c.length {
			c.length = newEnd
		}
		return
	}

	mergeStart, mergeEnd := newStart, newEnd
	var keep, overlap []span
	for _, sp := range c.spans {
		// Coalesce on overlap or adjacency so sequential writes form one prefix.
		if sp.end() < newStart || sp.start > newEnd {
			keep = append(keep, sp)
			continue
		}
		overlap = append(overlap, sp)
		if sp.start < mergeStart {
			mergeStart = sp.start
		}
		if sp.end() > mergeEnd {
			mergeEnd = sp.end()
		}
	}
	buf := make([]byte, mergeEnd-mergeStart)
	for _, sp := range overlap { // existing bytes first (lower priority)
		copy(buf[sp.start-mergeStart:], sp.data)
	}
	copy(buf[newStart-mergeStart:], data) // new bytes win
	keep = append(keep, span{start: mergeStart, data: buf})
	sort.Slice(keep, func(i, j int) bool { return keep[i].start < keep[j].start })
	c.spans = keep
	if newEnd > c.length {
		c.length = newEnd
	}
}

// Length returns the current logical file size.
func (c *writeCache) Length() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.length
}

// ReadAt assembles the logical range [off, off+length) from the overlay. Bytes
// past the current length read as EOF (the returned slice is short); holes
// within length read as zero.
func (c *writeCache) ReadAt(ctx context.Context, off, length int64) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readLocked(ctx, off, length)
}

func (c *writeCache) readLocked(ctx context.Context, off, length int64) ([]byte, error) {
	end := off + length
	if end > c.length {
		end = c.length
	}
	if end <= off {
		return nil, nil
	}
	out := make([]byte, end-off) // zero-filled: holes within length read as zero

	// Settled prefix [0, settledLen).
	if c.settledLen > 0 {
		lo, hi := off, min64(end, c.settledLen)
		if hi > lo {
			data, err := c.read(ctx, c.settled, lo, hi-lo)
			if err != nil {
				return nil, err
			}
			copy(out[lo-off:], data)
		}
	}
	// Base region [settledLen, baseLen).
	bstart, bend := max64(off, c.settledLen), min64(end, c.baseLen)
	if bend > bstart {
		data, err := c.read(ctx, c.base, bstart, bend-bstart)
		if err != nil {
			return nil, err
		}
		copy(out[bstart-off:], data)
	}
	// Dirty spans (highest priority).
	for _, sp := range c.spans {
		lo, hi := max64(off, sp.start), min64(end, sp.end())
		if hi > lo {
			copy(out[lo-off:], sp.data[lo-sp.start:hi-sp.start])
		}
	}
	return out, nil
}

// Truncate sets the logical size to n: dropping/clipping overlay and settled data
// beyond n when shrinking, or extending with a zero hole when growing.
func (c *writeCache) Truncate(n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.length = n
	var keep []span
	for _, sp := range c.spans {
		if sp.start >= n {
			continue
		}
		if sp.end() > n {
			sp = span{start: sp.start, data: sp.data[:n-sp.start]}
		}
		keep = append(keep, sp)
	}
	c.spans = keep
	if c.baseLen > n {
		c.baseLen = n
	}
	if c.settledLen > n {
		// Trim the settled prefix to whole chunks within [0,n). A truncate that
		// cuts mid-settled-chunk (only reachable after a spill) leaves the
		// remainder as a zero hole, which the first cut does not preserve.
		var ns int64
		var ks []meta.ChunkPlacement
		for _, p := range c.settled {
			if ns+int64(p.LogicalLen) > n {
				break
			}
			ns += int64(p.LogicalLen)
			ks = append(ks, p)
		}
		c.settled = ks
		c.settledLen = ns
	}
}

// spillPrefix returns the bytes of the contiguous dirty prefix above settledLen
// that should be spilled now, or nil if the prefix is under the threshold. The
// returned bytes are removed from the overlay and settledLen is conceptually
// advanced by the caller once they are durable; see commitSpill.
func (c *writeCache) spillPrefix() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.spans) == 0 {
		return nil
	}
	sp := c.spans[0]
	if sp.start != c.settledLen {
		return nil // a base gap sits between settledLen and the first dirty bytes
	}
	if int64(len(sp.data)) <= spillThreshold {
		return nil
	}
	n := int64(len(sp.data)) - spillCarry
	return append([]byte(nil), sp.data[:n]...)
}

// commitSpill records placements covering [settledLen, settledLen+n) as the new
// settled tail and drops those n bytes from the leading span. The caller must
// have made the placements' bytes durable; n must equal the total logical length
// of placements and match the prefix returned by spillPrefix.
func (c *writeCache) commitSpill(placements []meta.ChunkPlacement, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.settled = append(c.settled, placements...)
	c.settledLen += n
	sp := c.spans[0]
	rest := append([]byte(nil), sp.data[n:]...)
	c.spans[0] = span{start: sp.start + n, data: rest}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
