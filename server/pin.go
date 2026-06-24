package server

import (
	"context"

	"github.com/rmmh/rose/meta"
)

// pinAndResolveChunk records that write operation opID is reusing the chunk with
// the given content hash (deduplication) and returns its current placement. The
// pin is published before the chunk is resolved, and reclamation consults the pin
// set under pinMu, so once this returns ok the chunk cannot be collected or have
// its bytes reclaimed before the operation commits or is abandoned.
//
// It returns ok=false when no live chunk has the hash, signalling the caller to
// store the bytes fresh instead of deduplicating. Only live (refcount > 0) chunks
// are reused: a dead chunk may already be mid-reclamation, and pinning it could
// race compaction freeing its bytes.
func (s *Server) pinAndResolveChunk(ctx context.Context, opID int64, hash []byte) (meta.ChunkPlacement, bool, error) {
	key := string(hash)
	s.pinMu.Lock()
	set := s.pinnedChunks[opID]
	if set == nil {
		set = make(map[string]struct{})
		s.pinnedChunks[opID] = set
	}
	_, already := set[key]
	set[key] = struct{}{}
	s.pinMu.Unlock()

	// Resolve only after the pin is visible: any reclamation that runs from here on
	// observes the pin and spares the chunk, so a non-empty result is safe to reuse
	// for the rest of the operation. A miss means a concurrent GC collected the
	// chunk before we pinned it (or its last reference is gone); drop the pin we
	// just took -- unless an earlier spill in this same op already held it -- and
	// let the caller store the bytes fresh.
	p, ok, err := s.db.LiveChunkByHash(ctx, hash)
	if (err != nil || !ok) && !already {
		s.pinMu.Lock()
		if set := s.pinnedChunks[opID]; set != nil {
			delete(set, key)
			if len(set) == 0 {
				delete(s.pinnedChunks, opID)
			}
		}
		s.pinMu.Unlock()
	}
	if err != nil {
		return meta.ChunkPlacement{}, false, err
	}
	return p, ok, nil
}

// releasePins drops every chunk pin held by a write operation, called once its
// fate is durable: after commit (its file version now carries the real refcounts)
// or after abandonment (it references nothing). Reclamation may then treat those
// chunks normally. It is safe to call for an op that pinned nothing.
func (s *Server) releasePins(opID int64) {
	s.pinMu.Lock()
	delete(s.pinnedChunks, opID)
	s.pinMu.Unlock()
}

// pinnedHashesLocked returns the union of every in-flight operation's pinned
// chunk hashes, keyed by the raw hash bytes. Reclamation snapshots it while
// holding pinMu and excludes these hashes from collection. The caller must hold
// pinMu. It returns nil when nothing is pinned, which the reclamation paths treat
// as their fast path.
func (s *Server) pinnedHashesLocked() map[string]struct{} {
	if len(s.pinnedChunks) == 0 {
		return nil
	}
	out := make(map[string]struct{})
	for _, set := range s.pinnedChunks {
		for h := range set {
			out[h] = struct{}{}
		}
	}
	return out
}

// pinnedHashList returns the currently pinned hashes as a slice of raw hash
// bytes, snapshotted under pinMu. Compaction uses it to test whether a vlog it is
// about to retire still holds a pinned chunk.
func (s *Server) pinnedHashList() [][]byte {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	if len(s.pinnedChunks) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	var out [][]byte
	for _, set := range s.pinnedChunks {
		for h := range set {
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			out = append(out, []byte(h))
		}
	}
	return out
}
