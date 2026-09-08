package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/rmmh/rose/durability"
	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// rehomePublicationChunks applies the destination bucket's current policy to
// the entire new version, including untouched base chunks. namespaceMu keeps
// the path and policy stable. Old snapshots keep their original content domains.
func (s *Server) rehomePublicationChunks(ctx context.Context, h *FileHandle, planned []meta.ChunkPlacement) ([]meta.ChunkPlacement, error) {
	s.vlogMu.Lock()
	policy := s.bucketPolicyLocked(bucketOf(h.path()))
	s.vlogMu.Unlock()
	domain := policy.DedupDomain()
	var out []meta.ChunkPlacement
	var offset int64
	var previous uint64
	for i, p := range planned {
		info, err := s.db.GetVlog(ctx, p.VlogID)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(info.DedupDomain, domain) {
			out = append(out, p)
		} else {
			data, err := s.readChunksAt(ctx, []meta.ChunkPlacement{p}, 0, int64(p.LogicalLen))
			if err != nil {
				return nil, err
			}
			chunks, err := s.storeChunks(ctx, h, data, offset, previous, len(out), i == len(planned)-1)
			if err != nil {
				return nil, err
			}
			out = append(out, chunks...)
		}
		offset += int64(p.LogicalLen)
		if len(out) != 0 {
			previous = binary.LittleEndian.Uint64(out[len(out)-1].Hash[:8])
		}
	}
	return out, nil
}

// publishPreparedVersion binds the complete final extent list to durable
// canonical placements. The caller owns namespaceMu and the write-operation
// lock. Topology/relocation and GC are fenced through the metadata transaction,
// so a checked location cannot be failed, repointed or collected before publish.
func (s *Server) publishPreparedVersion(ctx context.Context, opID int64, path string, mtime int64, planned []meta.ChunkPlacement, retainResult bool) (int64, []meta.ChunkPlacement, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.pinMu.Lock()
	defer s.pinMu.Unlock()

	publication := &preparedPublication{s: s, opID: opID, path: path, mtime: mtime, planned: planned, retainResult: retainResult}
	coordinator := durability.Coordinator{Publication: publication, Hook: func(point durability.Point) error {
		return s.publicationCheckpoint(string(point))
	}}
	err := coordinator.Commit(ctx)
	return publication.fileID, publication.placements, err
}

// preparedPublication adapts the actual file publication effects. The enclosing
// call owns namespaceMu, the operation lock, vlogMu and pinMu for its lifetime.
type preparedPublication struct {
	s            *Server
	opID         int64
	path         string
	mtime        int64
	planned      []meta.ChunkPlacement
	retainResult bool
	placements   []meta.ChunkPlacement
	fileID       int64
}

func (p *preparedPublication) Admit(ctx context.Context) ([]uint32, error) {
	if err := p.s.publishedTopologyReadyLocked(ctx); err != nil {
		return nil, err
	}
	return p.s.db.WriteOpLeases(ctx, p.opID)
}

func (p *preparedPublication) Sync(ctx context.Context, id uint32) (int64, error) {
	v := p.s.vlogs[id]
	if v == nil {
		return 0, fmt.Errorf("leased vlog %d is not mounted", id)
	}
	return v.CommitPrefix(ctx, p.opID)
}

func (p *preparedPublication) RecordPrefix(ctx context.Context, id uint32, prefix int64) error {
	return p.s.db.SetVlogLength(ctx, id, prefix)
}

func (publication *preparedPublication) Verify(ctx context.Context) error {
	s, path, planned := publication.s, publication.path, publication.planned

	placements := make([]meta.ChunkPlacement, len(planned))
	canonical := make(map[string]meta.ChunkPlacement)
	domain := s.bucketPolicyLocked(bucketOf(path)).DedupDomain()
	for i, p := range planned {
		key := string(p.Hash)
		if previous, ok := canonical[key]; ok {
			placements[i] = previous
			continue
		}
		// A different writer may have won publication after this operation
		// planned fresh bytes. Validate the row the SQL upsert will retain.
		fresh, found, err := s.db.LiveChunkByHash(ctx, p.Hash)
		if err != nil {
			return err
		}
		if found {
			p = fresh
		}
		info, err := s.db.GetVlog(ctx, p.VlogID)
		if err != nil {
			return err
		}
		if !bytes.Equal(info.DedupDomain, domain) {
			return fmt.Errorf("chunk %x belongs to a different bucket or protection policy", p.Hash)
		}
		if p.LogicalLen < 0 || p.VaddrOffset < 0 ||
			p.VaddrOffset > info.Length ||
			int64(p.LogicalLen)+storage.ChunkHeaderSize > info.Length-p.VaddrOffset {
			return fmt.Errorf("chunk %x extends beyond durable vlog %d prefix", p.Hash, p.VlogID)
		}
		if err := s.requiredPlacementReadyLocked(ctx, info); err != nil {
			return err
		}
		ready, err := s.commitReadyLocked(ctx, p.VlogID)
		if err != nil {
			return err
		}
		v := s.vlogs[p.VlogID]
		if !ready || v == nil {
			return fmt.Errorf("chunk %x is not protected for publication", p.Hash)
		}
		if err := s.verifyProtectedChunk(ctx, v, p.VlogID, meta.ChunkLoc{Hash: p.Hash, VaddrOffset: p.VaddrOffset, LogicalLen: p.LogicalLen}); err != nil {
			return fmt.Errorf("verify published chunk %x: %w", p.Hash, err)
		}
		canonical[key] = p
		placements[i] = p
	}
	publication.placements = placements
	return nil
}

func (p *preparedPublication) Publish(ctx context.Context) error {
	var err error
	if p.retainResult {
		p.fileID, err = p.s.db.CommitWriteOpVersionWithRetention(ctx, p.opID, p.path, p.mtime, p.placements, p.s.now().Add(p.s.retryRetention).UnixNano())
	} else {
		p.fileID, err = p.s.db.CommitWriteOpVersion(ctx, p.opID, p.path, p.mtime, p.placements)
	}
	return err
}

func (s *Server) publicationCheckpoint(stage string) error {
	if s.publicationFault != nil {
		return s.publicationFault(stage)
	}
	return nil
}

// requiredPlacementReadyLocked compares surviving mappings with the immutable
// desired count, rather than treating a smaller mapping set as a smaller promise.
// Disk-level placement is the current policy; separate node-loss policy remains
// a distinct requirement and must not be inferred from disk uniqueness.
func (s *Server) requiredPlacementReadyLocked(ctx context.Context, info meta.VlogInfo) error {
	if info.RequiredShards <= 0 {
		return fmt.Errorf("vlog %d has no persisted protection requirement", info.ID)
	}
	shards, err := s.db.VlogShardDisks(ctx, info.ID)
	if err != nil {
		return err
	}
	if len(shards) != info.RequiredShards {
		return fmt.Errorf("vlog %d has %d mappings, requires %d", info.ID, len(shards), info.RequiredShards)
	}
	disks := make(map[uint32]bool, len(shards))
	for _, sh := range shards {
		if disks[sh.DiskID] || s.offlinePlogs[sh.PlogID] || !s.diskReachableLocked(sh.DiskID) {
			return fmt.Errorf("vlog %d required shard %d is unavailable or shares a disk", info.ID, sh.PlogID)
		}
		disks[sh.DiskID] = true
	}
	return nil
}

// publishedTopologyReadyLocked applies the strict global publication gate to
// known topology degradation of referenced file data, including snapshots.
// Unreferenced garbage is not a reason to freeze the namespace. Integrity failures
// discovered by verification need the separate durable health/repair tracking
// described in the correctness plan.
func (s *Server) publishedTopologyReadyLocked(ctx context.Context) error {
	vlogs, err := s.db.ListVlogs(ctx)
	if err != nil {
		return err
	}
	for _, info := range vlogs {
		if len(info.DedupDomain) == 0 {
			continue
		}
		live, err := s.db.LiveChunksInVlog(ctx, info.ID)
		if err != nil {
			return err
		}
		if len(live) == 0 {
			continue
		}
		if err := s.requiredPlacementReadyLocked(ctx, info); err != nil {
			return fmt.Errorf("publication blocked by degraded referenced data: %w", err)
		}
	}
	return nil
}
