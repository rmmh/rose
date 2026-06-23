package server

import (
	"context"
	"fmt"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// PromoteStaging promotes every replicated staging vlog that has accumulated at
// least one full EC stripe row of whole chunks into an EC vlog, reparenting the
// promoted chunks and leaving the sub-row remainder replicated in staging. It is
// the maintenance-pass counterpart to compaction: the deferred half of EC, where
// chunks that were written under replication for durability are repacked into the
// space-efficient coded layout once enough have accumulated to fill whole rows.
func (s *Server) PromoteStaging(ctx context.Context) (int, error) {
	vlogs, err := s.db.ListVlogs(ctx)
	if err != nil {
		return 0, err
	}
	promoted := 0
	for _, info := range vlogs {
		if !info.IsStaging() {
			continue
		}
		did, err := s.PromoteStagingVlog(ctx, info.ID)
		if err != nil {
			return promoted, err
		}
		if did {
			promoted++
		}
	}
	return promoted, nil
}

// PromoteStagingVlog packs the whole live chunks of one staging vlog into
// complete EC stripe rows, appends those rows to an EC vlog, and reparents the
// promoted chunks at their new location. The sub-row remainder (the chunks that
// do not complete a row) stays in the replicated staging vlog so it remains
// protected until later writes let it complete a row. It reports whether it
// promoted anything.
//
// Crash safety mirrors compaction: the coded rows are made durable in the EC
// vlog before any chunk row is repointed, so an interruption leaves every chunk
// resolving to its intact replicated copy in staging and the job re-runs.
func (s *Server) PromoteStagingVlog(ctx context.Context, stagingID uint32) (bool, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	leased, err := s.db.VlogLeased(ctx, stagingID)
	if err != nil {
		return false, fmt.Errorf("promote: check lease for vlog %d: %w", stagingID, err)
	}
	if leased {
		// A write operation owns the append cursor and may have planned chunks not
		// yet published; promoting underneath it would race that durable intent.
		return false, nil
	}

	info, err := s.db.GetVlog(ctx, stagingID)
	if err != nil {
		return false, fmt.Errorf("promote: load vlog %d: %w", stagingID, err)
	}
	if !info.IsStaging() {
		return false, nil
	}
	source, ok := s.vlogs[stagingID]
	if !ok {
		return false, fmt.Errorf("promote: staging vlog %d not mounted", stagingID)
	}

	live, err := s.db.LiveChunksInVlog(ctx, stagingID)
	if err != nil {
		return false, err
	}
	sw := storage.ECStripeWidth(int(info.TargetDataShards))
	count, padded := promotablePrefix(live, sw)
	if count == 0 {
		// Nothing yet fills a whole row. If a crash mid-promotion already reparented
		// enough chunks to drop staging below a row, retire the orphaned job so it is
		// not re-resumed forever.
		if _, err := s.db.FinishRunningPromoteJob(ctx, stagingID); err != nil {
			return false, err
		}
		return false, nil
	}

	job, err := s.db.GetOrCreatePromoteJob(ctx, stagingID)
	if err != nil {
		return false, err
	}

	// Establish the destination EC vlog, reusing a partially-built one on resume.
	destID := job.DestVlog
	var dest *storage.Vlog
	if destID == 0 {
		destID, dest, err = s.provisionVlogLocked(ctx, "EC", int(info.TargetDataShards), int(info.TargetParityShards))
		if err != nil {
			return false, fmt.Errorf("promote: provision destination EC vlog: %w", err)
		}
		if err := s.db.SetJobDest(ctx, job.ID, destID); err != nil {
			return false, err
		}
	} else {
		dest, ok = s.vlogs[destID]
		if !ok {
			return false, fmt.Errorf("promote: destination vlog %d not mounted", destID)
		}
	}

	// Concatenate the promoted chunks into a single padded run of whole stripe
	// rows. Reading each chunk back from the replicated staging vlog heals bitrot
	// in passing (the read verifies sector hashes against a surviving copy).
	buf := make([]byte, 0, padded)
	chunkOffsets := make([]int64, count)
	for i := 0; i < count; i++ {
		chunkOffsets[i] = int64(len(buf))
		data, err := source.Read(ctx, live[i].VaddrOffset, live[i].LogicalLen)
		if err != nil {
			return false, fmt.Errorf("promote: read chunk from staging vlog %d: %w", stagingID, err)
		}
		buf = append(buf, data...)
	}
	// Pad the final row to a stripe-row boundary so the EC vlog only ever stores
	// whole rows. The padding is dead space addressed by no chunk; it is bounded
	// by one stripe row per promotion and reclaimed when the EC vlog is compacted.
	buf = append(buf, make([]byte, padded-int64(len(buf)))...)

	base, err := dest.Write(ctx, job.ID, buf)
	if err != nil {
		return false, fmt.Errorf("promote: write rows to EC vlog %d: %w", destID, err)
	}
	if err := dest.Commit(ctx, job.ID); err != nil {
		return false, fmt.Errorf("promote: commit EC vlog %d: %w", destID, err)
	}
	if err := s.db.SetVlogLength(ctx, destID, dest.Length()); err != nil {
		return false, err
	}
	for i := 0; i < count; i++ {
		if err := s.db.RelocateChunk(ctx, live[i].Hash, destID, base+chunkOffsets[i]); err != nil {
			return false, fmt.Errorf("promote: reparent chunk: %w", err)
		}
	}

	return true, s.db.MarkJobDone(ctx, job.ID)
}

// promotablePrefix selects how many of the leading whole chunks to promote into
// complete EC stripe rows of width sw, and the padded byte length those chunks
// occupy. Chunks are content-addressed and unsplittable, so a promoted prefix
// almost never sums to an exact multiple of sw; the final row is padded up to a
// boundary. To keep that padding small the cut is chosen at the prefix whose
// cumulative size lands closest below a row boundary (least padding), preferring
// to drain more chunks on ties. It returns (0, 0) when no prefix yet fills a
// whole row, leaving everything replicated in staging.
func promotablePrefix(live []meta.ChunkLoc, sw int64) (count int, padded int64) {
	if sw <= 0 {
		return 0, 0
	}
	var cum int64
	bestPad := int64(-1)
	for i, c := range live {
		cum += int64(c.LogicalLen)
		if cum < sw {
			continue
		}
		pad := (sw - cum%sw) % sw
		// Prefer least padding; on ties take the larger prefix (this i), draining
		// more of staging per promotion.
		if bestPad == -1 || pad <= bestPad {
			bestPad = pad
			count = i + 1
			padded = cum + pad
		}
	}
	if bestPad == -1 {
		return 0, 0
	}
	return count, padded
}
