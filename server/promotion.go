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
		// Nothing yet fills a whole row. A staging vlog with no live chunks at all
		// (everything was promoted or deleted) is dead replicated space: retire it
		// now rather than waiting for the dead-byte floor to make it a compaction
		// candidate. A sub-row remainder stays replicated to coalesce with later
		// writes. Either way, retire any orphaned job (a crash mid-promotion may
		// have reparented enough chunks to drop staging below a row) so it is not
		// re-resumed forever.
		if len(live) == 0 {
			if err := s.retireVlogLocked(ctx, stagingID); err != nil {
				return false, err
			}
		}
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

	// Reading each chunk back from the replicated staging vlog heals bitrot in
	// passing (the read verifies sector hashes against a surviving copy).
	if err := s.writeChunksAsRows(ctx, job.ID, source, dest, destID, live[:count], padded); err != nil {
		return false, fmt.Errorf("promote: %w", err)
	}
	if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
		return false, err
	}

	// If the whole-row prefix drained every live chunk, the staging vlog is now an
	// empty replica: retire it so it does not linger holding only dead bytes.
	if count == len(live) {
		if err := s.retireVlogLocked(ctx, stagingID); err != nil {
			return false, err
		}
	}
	return true, nil
}

// writeChunksAsRows reads the given live chunks from source in order,
// concatenates and pads them to a whole number of EC stripe rows, appends those
// rows to the EC dest under txnID, makes them durable, and reparents each chunk
// at its new location. It is the shared coding step of promotion (staging ->
// EC) and EC compaction (EC -> EC): the coded rows are durable before any chunk
// is repointed, so a crash leaves every chunk resolving to its old, intact
// location and the job re-runs. padded must be the chunks' total byte length
// rounded up to a stripe-row boundary. The caller must hold vlogMu.
func (s *Server) writeChunksAsRows(ctx context.Context, txnID int64, source, dest *storage.Vlog, destID uint32, chunks []meta.ChunkLoc, padded int64) error {
	buf := make([]byte, 0, padded)
	offsets := make([]int64, len(chunks))
	for i, c := range chunks {
		offsets[i] = int64(len(buf))
		data, err := source.Read(ctx, c.VaddrOffset, c.LogicalLen)
		if err != nil {
			return fmt.Errorf("read chunk from vlog %d: %w", source.ID(), err)
		}
		buf = append(buf, data...)
	}
	// Pad the final row to a stripe-row boundary so the EC vlog only ever stores
	// whole rows. The padding is dead space addressed by no chunk; it is bounded
	// by one stripe row per call and reclaimed when the EC vlog is compacted.
	buf = append(buf, make([]byte, padded-int64(len(buf)))...)

	base, err := dest.Write(ctx, txnID, buf)
	if err != nil {
		return fmt.Errorf("write rows to EC vlog %d: %w", destID, err)
	}
	if err := dest.Commit(ctx, txnID); err != nil {
		return fmt.Errorf("commit EC vlog %d: %w", destID, err)
	}
	if err := s.db.SetVlogLength(ctx, destID, dest.Length()); err != nil {
		return err
	}
	for i, c := range chunks {
		if err := s.db.RelocateChunk(ctx, c.Hash, destID, base+offsets[i]); err != nil {
			return fmt.Errorf("reparent chunk: %w", err)
		}
	}
	return nil
}

// paddedRowLen returns the byte length the given chunks occupy once packed
// contiguously and the final partial row padded up to a stripe-row boundary of
// width sw. It is the all-chunks counterpart to promotablePrefix, used by EC
// compaction, which must drain every live chunk rather than a whole-row prefix.
func paddedRowLen(chunks []meta.ChunkLoc, sw int64) int64 {
	var total int64
	for _, c := range chunks {
		total += int64(c.LogicalLen)
	}
	if total == 0 || sw <= 0 {
		return 0
	}
	return (total + sw - 1) / sw * sw
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
