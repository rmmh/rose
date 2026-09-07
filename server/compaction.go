package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// CompactionPolicy tunes when dead space in a vlog is worth physically
// reclaiming. Compaction rewrites a vlog's live chunks into a fresh vlog and
// retires the old one, so it trades sequential IO now for reclaimed capacity
// later; these knobs keep that trade-off worthwhile.
type CompactionPolicy struct {
	// MinWasteRatio is the fraction of a vlog that must be dead before it is a
	// candidate, e.g. 0.20 compacts once at least a fifth is reclaimable.
	MinWasteRatio float64
	// MinDeadBytes is an absolute floor so tiny vlogs are not rewritten just to
	// reclaim a few kilobytes, regardless of their ratio.
	MinDeadBytes int64
	// MaxJobs caps how many candidates a single planning pass returns, bounding
	// the amount of concurrent rewrite work scheduled.
	MaxJobs int
}

// DefaultCompactionPolicy is a conservative starting point: reclaim a vlog once
// a quarter of it is dead and that quarter is at least a few megabytes.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{MinWasteRatio: 0.25, MinDeadBytes: 4 << 20, MaxJobs: 4}
}

// Candidates selects the vlogs worth compacting under the policy, most wasteful
// first, so the planner reclaims the largest holes before the marginal ones.
func (p CompactionPolicy) Candidates(usages []meta.VlogUsage) []meta.VlogUsage {
	var out []meta.VlogUsage
	for _, u := range usages {
		if u.DeadBytes() < p.MinDeadBytes {
			continue
		}
		if u.WasteRatio() < p.MinWasteRatio {
			continue
		}
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DeadBytes() != out[j].DeadBytes() {
			return out[i].DeadBytes() > out[j].DeadBytes()
		}
		return out[i].VlogID < out[j].VlogID
	})
	if p.MaxJobs > 0 && len(out) > p.MaxJobs {
		out = out[:p.MaxJobs]
	}
	return out
}

// Compact plans and runs compaction for every vlog the policy selects, most
// wasteful first. It is the scheduler entry point a background worker calls.
func (s *Server) Compact(ctx context.Context, policy CompactionPolicy) (int, error) {
	s.maintRunMu.Lock()
	defer s.maintRunMu.Unlock()
	return s.compactLocked(ctx, policy)
}

// compactLocked is the body of Compact for callers that already hold maintRunMu
// (the maintenance pass), so a pass does not deadlock on its own compaction step.
func (s *Server) compactLocked(ctx context.Context, policy CompactionPolicy) (int, error) {
	usages, err := s.db.VlogUsages(ctx)
	if err != nil {
		return 0, err
	}
	candidates := policy.Candidates(usages)
	for _, u := range candidates {
		if err := s.CompactVlog(ctx, u.VlogID); err != nil {
			return 0, err
		}
	}
	return len(candidates), nil
}

// CompactVlog rewrites a vlog's live chunks into a fresh vlog and retires the
// old one, physically reclaiming the dead space that row-level GC only marked
// free. It is crash-safe and resumable: chunk bytes are made durable in the
// destination before each chunk row is repointed, so an interruption leaves the
// chunk resolving to its old, intact location and the job is simply re-run.
func (s *Server) CompactVlog(ctx context.Context, sourceID uint32) error {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	busy, err := s.vlogRelocationDeferredLocked(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("compact: check lease for vlog %d: %w", sourceID, err)
	}
	if busy {
		// A write operation owns the append cursor and may have planned chunks
		// not yet published. Retiring it would invalidate that durable intent.
		return nil
	}

	source, ok := s.vlogs[sourceID]
	info, err := s.db.GetVlog(ctx, sourceID)
	if errors.Is(err, sql.ErrNoRows) && !ok {
		finished, finishErr := s.db.FinishRunningCompactionJob(ctx, sourceID)
		if finishErr != nil {
			return finishErr
		}
		if finished {
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("compact: load vlog %d: %w", sourceID, err)
	}
	if !ok {
		return fmt.Errorf("compact: source vlog %d not mounted", sourceID)
	}
	if info.ProtectionScheme == "EC" {
		// EC vlogs accept whole stripe rows only, so their live chunks cannot be
		// copied one at a time like a mirror's; they are repacked into rows on the
		// way out, the same coding promotion does.
		return s.compactECVlogLocked(ctx, sourceID, source, info)
	}
	job, err := s.db.GetOrCreateCompactionJob(ctx, sourceID)
	if err != nil {
		return err
	}

	// Establish the destination vlog, reusing a partially-built one on resume.
	destID := job.DestVlog
	var dest *storage.Vlog
	if destID == 0 {
		destID, dest, err = s.provisionCompactionDestinationLocked(ctx, info)
		if err != nil {
			return fmt.Errorf("compact: provision destination: %w", err)
		}
		if err := s.assignMaintenanceDestinationLocked(ctx, job.ID, destID); err != nil {
			return err
		}
	} else {
		dest, ok = s.vlogs[destID]
		if !ok {
			return fmt.Errorf("compact: destination vlog %d not mounted", destID)
		}
	}

	// The active vlog must not be retired out from under future writes.
	s.clearActiveVlogLocked(sourceID)

	live, err := s.db.LiveChunksInVlog(ctx, sourceID)
	if err != nil {
		return err
	}
	// Copy every live chunk into the destination first, then make the whole
	// batch durable with a single fsync before repointing any chunk row. This
	// keeps the crash-safety invariant -- bytes are durable before the metadata
	// references them -- while spending one fsync per job instead of one per
	// chunk. A crash before the commit leaves chunks resolving to their old,
	// intact source location and the job re-runs from scratch.
	relocations := make([]struct {
		hash   []byte
		offset int64
	}, 0, len(live))
	for _, c := range live {
		plain, err := s.readPlainChunkRecord(ctx, source, sourceID, c)
		if err != nil {
			return fmt.Errorf("compact: read chunk from vlog %d: %w", sourceID, err)
		}
		data, err := s.encryptPlainChunkRecord(ctx, destID, c.Hash, plain)
		if err != nil {
			return fmt.Errorf("compact: encrypt chunk for vlog %d: %w", destID, err)
		}
		offset, err := dest.Write(ctx, 0, data)
		if err != nil {
			return fmt.Errorf("compact: write chunk to vlog %d: %w", destID, err)
		}
		relocations = append(relocations, struct {
			hash   []byte
			offset int64
		}{c.Hash, offset})
	}
	if len(relocations) > 0 {
		durableLength, err := dest.CommitPrefix(ctx, 0)
		if err != nil {
			return fmt.Errorf("compact: commit destination: %w", err)
		}
		if err := s.db.SetVlogLength(ctx, destID, durableLength); err != nil {
			return err
		}
		for i, r := range relocations {
			if err := s.verifyProtectedChunk(ctx, dest, destID, meta.ChunkLoc{Hash: r.hash, VaddrOffset: r.offset, LogicalLen: live[i].LogicalLen}); err != nil {
				return fmt.Errorf("compact: verify destination: %w", err)
			}
			if err := s.db.RelocateChunk(ctx, r.hash, destID, r.offset); err != nil {
				return fmt.Errorf("compact: relocate chunk: %w", err)
			}
		}
	}

	return s.finishCompactionLocked(ctx, sourceID, job.ID)
}

// Compaction preserves the source's durable protection requirement and staging
// target. It must not reinterpret fewer currently active disks as a lower
// replication factor for already published content.
func (s *Server) provisionCompactionDestinationLocked(ctx context.Context, info meta.VlogInfo) (uint32, *storage.Vlog, error) {
	disks := s.placementDisksLocked(0)
	count := info.RequiredShards
	if count == 0 {
		count = len(disks)
		if info.ProtectionScheme == "NONE" {
			count = 1
		}
	}
	if count <= 0 || len(disks) < count {
		return 0, nil, fmt.Errorf("compact: %d active disks cannot meet required %d shards", len(disks), count)
	}
	id, v, err := s.provisionVlogCoreLocked(ctx, info.ProtectionScheme, int(info.DataShards), int(info.ParityShards), int(info.TargetDataShards), int(info.TargetParityShards), count, disks, info.DedupDomain, true)
	if err != nil {
		return 0, nil, err
	}
	if err := v.SetWriteQuorum(count); err != nil {
		return 0, nil, err
	}
	return id, v, nil
}

// compactECVlogLocked rewrites an EC vlog's live chunks into a fresh EC vlog and
// retires the old one. Unlike a mirror, an EC vlog stores whole stripe rows only,
// so its live chunks (which the dead holes between them have left scattered) are
// repacked into complete rows by the same coding promotion uses, padding the
// final partial row. It is crash-safe and resumable exactly like the mirror path:
// the coded rows are made durable in the destination before any chunk is
// repointed, so an interruption leaves every chunk resolving to its intact source
// location and the job re-runs (re-reading only the chunks still in the source).
// The caller must hold vlogMu.
func (s *Server) compactECVlogLocked(ctx context.Context, sourceID uint32, source *storage.Vlog, info meta.VlogInfo) error {
	live, err := s.db.LiveChunksInVlog(ctx, sourceID)
	if err != nil {
		return err
	}
	job, err := s.db.GetOrCreateCompactionJob(ctx, sourceID)
	if err != nil {
		return err
	}
	// The active vlog must not be retired out from under future writes.
	s.clearActiveVlogLocked(sourceID)

	if len(live) > 0 {
		destID := job.DestVlog
		var dest *storage.Vlog
		if destID == 0 {
			destID, dest, err = s.provisionVlogInDomainLocked(ctx, "EC", int(info.DataShards), int(info.ParityShards), info.DedupDomain, true)
			if err != nil {
				return fmt.Errorf("compact: provision destination EC vlog: %w", err)
			}
			if err := s.assignMaintenanceDestinationLocked(ctx, job.ID, destID); err != nil {
				return err
			}
		} else {
			var ok bool
			dest, ok = s.vlogs[destID]
			if !ok {
				return fmt.Errorf("compact: destination vlog %d not mounted", destID)
			}
		}
		padded := paddedRowLen(live, storage.ECStripeWidth(int(info.DataShards)))
		if err := s.writeChunksAsRows(ctx, job.ID, source, dest, destID, live, padded); err != nil {
			return fmt.Errorf("compact: %w", err)
		}
	}

	return s.finishCompactionLocked(ctx, sourceID, job.ID)
}

// finishCompactionLocked retires the drained source vlog and marks the job done,
// unless an in-flight write operation still pins a chunk residing in the source.
// A pinned chunk may have gone dead (refcount 0) after it was deduplicated
// against, so it was not relocated out with the live set; retiring the source
// would free its bytes out from under the uncommitted operation, leaving the
// op's published version pointing at a reclaimed hole. In that case the job is
// left running -- its live chunks are already relocated -- so a later pass retires
// the source once the pin clears (after the op commits or is abandoned). The
// caller must hold vlogMu.
//
// No newly-pinned chunk can name the source after this check: dedup only pins
// live chunks (LiveChunkByHash), and every chunk live when compaction read the
// source has already been relocated off it, so a fresh pin resolves to the
// destination rather than the source.
func (s *Server) finishCompactionLocked(ctx context.Context, sourceID uint32, jobID int64) error {
	if err := s.retireVlogLocked(ctx, sourceID); err != nil {
		if errors.Is(err, errVlogPinned) {
			return nil
		}
		return err
	}
	return s.db.MarkJobDone(ctx, jobID)
}

var errVlogPinned = errors.New("vlog contains pinned chunks")

// retireVlogLocked drops a fully-drained vlog from the catalog and unmounts and
// deletes its backing plog files, the shared tail of compaction, EC compaction,
// and staging retirement. RetireVlog refuses to drop a vlog that still has live
// chunks, so a caller that has not relocated everything fails loudly rather than
// losing data. The caller must hold vlogMu.
func (s *Server) retireVlogLocked(ctx context.Context, vlogID uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Pins are roots even when persistent reference counts are zero. Enforce
	// that at the shared deletion boundary, including empty staging retirement,
	// and keep pin admission fenced through the catalog transaction.
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	var hashes [][]byte
	for hash := range s.pinnedHashesLocked() {
		hashes = append(hashes, []byte(hash))
	}
	if len(hashes) > 0 {
		held, err := s.db.VlogHoldsAnyHash(ctx, vlogID, hashes)
		if err != nil {
			return err
		}
		if held {
			return fmt.Errorf("retire vlog %d: %w", vlogID, errVlogPinned)
		}
	}
	// RetireVlog is the authoritative deletion. Once admitted, finish both the
	// catalog transaction and in-memory unmount even if the maintenance caller
	// goes away, so no stale mounted vlog can accept writes after its row is gone.
	durableCtx := context.WithoutCancel(ctx)
	plogs, err := s.db.RetireVlog(durableCtx, vlogID)
	if err != nil {
		return err
	}
	delete(s.vlogs, vlogID)
	s.deleteVlogKey(vlogID)
	for _, p := range plogs {
		if plog, ok := s.plogs[p.ID]; ok {
			_ = plog.Close()
			delete(s.plogs, p.ID)
		}
		_ = storage.RemovePlogFiles(s.plogPath(p.DiskID, p.ID))
	}
	s.clearActiveVlogLocked(vlogID)
	return nil
}
