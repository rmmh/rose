package server

import (
	"context"
	"fmt"
	"os"
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
	leased, err := s.db.VlogLeased(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("compact: check lease for vlog %d: %w", sourceID, err)
	}
	if leased {
		// A write operation owns the append cursor and may have planned chunks
		// not yet published. Retiring it would invalidate that durable intent.
		return nil
	}

	source, ok := s.vlogs[sourceID]
	if !ok {
		return fmt.Errorf("compact: source vlog %d not mounted", sourceID)
	}
	info, err := s.db.GetVlog(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("compact: load vlog %d: %w", sourceID, err)
	}
	job, err := s.db.GetOrCreateCompactionJob(ctx, sourceID)
	if err != nil {
		return err
	}

	// Establish the destination vlog, reusing a partially-built one on resume.
	destID := job.DestVlog
	var dest *storage.Vlog
	if destID == 0 {
		destID, dest, err = s.provisionVlogLocked(ctx, info.ProtectionScheme, int(info.DataShards), int(info.ParityShards))
		if err != nil {
			return fmt.Errorf("compact: provision destination: %w", err)
		}
		if err := s.db.SetJobDest(ctx, job.ID, destID); err != nil {
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
		data, err := source.Read(ctx, c.VaddrOffset, c.LogicalLen)
		if err != nil {
			return fmt.Errorf("compact: read chunk from vlog %d: %w", sourceID, err)
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
		if err := dest.Commit(ctx, 0); err != nil {
			return fmt.Errorf("compact: commit destination: %w", err)
		}
		if err := s.db.SetVlogLength(ctx, destID, dest.Length()); err != nil {
			return err
		}
		for _, r := range relocations {
			if err := s.db.RelocateChunk(ctx, r.hash, destID, r.offset); err != nil {
				return fmt.Errorf("compact: relocate chunk: %w", err)
			}
		}
	}

	plogs, err := s.db.RetireVlog(ctx, sourceID)
	if err != nil {
		return err
	}
	delete(s.vlogs, sourceID)
	for _, p := range plogs {
		if plog, ok := s.plogs[p.ID]; ok {
			_ = plog.Close()
			delete(s.plogs, p.ID)
		}
		_ = os.Remove(s.plogPath(p.DiskID, p.ID))
	}
	return s.db.MarkJobDone(ctx, job.ID)
}
