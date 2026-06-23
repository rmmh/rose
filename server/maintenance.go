package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// RebalancePolicy bounds how aggressively rebalance evens disk-space usage across
// active disks. Balance is measured in bytes, not shard count: shards vary widely
// in size, so an even shard count can still leave very uneven disks. The point is
// deliberately not to reach perfect balance: shards are expensive to move, so some
// skew is tolerated and passes are rate limited.
type RebalancePolicy struct {
	// MinSkewBytes is the hysteresis band. A pass is a no-op unless the busiest
	// active disk holds at least MinSkewBytes more bytes than the idlest, and it
	// stops moving once the spread falls back within the band. This is what keeps
	// the system from thrashing shards to flatten a small byte difference.
	MinSkewBytes int64
	// MaxMovesPerPass caps how many shards a single pass relocates, bounding the
	// IO one rebalance triggers. Zero means unbounded within a pass.
	MaxMovesPerPass int
	// Cooldown is the minimum wall-clock time between passes that actually moved
	// something; a pass started inside the window is skipped. This is the backoff
	// that stops a balanced-enough cluster from rebalancing on every tick.
	Cooldown time.Duration
}

// DefaultRebalancePolicy tolerates a 10 GiB spread (roughly an order of magnitude
// over a single shard, so a handful of large shards never trips it), moves at
// most eight shards per pass, and waits five minutes between passes.
func DefaultRebalancePolicy() RebalancePolicy {
	return RebalancePolicy{MinSkewBytes: 10 << 30, MaxMovesPerPass: 8, Cooldown: 5 * time.Minute}
}

// SetRebalancePolicy replaces the rebalance tuning. It is the operator knob for
// the hysteresis band, per-pass move cap, and backoff window.
func (s *Server) SetRebalancePolicy(p RebalancePolicy) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.rebalance = p
}

// SetCompactionPolicy replaces the dead-space reclamation tuning the maintenance
// driver applies each pass. It is the operator knob for the waste ratio, dead-byte
// floor, and per-pass job cap.
func (s *Server) SetCompactionPolicy(p CompactionPolicy) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.compaction = p
}

// compactionPolicy returns a snapshot of the current compaction tuning.
func (s *Server) compactionPolicy() CompactionPolicy {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	return s.compaction
}

// SweepStrayPlogFiles removes plog files left on disk that the catalog no longer
// references on that disk, reclaiming the dead space a crash between a metadata
// flip and the source file's os.Remove leaks. The relocation primitives all
// commit the catalog repoint (RetireVlog, ReplaceShardPlog, migratePlog's flip)
// before deleting the old file, so an interruption strands a durable but
// unreferenced copy: dead space, not dead metadata. It is deliberately
// conservative -- a file is removed only when no plog row with its id lives on
// that disk -- so it never deletes a file the catalog still resolves to, and it
// skips unreachable disks (failed disk / failed node) whose media must not be
// touched. It runs under vlogMu so the catalog and on-disk views are consistent
// against concurrent provisioning and relocation.
func (s *Server) SweepStrayPlogFiles(ctx context.Context) (int, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	return s.sweepStrayPlogFilesLocked(ctx)
}

func (s *Server) sweepStrayPlogFilesLocked(ctx context.Context) (int, error) {
	plogs, err := s.db.ListPlogs(ctx)
	if err != nil {
		return 0, err
	}
	known := make(map[[2]uint32]bool, len(plogs))
	for _, p := range plogs {
		known[[2]uint32{p.DiskID, p.ID}] = true
	}
	removed := 0
	for diskID, root := range s.diskRoots {
		if !s.diskReachableLocked(diskID) {
			continue
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue // disk root not created yet; nothing to sweep
			}
			return removed, fmt.Errorf("sweep disk %d root: %w", diskID, err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			idStr, ok := strings.CutPrefix(e.Name(), "plog-")
			if !ok {
				continue
			}
			id, err := strconv.ParseUint(idStr, 10, 32)
			if err != nil {
				continue // not a plog file we manage
			}
			if known[[2]uint32{diskID, uint32(id)}] {
				continue
			}
			path := filepath.Join(root, e.Name())
			if err := os.Remove(path); err != nil {
				return removed, fmt.Errorf("sweep stray plog %s: %w", path, err)
			}
			slog.Info("removed stray plog file", "disk", diskID, "plog", id, "path", path)
			removed++
		}
	}
	return removed, nil
}

// DrainDisk evacuates every shard off a disk and detaches it, implementing the
// RoseStorage remove flow (StartRemove -> DrainStep* -> FinishJob). It runs the
// whole job under a durable `job` row so a crash mid-drain resumes from the
// shards still on the disk rather than restarting or stranding the disk
// half-drained. The disk is moved to draining immediately (so placement stops
// targeting it) and to detached only once it holds nothing, honoring the spec's
// NoDetachedData invariant.
//
// Each shard is relocated with the same copy-then-repoint discipline as
// compaction: the plog file is copied to the destination disk and made durable,
// then the placement metadata is atomically flipped, then the source file is
// removed. A crash before the flip leaves the old copy authoritative; a crash
// after it leaves only a stray file on a disk that is being removed anyway.
func (s *Server) DrainDisk(ctx context.Context, diskID uint32) error {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	if _, ok := s.diskRoots[diskID]; !ok {
		return fmt.Errorf("drain: disk %d is not configured", diskID)
	}
	switch s.diskState[diskID] {
	case meta.DiskActive, meta.DiskDraining: // startable, or resuming a started drain
	default:
		return fmt.Errorf("drain: disk %d is %s, cannot drain", diskID, s.diskState[diskID])
	}

	job, err := s.db.GetOrCreateDrainJob(ctx, diskID)
	if err != nil {
		return err
	}
	if s.diskState[diskID] != meta.DiskDraining {
		if err := s.setDiskStateLocked(ctx, diskID, meta.DiskDraining); err != nil {
			return err
		}
	}

	plogs, err := s.db.PlogsOnDisk(ctx, diskID)
	if err != nil {
		return err
	}
	for _, p := range plogs {
		dest, err := s.pickDrainDestinationLocked(ctx, p.VlogID, diskID)
		if err != nil {
			return err
		}
		if err := s.migratePlogLocked(ctx, p.PlogID, p.VlogID, diskID, dest); err != nil {
			return err
		}
	}

	if err := s.setDiskStateLocked(ctx, diskID, meta.DiskDetached); err != nil {
		return err
	}
	return s.db.MarkJobDone(ctx, job.ID)
}

// ReprotectDisk regenerates every shard lost with a failed disk onto healthy
// disks, restoring the redundancy the failure took away. It implements the
// RoseStorage reprotect flow (StartReprotect -> ReprotectStep* -> FinishJob)
// under a durable `job` row keyed by the failed disk, so a crash mid-reprotect
// resumes from the shards still mapped to the failed disk's plogs.
//
// Unlike drain, the failed disk's bytes are gone: each shard is rebuilt from the
// surviving redundancy (a sibling copy for DUPLICATE, a reed-solomon reconstruct
// for EC) rather than copied off the disk. The regenerated bytes are made durable
// in a fresh plog on a placement-allowed disk before the shard mapping is
// atomically flipped (ReplaceShardPlog), so a crash before the flip leaves the
// shard referencing the failed disk and the step re-runs. The disk stays failed
// afterward: reprotect restores durability, it does not repair hardware.
func (s *Server) ReprotectDisk(ctx context.Context, diskID uint32) error {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	if _, ok := s.diskRoots[diskID]; !ok {
		return fmt.Errorf("reprotect: disk %d is not configured", diskID)
	}
	if s.diskState[diskID] != meta.DiskFailed && s.diskState[diskID] != meta.DiskDraining {
		return fmt.Errorf("reprotect: disk %d is %s, only failed or draining disks are reprotected", diskID, s.diskState[diskID])
	}

	job, err := s.db.GetOrCreateReprotectJob(ctx, diskID)
	if err != nil {
		return err
	}

	plogs, err := s.db.PlogsOnDisk(ctx, diskID)
	if err != nil {
		return err
	}
	for _, p := range plogs {
		dest, err := s.pickDrainDestinationLocked(ctx, p.VlogID, diskID)
		if err != nil {
			return err
		}
		if err := s.regenerateShardLocked(ctx, p.VlogID, p.ShardIndex, p.PlogID, dest); err != nil {
			return err
		}
	}

	return s.db.MarkJobDone(ctx, job.ID)
}

// regenerateShardLocked rebuilds one shard lost on a failed disk and repoints the
// vlog at the regenerated plog. The lost shard's bytes are reconstructed from the
// surviving redundancy, written durably to a fresh plog on toDisk, then the shard
// mapping is atomically flipped and the owning vlog re-mounted. The caller must
// hold vlogMu.
func (s *Server) regenerateShardLocked(ctx context.Context, vlogID uint32, shardIdx int, lostPlogID, toDisk uint32) error {
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return err
	}

	var shardData []byte
	switch info.ProtectionScheme {
	case "DUPLICATE":
		shardData, err = s.readSurvivingCopyLocked(vlogID, lostPlogID)
	case "EC":
		shardData, err = s.reconstructECShardLocked(ctx, info, shardIdx, lostPlogID)
	default:
		return fmt.Errorf("reprotect: vlog %d scheme %s has no redundancy to regenerate from (data lost)", vlogID, info.ProtectionScheme)
	}
	if err != nil {
		return err
	}

	// Make the regenerated bytes durable in a fresh plog before flipping the
	// shard mapping, so a crash before the flip leaves the old (lost) mapping and
	// the step re-runs rather than exposing a half-written shard.
	newPlogID, err := s.db.MakePlog(ctx, toDisk)
	if err != nil {
		return err
	}
	np, err := storage.OpenPlog(s.plogPath(toDisk, newPlogID), newPlogID)
	if err != nil {
		return fmt.Errorf("reprotect: open regenerated plog %d on disk %d: %w", newPlogID, toDisk, err)
	}
	if _, err := np.Write(0, shardData); err != nil {
		return fmt.Errorf("reprotect: write regenerated shard to plog %d: %w", newPlogID, err)
	}
	if err := np.Commit(); err != nil {
		return fmt.Errorf("reprotect: commit regenerated plog %d: %w", newPlogID, err)
	}
	s.plogs[newPlogID] = np

	if err := s.db.ReplaceShardPlog(ctx, vlogID, shardIdx, lostPlogID, newPlogID); err != nil {
		return err
	}
	delete(s.plogs, lostPlogID)
	delete(s.offlinePlogs, lostPlogID)

	s.clearActiveVlogLocked(vlogID)
	return s.remountVlogLocked(ctx, vlogID)
}

// readSurvivingCopyLocked returns the full logical bytes of any surviving mirror
// of a DUPLICATE vlog other than the lost copy, healing bitrot in passing (the
// read verifies sector hashes). The caller must hold vlogMu.
func (s *Server) readSurvivingCopyLocked(vlogID, lostPlogID uint32) ([]byte, error) {
	mappings, err := s.db.ListVlogPlogs(context.Background(), vlogID)
	if err != nil {
		return nil, err
	}
	for _, m := range mappings {
		if m.PlogID == lostPlogID {
			continue
		}
		p, ok := s.plogs[m.PlogID]
		if !ok {
			continue
		}
		return p.Read(0, int(p.LogicalLength()))
	}
	return nil, fmt.Errorf("reprotect: vlog %d has no surviving copy to regenerate from (data lost)", vlogID)
}

// reconstructECShardLocked rebuilds one EC shard's full byte stream from the
// surviving data and parity shards. Every shard plog holds an equal-length
// stream (each write contributes equal-size pieces to every shard), so a single
// reconstruct over the full length recovers the lost shard element-wise. The
// caller must hold vlogMu.
func (s *Server) reconstructECShardLocked(ctx context.Context, info meta.VlogInfo, lostShard int, lostPlogID uint32) ([]byte, error) {
	mappings, err := s.db.ListVlogPlogs(ctx, info.ID)
	if err != nil {
		return nil, err
	}
	total := int(info.DataShards + info.ParityShards)
	shards := make([][]byte, total)
	present := 0
	for _, m := range mappings {
		if m.PlogID == lostPlogID || m.ShardIndex == lostShard {
			continue // the shard we are regenerating: leave nil
		}
		p, ok := s.plogs[m.PlogID]
		if !ok {
			continue // another shard also lost: leave nil for reconstruct
		}
		data, err := p.Read(0, int(p.LogicalLength()))
		if err != nil {
			return nil, fmt.Errorf("reprotect: read surviving shard %d of vlog %d: %w", m.ShardIndex, info.ID, err)
		}
		shards[m.ShardIndex] = data
		present++
	}
	if present < int(info.DataShards) {
		return nil, fmt.Errorf("reprotect: vlog %d has %d surviving shards, need %d to reconstruct (data lost)", info.ID, present, info.DataShards)
	}
	if err := storage.ReconstructECShard(int(info.DataShards), int(info.ParityShards), shards); err != nil {
		return nil, fmt.Errorf("reprotect: reconstruct vlog %d shard %d: %w", info.ID, lostShard, err)
	}
	return shards[lostShard], nil
}

// ScrubRepairResult summarizes one ScrubAndRepair pass: how many shards were
// scrubbed, how many corrupt shards were rebuilt from surviving redundancy, and
// any that could not be repaired because the vlog lacked a readable source
// quorum (too much other damage). A non-empty Unrepairable means real data loss,
// not a transient failure.
type ScrubRepairResult struct {
	ShardsScrubbed int
	ShardsRepaired int
	Unrepairable   []RepairFailure
}

// RepairFailure records a corrupt shard that ScrubAndRepair could not heal.
type RepairFailure struct {
	VlogID uint32
	Shard  int
	Reason string
}

// ScrubAndRepair scrubs every mounted vlog and rebuilds any shard whose bytes
// fail integrity from the surviving redundancy — the self-healing pass that
// closes the loop on the bitrot Scrub() only detects. It is the Go reflection of
// RoseTxnCommit's RepairShard: a corrupt shard is repaired only when a *readable
// source quorum* survives (the vlog's other shards still meet its read
// threshold), and the rebuilt bytes are placed on a PlacementAllowed disk via
// the same regenerate path reprotect uses (RoseStorage ReprotectStep). Each
// repaired vlog runs under a durable scrubrepair job so a crash mid-repair
// re-scrubs and re-repairs on recovery rather than leaving a shard silently bad.
func (s *Server) ScrubAndRepair(ctx context.Context) (ScrubRepairResult, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	ids := make([]uint32, 0, len(s.vlogs))
	for id := range s.vlogs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var res ScrubRepairResult
	for _, id := range ids {
		scrubbed, repaired, failures, err := s.repairOneVlogLocked(ctx, id)
		if err != nil {
			return res, err
		}
		res.ShardsScrubbed += scrubbed
		res.ShardsRepaired += repaired
		res.Unrepairable = append(res.Unrepairable, failures...)
	}
	return res, nil
}

// RepairVlog scrubs and repairs a single vlog, the unit ScrubAndRepair runs over
// every vlog and the entry point a resumed scrubrepair job re-enters after a
// crash.
func (s *Server) RepairVlog(ctx context.Context, vlogID uint32) (ScrubRepairResult, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	scrubbed, repaired, failures, err := s.repairOneVlogLocked(ctx, vlogID)
	if err != nil {
		return ScrubRepairResult{}, err
	}
	return ScrubRepairResult{ShardsScrubbed: scrubbed, ShardsRepaired: repaired, Unrepairable: failures}, nil
}

// repairOneVlogLocked scrubs one vlog and rebuilds its corrupt shards under a
// durable scrubrepair job. A clean vlog touches no job (the common case stays
// cheap); the job row only exists while there is real repair to make crash-safe.
// The caller must hold vlogMu.
func (s *Server) repairOneVlogLocked(ctx context.Context, vlogID uint32) (scrubbed, repaired int, failures []RepairFailure, err error) {
	vlog, ok := s.vlogs[vlogID]
	if !ok {
		return 0, 0, nil, fmt.Errorf("repair: vlog %d is not mounted", vlogID)
	}
	shards, err := vlog.Scrub()
	if err != nil {
		return 0, 0, nil, err
	}
	var corrupt []int
	for _, sh := range shards {
		scrubbed++
		if !sh.Result.Healthy() {
			corrupt = append(corrupt, sh.Shard)
		}
	}
	// Fold in shards stubbed offline at recovery (their file went missing on an
	// otherwise-active disk). Scrub skips them — they are an availability, not an
	// integrity, condition — so a deep ScrubAndRepair pass would not see them
	// without this. They share the regenerate path with corrupt shards.
	offline, err := s.offlineShardsLocked(ctx, vlogID)
	if err != nil {
		return scrubbed, 0, nil, err
	}
	corrupt = mergeShards(corrupt, offline)
	if len(corrupt) == 0 {
		return scrubbed, 0, nil, nil
	}

	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return scrubbed, 0, nil, err
	}
	job, err := s.db.GetOrCreateScrubRepairJob(ctx, vlogID)
	if err != nil {
		return scrubbed, 0, nil, err
	}
	repaired, failures, err = s.repairVlogShardsLocked(ctx, info, corrupt)
	if err != nil {
		return scrubbed, repaired, failures, err
	}
	if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
		return scrubbed, repaired, failures, err
	}
	return scrubbed, repaired, failures, nil
}

// repairVlogShardsLocked rebuilds the given corrupt shards of one vlog from its
// surviving redundancy. It first enforces RepairShard's precondition: the shards
// other than the corrupt ones must still meet the vlog's read threshold (a
// readable source quorum) — otherwise the bytes are genuinely lost and every
// corrupt shard is reported unrepairable rather than fabricated. Each repairable
// shard is regenerated onto a placement-allowed disk and its old (corrupt) plog
// file removed. The caller must hold vlogMu.
func (s *Server) repairVlogShardsLocked(ctx context.Context, info meta.VlogInfo, corrupt []int) (repaired int, failures []RepairFailure, err error) {
	shardDisks, err := s.db.VlogShardDisks(ctx, info.ID)
	if err != nil {
		return 0, nil, err
	}
	mappings, err := s.db.ListVlogPlogs(ctx, info.ID)
	if err != nil {
		return 0, nil, err
	}
	diskOf := make(map[int]uint32, len(shardDisks))
	for _, sd := range shardDisks {
		diskOf[sd.ShardIndex] = sd.DiskID
	}
	plogOf := make(map[int]uint32, len(mappings))
	for _, m := range mappings {
		plogOf[m.ShardIndex] = m.PlogID
	}
	corruptSet := make(map[int]bool, len(corrupt))
	for _, c := range corrupt {
		corruptSet[c] = true
	}

	// Readable source quorum: count the shards that are both on a live disk and
	// not themselves corrupt. RepairShard only restores a missing shard while
	// these still meet the read threshold (EC: data_shards survivors to
	// reconstruct; DUPLICATE: one intact copy).
	goodLive := 0
	for _, sd := range shardDisks {
		if corruptSet[sd.ShardIndex] {
			continue
		}
		if s.diskLiveLocked(sd.DiskID) {
			goodLive++
		}
	}
	if goodLive < s.readThreshold(info) {
		for _, c := range corrupt {
			failures = append(failures, RepairFailure{VlogID: info.ID, Shard: c, Reason: "no readable source quorum"})
		}
		return 0, failures, nil
	}

	for _, c := range corrupt {
		corruptDisk := diskOf[c]
		corruptPlog := plogOf[c]
		dest, derr := s.pickRepairDestinationLocked(ctx, info.ID, corruptDisk)
		if derr != nil {
			failures = append(failures, RepairFailure{VlogID: info.ID, Shard: c, Reason: derr.Error()})
			continue
		}
		// Capture the corrupt plog handle before regenerate unmaps it, so the
		// stale file can be closed and deleted once the shard is repointed.
		oldPlog := s.plogs[corruptPlog]
		if rerr := s.regenerateShardLocked(ctx, info.ID, c, corruptPlog, dest); rerr != nil {
			failures = append(failures, RepairFailure{VlogID: info.ID, Shard: c, Reason: rerr.Error()})
			continue
		}
		if oldPlog != nil {
			_ = oldPlog.Close()
		}
		_ = os.Remove(s.plogPath(corruptDisk, corruptPlog))
		repaired++
	}
	return repaired, failures, nil
}

// RepairOfflineShards regenerates every shard stubbed offline at recovery — its
// backing plog file gone on an otherwise-active disk — onto a placement-allowed
// disk, restoring full redundancy without scrubbing live bytes. It is the
// maintenance-pass counterpart to ScrubAndRepair for the missing-file case: a
// still-active disk that silently dropped one shard is not condemned (its other
// shards keep serving), but the lost shard no longer sits unprotected forever
// with no path back to full redundancy. The work is catalog-driven (no full
// read), so it is cheap enough to run every interval, and a no-op when nothing
// is offline. Each repaired vlog runs under the same durable scrubrepair job as
// ScrubAndRepair so a crash mid-repair resumes.
func (s *Server) RepairOfflineShards(ctx context.Context) (ScrubRepairResult, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	var res ScrubRepairResult
	if len(s.offlinePlogs) == 0 {
		return res, nil
	}
	ids := make([]uint32, 0, len(s.vlogs))
	for id := range s.vlogs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		repaired, failures, err := s.repairOfflineShardsOneVlogLocked(ctx, id)
		if err != nil {
			return res, err
		}
		res.ShardsRepaired += repaired
		res.Unrepairable = append(res.Unrepairable, failures...)
	}
	return res, nil
}

// repairOfflineShardsOneVlogLocked regenerates one vlog's offline shards from its
// surviving redundancy, without the byte scrub ScrubAndRepair does. A vlog with
// no offline shard touches no job (the common case stays cheap). The caller must
// hold vlogMu.
func (s *Server) repairOfflineShardsOneVlogLocked(ctx context.Context, vlogID uint32) (repaired int, failures []RepairFailure, err error) {
	offline, err := s.offlineShardsLocked(ctx, vlogID)
	if err != nil || len(offline) == 0 {
		return 0, nil, err
	}
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return 0, nil, err
	}
	job, err := s.db.GetOrCreateScrubRepairJob(ctx, vlogID)
	if err != nil {
		return 0, nil, err
	}
	repaired, failures, err = s.repairVlogShardsLocked(ctx, info, offline)
	if err != nil {
		return repaired, failures, err
	}
	if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
		return repaired, failures, err
	}
	return repaired, failures, nil
}

// offlineShardsLocked returns the shard indices of vlogID whose backing plog was
// stubbed offline at recovery, read from the catalog's shard→plog mappings. The
// caller must hold vlogMu.
func (s *Server) offlineShardsLocked(ctx context.Context, vlogID uint32) ([]int, error) {
	if len(s.offlinePlogs) == 0 {
		return nil, nil
	}
	mappings, err := s.db.ListVlogPlogs(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	var offline []int
	for _, m := range mappings {
		if s.offlinePlogs[m.PlogID] {
			offline = append(offline, m.ShardIndex)
		}
	}
	return offline, nil
}

// mergeShards unions two shard-index lists, dropping duplicates, so a shard that
// is both scrub-corrupt and offline (it cannot be, but the union stays correct)
// is regenerated once. The result is sorted for deterministic repair order.
func mergeShards(a, b []int) []int {
	if len(b) == 0 {
		return a
	}
	seen := make(map[int]bool, len(a)+len(b))
	out := make([]int, 0, len(a)+len(b))
	for _, xs := range [][]int{a, b} {
		for _, x := range xs {
			if !seen[x] {
				seen[x] = true
				out = append(out, x)
			}
		}
	}
	sort.Ints(out)
	return out
}

// pickRepairDestinationLocked selects a placement-allowed disk to host a shard
// rebuilt by ScrubAndRepair. Unlike drain/reprotect, the corrupt shard's own
// disk is still live — only its bytes are bad — so the shard's current node is a
// legal home (it holds no other shard of the vlog). It prefers a different disk,
// so a still-suspect disk is not immediately reused, but falls back to the
// shard's own disk when that is the only placement-allowed home — the case for
// EC k+m exactly filling the cluster's nodes. The caller must hold vlogMu.
func (s *Server) pickRepairDestinationLocked(ctx context.Context, vlogID, corruptDisk uint32) (uint32, error) {
	occupied, err := s.occupiedNodesLocked(ctx, vlogID, corruptDisk)
	if err != nil {
		return 0, err
	}
	fallback, haveFallback := uint32(0), false
	for _, id := range s.activeDiskIDs() {
		if occupied[s.nodeOf(id)] {
			continue // another shard of this vlog lives on that node
		}
		if id == corruptDisk {
			fallback, haveFallback = id, true
			continue
		}
		return id, nil
	}
	if haveFallback {
		return fallback, nil
	}
	return 0, fmt.Errorf("repair: no placement-allowed destination for vlog %d shard", vlogID)
}

// AttachDisk configures a new local disk and registers it active in the durable
// catalog, making it eligible for placement and as a replace destination. It is
// the operator action that brings fresh capacity online. (AddDisk is reserved by
// the gRPC surface for the future RPC driver.)
func (s *Server) AttachDisk(ctx context.Context, diskID uint32, root string) error {
	return s.AttachDiskOnNode(ctx, diskID, diskID, root, 0)
}

// AttachDiskOnNode adds a previously absent disk to a specific node fault
// domain. The capacity is catalog metadata used by external schedulers; local
// placement currently measures actual plog bytes.
func (s *Server) AttachDiskOnNode(ctx context.Context, diskID, nodeID uint32, root string, totalBytes uint64) error {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if _, ok := s.diskRoots[diskID]; ok {
		return fmt.Errorf("attach disk: disk %d already configured", diskID)
	}
	disks, err := s.db.ListDisks(ctx)
	if err != nil {
		return err
	}
	for _, disk := range disks {
		if disk.ID == diskID {
			return fmt.Errorf("attach disk: disk %d already exists in catalog", diskID)
		}
	}
	if err := s.db.RegisterNode(ctx, nodeID); err != nil {
		return err
	}
	if err := s.db.RegisterDiskWithCapacity(ctx, diskID, nodeID, totalBytes); err != nil {
		return err
	}
	s.diskRoots[diskID] = root
	s.diskNodes[diskID] = nodeID
	s.diskState[diskID] = meta.DiskActive
	return nil
}

// ReplaceDisk evacuates every shard off oldDisk onto a freshly added newDisk and
// detaches oldDisk, implementing the RoseStorage replace flow (ReplaceDisk ->
// DrainStep* onto the pinned destination -> FinishJob). It is drain with the
// destination pinned: rather than scattering shards across whatever active disks
// have room, every shard lands on the one new disk, the swap-in-place an operator
// expects when retiring a disk for a replacement.
//
// It runs under a durable `job` row (kind=replace, target_disk=old,
// dest_disk=new) so a crash mid-replace resumes onto the same destination from
// the shards still on the old disk. Each shard is relocated with the same
// copy-then-repoint discipline as drain. (ReplaceDisk is reserved by the gRPC
// surface for the future RPC driver.)
func (s *Server) ReplaceDiskWith(ctx context.Context, oldDisk, newDisk uint32) error {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	if _, ok := s.diskRoots[oldDisk]; !ok {
		return fmt.Errorf("replace: disk %d is not configured", oldDisk)
	}
	if _, ok := s.diskRoots[newDisk]; !ok {
		return fmt.Errorf("replace: destination disk %d is not configured (add it first)", newDisk)
	}
	if oldDisk == newDisk {
		return fmt.Errorf("replace: source and destination are the same disk %d", oldDisk)
	}
	switch s.diskState[oldDisk] {
	case meta.DiskActive, meta.DiskDraining: // startable, or resuming a started replace
	default:
		return fmt.Errorf("replace: disk %d is %s, cannot replace", oldDisk, s.diskState[oldDisk])
	}
	if s.diskState[newDisk] != meta.DiskActive {
		return fmt.Errorf("replace: destination disk %d is %s, must be active", newDisk, s.diskState[newDisk])
	}

	job, err := s.db.GetOrCreateReplaceJob(ctx, oldDisk, newDisk)
	if err != nil {
		return err
	}
	newDisk = job.DestDisk // honor a resumed job's pinned destination over the argument
	if s.diskState[oldDisk] != meta.DiskDraining {
		if err := s.setDiskStateLocked(ctx, oldDisk, meta.DiskDraining); err != nil {
			return err
		}
	}

	plogs, err := s.db.PlogsOnDisk(ctx, oldDisk)
	if err != nil {
		return err
	}
	for _, p := range plogs {
		if err := s.ensurePlacementAllowedLocked(ctx, p.VlogID, newDisk, oldDisk); err != nil {
			return err
		}
		if err := s.migratePlogLocked(ctx, p.PlogID, p.VlogID, oldDisk, newDisk); err != nil {
			return err
		}
	}

	if err := s.setDiskStateLocked(ctx, oldDisk, meta.DiskDetached); err != nil {
		return err
	}
	return s.db.MarkJobDone(ctx, job.ID)
}

// occupiedNodesLocked returns the set of node fault domains already holding a
// shard of vlogID, excluding any shard on excludeDisk (the shard being
// relocated, which is leaving that node). A destination on an occupied node would
// collapse two shards/copies of the vlog onto one node, violating
// PlacementAllowed's NodeLevelDurability. The caller must hold vlogMu.
func (s *Server) occupiedNodesLocked(ctx context.Context, vlogID, excludeDisk uint32) (map[uint32]bool, error) {
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	occ := make(map[uint32]bool, len(shards))
	for _, sh := range shards {
		if sh.DiskID == excludeDisk {
			continue
		}
		occ[s.nodeOf(sh.DiskID)] = true
	}
	return occ, nil
}

// ensurePlacementAllowedLocked verifies relocating vlogID's shard from fromDisk
// to toDisk does not collapse two shards/copies onto one node: toDisk's node must
// not already hold another shard of the vlog (the shard being moved, still on
// fromDisk, is excluded). The caller must hold vlogMu.
func (s *Server) ensurePlacementAllowedLocked(ctx context.Context, vlogID, toDisk, fromDisk uint32) error {
	occ, err := s.occupiedNodesLocked(ctx, vlogID, fromDisk)
	if err != nil {
		return err
	}
	if occ[s.nodeOf(toDisk)] {
		return fmt.Errorf("replace: node %d already holds a shard of vlog %d, would collapse redundancy", s.nodeOf(toDisk), vlogID)
	}
	return nil
}

// Rebalance evens disk-space usage across active disks, implementing the
// RoseStorage RebalanceStep as a bounded, best-effort pass. Balance is measured
// in bytes, not shard count: shards vary widely in size, so an even count can
// still leave very uneven disks. It moves shards off the busiest disk onto the
// idlest, but only while the byte spread exceeds the hysteresis band and only up
// to the per-pass move cap, and it skips entirely inside the cooldown window after
// a prior pass moved something. It returns how many shards it moved.
//
// Unlike drain/reprotect/replace, rebalance has no must-finish obligation: every
// move is individually crash-safe (copy-then-repoint via migratePlogLocked) and a
// partially completed pass leaves a valid, merely-less-even cluster that the next
// pass continues from. So it carries no durable job row to resume.
func (s *Server) Rebalance(ctx context.Context) (int, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	pol := s.rebalance
	if pol.Cooldown > 0 && !s.lastRebalance.IsZero() && time.Since(s.lastRebalance) < pol.Cooldown {
		return 0, nil // within the backoff window: leave the minor imbalance alone
	}

	active := s.activeDiskIDs()
	if len(active) < 2 {
		return 0, nil
	}

	usage := make(map[uint32]int64, len(active))
	plogsByDisk := make(map[uint32][]meta.PlogOnDisk, len(active))
	sizeByPlog := make(map[uint32]int64)
	for _, d := range active {
		usage[d] = 0 // ensure empty disks are still candidate destinations
		ps, err := s.db.PlogsOnDisk(ctx, d)
		if err != nil {
			return 0, err
		}
		for _, p := range ps {
			sz, err := s.plogSizeLocked(d, p.PlogID)
			if err != nil {
				return 0, err
			}
			sizeByPlog[p.PlogID] = sz
			usage[d] += sz
		}
		plogsByDisk[d] = ps
	}

	moves := 0
	for pol.MaxMovesPerPass <= 0 || moves < pol.MaxMovesPerPass {
		src, dst := diskExtremes(active, usage)
		if usage[src]-usage[dst] <= pol.MinSkewBytes {
			break // spread is within the hysteresis band; minor imbalance is fine
		}
		moved, err := s.rebalanceOneLocked(ctx, src, pol.MinSkewBytes, usage, sizeByPlog, plogsByDisk)
		if err != nil {
			return moves, err
		}
		if !moved {
			break // nothing on the busiest disk can legally move to a lighter one
		}
		moves++
	}
	if moves > 0 {
		s.lastRebalance = time.Now()
	}
	return moves, nil
}

// plogSizeLocked reports a plog's physical size on disk, the unit rebalance
// equalizes. The caller must hold vlogMu.
func (s *Server) plogSizeLocked(diskID, plogID uint32) (int64, error) {
	fi, err := os.Stat(s.plogPath(diskID, plogID))
	if err != nil {
		return 0, fmt.Errorf("rebalance: stat plog %d on disk %d: %w", plogID, diskID, err)
	}
	return fi.Size(), nil
}

// diskExtremes returns the busiest and idlest disk by byte usage, scanning in the
// (sorted) active order so ties break deterministically.
func diskExtremes(active []uint32, usage map[uint32]int64) (busiest, idlest uint32) {
	busiest, idlest = active[0], active[0]
	for _, d := range active {
		if usage[d] > usage[busiest] {
			busiest = d
		}
		if usage[d] < usage[idlest] {
			idlest = d
		}
	}
	return busiest, idlest
}

// rebalanceOneLocked relocates a single shard off src onto a lighter active disk.
// It picks the largest shard on src that fits the gap to a placement-allowed
// destination (one not already holding a shard of the same vlog) without
// overshooting it past the source, so each move shrinks the spread and cannot
// immediately bounce back. It only moves when the gap still exceeds the
// hysteresis band. It updates usage and plogsByDisk in place and reports whether
// it moved anything. The caller must hold vlogMu.
func (s *Server) rebalanceOneLocked(ctx context.Context, src uint32, minSkewBytes int64, usage map[uint32]int64, sizeByPlog map[uint32]int64, plogsByDisk map[uint32][]meta.PlogOnDisk) (bool, error) {
	// Largest shard first, so a pass makes the most progress per move.
	candidates := append([]meta.PlogOnDisk(nil), plogsByDisk[src]...)
	sort.Slice(candidates, func(i, j int) bool {
		return sizeByPlog[candidates[i].PlogID] > sizeByPlog[candidates[j].PlogID]
	})

	for _, p := range candidates {
		sz := sizeByPlog[p.PlogID]
		occupied, err := s.occupiedNodesLocked(ctx, p.VlogID, src)
		if err != nil {
			return false, err
		}

		dst, dstUsage := uint32(0), int64(-1)
		for d, u := range usage {
			if d == src || occupied[s.nodeOf(d)] {
				continue
			}
			if dstUsage == -1 || u < dstUsage {
				dst, dstUsage = d, u
			}
		}
		if dstUsage == -1 {
			continue // no placement-allowed destination for this vlog
		}
		gap := usage[src] - dstUsage
		if gap <= minSkewBytes || sz > gap {
			// Either the pair is within the band, or moving this shard would
			// overshoot the destination past the source and could bounce back.
			continue
		}

		if err := s.migratePlogLocked(ctx, p.PlogID, p.VlogID, src, dst); err != nil {
			return false, err
		}
		usage[src] -= sz
		usage[dst] += sz
		plogsByDisk[src] = removePlog(plogsByDisk[src], p.PlogID)
		plogsByDisk[dst] = append(plogsByDisk[dst], p)
		return true, nil
	}
	return false, nil
}

func removePlog(plogs []meta.PlogOnDisk, plogID uint32) []meta.PlogOnDisk {
	for i, p := range plogs {
		if p.PlogID == plogID {
			return append(plogs[:i], plogs[i+1:]...)
		}
	}
	return plogs
}

// setDiskStateLocked persists a disk transition and updates the cache. The
// caller must hold vlogMu.
func (s *Server) setDiskStateLocked(ctx context.Context, diskID uint32, state string) error {
	if err := s.db.SetDiskState(ctx, diskID, state); err != nil {
		return err
	}
	s.diskState[diskID] = state
	return nil
}

// pickDrainDestinationLocked selects a live disk that can legally host a shard of
// vlogID being moved off fromDisk. It enforces PlacementAllowed's node fault
// domain: the destination must be on a node that does not already hold another
// shard (EC) or copy (DUPLICATE) of the same vlog, so a relocation never
// collapses two shards onto one node. The caller must hold vlogMu.
func (s *Server) pickDrainDestinationLocked(ctx context.Context, vlogID, fromDisk uint32) (uint32, error) {
	occupied, err := s.occupiedNodesLocked(ctx, vlogID, fromDisk)
	if err != nil {
		return 0, err
	}
	for _, id := range s.activeDiskIDs() { // live disks only; excludes fromDisk (draining/failed)
		if id == fromDisk || occupied[s.nodeOf(id)] {
			continue
		}
		return id, nil
	}
	return 0, fmt.Errorf("drain: no placement-allowed destination for vlog %d shard (need a live disk on a node not already holding a shard)", vlogID)
}

// migratePlogLocked relocates one plog's bytes from fromDisk to toDisk and
// repoints its placement, then re-mounts the owning vlog so in-memory clients
// resolve to the relocated file. The caller must hold vlogMu.
func (s *Server) migratePlogLocked(ctx context.Context, plogID, vlogID, fromDisk, toDisk uint32) error {
	oldPath := s.plogPath(fromDisk, plogID)
	newPath := s.plogPath(toDisk, plogID)

	// Flush and close the live handle so we copy a consistent, durable file.
	if p, ok := s.plogs[plogID]; ok {
		if err := p.Commit(); err != nil {
			return fmt.Errorf("drain: flush plog %d: %w", plogID, err)
		}
		_ = p.Close()
	}
	if err := copyFile(oldPath, newPath); err != nil {
		return fmt.Errorf("drain: copy plog %d to disk %d: %w", plogID, toDisk, err)
	}
	// Until this commits, the source copy remains authoritative.
	if err := s.db.MovePlogToDisk(ctx, plogID, toDisk); err != nil {
		return err
	}
	_ = os.Remove(oldPath)

	reopened, err := storage.OpenPlog(newPath, plogID)
	if err != nil {
		return fmt.Errorf("drain: reopen plog %d on disk %d: %w", plogID, toDisk, err)
	}
	s.plogs[plogID] = reopened

	// The active vlog must not stay pinned to a relocated shard mid-write; force
	// a fresh active vlog for subsequent writes, as compaction does.
	s.clearActiveVlogLocked(vlogID)
	return s.remountVlogLocked(ctx, vlogID)
}

// remountVlogLocked rebuilds a vlog's in-memory client set from current
// placement metadata, used after a shard's backing plog moves. The caller must
// hold vlogMu.
func (s *Server) remountVlogLocked(ctx context.Context, vlogID uint32) error {
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return err
	}
	vlog, err := s.mountVlogLocked(ctx, info)
	if err != nil {
		return fmt.Errorf("remount vlog %d: %w", vlogID, err)
	}
	s.vlogs[vlogID] = vlog
	return nil
}

// copyFile copies src to dst verbatim and fsyncs it. Plog files are byte-exact
// (fixed sectors, interposed hash sectors), so a verbatim copy preserves every
// logical offset the placement metadata already points at.
func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
