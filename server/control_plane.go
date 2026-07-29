package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// SetDiskState transitions a configured disk's lifecycle state and persists it
// to the durable catalog. Placement and commit-durability decisions read the
// cached state, so the in-memory cache and catalog are updated together under
// vlogMu.
func (s *Server) SetDiskState(ctx context.Context, diskID uint32, state string) error {
	switch state {
	case meta.DiskActive, meta.DiskDraining, meta.DiskFailed, meta.DiskDetached:
	default:
		return fmt.Errorf("invalid disk state %q", state)
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if _, ok := s.diskRoots[diskID]; !ok {
		return fmt.Errorf("disk %d is not configured", diskID)
	}
	previous := s.diskState[diskID]
	if state == meta.DiskActive && previous == meta.DiskFailed &&
		s.nodeState[s.nodeOf(diskID)] != meta.NodeFailed {
		if err := s.validateDiskRootIdentity(ctx, diskID); err != nil {
			return err
		}
	}
	if err := s.setDiskStateLocked(ctx, diskID, state); err != nil {
		return err
	}
	durableCtx := context.WithoutCancel(ctx)
	if state == meta.DiskFailed {
		return s.offlineDiskPlogsLocked(durableCtx, diskID)
	}
	if state == meta.DiskActive && previous == meta.DiskFailed &&
		s.nodeState[s.nodeOf(diskID)] != meta.NodeFailed {
		if err := s.reopenDiskPlogsLocked(durableCtx, diskID); err != nil {
			_ = s.db.SetDiskState(durableCtx, diskID, meta.DiskFailed)
			s.diskState[diskID] = meta.DiskFailed
			return err
		}
	}
	return nil
}

// DiskStates returns a snapshot of every configured disk's lifecycle state.
func (s *Server) DiskStates() map[uint32]string {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	out := make(map[uint32]string, len(s.diskState))
	for id, state := range s.diskState {
		out[id] = state
	}
	return out
}

// SetNodeState transitions a node's liveness and persists it. Marking a node
// failed drops its disks out of the live set (commit/read gating and placement
// react immediately) without touching their disk_state, so the loss is transient.
// Marking it working again restores its disks and abandons any reprotect the
// outage triggered: the node's bytes are back, so regenerating them elsewhere is
// wasted work (see cancelNodeReprotectsLocked).
func (s *Server) SetNodeState(ctx context.Context, nodeID uint32, state string) error {
	switch state {
	case meta.NodeWorking, meta.NodeFailed:
	default:
		return fmt.Errorf("invalid node state %q", state)
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if !s.nodeConfiguredLocked(nodeID) {
		return fmt.Errorf("node %d is not configured", nodeID)
	}
	if state == meta.NodeWorking {
		for diskID, diskState := range s.diskState {
			if s.nodeOf(diskID) != nodeID ||
				(diskState != meta.DiskActive && diskState != meta.DiskDraining) {
				continue
			}
			if err := s.validateDiskRootIdentity(ctx, diskID); err != nil {
				return err
			}
		}
	}
	if err := s.db.SetNodeState(ctx, nodeID, state); err != nil {
		return err
	}
	durableCtx := context.WithoutCancel(ctx)
	if state == meta.NodeWorking {
		delete(s.nodeState, nodeID)
		// A recovered server intentionally leaves a failed node's plogs closed.
		// Before cancelling its reprotect, reopen those original files and remount
		// the affected vlogs.  Otherwise a cold restart followed by node return
		// would mark the disk active while its vlog still contains offline clients.
		if err := s.reopenNodePlogsLocked(durableCtx, nodeID); err != nil {
			// Do not leave the disk live if its promised return did not make the
			// original bytes reachable. Keep the durable and cached liveness gates
			// conservative so commits remain read-only until repair can proceed.
			_ = s.db.SetNodeState(durableCtx, nodeID, meta.NodeFailed)
			s.nodeState[nodeID] = meta.NodeFailed
			return err
		}
		return s.cancelNodeReprotectsLocked(durableCtx, nodeID)
	}
	s.nodeState[nodeID] = state
	return s.offlineNodePlogsLocked(durableCtx, nodeID)
}

// offlineNodePlogsLocked makes an in-process node failure behave like a real
// disconnected node: its open plog clients stop serving immediately and every
// affected vlog is rebuilt with offline clients. The caller must hold vlogMu.
func (s *Server) offlineNodePlogsLocked(ctx context.Context, nodeID uint32) error {
	return s.offlinePlogsLocked(ctx, func(info meta.PlogInfo) bool {
		return s.nodeOf(info.DiskID) == nodeID
	})
}

func (s *Server) offlineDiskPlogsLocked(ctx context.Context, diskID uint32) error {
	return s.offlinePlogsLocked(ctx, func(info meta.PlogInfo) bool {
		return info.DiskID == diskID
	})
}

func (s *Server) offlinePlogsLocked(ctx context.Context, matches func(meta.PlogInfo) bool) error {
	infos, err := s.db.ListPlogs(ctx)
	if err != nil {
		return err
	}
	var offline []uint32
	affected := make(map[uint32]bool)
	for _, info := range infos {
		if !matches(info) {
			continue
		}
		offline = append(offline, info.ID)
		vlogIDs, err := s.db.VlogsForPlog(ctx, info.ID)
		if err != nil {
			return err
		}
		for _, vlogID := range vlogIDs {
			affected[vlogID] = true
		}
	}
	offlineSet := make(map[uint32]bool, len(offline))
	for _, plogID := range offline {
		offlineSet[plogID] = true
	}
	// A mirrored quorum write can return before every extra copy catches up.
	// Before taking the selected copies away, bring each survivor to the mounted
	// vlog cursor while every possible source is still open.
	for vlogID := range affected {
		info, err := s.db.GetVlog(ctx, vlogID)
		if err != nil {
			return err
		}
		if info.ProtectionScheme != "DUPLICATE" {
			continue
		}
		source := s.vlogs[vlogID]
		if source == nil {
			continue
		}
		targetLength := max(info.Length, source.Length())
		mappings, err := s.db.ListVlogPlogs(ctx, vlogID)
		if err != nil {
			return err
		}
		for _, mapping := range mappings {
			plog := s.plogs[mapping.PlogID]
			if offlineSet[mapping.PlogID] || plog == nil {
				continue
			}
			if err := s.catchUpDuplicatePlogLocked(ctx, source, vlogID, mapping.PlogID, plog, targetLength); err != nil {
				return err
			}
		}
	}
	for _, plogID := range offline {
		if p, ok := s.plogs[plogID]; ok {
			_ = p.Close()
			delete(s.plogs, plogID)
		}
		s.offlinePlogs[plogID] = true
	}
	for vlogID := range affected {
		s.clearActiveVlogLocked(vlogID)
		if err := s.remountVlogLocked(ctx, vlogID); err != nil {
			return err
		}
	}
	return nil
}

// reopenNodePlogsLocked reattaches plogs that Recover left offline because their
// node was unavailable. It deliberately fails before cancelling reprotect if a
// supposedly returned disk still lacks a file: treating a genuinely lost file as
// healthy would violate the durability gate. The caller must hold vlogMu.
func (s *Server) reopenNodePlogsLocked(ctx context.Context, nodeID uint32) error {
	return s.reopenPlogsLocked(ctx, fmt.Sprintf("node %d", nodeID), func(info meta.PlogInfo) bool {
		return s.nodeOf(info.DiskID) == nodeID
	})
}

func (s *Server) reopenDiskPlogsLocked(ctx context.Context, diskID uint32) error {
	return s.reopenPlogsLocked(ctx, fmt.Sprintf("disk %d", diskID), func(info meta.PlogInfo) bool {
		return info.DiskID == diskID
	})
}

func (s *Server) reopenPlogsLocked(ctx context.Context, owner string, matches func(meta.PlogInfo) bool) error {
	infos, err := s.db.ListPlogs(ctx)
	if err != nil {
		return err
	}
	affected := make(map[uint32]bool)
	reopened := make(map[uint32]*storage.Plog)
	discardReopened := func() {
		for _, p := range reopened {
			_ = p.Close()
		}
	}
	for _, info := range infos {
		if !matches(info) || !s.offlinePlogs[info.ID] {
			continue
		}
		// OpenExistingPlog, not OpenPlog: a returned node whose file is genuinely
		// gone must fail here rather than have O_CREATE resurrect it as an empty
		// shard and pass it off as healthy, which would violate the durability gate.
		p, err := storage.OpenExistingPlog(s.plogPath(info.DiskID, info.ID), info.ID)
		if err != nil {
			discardReopened()
			return fmt.Errorf("reopen plog %d on returned %s: %w", info.ID, owner, err)
		}
		if err := s.validatePlogIdentity(ctx, info, p); err != nil {
			_ = p.Close()
			discardReopened()
			return fmt.Errorf("reopen plog %d on returned %s: %w", info.ID, owner, err)
		}
		mappings, err := s.db.VlogsForPlog(ctx, info.ID)
		if err != nil {
			_ = p.Close()
			discardReopened()
			return err
		}
		reopened[info.ID] = p
		for _, vlogID := range mappings {
			affected[vlogID] = true
		}
	}
	if err := s.catchUpReturnedDuplicatesLocked(ctx, reopened); err != nil {
		discardReopened()
		return err
	}
	// Publish the reopened handles only after every expected file was reachable.
	// A partial node return must leave the whole node offline so the next retry
	// revisits every plog and remounts every affected vlog.
	for id, p := range reopened {
		s.plogs[id] = p
		delete(s.offlinePlogs, id)
	}
	for vlogID := range affected {
		if err := s.remountVlogLocked(ctx, vlogID); err != nil {
			return err
		}
	}
	for id := range reopened {
		if p, ok := s.plogs[id]; ok {
			if err := p.RecoverHashes(ctx, s); err != nil {
				slog.Warn("failed to recover hashes for reopened plog, continuing", "plogID", id, "error", err)
			}
		}
	}
	return nil
}

// catchUpReturnedDuplicatesLocked copies the committed tail a mirror missed
// while its node or disk was offline. The existing mounted vlog still contains
// offline stubs for the returned plogs, so its reads come from surviving copies.
func (s *Server) catchUpReturnedDuplicatesLocked(ctx context.Context, reopened map[uint32]*storage.Plog) error {
	for plogID, plog := range reopened {
		vlogIDs, err := s.db.VlogsForPlog(ctx, plogID)
		if err != nil {
			return err
		}
		for _, vlogID := range vlogIDs {
			info, err := s.db.GetVlog(ctx, vlogID)
			if err != nil {
				return err
			}
			if info.ProtectionScheme != "DUPLICATE" {
				continue
			}
			source := s.vlogs[vlogID]
			if source == nil {
				return fmt.Errorf("catch up returned plog %d: vlog %d is not mounted", plogID, vlogID)
			}
			targetLength := info.Length
			if source.Length() > targetLength {
				leased, err := s.db.VlogLeased(ctx, vlogID)
				if err != nil {
					return err
				}
				if leased {
					targetLength = source.Length()
				}
			}
			if err := s.catchUpDuplicatePlogLocked(ctx, source, vlogID, plogID, plog, targetLength); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) catchUpDuplicatePlogLocked(ctx context.Context, source *storage.Vlog, vlogID, plogID uint32, plog *storage.Plog, targetLength int64) error {
	const copyChunk = int64(1 << 20)
	if plog.LogicalLength() >= targetLength {
		return nil
	}
	for offset := plog.LogicalLength(); offset < targetLength; {
		length := min(copyChunk, targetLength-offset)
		data, err := source.Read(ctx, offset, int(length))
		if err != nil {
			return fmt.Errorf("read vlog %d to catch up plog %d: %w", vlogID, plogID, err)
		}
		if err := plog.EnsureAppend(offset, data); err != nil {
			return fmt.Errorf("catch up plog %d at %d: %w", plogID, offset, err)
		}
		offset += length
	}
	if err := plog.Commit(); err != nil {
		return fmt.Errorf("commit caught-up plog %d: %w", plogID, err)
	}
	return nil
}

// nodeConfiguredLocked reports whether any configured disk lives on a node. The
// caller must hold vlogMu.
func (s *Server) nodeConfiguredLocked(nodeID uint32) bool {
	for id := range s.diskRoots {
		if s.nodeOf(id) == nodeID {
			return true
		}
	}
	return false
}

// cancelNodeReprotectsLocked abandons reprotects triggered by a node outage now
// that the node is back. For each failed disk on the returning node whose
// reprotect is still running, it cancels the job and restores the disk to active:
// the disk's bytes survived the outage, so its not-yet-regenerated shards resolve
// to it again and the redundancy is whole without finishing the regeneration.
// The caller must hold vlogMu.
func (s *Server) cancelNodeReprotectsLocked(ctx context.Context, nodeID uint32) error {
	for id := range s.diskRoots {
		if s.nodeOf(id) != nodeID {
			continue
		}
		cancelled, err := s.db.CancelRunningReprotect(ctx, id)
		if err != nil {
			return err
		}
		if cancelled && s.diskState[id] == meta.DiskFailed {
			if err := s.setDiskStateLocked(ctx, id, meta.DiskActive); err != nil {
				return err
			}
		}
	}
	return nil
}

// NodeStates returns a snapshot of node liveness for every node a configured
// disk lives on (working unless recorded otherwise).
func (s *Server) NodeStates() map[uint32]string {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	out := make(map[uint32]string)
	for id := range s.diskRoots {
		n := s.nodeOf(id)
		if state, ok := s.nodeState[n]; ok {
			out[n] = state
		} else {
			out[n] = meta.NodeWorking
		}
	}
	return out
}

// commitThreshold reports the minimum number of live shards a vlog must have to
// durably commit a write, mirroring RoseStorage's CommitReady. total is the
// number of shards actually provisioned for the vlog.
//
//   - NONE: a single copy is the whole durability story.
//   - DUPLICATE: at least minCopies live copies, never more than provisioned, so
//     a single-disk deployment still commits while a multi-copy one tolerates loss.
//   - EC: every coded shard must be placed at commit time; reads later tolerate
//     losing up to parity_shards of them (that weaker gate is Readable, not this).
func (s *Server) commitThreshold(info meta.VlogInfo, total int) int {
	switch info.ProtectionScheme {
	case "EC":
		return int(info.DataShards + info.ParityShards)
	case "DUPLICATE":
		if s.minCopies < total {
			return s.minCopies
		}
		return total
	default:
		return 1
	}
}

// readThreshold reports the minimum number of live shards needed to still read a
// committed vlog. For EC that is data_shards (enough to reconstruct); duplicated
// and unprotected data needs a single surviving copy.
func (s *Server) readThreshold(info meta.VlogInfo) int {
	if info.ProtectionScheme == "EC" {
		return int(info.DataShards)
	}
	return 1
}

// liveShardCountLocked reports how many of a vlog's existing shards are
// reachable, plus the total shards provisioned. Draining and detached disks
// remain reachable while their files exist, so an in-flight leased write can
// finish during evacuation. Failed disks and disks on failed nodes do not count.
// The caller must hold vlogMu.
func (s *Server) liveShardCountLocked(ctx context.Context, vlogID uint32) (live, total int, err error) {
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		return 0, 0, err
	}
	for _, sh := range shards {
		if !s.offlinePlogs[sh.PlogID] && s.diskReachableLocked(sh.DiskID) {
			live++
		}
	}
	return live, len(shards), nil
}

// commitReadyLocked reports whether a vlog has enough live shards to durably
// commit a write under its protection scheme. The caller must hold vlogMu.
func (s *Server) commitReadyLocked(ctx context.Context, vlogID uint32) (bool, error) {
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return false, err
	}
	live, total, err := s.liveShardCountLocked(ctx, vlogID)
	if err != nil {
		return false, err
	}
	return live >= s.commitThreshold(info, total), nil
}

// CommitReady reports whether new writes to a vlog can be durably committed. It
// is the read-only-degradation gate: when too many disks under a vlog are down,
// the server refuses to acknowledge writes rather than claim durability it
// cannot provide.
func (s *Server) CommitReady(ctx context.Context, vlogID uint32) (bool, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	return s.commitReadyLocked(ctx, vlogID)
}

// Readable reports whether a committed vlog can still be served from its
// surviving shards.
func (s *Server) Readable(ctx context.Context, vlogID uint32) (bool, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return false, err
	}
	live, _, err := s.liveShardCountLocked(ctx, vlogID)
	if err != nil {
		return false, err
	}
	return live >= s.readThreshold(info), nil
}
