package server

import (
	"context"
	"fmt"

	"github.com/rmmh/rose/meta"
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
	if err := s.db.SetDiskState(ctx, diskID, state); err != nil {
		return err
	}
	s.diskState[diskID] = state
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

// liveShardCountLocked reports how many of a vlog's shards currently sit on an
// active disk, plus the total shards provisioned. The caller must hold vlogMu.
func (s *Server) liveShardCountLocked(ctx context.Context, vlogID uint32) (live, total int, err error) {
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		return 0, 0, err
	}
	for _, sh := range shards {
		if s.diskState[sh.DiskID] == meta.DiskActive {
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
