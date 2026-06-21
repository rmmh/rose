package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

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

// setDiskStateLocked persists a disk transition and updates the cache. The
// caller must hold vlogMu.
func (s *Server) setDiskStateLocked(ctx context.Context, diskID uint32, state string) error {
	if err := s.db.SetDiskState(ctx, diskID, state); err != nil {
		return err
	}
	s.diskState[diskID] = state
	return nil
}

// pickDrainDestinationLocked selects an active disk that can legally host a
// shard of vlogID being moved off fromDisk. It enforces PlacementAllowed: the
// destination must not already hold another shard (EC) or copy (DUPLICATE) of
// the same vlog, so a relocation never collapses two shards onto one disk. The
// caller must hold vlogMu.
func (s *Server) pickDrainDestinationLocked(ctx context.Context, vlogID, fromDisk uint32) (uint32, error) {
	shards, err := s.db.VlogShardDisks(ctx, vlogID)
	if err != nil {
		return 0, err
	}
	occupied := make(map[uint32]bool, len(shards))
	for _, sh := range shards {
		occupied[sh.DiskID] = true
	}
	for _, id := range s.activeDiskIDs() { // active disks only; excludes fromDisk (now draining)
		if id == fromDisk || occupied[id] {
			continue
		}
		return id, nil
	}
	return 0, fmt.Errorf("drain: no placement-allowed destination for vlog %d shard (need an active disk not already holding a shard)", vlogID)
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
	if s.activeVlog == vlogID {
		s.activeVlog = 0
	}
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
	mappings, err := s.db.ListVlogPlogs(ctx, vlogID)
	if err != nil {
		return err
	}
	clients := make([]storage.PlogClient, len(mappings))
	for i, m := range mappings {
		if m.ShardIndex != i {
			return fmt.Errorf("vlog %d has non-contiguous shard mapping", vlogID)
		}
		plog, ok := s.plogs[m.PlogID]
		if !ok {
			return fmt.Errorf("vlog %d references missing plog %d", vlogID, m.PlogID)
		}
		clients[i] = &localPlogClient{plog: plog}
	}
	vlog, err := storage.NewVlog(info.ID, info.ProtectionScheme, int(info.DataShards), int(info.ParityShards), clients, info.Length)
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
