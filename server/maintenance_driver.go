package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/rmmh/rose/meta"
)

// SetMaintenanceInterval changes the control-plane driver's polling interval.
// It is primarily an operator/test tuning knob; rebalance's own policy remains
// the authority on how often bytes are moved.
func (s *Server) SetMaintenanceInterval(interval time.Duration) {
	s.maintenanceMu.Lock()
	s.maintenanceEvery = interval
	running := s.maintenanceCancel != nil
	s.maintenanceMu.Unlock()
	if running {
		s.StopMaintenanceDriver()
		s.startMaintenanceDriver()
	}
}

// startMaintenanceDriver starts one background control-plane loop. Recover
// calls it only after rebuilding clients and resuming durable jobs, so its first
// tick sees a consistent catalog.
func (s *Server) startMaintenanceDriver() {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if s.maintenanceCancel != nil || s.maintenanceEvery <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.maintenanceCancel = cancel
	interval := s.maintenanceEvery
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := s.RunMaintenanceOnce(ctx); err != nil && ctx.Err() == nil {
					slog.Error("storage maintenance pass failed", "error", err)
				}
			}
		}
	}()
}

// StopMaintenanceDriver stops the background loop. Embedders should call it
// before closing the catalog; it is idempotent and intentionally separate from
// the Close RPC.
func (s *Server) StopMaintenanceDriver() {
	s.maintenanceMu.Lock()
	cancel := s.maintenanceCancel
	s.maintenanceCancel = nil
	s.maintenanceMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// RunMaintenanceOnce is one deterministic control-plane pass. Failed disks are
// reprotected first, preserving the TLA model's requirement that every published
// object regain its full placement before the normal commit gate re-admits
// writes. Rebalance then gets a chance to run; its policy enforces the cooldown.
// Finally GC reclaims refcount-0 chunk rows and compaction physically reclaims
// the dead space they leave, so dead-space reclamation is driven on the interval
// rather than only on an explicit call. Each step's errors are independent: a
// failing repair must not stop space reclamation, and vice versa.
func (s *Server) RunMaintenanceOnce(ctx context.Context) error {
	// Hold the maintenance lock for the whole pass so reclamation steps that move
	// chunks across vlogs (promotion repacking staging into EC, then GC and
	// compaction reclaiming the drained space) cannot interleave with a concurrent
	// explicit GC/Compact. Without this, two actors plan against the same catalog
	// snapshot and race to retire/repoint the same vlogs and chunks -- surfacing as
	// "source vlog N not mounted", "promote: load vlog N: no rows", or, worse,
	// silently repointing a chunk to the wrong bytes.
	s.maintRunMu.Lock()
	defer s.maintRunMu.Unlock()
	states := s.DiskStates()
	var firstErr error
	recordErr := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for diskID, state := range states {
		if state != meta.DiskFailed {
			continue
		}
		// One unrecoverable vlog must not stop repairs for independent failed
		// disks. Its failed state remains the durable retry signal.
		recordErr(s.ReprotectDisk(ctx, diskID))
	}
	// Regenerate shards stubbed offline at recovery (their file went missing on a
	// still-active disk, so reprotect — keyed on a failed disk — never sees them).
	// Catalog-driven and a no-op when nothing is offline, so it is cheap to run
	// every pass; it closes the loop on a single lost file restoring full
	// redundancy without condemning the disk.
	if _, err := s.RepairOfflineShards(ctx); err != nil {
		recordErr(err)
	}
	_, err := s.Rebalance(ctx)
	recordErr(err)
	if _, err := s.ReapAbandonedWriteOps(ctx, s.writeOpExpiryDuration()); err != nil {
		recordErr(err)
	}
	// Promote staged chunks into EC before reclamation: it reparents chunks out of
	// the replicated staging vlogs, turning their old locations into dead space
	// that the GC/compaction steps below can then reclaim in the same pass.
	if _, err := s.PromoteStaging(ctx); err != nil {
		recordErr(err)
	}
	if _, err := s.gcLocked(ctx); err != nil {
		recordErr(err)
	}
	if _, err := s.compactLocked(ctx, s.compactionPolicy()); err != nil {
		recordErr(err)
	}
	if _, err := s.SweepStrayPlogFiles(ctx); err != nil {
		recordErr(err)
	}
	return firstErr
}
