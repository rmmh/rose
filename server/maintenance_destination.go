package server

import (
	"context"
	"errors"

	"github.com/rmmh/rose/storage"
)

// assignMaintenanceDestinationLocked finishes provisioning or reclaims an
// unclaimed destination. The caller holds vlogMu. A failed assignment can have
// committed, so cleanup must inspect durable ownership instead of inferring it
// from the returned error. Cancellation cannot interrupt that cleanup decision.
func (s *Server) assignMaintenanceDestinationLocked(ctx context.Context, jobID int64, destID uint32) error {
	err := s.maintenanceCheckpoint("maintenance-provisioned")
	if err == nil {
		err = s.db.SetJobDest(ctx, jobID, destID)
	}
	if err == nil {
		err = s.maintenanceCheckpoint("maintenance-assigned")
	}
	if err == nil {
		return nil
	}
	removed, plogs, cleanupErr := s.db.RetireUnassignedMaintenanceDestination(context.WithoutCancel(ctx), destID)
	if cleanupErr != nil {
		return errors.Join(err, cleanupErr)
	}
	if !removed {
		return err // an assignment, lease, or recorded content still owns it
	}
	delete(s.vlogs, destID)
	s.deleteVlogKey(destID)
	s.clearActiveVlogLocked(destID)
	for _, info := range plogs {
		if plog := s.plogs[info.ID]; plog != nil {
			_ = plog.Close()
			delete(s.plogs, info.ID)
		}
		if removeErr := storage.RemovePlogFiles(s.plogPath(info.DiskID, info.ID)); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}
