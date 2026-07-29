package server

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

// AddDisk wires RoseStorage's AddDisk transition to the local disk catalog. The
// RPC has no path because a production node owns its disk roots; this local
// implementation gives dynamically added disks a deterministic root below the
// server data directory.
func (s *Server) AddDisk(ctx context.Context, req *pb.AddDiskRequest) (*pb.AddDiskResponse, error) {
	if req.GetDiskId() == 0 || req.GetNodeId() == 0 {
		return nil, fmt.Errorf("disk_id and node_id are required")
	}
	root := filepath.Join(s.dataDir, fmt.Sprintf("disk-%d", req.GetDiskId()))
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if _, configured := s.diskRoots[req.GetDiskId()]; configured {
		if s.nodeOf(req.GetDiskId()) != req.GetNodeId() {
			return nil, fmt.Errorf("disk %d is already attached to node %d", req.GetDiskId(), s.nodeOf(req.GetDiskId()))
		}
		totalBytes, err := s.db.DiskCapacity(ctx, req.GetDiskId())
		if err != nil {
			return nil, err
		}
		if totalBytes != req.GetTotalBytes() {
			return nil, fmt.Errorf("disk %d is already attached with capacity %d", req.GetDiskId(), totalBytes)
		}
		return &pb.AddDiskResponse{}, nil
	}
	if err := s.attachDiskOnNodeLocked(ctx, req.GetDiskId(), req.GetNodeId(), root, req.GetTotalBytes()); err != nil {
		return nil, err
	}
	return &pb.AddDiskResponse{}, nil
}

func (s *Server) RemoveDisk(ctx context.Context, req *pb.RemoveDiskRequest) (*pb.MaintenanceJobResponse, error) {
	state, ok := s.DiskStates()[req.GetDiskId()]
	if !ok {
		return nil, fmt.Errorf("disk %d is not configured", req.GetDiskId())
	}
	if state == meta.DiskDetached {
		job, exists, err := s.db.LatestDiskJob(ctx, meta.JobDrain, req.GetDiskId())
		if err != nil {
			return nil, err
		}
		if exists {
			if job.State == meta.JobDone {
				return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
			}
			if job.State == meta.JobRunning {
				plogs, err := s.db.PlogsOnDisk(ctx, req.GetDiskId())
				if err != nil {
					return nil, err
				}
				if len(plogs) == 0 {
					if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
						return nil, err
					}
					return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
				}
			}
		}
	}
	if state != meta.DiskActive && state != meta.DiskDraining {
		return nil, fmt.Errorf("disk %d is %s, cannot remove", req.GetDiskId(), state)
	}
	job, err := s.db.GetOrCreateDrainJob(ctx, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	if err := s.DrainDisk(ctx, req.GetDiskId()); err != nil {
		return nil, err
	}
	return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
}

func (s *Server) ReplaceDisk(ctx context.Context, req *pb.ReplaceDiskRequest) (*pb.MaintenanceJobResponse, error) {
	if req.GetOldDiskId() == 0 || req.GetNewDiskId() == 0 || req.GetNodeId() == 0 {
		return nil, fmt.Errorf("old_disk_id, new_disk_id, and node_id are required")
	}
	state, ok := s.DiskStates()[req.GetOldDiskId()]
	if !ok {
		return nil, fmt.Errorf("disk %d is not configured", req.GetOldDiskId())
	}
	if state == meta.DiskDetached {
		job, exists, err := s.db.LatestDiskJob(ctx, meta.JobReplace, req.GetOldDiskId())
		if err != nil {
			return nil, err
		}
		if exists && job.DestDisk == req.GetNewDiskId() {
			if job.State == meta.JobDone {
				return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
			}
			if job.State == meta.JobRunning {
				plogs, err := s.db.PlogsOnDisk(ctx, req.GetOldDiskId())
				if err != nil {
					return nil, err
				}
				if len(plogs) == 0 {
					if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
						return nil, err
					}
					return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
				}
			}
		}
	}
	if state != meta.DiskActive && state != meta.DiskDraining {
		return nil, fmt.Errorf("disk %d is %s, cannot replace", req.GetOldDiskId(), state)
	}

	// A failed replacement pass has already attached its destination and left a
	// durable running job. Retrying the RPC must resume that job rather than fail
	// at AttachDiskOnNode because the destination now exists.
	var job meta.Job
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		return nil, err
	}
	for _, running := range jobs {
		if running.Kind == meta.JobReplace && running.TargetDisk == req.GetOldDiskId() {
			job = running
			break
		}
	}
	if job.ID != 0 {
		if job.DestDisk != req.GetNewDiskId() {
			return nil, fmt.Errorf("replace: disk %d already has running replacement onto disk %d", req.GetOldDiskId(), job.DestDisk)
		}
		if _, ok := s.DiskStates()[job.DestDisk]; !ok {
			return nil, fmt.Errorf("replace: destination disk %d for running job is not configured", job.DestDisk)
		}
	} else {
		if destState, configured := s.DiskStates()[req.GetNewDiskId()]; configured {
			if destState != meta.DiskActive {
				return nil, fmt.Errorf("replace: pre-attached destination disk %d is %s, must be active", req.GetNewDiskId(), destState)
			}
			s.vlogMu.Lock()
			destNode := s.nodeOf(req.GetNewDiskId())
			s.vlogMu.Unlock()
			if destNode != req.GetNodeId() {
				return nil, fmt.Errorf("replace: destination disk %d belongs to node %d, not node %d", req.GetNewDiskId(), destNode, req.GetNodeId())
			}
			plogs, err := s.db.PlogsOnDisk(ctx, req.GetNewDiskId())
			if err != nil {
				return nil, err
			}
			if len(plogs) != 0 {
				return nil, fmt.Errorf("replace: pre-attached destination disk %d is not empty", req.GetNewDiskId())
			}
		} else {
			root := filepath.Join(s.dataDir, fmt.Sprintf("disk-%d", req.GetNewDiskId()))
			if err := s.AttachDiskOnNode(ctx, req.GetNewDiskId(), req.GetNodeId(), root, req.GetTotalBytes()); err != nil {
				return nil, err
			}
		}
		job, err = s.db.GetOrCreateReplaceJob(ctx, req.GetOldDiskId(), req.GetNewDiskId())
		if err != nil {
			return nil, err
		}
	}
	if err := s.ReplaceDiskWith(ctx, req.GetOldDiskId(), req.GetNewDiskId()); err != nil {
		return nil, err
	}
	return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
}

func (s *Server) StartReprotect(ctx context.Context, req *pb.StartReprotectRequest) (*pb.MaintenanceJobResponse, error) {
	state, ok := s.DiskStates()[req.GetDiskId()]
	if !ok {
		return nil, fmt.Errorf("disk %d is not configured", req.GetDiskId())
	}
	if state != meta.DiskFailed && state != meta.DiskDraining {
		return nil, fmt.Errorf("disk %d is %s, only failed or draining disks are reprotected", req.GetDiskId(), state)
	}
	previous, exists, err := s.db.LatestDiskJob(ctx, meta.JobReprotect, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	if exists && previous.State == meta.JobDone {
		return &pb.MaintenanceJobResponse{JobId: uint64(previous.ID)}, nil
	}
	job, err := s.db.GetOrCreateReprotectJob(ctx, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	if err := s.ReprotectDisk(ctx, req.GetDiskId()); err != nil {
		return nil, err
	}
	return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
}

func (s *Server) StartRebalance(ctx context.Context, _ *pb.StartRebalanceRequest) (*pb.MaintenanceJobResponse, error) {
	job, err := s.db.GetOrCreateRebalanceJob(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.Rebalance(ctx); err != nil {
		return nil, err
	}
	if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
		return nil, err
	}
	return &pb.MaintenanceJobResponse{JobId: uint64(job.ID)}, nil
}

func (s *Server) GetMaintenanceJob(ctx context.Context, req *pb.GetMaintenanceJobRequest) (*pb.GetMaintenanceJobResponse, error) {
	job, err := s.db.GetJob(ctx, int64(req.GetJobId()))
	if err != nil {
		return nil, err
	}
	state := pb.MaintenanceJobState_MAINTENANCE_JOB_STATE_RUNNING
	if job.State == meta.JobDone || job.State == meta.JobCancelled {
		state = pb.MaintenanceJobState_MAINTENANCE_JOB_STATE_COMPLETED
	}
	return &pb.GetMaintenanceJobResponse{JobId: uint64(job.ID), State: state}, nil
}
