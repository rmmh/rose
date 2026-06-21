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
	if err := s.AttachDiskOnNode(ctx, req.GetDiskId(), req.GetNodeId(), root, req.GetTotalBytes()); err != nil {
		return nil, err
	}
	return &pb.AddDiskResponse{}, nil
}

func (s *Server) RemoveDisk(ctx context.Context, req *pb.RemoveDiskRequest) (*pb.MaintenanceJobResponse, error) {
	state, ok := s.DiskStates()[req.GetDiskId()]
	if !ok {
		return nil, fmt.Errorf("disk %d is not configured", req.GetDiskId())
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
	if state != meta.DiskActive && state != meta.DiskDraining {
		return nil, fmt.Errorf("disk %d is %s, cannot replace", req.GetOldDiskId(), state)
	}
	root := filepath.Join(s.dataDir, fmt.Sprintf("disk-%d", req.GetNewDiskId()))
	if err := s.AttachDiskOnNode(ctx, req.GetNewDiskId(), req.GetNodeId(), root, req.GetTotalBytes()); err != nil {
		return nil, err
	}
	job, err := s.db.GetOrCreateReplaceJob(ctx, req.GetOldDiskId(), req.GetNewDiskId())
	if err != nil {
		return nil, err
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
