package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

type Server struct {
	pb.UnimplementedRoseServer
	db    *meta.DB
	plogs map[uint32]*storage.Plog
	vlogs map[uint32]*storage.Vlog

	vlogMu        sync.Mutex
	activeVlog    uint32
	dataDir       string
	diskRoots     map[uint32]string
	handlesMu     sync.Mutex
	handles       map[int64]*FileHandle
	handleCounter int64
}

func NewServer(db *meta.DB) *Server {
	return &Server{
		db:        db,
		plogs:     make(map[uint32]*storage.Plog),
		vlogs:     make(map[uint32]*storage.Vlog),
		dataDir:   "data",
		diskRoots: map[uint32]string{1: "data"},
		handles:   make(map[int64]*FileHandle),
	}
}

// NewServerWithDataDir is intended for embedding and integration tests that
// need isolated physical-log files without relying on a FUSE mount.
func NewServerWithDataDir(db *meta.DB, dataDir string) *Server {
	s := NewServer(db)
	s.dataDir = dataDir
	s.diskRoots = map[uint32]string{1: dataDir}
	return s
}

// NewServerWithDiskRoots configures independent local storage roots for each
// disk ID. It is the local multi-disk shape used by recovery and placement.
func NewServerWithDiskRoots(db *meta.DB, diskRoots map[uint32]string) *Server {
	s := NewServer(db)
	s.diskRoots = make(map[uint32]string, len(diskRoots))
	for diskID, root := range diskRoots {
		s.diskRoots[diskID] = root
	}
	return s
}

func (s *Server) plogPath(diskID, plogID uint32) string {
	root, ok := s.diskRoots[diskID]
	if !ok {
		root = filepath.Join(s.dataDir, "disk-"+fmt.Sprint(diskID))
	}
	return filepath.Join(root, "plog-"+fmt.Sprint(plogID))
}

func (s *Server) activeDiskIDs() []uint32 {
	ids := make([]uint32, 0, len(s.diskRoots))
	for id := range s.diskRoots {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *Server) GetDB() *meta.DB {
	return s.db
}

// Recover rebuilds local plog and vlog clients from persisted metadata. A
// missing locally configured disk fails startup rather than silently exposing
// metadata that cannot be read.
func (s *Server) Recover(ctx context.Context) error {
	plogInfos, err := s.db.ListPlogs(ctx)
	if err != nil {
		return err
	}
	plogByID := make(map[uint32]*storage.Plog, len(plogInfos))
	for _, info := range plogInfos {
		plog, err := storage.OpenPlog(s.plogPath(info.DiskID, info.ID), info.ID)
		if err != nil {
			return fmt.Errorf("recover plog %d on disk %d: %w", info.ID, info.DiskID, err)
		}
		plogByID[info.ID] = plog
	}
	vlogInfos, err := s.db.ListVlogs(ctx)
	if err != nil {
		return err
	}
	vlogs := make(map[uint32]*storage.Vlog, len(vlogInfos))
	for _, info := range vlogInfos {
		mappings, err := s.db.ListVlogPlogs(ctx, info.ID)
		if err != nil {
			return err
		}
		clients := make([]storage.PlogClient, len(mappings))
		for index, mapping := range mappings {
			if mapping.ShardIndex != index {
				return fmt.Errorf("vlog %d has non-contiguous shard mapping", info.ID)
			}
			plog, ok := plogByID[mapping.PlogID]
			if !ok {
				return fmt.Errorf("vlog %d references missing plog %d", info.ID, mapping.PlogID)
			}
			clients[index] = &localPlogClient{plog: plog}
		}
		vlog, err := storage.NewVlog(info.ID, info.ProtectionScheme, int(info.DataShards), int(info.ParityShards), clients, info.Length)
		if err != nil {
			return fmt.Errorf("recover vlog %d: %w", info.ID, err)
		}
		vlogs[info.ID] = vlog
	}
	s.plogs = plogByID
	s.vlogs = vlogs

	// Resume any maintenance work interrupted by the crash/restart.
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.Kind == meta.JobCompact {
			if err := s.CompactVlog(ctx, job.TargetVlog); err != nil {
				return fmt.Errorf("resume compaction of vlog %d: %w", job.TargetVlog, err)
			}
		}
	}
	return nil
}

// provisionVlogLocked creates a vlog, its backing plogs across the configured
// disks, and the in-memory clients, registering everything. The caller must
// hold vlogMu.
func (s *Server) provisionVlogLocked(ctx context.Context, scheme string, dataShards, parityShards int) (uint32, *storage.Vlog, error) {
	id, err := s.db.MakeVlog(ctx, scheme, int32(dataShards), int32(parityShards))
	if err != nil {
		return 0, nil, err
	}
	diskIDs := s.activeDiskIDs()
	if len(diskIDs) == 0 {
		return 0, nil, fmt.Errorf("no active disks configured")
	}
	clientCount := 1
	switch scheme {
	case "DUPLICATE":
		clientCount = len(diskIDs)
	case "EC":
		clientCount = dataShards + parityShards
		if clientCount == 0 || clientCount > len(diskIDs) {
			return 0, nil, fmt.Errorf("EC vlog needs 1..%d shards, got %d", len(diskIDs), clientCount)
		}
	}
	clients := make([]storage.PlogClient, 0, clientCount)
	for shard := 0; shard < clientCount; shard++ {
		diskID := diskIDs[shard]
		plogID, err := s.db.MakePlog(ctx, diskID)
		if err != nil {
			return 0, nil, err
		}
		if err := s.db.AssignPlogToVlog(ctx, id, shard, plogID); err != nil {
			return 0, nil, err
		}
		plog, err := storage.OpenPlog(s.plogPath(diskID, plogID), plogID)
		if err != nil {
			return 0, nil, fmt.Errorf("open plog %d: %w", plogID, err)
		}
		s.plogs[plogID] = plog
		clients = append(clients, &localPlogClient{plog: plog})
	}
	vlog, err := storage.NewVlog(id, scheme, dataShards, parityShards, clients, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("create vlog in memory: %w", err)
	}
	s.vlogs[id] = vlog
	return id, vlog, nil
}

type localPlogClient struct {
	plog *storage.Plog
}

func (c *localPlogClient) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	return c.plog.Write(txnID, data)
}

func (c *localPlogClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	return c.plog.Read(offset, length)
}

func (c *localPlogClient) Commit(ctx context.Context, txnID int64) error {
	return c.plog.Commit()
}

func (c *localPlogClient) Scrub() (storage.ScrubResult, error) {
	return c.plog.Scrub()
}

// GC reclaims unreferenced chunk metadata (refcount zero) and reports how many
// chunks were collected. The chunks' log bytes become orphan data eligible for
// later segment compaction.
func (s *Server) GC(ctx context.Context) (int, error) {
	collected, err := s.db.GCChunks(ctx)
	if err != nil {
		return 0, err
	}
	return len(collected), nil
}

// VlogScrub reports a scrub of one mounted virtual log.
type VlogScrub struct {
	VlogID uint32
	Shards []storage.ShardScrub
}

// Scrub validates every mounted vlog's backing shards in order. It is the bulk
// sequential integrity pass the README describes; callers can use the reported
// corrupt shards to schedule repair from surviving redundancy.
func (s *Server) Scrub() ([]VlogScrub, error) {
	out := make([]VlogScrub, 0, len(s.vlogs))
	for id, vlog := range s.vlogs {
		shards, err := vlog.Scrub()
		if err != nil {
			return nil, err
		}
		out = append(out, VlogScrub{VlogID: id, Shards: shards})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VlogID < out[j].VlogID })
	return out, nil
}

// Ensure localPlogClient implements storage.PlogClient
var _ storage.PlogClient = &localPlogClient{}
