package server

import (
	"context"
	"fmt"
	"path/filepath"
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
	return nil
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

// Ensure localPlogClient implements storage.PlogClient
var _ storage.PlogClient = &localPlogClient{}
