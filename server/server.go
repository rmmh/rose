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

	vlogMu     sync.Mutex
	activeVlog uint32
	dataDir    string
}

func NewServer(db *meta.DB) *Server {
	return &Server{
		db:      db,
		plogs:   make(map[uint32]*storage.Plog),
		vlogs:   make(map[uint32]*storage.Vlog),
		dataDir: "data",
	}
}

// NewServerWithDataDir is intended for embedding and integration tests that
// need isolated physical-log files without relying on a FUSE mount.
func NewServerWithDataDir(db *meta.DB, dataDir string) *Server {
	s := NewServer(db)
	s.dataDir = dataDir
	return s
}

func (s *Server) plogPath(id uint32) string {
	return filepath.Join(s.dataDir, "plog-"+fmt.Sprint(id))
}

func (s *Server) GetDB() *meta.DB {
	return s.db
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

// Ensure localPlogClient implements storage.PlogClient
var _ storage.PlogClient = &localPlogClient{}
