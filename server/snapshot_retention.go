package server

import (
	"context"

	"github.com/rmmh/rose/meta"
)

// ListSnapshots discovers retained explicit snapshots, newest first.
func (s *Server) ListSnapshots(ctx context.Context) ([]meta.SnapshotInfo, error) {
	return s.db.ListSnapshots(ctx)
}

func (s *Server) SetSnapshotRetention(ctx context.Context, p *meta.SnapshotRetention) error {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	return s.db.SetSnapshotRetention(ctx, p)
}

func (s *Server) ExpireSnapshots(ctx context.Context) ([]uint64, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	return s.db.ExpireSnapshots(ctx, s.now())
}
