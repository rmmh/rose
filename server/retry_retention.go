package server

import (
	"context"
	"fmt"
	"time"
)

// SetRetryRetention changes the retention period of future explicit-key results.
// Already committed results retain their original deadlines.
func (s *Server) SetRetryRetention(d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("retry retention must be positive")
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.retryRetention = d
	return nil
}

// ExpireWriteResults releases elapsed retry roots. Active handle pins continue
// protecting their bytes, but expired operation keys cannot publish again.
func (s *Server) ExpireWriteResults(ctx context.Context) (int, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()
	return s.expireWriteResultsLocked(ctx)
}

func (s *Server) expireWriteResultsLocked(ctx context.Context) (int, error) {
	s.pinMu.Lock()
	defer s.pinMu.Unlock()
	return s.db.ExpireWriteResults(ctx, s.now().UnixNano())
}
