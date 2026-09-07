package server

import (
	"context"
	"fmt"
	"time"

	"github.com/rmmh/rose/meta"
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

// writeOpForMutation admits a request against one state/deadline snapshot.
// Reclamation remains separate: denying an expired mutation does not drop the
// active handle's pins or prevent reading its already-open immutable version.
func (s *Server) writeOpForMutation(ctx context.Context, key string) (meta.WriteOp, error) {
	op, err := s.db.WriteOpByKey(ctx, key)
	if err != nil {
		return op, err
	}
	if op.State == meta.WriteOpExpired || op.RetryExpiresAt > 0 && s.now().UnixNano() >= op.RetryExpiresAt {
		return op, fmt.Errorf("write operation key is expired")
	}
	if op.State != meta.WriteOpPrepared && op.State != meta.WriteOpCommitted {
		return op, fmt.Errorf("write operation is %s", op.State)
	}
	return op, nil
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
