package server

import (
	"context"
	"log/slog"
	"time"
)

// ReapAbandonedWriteOps finds prepared write operations that have no active
// in-memory handles, and transitions them to the abandoned state (releasing
// their vlog leases) if they have exceeded the ageThreshold.
// It returns the count of operations abandoned, or an error.
func (s *Server) ReapAbandonedWriteOps(ctx context.Context, ageThreshold time.Duration) (int, error) {
	ops, err := s.db.ListPreparedWriteOps(ctx)
	if err != nil {
		return 0, err
	}

	s.handlesMu.Lock()
	activeIDs := make(map[int64]bool, len(s.handles))
	for _, h := range s.handles {
		if h.writeOpID != 0 {
			activeIDs[h.writeOpID] = true
		}
	}
	s.handlesMu.Unlock()

	now := time.Now()
	reaped := 0

	for _, op := range ops {
		if activeIDs[op.ID] {
			continue
		}

		// Convert CreatedAt (could be stored as seconds or nanoseconds) to time.Time.
		var created time.Time
		if op.CreatedAt > 5e10 { // nano threshold
			created = time.Unix(0, op.CreatedAt)
		} else {
			created = time.Unix(op.CreatedAt, 0)
		}

		// Apply startup grace period logic:
		// 1. If created before server start, give a grace period of ageThreshold from server startup time.
		// 2. If created after server start, give a grace period of ageThreshold from the op's creation time.
		var age time.Duration
		if created.Before(s.startTime) {
			age = now.Sub(s.startTime)
		} else {
			age = now.Sub(created)
		}

		if age > ageThreshold {
			slog.Info("reaper abandoning write op", "id", op.ID, "created", created, "age", age)
			if err := s.db.AbandonWriteOp(ctx, op.ID); err != nil {
				return reaped, err
			}
			reaped++
		}
	}

	return reaped, nil
}
