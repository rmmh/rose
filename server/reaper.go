package server

import (
	"context"
	"log/slog"
	"time"
)

// ReapAbandonedWriteOps fences idle network handles, including readers, then
// abandons prepared operations with no remaining active owner. Handle RPCs
// renew activity; in-process adapters retain an explicit local owner.
// It returns the count of operations abandoned, or an error.
func (s *Server) ReapAbandonedWriteOps(ctx context.Context, ageThreshold time.Duration) (int, error) {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()

	ops, err := s.db.ListPreparedWriteOps(ctx)
	if err != nil {
		return 0, err
	}

	s.handlesMu.Lock()
	type entry struct {
		id int64
		h  *FileHandle
	}
	handles := make([]entry, 0, len(s.handles))
	for id, h := range s.handles {
		handles = append(handles, entry{id, h})
	}
	s.handlesMu.Unlock()

	now := s.now()
	activeIDs := make(map[int64]bool, len(handles))
	expiredIDs := make(map[int64]bool)
	for _, entry := range handles {
		h := entry.h
		h.stateMu.Lock()
		if h.localOwners == 0 && now.Sub(h.lastUsed) > ageThreshold {
			// The same state lock guards request renewal and this removal.
			// Once removed, a request paused after lookup cannot revive the
			// handle or report a successful mutation of an unpinned cache.
			s.handlesMu.Lock()
			delete(s.handles, entry.id)
			s.handlesMu.Unlock()
			s.releasePins(h.pinOwner)
			if h.writeOpID != 0 {
				expiredIDs[h.writeOpID] = true
			}
		} else if h.writeOpID != 0 {
			activeIDs[h.writeOpID] = true
		}
		h.stateMu.Unlock()
	}

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

		if expiredIDs[op.ID] || age > ageThreshold {
			slog.Info("reaper abandoning write op", "id", op.ID, "created", created, "age", age)
			if err := s.db.AbandonWriteOp(ctx, op.ID); err != nil {
				return reaped, err
			}
			// The op references nothing now; drop any chunk pins it held so the
			// chunks it had deduplicated against become reclaimable again.
			s.releasePins(op.ID)
			reaped++
		}
	}

	return reaped, nil
}
