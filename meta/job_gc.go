package meta

import (
	"context"
	"fmt"
)

// GCTerminalVlogJobs bounds automatic maintenance history. A live destination
// still needs its job row to prevent another job claiming the same output.
// Source IDs alone do not require terminal history: automatic passes resume only
// running jobs and allocate fresh IDs for later work. Public disk-operation jobs
// retain their retry/status records. No physical resource is deleted here.
func (d *DB) GCTerminalVlogJobs(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("job collection limit must be positive")
	}
	res, err := d.db.ExecContext(ctx, `DELETE FROM job WHERE id IN (
 SELECT j.id FROM job j
 WHERE j.state IN ('done','cancelled')
 AND j.kind IN ('compact','promote','scrubrepair')
 AND j.target_disk=0 AND j.dest_disk=0
 AND NOT EXISTS(SELECT 1 FROM vlog WHERE id=j.dest_vlog)
 ORDER BY j.id LIMIT ?)`, limit)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}
