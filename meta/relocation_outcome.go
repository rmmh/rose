package meta

import (
	"context"
	"fmt"
)

// RelocationCommitError means that publication was attempted. Callers must not
// infer rollback from this error or delete a candidate until resolving ownership.
// MovePlogToDisk also returns the attempted post-update generations in this case.
type RelocationCommitError struct{ Err error }

func (e *RelocationCommitError) Error() string {
	return fmt.Sprintf("relocation commit outcome uncertain: %v", e.Err)
}
func (e *RelocationCommitError) Unwrap() error { return e.Err }

// ResolvePlogRelocation recognizes only this attempt's unchanged source or exact
// post-update destination. A returned/reused placement must remain unresolved.
// The caller must retain topology ownership through reconciliation and cleanup.
func (d *DB) ResolvePlogRelocation(ctx context.Context, plogID, vlogID, source, destination uint32, before, after RelocationEpochs) (uint32, error) {
	var disk uint32
	var current RelocationEpochs
	err := d.db.QueryRowContext(ctx, `SELECT p.disk_id,p.placement_epoch,v.placement_epoch
 FROM plog p JOIN vlog_plog vp ON vp.plog_id=p.id JOIN vlog v ON v.id=vp.vlog_id
 WHERE p.id=? AND v.id=? AND (SELECT count(*) FROM vlog_plog WHERE plog_id=p.id)=1`, plogID, vlogID).Scan(&disk, &current.Plog, &current.Vlog)
	if err != nil {
		return 0, err
	}
	if (disk == source && current == before) || (disk == destination && current == after) {
		return disk, nil
	}
	return 0, fmt.Errorf("relocation %d placement changed during outcome resolution", plogID)
}
