package meta

import (
	"context"
	"fmt"
)

// GCFileVersions deletes at most limit immutable versions with no durable owner.
// Root removal already applied all chunk reference deltas: deleting an unowned
// version must not decrement references again. Handles cache their ordered
// extents/mtime and pin content independently of this row. Committed operations
// (including legacy results without retention roots) still need their version
// for retry validation and are preserved until their own retirement policy acts.
func (d *DB) GCFileVersions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("version collection limit must be positive")
	}
	result, err := d.db.ExecContext(ctx, `DELETE FROM file WHERE id IN (
		SELECT f.id FROM file f
		WHERE NOT EXISTS (SELECT 1 FROM file_head WHERE file_id=f.id)
		AND NOT EXISTS (SELECT 1 FROM snapshot_file WHERE file_id=f.id)
		AND NOT EXISTS (SELECT 1 FROM write_result_root WHERE file_id=f.id)
		AND NOT EXISTS (SELECT 1 FROM write_op WHERE file_id=f.id AND state IN ('prepared','committed'))
		ORDER BY f.id LIMIT ?)`, limit)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	return int(n), err
}
