package meta

import (
	"context"
	"fmt"

	"github.com/rmmh/rose/uid"
)

// MakeRepairPlog marks ownership in the allocation statement, before a crash can
// strand a destination indistinguishable from an intentionally unassigned raw
// plog. The marker remains after publication; a mapping prevents reclamation.
func (d *DB) MakeRepairPlog(ctx context.Context, u uid.UID, diskID uint32) (uint32, error) {
	res, err := d.db.ExecContext(ctx, "INSERT INTO plog(uid,disk_id,repair_owned) VALUES(?,?,1)", u[:], diskID)
	if err != nil {
		return 0, fmt.Errorf("make repair plog: %w", err)
	}
	id, err := res.LastInsertId()
	return uint32(id), err
}

// RetireUnassignedRepairPlogs runs only during quiescent recovery, before any
// repair or raw I/O starts. The deletion rechecks ownership and mapping in the
// same transaction. Physical cleanup must wait for successful transaction commit.
func (d *DB) RetireUnassignedRepairPlogs(ctx context.Context) ([]PlogInfo, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `DELETE FROM plog WHERE repair_owned=1
 AND NOT EXISTS (SELECT 1 FROM vlog_plog WHERE plog_id=plog.id)
 RETURNING id,disk_id`)
	if err != nil {
		return nil, err
	}
	var out []PlogInfo
	for rows.Next() {
		var p PlogInfo
		if err := rows.Scan(&p.ID, &p.DiskID); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, p)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
