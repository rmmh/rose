package meta

import "context"

// RetireUnassignedMaintenanceVlogs reclaims interrupted destination provisioning.
// Ownership is stamped in the initial vlog INSERT, before any physical files or
// job destination are created. Only empty, unleased logs with no chunk rows and
// no job destination reference qualify. Selection and deletion share one write
// transaction, so a concurrently completed assignment cannot be swept.
func (d *DB) RetireUnassignedMaintenanceVlogs(ctx context.Context) ([]PlogInfo, error) {
	_, plogs, err := d.retireUnassignedMaintenance(ctx, 0)
	return plogs, err
}

// RetireUnassignedMaintenanceDestination conditionally removes one abandoned
// destination. A false result means ownership or contents require retaining it.
func (d *DB) RetireUnassignedMaintenanceDestination(ctx context.Context, id uint32) (bool, []PlogInfo, error) {
	if id == 0 {
		return false, nil, nil
	}
	return d.retireUnassignedMaintenance(ctx, id)
}

func (d *DB) retireUnassignedMaintenance(ctx context.Context, only uint32) (bool, []PlogInfo, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM vlog v
		WHERE maintenance_owned=1 AND length=0 AND (?=0 OR id=?)
		AND NOT EXISTS (SELECT 1 FROM job WHERE dest_vlog=v.id)
		AND NOT EXISTS (SELECT 1 FROM chunk WHERE vlog_id=v.id)
		AND NOT EXISTS (SELECT 1 FROM vlog_lease WHERE vlog_id=v.id)`, only, only)
	if err != nil {
		return false, nil, err
	}
	var ids []uint32
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, nil, err
	}
	var removed []PlogInfo
	for _, id := range ids {
		plogs, err := retireVlogTx(ctx, tx, id)
		if err != nil {
			return false, nil, err
		}
		removed = append(removed, plogs...)
	}
	if err := tx.Commit(); err != nil {
		return false, nil, err
	}
	return len(ids) > 0, removed, nil
}
