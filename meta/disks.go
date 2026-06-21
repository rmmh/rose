package meta

import (
	"context"
	"fmt"
)

// Disk lifecycle states, mirroring RoseStorage's disk_state. A disk is created
// active, moves to draining while its shards are being evacuated, and finally to
// detached once empty; an abrupt loss moves it straight to failed.
const (
	DiskActive   = "active"
	DiskDraining = "draining"
	DiskFailed   = "failed"
	DiskDetached = "detached"
)

// DiskInfo is one row of the durable disk catalog.
type DiskInfo struct {
	ID     uint32
	NodeID uint32
	State  string
}

// RegisterDisk records a disk as active if it is not already known. It is
// idempotent: startup declares its configured disks without clobbering a
// draining/failed/detached state a prior maintenance operation persisted.
func (d *DB) RegisterDisk(ctx context.Context, id, nodeID uint32) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO disk (id, node_id, total_bytes, used_bytes, state)
		 VALUES (?, ?, 0, 0, ?) ON CONFLICT(id) DO NOTHING`,
		id, nodeID, DiskActive)
	if err != nil {
		return fmt.Errorf("register disk %d: %w", id, err)
	}
	return nil
}

// SetDiskState transitions a disk's lifecycle state.
func (d *DB) SetDiskState(ctx context.Context, id uint32, state string) error {
	res, err := d.db.ExecContext(ctx, "UPDATE disk SET state = ? WHERE id = ?", state, id)
	if err != nil {
		return fmt.Errorf("set disk %d state: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("set disk %d state: no such disk", id)
	}
	return nil
}

// ListDisks returns the durable disk catalog ordered by id.
func (d *DB) ListDisks(ctx context.Context) ([]DiskInfo, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, node_id, state FROM disk ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiskInfo
	for rows.Next() {
		var info DiskInfo
		if err := rows.Scan(&info.ID, &info.NodeID, &info.State); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// VlogShardDisk maps one shard of a vlog to the disk currently backing it.
type VlogShardDisk struct {
	ShardIndex int
	DiskID     uint32
}

// VlogShardDisks lists, per shard index, the disk that stores that shard's plog.
// Live-shard accounting for commit/read durability gating cross-references these
// disk IDs against the disk catalog's lifecycle states.
func (d *DB) VlogShardDisks(ctx context.Context, vlogID uint32) ([]VlogShardDisk, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT vp.shard_idx, p.disk_id
		FROM vlog_plog vp JOIN plog p ON p.id = vp.plog_id
		WHERE vp.vlog_id = ? ORDER BY vp.shard_idx`, vlogID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VlogShardDisk
	for rows.Next() {
		var s VlogShardDisk
		if err := rows.Scan(&s.ShardIndex, &s.DiskID); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
