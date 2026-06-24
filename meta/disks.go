package meta

import (
	"context"
	"fmt"

	"github.com/rmmh/rose/uid"
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
	UID    uid.UID
	NodeID uint32
	State  string
}

// RegisterDisk records a disk as active if it is not already known. It is
// idempotent: startup declares its configured disks without clobbering a
// draining/failed/detached state a prior maintenance operation persisted.
func (d *DB) RegisterDisk(ctx context.Context, id, nodeID uint32, u uid.UID) error {
	return d.RegisterDiskWithCapacity(ctx, id, nodeID, 0, u)
}

// RegisterDiskWithCapacity is RegisterDisk with the capacity reported by the
// control-plane AddDisk RPC. Existing catalog entries are never overwritten, so
// a disk's UID is fixed at first registration. u carries the identity adopted
// from (or freshly written to) the disk's rose_disk_uid marker.
func (d *DB) RegisterDiskWithCapacity(ctx context.Context, id, nodeID uint32, totalBytes uint64, u uid.UID) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO disk (id, uid, node_id, total_bytes, used_bytes, state)
		 VALUES (?, ?, ?, ?, 0, ?) ON CONFLICT(id) DO NOTHING`,
		id, u[:], nodeID, totalBytes, DiskActive)
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
	rows, err := d.db.QueryContext(ctx, "SELECT id, uid, node_id, state FROM disk ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DiskInfo
	for rows.Next() {
		var info DiskInfo
		var rawUID []byte
		if err := rows.Scan(&info.ID, &rawUID, &info.NodeID, &info.State); err != nil {
			return nil, err
		}
		if info.UID, err = uid.FromBytes(rawUID); err != nil {
			return nil, fmt.Errorf("decode disk %d uid: %w", info.ID, err)
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// DiskUID returns the persistent UID recorded for a disk, or the zero UID if the
// disk is unknown.
func (d *DB) DiskUID(ctx context.Context, diskID uint32) (uid.UID, error) {
	var rawUID []byte
	err := d.db.QueryRowContext(ctx, "SELECT uid FROM disk WHERE id = ?", diskID).Scan(&rawUID)
	if err != nil {
		return uid.UID{}, err
	}
	return uid.FromBytes(rawUID)
}

// VlogShardDisk maps one shard of a vlog to the disk currently backing it.
type VlogShardDisk struct {
	ShardIndex int
	DiskID     uint32
}

// PlogOnDisk is one plog stored on a disk, with the vlog shard it backs.
type PlogOnDisk struct {
	PlogID     uint32
	VlogID     uint32
	ShardIndex int
	Length     int64
}

// PlogsOnDisk lists the plogs physically stored on a disk together with the vlog
// shard each one backs. Draining a disk migrates exactly these plogs elsewhere.
func (d *DB) PlogsOnDisk(ctx context.Context, diskID uint32) ([]PlogOnDisk, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT p.id, vp.vlog_id, vp.shard_idx, p.length
		FROM plog p JOIN vlog_plog vp ON vp.plog_id = p.id
		WHERE p.disk_id = ? ORDER BY p.id`, diskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlogOnDisk
	for rows.Next() {
		var p PlogOnDisk
		if err := rows.Scan(&p.PlogID, &p.VlogID, &p.ShardIndex, &p.Length); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MovePlogToDisk repoints a plog at a new disk. The bytes must already be durable
// at the destination path; this atomic metadata flip is what makes the plog
// resolve to its new home, so a crash before it leaves the old copy authoritative.
func (d *DB) MovePlogToDisk(ctx context.Context, plogID, newDiskID uint32) error {
	_, err := d.db.ExecContext(ctx, "UPDATE plog SET disk_id = ? WHERE id = ?", newDiskID, plogID)
	return err
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
