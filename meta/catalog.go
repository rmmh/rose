package meta

import "context"

// This file holds whole-catalog scans and aggregates the control plane uses to
// reason about the cluster as a unit. They deliberately resolve every vlog in a
// single SQL statement: the per-vlog query helpers (VlogShardDisks et al.) are
// fine for one extent, but issuing one round trip per vlog turns an audit or a
// readiness sweep into millions of statements at petabyte scale, where the
// database/sql call overhead dwarfs the actual work.

// ShardPlacement is one (vlog, shard) -> disk mapping. AllShardPlacements emits
// these ordered by vlog so a caller can group consecutive rows into vlogs.
type ShardPlacement struct {
	VlogID     uint32
	ShardIndex int
	DiskID     uint32
}

// AllShardPlacements streams the disk backing every vlog shard in one scan,
// ordered by vlog then shard. It is the bulk form of VlogShardDisks used by
// placement audits that must visit the entire catalog.
func (d *DB) AllShardPlacements(ctx context.Context, fn func(ShardPlacement)) error {
	rows, err := d.db.QueryContext(ctx, `
		SELECT vp.vlog_id, vp.shard_idx, p.disk_id
		FROM vlog_plog vp JOIN plog p ON p.id = vp.plog_id
		ORDER BY vp.vlog_id, vp.shard_idx`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var s ShardPlacement
		if err := rows.Scan(&s.VlogID, &s.ShardIndex, &s.DiskID); err != nil {
			return err
		}
		fn(s)
	}
	return rows.Err()
}

// PlogCountsByDisk returns, per disk, how many mapped plogs it holds. Disks with
// no plogs are absent from the map.
func (d *DB) PlogCountsByDisk(ctx context.Context) (map[uint32]int, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT p.disk_id, COUNT(*)
		FROM plog p JOIN vlog_plog vp ON vp.plog_id = p.id
		GROUP BY p.disk_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint32]int{}
	for rows.Next() {
		var disk uint32
		var count int
		if err := rows.Scan(&disk, &count); err != nil {
			return nil, err
		}
		out[disk] = count
	}
	return out, rows.Err()
}

// DiskUsageBytes returns, per disk, the summed logical length of the mapped plogs
// it holds. Disks with no plogs are absent from the map.
func (d *DB) DiskUsageBytes(ctx context.Context) (map[uint32]int64, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT p.disk_id, COALESCE(SUM(p.length), 0)
		FROM plog p JOIN vlog_plog vp ON vp.plog_id = p.id
		GROUP BY p.disk_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uint32]int64{}
	for rows.Next() {
		var disk uint32
		var bytes int64
		if err := rows.Scan(&disk, &bytes); err != nil {
			return nil, err
		}
		out[disk] = bytes
	}
	return out, rows.Err()
}

// vlogReadinessQuery counts, per vlog, its total shards and how many are live: a
// shard is live when its disk is active and the disk's node is working, the same
// gate RoseStorage applies for commit durability. The optional filter narrows the
// scan to the vlogs touched by a single disk or node so a failure can be applied
// in time proportional to the affected extents rather than the whole catalog. The
// state literals are package constants, not user input.
func vlogReadinessQuery(filter string) string {
	return `
		SELECT v.id, v.data_shards,
		       COUNT(*) AS total_shards,
		       SUM(CASE WHEN d.state = '` + DiskActive + `' AND n.state = '` + NodeWorking + `'
		                THEN 1 ELSE 0 END) AS live
		FROM vlog v
		JOIN vlog_plog vp ON vp.vlog_id = v.id
		JOIN plog p ON p.id = vp.plog_id
		JOIN disk d ON d.id = p.disk_id
		JOIN node n ON n.id = d.node_id
		` + filter + `
		GROUP BY v.id, v.data_shards`
}

func (d *DB) eachReadiness(ctx context.Context, query string, args []any, fn func(vlogID uint32, commitReady, readable bool)) error {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint32
		var dataShards int32
		var total, live int
		if err := rows.Scan(&id, &dataShards, &total, &live); err != nil {
			return err
		}
		fn(id, live == total, live >= int(dataShards))
	}
	return rows.Err()
}

// EachVlogReadiness reports the commit and read readiness of every vlog in a
// single scan. commitReady means all shards are live; readable means at least
// data_shards remain.
func (d *DB) EachVlogReadiness(ctx context.Context, fn func(vlogID uint32, commitReady, readable bool)) error {
	return d.eachReadiness(ctx, vlogReadinessQuery(""), nil, fn)
}

// EachVlogReadinessOnDisk reports readiness for only the vlogs with a shard on
// the given disk. These are exactly the extents whose readiness a disk state
// change can alter, so an incremental tracker can recompute just them.
func (d *DB) EachVlogReadinessOnDisk(ctx context.Context, diskID uint32, fn func(vlogID uint32, commitReady, readable bool)) error {
	filter := `WHERE v.id IN (
		SELECT vp2.vlog_id FROM vlog_plog vp2
		JOIN plog p2 ON p2.id = vp2.plog_id
		WHERE p2.disk_id = ?)`
	return d.eachReadiness(ctx, vlogReadinessQuery(filter), []any{diskID}, fn)
}

// EachVlogReadinessOnNode reports readiness for only the vlogs with a shard on
// any disk of the given node, the extents a node state change can alter.
func (d *DB) EachVlogReadinessOnNode(ctx context.Context, nodeID uint32, fn func(vlogID uint32, commitReady, readable bool)) error {
	filter := `WHERE v.id IN (
		SELECT vp2.vlog_id FROM vlog_plog vp2
		JOIN plog p2 ON p2.id = vp2.plog_id
		JOIN disk d2 ON d2.id = p2.disk_id
		WHERE d2.node_id = ?)`
	return d.eachReadiness(ctx, vlogReadinessQuery(filter), []any{nodeID}, fn)
}
