package meta

import (
	"context"
	"fmt"
)

// Node liveness states, mirroring RoseStorage's node_state. A node is working
// while reachable and failed while offline; a failed node's disks drop out of
// the live set (DiskLive) without their disk_state changing, so the loss is
// transient and reverses when the node returns.
const (
	NodeWorking = "working"
	NodeFailed  = "failed"
)

// NodeInfo is one row of the durable node catalog.
type NodeInfo struct {
	ID    uint32
	State string
}

// RegisterNode records a node as working if it is not already known. It is
// idempotent: startup declares the nodes its configured disks live on without
// clobbering a failed state a prior run persisted. The mac/hostname are derived
// placeholders for the local multi-disk shape; a real cluster fills them in.
func (d *DB) RegisterNode(ctx context.Context, id uint32) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO node (id, mac, hostname, state)
		 VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`,
		id, fmt.Sprintf("node-%d-mac", id), fmt.Sprintf("node-%d", id), NodeWorking)
	if err != nil {
		return fmt.Errorf("register node %d: %w", id, err)
	}
	return nil
}

// SetNodeState transitions a node's liveness state.
func (d *DB) SetNodeState(ctx context.Context, id uint32, state string) error {
	res, err := d.db.ExecContext(ctx, "UPDATE node SET state = ? WHERE id = ?", state, id)
	if err != nil {
		return fmt.Errorf("set node %d state: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("set node %d state: no such node", id)
	}
	return nil
}

// ListNodes returns the durable node catalog ordered by id.
func (d *DB) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, state FROM node ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NodeInfo
	for rows.Next() {
		var info NodeInfo
		if err := rows.Scan(&info.ID, &info.State); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}
