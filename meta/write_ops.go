package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	WriteOpPrepared  = "prepared"
	WriteOpCommitted = "committed"
	WriteOpAbandoned = "abandoned"
)

type WriteOp struct {
	ID                 int64
	IdempotencyKey     string
	Path               string
	State              string
	FileID             int64
	AcknowledgedOffset int64
	Tail               []byte
}

// CreateWriteOp records the client's stable write intent before any bytes are
// written.  A duplicate key returns the original operation, allowing callers to
// retry after an unknown RPC outcome without creating another append.
func (d *DB) CreateWriteOp(ctx context.Context, key, path string) (WriteOp, error) {
	if key == "" || path == "" {
		return WriteOp{}, fmt.Errorf("write operation key and path are required")
	}
	res, err := d.db.ExecContext(ctx, "INSERT INTO write_op (idempotency_key, path, state, created_at) VALUES (?, ?, ?, ?)", key, path, WriteOpPrepared, time.Now().UnixNano())
	if err != nil {
		var op WriteOp
		lookupErr := d.db.QueryRowContext(ctx, "SELECT id, idempotency_key, path, state, file_id, acknowledged_offset, tail FROM write_op WHERE idempotency_key = ?", key).Scan(&op.ID, &op.IdempotencyKey, &op.Path, &op.State, &op.FileID, &op.AcknowledgedOffset, &op.Tail)
		if lookupErr != nil {
			return WriteOp{}, fmt.Errorf("create write op: %w", err)
		}
		return op, nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return WriteOp{}, err
	}
	return WriteOp{ID: id, IdempotencyKey: key, Path: path, State: WriteOpPrepared}, nil
}

func (d *DB) WriteOpByKey(ctx context.Context, key string) (WriteOp, error) {
	var op WriteOp
	err := d.db.QueryRowContext(ctx, "SELECT id, idempotency_key, path, state, file_id, acknowledged_offset, tail FROM write_op WHERE idempotency_key = ?", key).Scan(&op.ID, &op.IdempotencyKey, &op.Path, &op.State, &op.FileID, &op.AcknowledgedOffset, &op.Tail)
	if err != nil {
		return WriteOp{}, err
	}
	return op, nil
}

func (d *DB) ClaimVlogLease(ctx context.Context, vlogID uint32, opID int64, ordinal int) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT state FROM write_op WHERE id = ?", opID).Scan(&state); err != nil {
		return err
	}
	if state != WriteOpPrepared {
		return fmt.Errorf("write operation %d is not prepared", opID)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO vlog_lease (vlog_id, write_op_id, ordinal) VALUES (?, ?, ?)", vlogID, opID, ordinal); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) ReleaseWriteOpLeases(ctx context.Context, opID int64) error {
	_, err := d.db.ExecContext(ctx, "DELETE FROM vlog_lease WHERE write_op_id = ?", opID)
	return err
}

func (d *DB) WriteOpLeases(ctx context.Context, opID int64) ([]uint32, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT vlog_id FROM vlog_lease WHERE write_op_id = ? ORDER BY ordinal", opID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uint32
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (d *DB) VlogLeased(ctx context.Context, vlogID uint32) (bool, error) {
	var one int
	err := d.db.QueryRowContext(ctx, "SELECT 1 FROM vlog_lease WHERE vlog_id = ?", vlogID).Scan(&one)
	if err == nil {
		return true, nil
	}
	if err == sql.ErrNoRows {
		return false, nil
	}
	return false, err
}

func (d *DB) AbandonWriteOp(ctx context.Context, id int64) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE write_op SET state = ? WHERE id = ? AND state = ?", WriteOpAbandoned, id, WriteOpPrepared); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM vlog_lease WHERE write_op_id = ?", id); err != nil {
		return err
	}
	return tx.Commit()
}

// CommitWriteOpVersion publishes an explicit ordered placement list as the
// operation's committed file version, then marks the operation committed and
// releases its leases in the same metadata transaction. The splice path computes
// the final placement list directly (a mix of reused base chunks and freshly
// stored window chunks), and the bytes of any new chunk are already durable in
// their vlogs before this is called. Repeating it after commit is a no-op that
// returns the published file id, keeping Close idempotent by key.
func (d *DB) CommitWriteOpVersion(ctx context.Context, opID int64, path string, mtime int64, placements []ChunkPlacement) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var state string
	var fileID int64
	if err := tx.QueryRowContext(ctx, "SELECT state, file_id FROM write_op WHERE id = ?", opID).Scan(&state, &fileID); err != nil {
		return 0, err
	}
	if state == WriteOpCommitted {
		return fileID, nil
	}
	if state != WriteOpPrepared {
		return 0, fmt.Errorf("write operation %d is %s", opID, state)
	}
	fileID, err = publishFileVersion(ctx, tx, path, mtime, placements)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE write_op SET state = ?, file_id = ?, tail = X'' WHERE id = ?", WriteOpCommitted, fileID, opID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM vlog_lease WHERE write_op_id = ?", opID); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return fileID, nil
}
