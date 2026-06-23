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

const (
	WriteChunkPlanned = "planned"
	WriteChunkDurable = "durable"
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

type WriteOpChunk struct {
	Index       int
	Data        []byte
	Hash        []byte
	VlogID      uint32
	VaddrOffset int64
	LogicalLen  int
	State       string
}

// CreateWriteOp stores a complete immutable write intent before any physical
// shard is written.  A duplicate key returns the original operation, allowing
// callers to retry after an unknown RPC outcome without creating another append.
func (d *DB) CreateWriteOp(ctx context.Context, key, path string, chunks []WriteOpChunk) (WriteOp, error) {
	if key == "" || path == "" {
		return WriteOp{}, fmt.Errorf("write operation key and path are required")
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return WriteOp{}, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "INSERT INTO write_op (idempotency_key, path, state, created_at) VALUES (?, ?, ?, ?)", key, path, WriteOpPrepared, time.Now().UnixNano())
	if err != nil {
		var op WriteOp
		lookupErr := tx.QueryRowContext(ctx, "SELECT id, idempotency_key, path, state, file_id, acknowledged_offset, tail FROM write_op WHERE idempotency_key = ?", key).Scan(&op.ID, &op.IdempotencyKey, &op.Path, &op.State, &op.FileID, &op.AcknowledgedOffset, &op.Tail)
		if lookupErr != nil {
			return WriteOp{}, fmt.Errorf("create write op: %w", err)
		}
		return op, nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return WriteOp{}, err
	}
	for _, c := range chunks {
		if _, err := tx.ExecContext(ctx, "INSERT INTO write_op_chunk (write_op_id, chunk_idx, data, hash, vlog_id, vaddr_offset, logical_len, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", id, c.Index, c.Data, c.Hash, c.VlogID, c.VaddrOffset, c.LogicalLen, WriteChunkPlanned); err != nil {
			return WriteOp{}, err
		}
	}
	if err := tx.Commit(); err != nil {
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

func (d *DB) PreparedWriteOps(ctx context.Context) ([]WriteOp, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, idempotency_key, path, state, file_id, acknowledged_offset, tail FROM write_op WHERE state = ? ORDER BY id", WriteOpPrepared)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ops []WriteOp
	for rows.Next() {
		var op WriteOp
		if err := rows.Scan(&op.ID, &op.IdempotencyKey, &op.Path, &op.State, &op.FileID, &op.AcknowledgedOffset, &op.Tail); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}

func (d *DB) UpdateWriteOpStream(ctx context.Context, id, acknowledged int64, tail []byte) error {
	if tail == nil {
		tail = []byte{}
	}
	res, err := d.db.ExecContext(ctx, "UPDATE write_op SET acknowledged_offset = ?, tail = ? WHERE id = ? AND state = ?", acknowledged, tail, id, WriteOpPrepared)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("write operation %d is not prepared", id)
	}
	return nil
}

// AppendWriteOpChunk records the exact reserved placement before any shard is
// written.  Chunk indexes are monotonic per operation and cannot be replaced.
func (d *DB) AppendWriteOpChunk(ctx context.Context, opID int64, chunk WriteOpChunk) error {
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
	_, err = tx.ExecContext(ctx, "INSERT INTO write_op_chunk (write_op_id, chunk_idx, data, hash, vlog_id, vaddr_offset, logical_len, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", opID, chunk.Index, chunk.Data, chunk.Hash, chunk.VlogID, chunk.VaddrOffset, chunk.LogicalLen, WriteChunkPlanned)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) MarkWriteOpChunkDurable(ctx context.Context, opID int64, index int) error {
	res, err := d.db.ExecContext(ctx, "UPDATE write_op_chunk SET state = ? WHERE write_op_id = ? AND chunk_idx = ? AND state = ?", WriteChunkDurable, opID, index, WriteChunkPlanned)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var state string
		if err := d.db.QueryRowContext(ctx, "SELECT state FROM write_op_chunk WHERE write_op_id = ? AND chunk_idx = ?", opID, index).Scan(&state); err != nil {
			return err
		}
		if state != WriteChunkDurable {
			return fmt.Errorf("chunk %d for operation %d is not durable", index, opID)
		}
	}
	return nil
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

func (d *DB) WriteOpChunks(ctx context.Context, id int64) ([]WriteOpChunk, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT chunk_idx, data, hash, vlog_id, vaddr_offset, logical_len, state FROM write_op_chunk WHERE write_op_id = ? ORDER BY chunk_idx", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WriteOpChunk
	for rows.Next() {
		var c WriteOpChunk
		if err := rows.Scan(&c.Index, &c.Data, &c.Hash, &c.VlogID, &c.VaddrOffset, &c.LogicalLen, &c.State); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
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

// CommitWriteOp publishes every durable planned chunk as one immutable file
// version, marks the operation committed, and releases its leases in the same
// metadata transaction.  Physical vlog commits must have completed first.
func (d *DB) CommitWriteOp(ctx context.Context, opID int64, mtime int64) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var op WriteOp
	if err := tx.QueryRowContext(ctx, "SELECT id, idempotency_key, path, state, file_id, acknowledged_offset, tail FROM write_op WHERE id = ?", opID).Scan(&op.ID, &op.IdempotencyKey, &op.Path, &op.State, &op.FileID, &op.AcknowledgedOffset, &op.Tail); err != nil {
		return 0, err
	}
	if op.State == WriteOpCommitted {
		return op.FileID, nil
	}
	if op.State != WriteOpPrepared {
		return 0, fmt.Errorf("write operation %d is %s", opID, op.State)
	}
	rows, err := tx.QueryContext(ctx, "SELECT chunk_idx, hash, vlog_id, vaddr_offset, logical_len, state FROM write_op_chunk WHERE write_op_id = ? ORDER BY chunk_idx", opID)
	if err != nil {
		return 0, err
	}
	var placements []ChunkPlacement
	for rows.Next() {
		var idx int
		var p ChunkPlacement
		var state string
		if err := rows.Scan(&idx, &p.Hash, &p.VlogID, &p.VaddrOffset, &p.LogicalLen, &state); err != nil {
			rows.Close()
			return 0, err
		}
		if state != WriteChunkDurable {
			rows.Close()
			return 0, fmt.Errorf("write operation %d chunk %d is not durable", opID, idx)
		}
		p.CompressedLen = p.LogicalLen
		placements = append(placements, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	fileID, err := publishFileVersion(ctx, tx, op.Path, mtime, placements)
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

// CommitWriteOpVersion publishes an explicit ordered placement list as the
// operation's committed file version, then marks the operation committed and
// releases its leases in the same metadata transaction. Unlike CommitWriteOp it
// does not derive the version from the write_op_chunk rows: the splice path
// computes the final placement list directly (a mix of reused base chunks and
// freshly stored window chunks), and the bytes of any new chunk are already
// durable in their vlogs before this is called. Repeating it after commit is a
// no-op that returns the published file id, keeping Close idempotent by key.
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
