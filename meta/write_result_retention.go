package meta

import (
	"context"
	"fmt"
)

// ExpireWriteResults releases elapsed retry roots and fences their keys in one
// transaction. It does not delete historical operation rows or reuse keys.
// A deadline equal to now is expired. Calling it again cannot decrement twice.
func (d *DB) ExpireWriteResults(ctx context.Context, now int64) (int, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT write_op_id,file_id FROM write_result_root WHERE expires_at<=? ORDER BY write_op_id", now)
	if err != nil {
		return 0, err
	}
	type root struct{ op, file int64 }
	var roots []root
	for rows.Next() {
		var r root
		if err := rows.Scan(&r.op, &r.file); err != nil {
			rows.Close()
			return 0, err
		}
		roots = append(roots, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, r := range roots {
		result, err := tx.ExecContext(ctx, "UPDATE write_op SET state=? WHERE id=? AND file_id=? AND state=?", WriteOpExpired, r.op, r.file, WriteOpCommitted)
		if err != nil {
			return 0, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if n != 1 {
			return 0, fmt.Errorf("retry root %d does not own its committed result", r.op)
		}
		chunks, err := fileChunks(ctx, tx, r.file)
		if err != nil {
			return 0, err
		}
		if err := adjustChunkRefs(ctx, tx, chunks, -1); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM write_result_root WHERE write_op_id=?", r.op); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(roots), nil
}
