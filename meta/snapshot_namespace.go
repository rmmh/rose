package meta

import (
	"context"
	"database/sql"
	"fmt"
)

func snapshotStat(ctx context.Context, tx *sql.Tx, id uint64, path string) (DirEntry, bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM snapshot WHERE id=?)", id).Scan(&exists); err != nil {
		return DirEntry{}, false, err
	}
	if !exists {
		return DirEntry{}, false, fmt.Errorf("snapshot %d not found", id)
	}
	path = cleanPath(path)
	if path == "" {
		return DirEntry{IsDir: true}, true, nil
	}
	_, name := splitPath(path)
	var mtime int64
	var chunks []byte
	err := tx.QueryRowContext(ctx, `SELECT sf.mtime,f.chunks FROM snapshot_file sf JOIN file f ON f.id=sf.file_id WHERE sf.snapshot_id=? AND sf.path=?`, id, path).Scan(&mtime, &chunks)
	if err == nil {
		return DirEntry{Name: name, Mtime: mtime, Size: chunksLogicalSize(chunks)}, true, nil
	}
	if err != sql.ErrNoRows {
		return DirEntry{}, false, err
	}
	err = tx.QueryRowContext(ctx, "SELECT mtime FROM snapshot_dir WHERE snapshot_id=? AND path=?", id, path).Scan(&mtime)
	if err == sql.ErrNoRows {
		return DirEntry{}, false, nil
	}
	if err != nil {
		return DirEntry{}, false, err
	}
	return DirEntry{Name: name, IsDir: true, Mtime: mtime}, true, nil
}

// StatSnapshotPath and ListSnapshotDir read one immutable namespace transaction,
// including explicit empty directories. Deleting a snapshot concurrently cannot
// turn an existing directory into a successful empty listing mid-request.
func (d *DB) StatSnapshotPath(ctx context.Context, id uint64, path string) (DirEntry, bool, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DirEntry{}, false, err
	}
	defer tx.Rollback()
	return snapshotStat(ctx, tx, id, path)
}

func (d *DB) ListSnapshotDir(ctx context.Context, id uint64, path string) ([]DirEntry, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	e, exists, err := snapshotStat(ctx, tx, id, path)
	if err != nil {
		return nil, err
	}
	if !exists || !e.IsDir {
		return nil, fmt.Errorf("snapshot directory %q not found", path)
	}
	rows, err := tx.QueryContext(ctx, `SELECT name,1,mtime,X'' FROM snapshot_dir WHERE snapshot_id=? AND parent=?
 UNION ALL SELECT sf.name,0,sf.mtime,f.chunks FROM snapshot_file sf JOIN file f ON f.id=sf.file_id WHERE sf.snapshot_id=? AND sf.parent=? ORDER BY 1`, id, cleanPath(path), id, cleanPath(path))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []DirEntry
	for rows.Next() {
		var e DirEntry
		var chunks []byte
		if err := rows.Scan(&e.Name, &e.IsDir, &e.Mtime, &chunks); err != nil {
			return nil, err
		}
		if !e.IsDir {
			e.Size = chunksLogicalSize(chunks)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
