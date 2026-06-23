package meta

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DirEntry is one immediate child of a directory returned by ListDir.
type DirEntry struct {
	Name  string
	IsDir bool
	Size  int64
	Mtime int64
}

// splitPath separates a namespace path into its containing directory and base
// name.  Leading slashes are stripped so "/a/b" and "a/b" share one canonical
// form; a root entry ("a") has an empty parent.
func splitPath(path string) (parent, name string) {
	path = strings.TrimLeft(path, "/")
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[:i], path[i+1:]
	}
	return "", path
}

// cleanPath returns the canonical (leading-slash-stripped) form stored in the
// namespace, so callers can pass either form.
func cleanPath(path string) string {
	return strings.TrimLeft(path, "/")
}

// ancestorsOf lists every ancestor directory of path, deepest first.  For
// "a/b/c" it yields "a/b" then "a".
func ancestorsOf(path string) []string {
	path = cleanPath(path)
	var out []string
	for {
		parent, _ := splitPath(path)
		if parent == "" {
			break
		}
		out = append(out, parent)
		path = parent
	}
	return out
}

// ensureDirs inserts a dir row for every ancestor directory of path that does
// not already have one, within the given transaction.
func ensureDirs(ctx context.Context, tx *sql.Tx, path string, mtime int64) error {
	for _, dir := range ancestorsOf(path) {
		parent, name := splitPath(dir)
		if _, err := tx.ExecContext(ctx, `INSERT INTO dir (path, parent, name, mtime)
			VALUES (?, ?, ?, ?) ON CONFLICT(path) DO NOTHING`, dir, parent, name, mtime); err != nil {
			return fmt.Errorf("ensure dir %q: %w", dir, err)
		}
	}
	return nil
}

// ListDir returns the immediate children of a directory: its subdirectories and
// the files directly under it.  Each side is one indexed equality scan on the
// parent column, so the cost scales with the number of children, not the size of
// the subtree.  dir "" is the namespace root.
func (d *DB) ListDir(ctx context.Context, dir string) ([]DirEntry, error) {
	dir = cleanPath(dir)
	var out []DirEntry

	dirRows, err := d.db.QueryContext(ctx, `SELECT name, mtime FROM dir WHERE parent = ? ORDER BY name`, dir)
	if err != nil {
		return nil, err
	}
	defer dirRows.Close()
	for dirRows.Next() {
		var e DirEntry
		e.IsDir = true
		if err := dirRows.Scan(&e.Name, &e.Mtime); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := dirRows.Err(); err != nil {
		return nil, err
	}

	fileRows, err := d.db.QueryContext(ctx, `SELECT fh.name, f.mtime, f.chunks
		FROM file_head fh JOIN file f ON f.id = fh.file_id
		WHERE fh.parent = ? ORDER BY fh.name`, dir)
	if err != nil {
		return nil, err
	}
	defer fileRows.Close()
	for fileRows.Next() {
		var e DirEntry
		var chunks []byte
		if err := fileRows.Scan(&e.Name, &e.Mtime, &chunks); err != nil {
			return nil, err
		}
		e.Size = chunksLogicalSize(chunks)
		out = append(out, e)
	}
	return out, fileRows.Err()
}

// StatPath resolves a path to its directory entry, reporting whether it exists
// and whether it is a directory.  A file head is checked first, then the dir
// table; the namespace root ("") is always a directory.
func (d *DB) StatPath(ctx context.Context, path string) (DirEntry, bool, error) {
	path = cleanPath(path)
	if path == "" {
		return DirEntry{Name: "", IsDir: true}, true, nil
	}
	_, name := splitPath(path)

	var chunks []byte
	var mtime int64
	err := d.db.QueryRowContext(ctx, `SELECT f.mtime, f.chunks FROM file_head fh
		JOIN file f ON f.id = fh.file_id WHERE fh.path = ?`, path).Scan(&mtime, &chunks)
	if err == nil {
		return DirEntry{Name: name, IsDir: false, Size: chunksLogicalSize(chunks), Mtime: mtime}, true, nil
	}
	if err != sql.ErrNoRows {
		return DirEntry{}, false, err
	}

	err = d.db.QueryRowContext(ctx, `SELECT mtime FROM dir WHERE path = ?`, path).Scan(&mtime)
	if err == nil {
		return DirEntry{Name: name, IsDir: true, Mtime: mtime}, true, nil
	}
	if err == sql.ErrNoRows {
		return DirEntry{}, false, nil
	}
	return DirEntry{}, false, err
}

// Mkdir creates an explicit directory (and any missing ancestors).  It fails if
// a file already occupies the path; creating an existing directory is a no-op.
func (d *DB) Mkdir(ctx context.Context, path string, mtime int64) error {
	path = cleanPath(path)
	if path == "" {
		return nil
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_head WHERE path = ?`, path).Scan(&exists); err == nil {
		return fmt.Errorf("mkdir %q: a file exists at that path", path)
	} else if err != sql.ErrNoRows {
		return err
	}
	if err := ensureDirs(ctx, tx, path, mtime); err != nil {
		return err
	}
	parent, name := splitPath(path)
	if _, err := tx.ExecContext(ctx, `INSERT INTO dir (path, parent, name, mtime)
		VALUES (?, ?, ?, ?) ON CONFLICT(path) DO NOTHING`, path, parent, name, mtime); err != nil {
		return err
	}
	return tx.Commit()
}

// Rmdir removes an empty directory.  It fails if the directory still has any
// child file or subdirectory.
func (d *DB) Rmdir(ctx context.Context, path string) error {
	path = cleanPath(path)
	if path == "" {
		return fmt.Errorf("rmdir: cannot remove the namespace root")
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var child int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_head WHERE parent = ? LIMIT 1`, path).Scan(&child); err == nil {
		return fmt.Errorf("rmdir %q: directory not empty", path)
	} else if err != sql.ErrNoRows {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM dir WHERE parent = ? LIMIT 1`, path).Scan(&child); err == nil {
		return fmt.Errorf("rmdir %q: directory not empty", path)
	} else if err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM dir WHERE path = ?`, path); err != nil {
		return err
	}
	return tx.Commit()
}

// chunksLogicalSize sums the logical lengths packed into a file's chunk blob
// (15-byte hash + 4-byte little-endian length per chunk).
func chunksLogicalSize(chunks []byte) int64 {
	var size int64
	for i := 0; i+19 <= len(chunks); i += 19 {
		size += int64(uint32(chunks[i+15]) | uint32(chunks[i+16])<<8 | uint32(chunks[i+17])<<16 | uint32(chunks[i+18])<<24)
	}
	return size
}

// backfillNamespace populates parent/name on file_head rows that lack them and
// creates the ancestor dir rows for the existing namespace.  It is idempotent.
func backfillNamespace(db *sql.DB) error {
	rows, err := db.Query(`SELECT path FROM file_head WHERE name = '' AND path != ''`)
	if err != nil {
		return err
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return err
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(paths) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ctx := context.Background()
	for _, p := range paths {
		parent, name := splitPath(p)
		if _, err := tx.ExecContext(ctx, `UPDATE file_head SET parent = ?, name = ? WHERE path = ?`, parent, name, p); err != nil {
			return err
		}
		if err := ensureDirs(ctx, tx, p, 0); err != nil {
			return err
		}
	}
	return tx.Commit()
}
