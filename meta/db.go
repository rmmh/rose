package meta

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// DB provides thread-safe access to the Rose metadata.
type DB struct {
	db *sql.DB
}

// Open creates or opens a metadata database at the given path.
func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	return &DB{db: db}, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		-- Enable write-ahead logging for better concurrency and durability
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = FULL;

		CREATE TABLE IF NOT EXISTS file (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL,
			mtime INTEGER NOT NULL,
			chunks BLOB NOT NULL DEFAULT ''
		);

		-- The current namespace points at immutable file versions.  Snapshots
		-- retain those version IDs even after a head is replaced or removed.
		CREATE TABLE IF NOT EXISTS file_head (
			path TEXT PRIMARY KEY,
			file_id INTEGER NOT NULL REFERENCES file(id)
		);

		CREATE TABLE IF NOT EXISTS snapshot (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			created_at INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS snapshot_file (
			snapshot_id INTEGER NOT NULL REFERENCES snapshot(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			file_id INTEGER NOT NULL REFERENCES file(id),
			PRIMARY KEY (snapshot_id, path)
		);

		CREATE TABLE IF NOT EXISTS chunk (
			hash BLOB PRIMARY KEY,
			refcount INTEGER NOT NULL DEFAULT 0,
			vlog_id INTEGER NOT NULL,
			vaddr_offset INTEGER NOT NULL,
			logical_len INTEGER NOT NULL,
			compressed_len INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS vlog (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			length INTEGER NOT NULL DEFAULT 0,
			protection_scheme TEXT NOT NULL,
			data_shards INTEGER NOT NULL,
			parity_shards INTEGER NOT NULL
		);

		-- Maps plogs to their containing vlog
		CREATE TABLE IF NOT EXISTS vlog_plog (
			vlog_id INTEGER NOT NULL,
			shard_idx INTEGER NOT NULL, -- 0 for duplicate, 1..N+K for EC
			plog_id INTEGER NOT NULL,
			PRIMARY KEY (vlog_id, shard_idx),
			FOREIGN KEY (vlog_id) REFERENCES vlog(id) ON DELETE CASCADE
		);

		CREATE TABLE IF NOT EXISTS plog (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			disk_id INTEGER NOT NULL,
			length INTEGER NOT NULL DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS node (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			mac TEXT UNIQUE NOT NULL,
			hostname TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS disk (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			node_id INTEGER NOT NULL,
			total_bytes INTEGER NOT NULL,
			used_bytes INTEGER NOT NULL,
			FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE
		);
	`)
	if err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	// Existing databases predate file_head.  Preserve their newest immutable
	// version as the live namespace entry during the lightweight migration.
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO file_head (path, file_id)
		SELECT f.path, f.id FROM file AS f
		WHERE f.id = (SELECT MAX(latest.id) FROM file AS latest WHERE latest.path = f.path)
	`); err != nil {
		return fmt.Errorf("backfill file heads: %w", err)
	}
	return nil
}
