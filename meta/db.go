package meta

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

// DB provides thread-safe access to the Rose metadata.
type DB struct {
	db *sql.DB
}

// Open creates or opens a metadata database at the given path.
func Open(path string) (*DB, error) {
	return open(path, true)
}

// OpenEphemeral opens a process-local metadata catalog for simulations and
// scale tests. It uses SQLite's in-memory database, disables journaling and
// synchronous writes, and constrains the pool to one connection (required for
// :memory: databases). It must never be used for production metadata.
func OpenEphemeral() (*DB, error) {
	return open(":memory:", false)
}

func open(path string, durable bool) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}
	// Serialize all access through a single connection. SQLite admits one writer
	// at a time; with WAL and several pooled connections, concurrent writers from
	// independent gRPC requests race into SQLITE_BUSY/BUSY_SNAPSHOT, which
	// busy_timeout does not retry. One connection turns that contention into
	// in-process queueing instead. Metadata transactions are short, so the
	// throughput cost is small next to the correctness win.
	db.SetMaxOpenConns(1)

	if err := initSchema(db, durable); err != nil {
		db.Close()
		return nil, err
	}

	return &DB{db: db}, nil
}

// Close closes the database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

func initSchema(db *sql.DB, durable bool) error {
	pragmas := "PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;"
	if !durable {
		pragmas = "PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF; PRAGMA temp_store = MEMORY;"
	}
	_, err := db.Exec(pragmas + `

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

		-- Per-bucket protection policy.  A bucket is a top-level path component;
		-- its files are written to vlogs provisioned under this scheme.  Buckets
		-- with no row fall back to DefaultBucketPolicy (DUPLICATE across every disk).
		CREATE TABLE IF NOT EXISTS bucket (
			name TEXT PRIMARY KEY,
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
			hostname TEXT NOT NULL,
			-- Liveness mirrors RoseStorage's node_state: a working node's disks are
			-- live, a failed (offline) node's disks drop out of placement and commit
			-- durability even though their disk_state is still active.
			state TEXT NOT NULL DEFAULT 'working'
		);

		CREATE TABLE IF NOT EXISTS disk (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			node_id INTEGER NOT NULL,
			total_bytes INTEGER NOT NULL,
			used_bytes INTEGER NOT NULL,
			-- Lifecycle state mirrors RoseStorage's disk_state: a disk moves
			-- active -> draining -> detached as it is removed, or -> failed on
			-- loss.  It gates placement (active only) and commit durability.
			state TEXT NOT NULL DEFAULT 'active',
			FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE
		);

		-- Durable maintenance work stream.  Jobs (compaction today, repair and
		-- rebalance later) survive restarts so a rewrite interrupted by a crash
		-- is resumed rather than lost or restarted from scratch.
		CREATE TABLE IF NOT EXISTS job (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			state TEXT NOT NULL,         -- 'running' | 'done'
			target_vlog INTEGER NOT NULL DEFAULT 0,
			dest_vlog INTEGER NOT NULL DEFAULT 0,
			target_disk INTEGER NOT NULL DEFAULT 0, -- disk-maintenance jobs (drain)
			dest_disk INTEGER NOT NULL DEFAULT 0,   -- replace: the disk to move onto
			created_at INTEGER NOT NULL
		);

		-- Reverse lookups for the control plane: a plog -> the vlog shard it backs,
		-- and a disk -> the plogs it holds.  Without these, PlogsOnDisk and the
		-- per-disk repair/rebalance scans force SQLite to build a transient
		-- automatic index on every call, which dominates at multi-million-plog
		-- scale.
		CREATE INDEX IF NOT EXISTS idx_vlog_plog_plog ON vlog_plog(plog_id);
		CREATE INDEX IF NOT EXISTS idx_plog_disk ON plog(disk_id);
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
	// Disk catalogs predating the lifecycle column default their existing rows to
	// active; a duplicate-column error just means the column already exists.
	if _, err := db.Exec(`ALTER TABLE disk ADD COLUMN state TEXT NOT NULL DEFAULT 'active'`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add disk.state column: %w", err)
		}
	}
	// Disk-maintenance jobs (drain) target a disk rather than a vlog; older job
	// streams default the column to 0.
	if _, err := db.Exec(`ALTER TABLE job ADD COLUMN target_disk INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add job.target_disk column: %w", err)
		}
	}
	// Replace jobs pin a destination disk so the evacuated shards land on the
	// freshly added disk; older job streams default the column to 0.
	if _, err := db.Exec(`ALTER TABLE job ADD COLUMN dest_disk INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add job.dest_disk column: %w", err)
		}
	}
	// Node catalogs predating the liveness column default their existing rows to
	// working; a duplicate-column error just means the column already exists.
	if _, err := db.Exec(`ALTER TABLE node ADD COLUMN state TEXT NOT NULL DEFAULT 'working'`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add node.state column: %w", err)
		}
	}
	return nil
}
