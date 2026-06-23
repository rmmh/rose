package meta

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"strings"
	"time"
)

// MakeVlog creates a new vlog with the specified protection scheme.
func (d *DB) MakeVlog(ctx context.Context, protectionScheme string, dataShards, parityShards int32) (uint32, error) {
	return d.MakeStagingVlog(ctx, protectionScheme, dataShards, parityShards, 0, 0)
}

// MakeStagingVlog records a vlog and, when targetParityShards is nonzero, marks
// it as a replicated staging vlog whose chunks will later be promoted into an EC
// vlog with the given target shard counts.
func (d *DB) MakeStagingVlog(ctx context.Context, protectionScheme string, dataShards, parityShards, targetDataShards, targetParityShards int32) (uint32, error) {
	res, err := d.db.ExecContext(ctx, "INSERT INTO vlog (protection_scheme, data_shards, parity_shards, target_data_shards, target_parity_shards) VALUES (?, ?, ?, ?, ?)", protectionScheme, dataShards, parityShards, targetDataShards, targetParityShards)
	if err != nil {
		return 0, fmt.Errorf("make vlog: %w", err)
	}
	id, err := res.LastInsertId()
	return uint32(id), err
}

// MakePlog creates a new physical log assigned to a disk.
func (d *DB) MakePlog(ctx context.Context, diskID uint32) (uint32, error) {
	res, err := d.db.ExecContext(ctx, "INSERT INTO plog (disk_id) VALUES (?)", diskID)
	if err != nil {
		return 0, fmt.Errorf("make plog: %w", err)
	}
	id, err := res.LastInsertId()
	return uint32(id), err
}

type PlogInfo struct {
	ID     uint32
	DiskID uint32
	Length int64
}

type VlogInfo struct {
	ID               uint32
	Length           int64
	ProtectionScheme string
	DataShards       int32
	ParityShards     int32
	// TargetDataShards/TargetParityShards are nonzero only on replicated staging
	// vlogs: they record the EC scheme the staged chunks will be promoted into.
	TargetDataShards   int32
	TargetParityShards int32
}

// IsStaging reports whether this is a replicated staging vlog awaiting promotion
// into an EC vlog with the recorded target shard counts.
func (v VlogInfo) IsStaging() bool { return v.TargetParityShards > 0 || v.TargetDataShards > 0 }

type VlogPlogInfo struct {
	ShardIndex int
	PlogID     uint32
}

func (d *DB) ListPlogs(ctx context.Context) ([]PlogInfo, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, disk_id, length FROM plog ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlogInfo
	for rows.Next() {
		var info PlogInfo
		if err := rows.Scan(&info.ID, &info.DiskID, &info.Length); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

func (d *DB) ListVlogs(ctx context.Context) ([]VlogInfo, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT id, length, protection_scheme, data_shards, parity_shards, target_data_shards, target_parity_shards FROM vlog ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VlogInfo
	for rows.Next() {
		var info VlogInfo
		if err := rows.Scan(&info.ID, &info.Length, &info.ProtectionScheme, &info.DataShards, &info.ParityShards, &info.TargetDataShards, &info.TargetParityShards); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

func (d *DB) ListVlogPlogs(ctx context.Context, vlogID uint32) ([]VlogPlogInfo, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT shard_idx, plog_id FROM vlog_plog WHERE vlog_id = ? ORDER BY shard_idx", vlogID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VlogPlogInfo
	for rows.Next() {
		var info VlogPlogInfo
		if err := rows.Scan(&info.ShardIndex, &info.PlogID); err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, rows.Err()
}

// VlogsForPlog returns the vlogs currently referencing a plog.
func (d *DB) VlogsForPlog(ctx context.Context, plogID uint32) ([]uint32, error) {
	rows, err := d.db.QueryContext(ctx, "SELECT vlog_id FROM vlog_plog WHERE plog_id = ? ORDER BY vlog_id", plogID)
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

// VlogUsage accounts for the live and dead bytes in a vlog. Dead bytes are
// space occupied by chunks no longer referenced by any live head or snapshot;
// they are only physically reclaimed by compaction, which rewrites the live
// chunks into a fresh vlog and retires this one.
type VlogUsage struct {
	VlogID     uint32
	TotalBytes int64 // the vlog write cursor: every byte ever appended
	LiveBytes  int64 // bytes still referenced (chunk refcount > 0)
}

func (u VlogUsage) DeadBytes() int64 {
	dead := u.TotalBytes - u.LiveBytes
	if dead < 0 {
		return 0
	}
	return dead
}

func (u VlogUsage) WasteRatio() float64 {
	if u.TotalBytes <= 0 {
		return 0
	}
	return float64(u.DeadBytes()) / float64(u.TotalBytes)
}

// VlogUsages reports live/dead accounting for every vlog. A chunk counts as live
// while its refcount is positive; refcount-zero chunks (whether or not their
// rows have been collected) are dead space awaiting compaction.
func (d *DB) VlogUsages(ctx context.Context) ([]VlogUsage, error) {
	rows, err := d.db.QueryContext(ctx, `
		SELECT v.id, v.length,
		       COALESCE(SUM(CASE WHEN c.refcount > 0 THEN c.compressed_len ELSE 0 END), 0)
		FROM vlog v
		LEFT JOIN chunk c ON c.vlog_id = v.id
		GROUP BY v.id, v.length
		ORDER BY v.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VlogUsage
	for rows.Next() {
		var u VlogUsage
		if err := rows.Scan(&u.VlogID, &u.TotalBytes, &u.LiveBytes); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (d *DB) SetVlogLength(ctx context.Context, vlogID uint32, length int64) error {
	_, err := d.db.ExecContext(ctx, "UPDATE vlog SET length = ? WHERE id = ?", length, vlogID)
	return err
}

// SetPlogLength records a logical plog length. Normal byte-backed writes derive
// this from the file; virtual scale tests use it to model large extents without
// allocating their contents.
func (d *DB) SetPlogLength(ctx context.Context, plogID uint32, length int64) error {
	_, err := d.db.ExecContext(ctx, "UPDATE plog SET length = ? WHERE id = ?", length, plogID)
	return err
}

// AssignPlogToVlog maps a plog to a shard of a vlog.
func (d *DB) AssignPlogToVlog(ctx context.Context, vlogID uint32, shardIdx int, plogID uint32) error {
	_, err := d.db.ExecContext(ctx, "INSERT INTO vlog_plog (vlog_id, shard_idx, plog_id) VALUES (?, ?, ?)", vlogID, shardIdx, plogID)
	return err
}

// ReplaceShardPlog atomically repoints a vlog shard from a lost plog to a freshly
// regenerated one and drops the lost plog row, in a single transaction. The
// regenerated bytes must already be durable at newPlogID's path; until this
// commits the shard still maps to oldPlogID, so a crash mid-reprotect leaves the
// shard referencing the failed disk and the job resumes from PlogsOnDisk. It
// fails if the mapping does not currently point at oldPlogID, guarding against a
// double-apply on resume.
func (d *DB) ReplaceShardPlog(ctx context.Context, vlogID uint32, shardIdx int, oldPlogID, newPlogID uint32) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		"UPDATE vlog_plog SET plog_id = ? WHERE vlog_id = ? AND shard_idx = ? AND plog_id = ?",
		newPlogID, vlogID, shardIdx, oldPlogID)
	if err != nil {
		return fmt.Errorf("repoint vlog %d shard %d: %w", vlogID, shardIdx, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("repoint vlog %d shard %d: not currently mapped to plog %d", vlogID, shardIdx, oldPlogID)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM plog WHERE id = ?", oldPlogID); err != nil {
		return fmt.Errorf("delete lost plog %d: %w", oldPlogID, err)
	}
	return tx.Commit()
}

// OpenFile looks up the latest file ID for a path. If it does not exist, returns 0.
func (d *DB) OpenFile(ctx context.Context, path string) (int64, error) {
	var id int64
	err := d.db.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", path).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// ChunkPlacement describes one chunk of a file version: its content hash and
// where the bytes live in a virtual log.
type ChunkPlacement struct {
	Hash          []byte
	VlogID        uint32
	VaddrOffset   int64
	LogicalLen    int
	CompressedLen int
}

const maxChunkUpsertRows = 150

// upsertChunkRefs records chunk placement rows and takes their file references in
// the same statement. New chunks are inserted at refcount 1; existing chunks
// reuse their placement row and increment its refcount.
func upsertChunkRefs(ctx context.Context, tx *sql.Tx, placements []ChunkPlacement) error {
	for start := 0; start < len(placements); start += maxChunkUpsertRows {
		end := start + maxChunkUpsertRows
		if end > len(placements) {
			end = len(placements)
		}
		batch := placements[start:end]

		var sqlText strings.Builder
		sqlText.Grow(128 + len(batch)*24)
		sqlText.WriteString("INSERT INTO chunk (hash, refcount, vlog_id, vaddr_offset, logical_len, compressed_len) VALUES ")
		args := make([]any, 0, len(batch)*5)
		for i, p := range batch {
			if i > 0 {
				sqlText.WriteByte(',')
			}
			sqlText.WriteString("(?, 1, ?, ?, ?, ?)")
			args = append(args, p.Hash, p.VlogID, p.VaddrOffset, p.LogicalLen, p.CompressedLen)
		}
		sqlText.WriteString(" ON CONFLICT(hash) DO UPDATE SET refcount = refcount + 1")

		if _, err := tx.ExecContext(ctx, sqlText.String(), args...); err != nil {
			return fmt.Errorf("upsert chunk refs batch %d-%d: %w", start, end, err)
		}
	}
	return nil
}

func chunkHashes(chunks []byte) [][]byte {
	var hashes [][]byte
	for i := 0; i+19 <= len(chunks); i += 19 {
		hashes = append(hashes, chunks[i:i+15])
	}
	return hashes
}

func adjustChunkRefs(ctx context.Context, tx *sql.Tx, chunks []byte, delta int) error {
	for _, hash := range chunkHashes(chunks) {
		if _, err := tx.ExecContext(ctx, "UPDATE chunk SET refcount = refcount + ? WHERE hash = ?", delta, hash); err != nil {
			return err
		}
	}
	return nil
}

func fileChunks(ctx context.Context, tx *sql.Tx, fileID int64) ([]byte, error) {
	var chunks []byte
	err := tx.QueryRowContext(ctx, "SELECT chunks FROM file WHERE id = ?", fileID).Scan(&chunks)
	return chunks, err
}

// publishFileVersion inserts a new immutable file version from an explicit
// ordered placement list and transfers the namespace reference from the previous
// head, all inside tx. Chunk rows are inserted and referenced in the same
// transaction, so no committed chunk is ever durably visible at refcount 0 in
// the window before it is referenced. Placements may freely mix freshly stored
// chunks and reused (already-referenced) ones; insertChunk is ON CONFLICT
// DO NOTHING and the refcount transfer keeps unchanged chunks live across the
// version bump. It is the shared body of CommitFile, CommitWriteOp, and
// CommitWriteOpVersion.
func publishFileVersion(ctx context.Context, tx *sql.Tx, path string, mtime int64, placements []ChunkPlacement) (int64, error) {
	chunks := make([]byte, 0, len(placements)*19)
	lenBytes := make([]byte, 4)
	for _, p := range placements {
		chunks = append(chunks, p.Hash...)
		binary.LittleEndian.PutUint32(lenBytes, uint32(p.LogicalLen))
		chunks = append(chunks, lenBytes...)
	}

	var oldID int64
	err := tx.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", path).Scan(&oldID)
	if err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("load file head: %w", err)
	}
	if err == nil {
		oldChunks, err := fileChunks(ctx, tx, oldID)
		if err != nil {
			return 0, fmt.Errorf("load old file chunks: %w", err)
		}
		if err := adjustChunkRefs(ctx, tx, oldChunks, -1); err != nil {
			return 0, fmt.Errorf("decrement old chunk refs: %w", err)
		}
	}

	if err := upsertChunkRefs(ctx, tx, placements); err != nil {
		return 0, fmt.Errorf("upsert new chunk refs: %w", err)
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO file (path, mtime, chunks) VALUES (?, ?, ?)", path, mtime, chunks)
	if err != nil {
		return 0, fmt.Errorf("insert file: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	parent, name := splitPath(path)
	if _, err := tx.ExecContext(ctx, `INSERT INTO file_head (path, file_id, parent, name) VALUES (?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET file_id = excluded.file_id, parent = excluded.parent, name = excluded.name`, path, id, parent, name); err != nil {
		return 0, fmt.Errorf("publish file head: %w", err)
	}
	if err := ensureDirs(ctx, tx, path, mtime); err != nil {
		return 0, fmt.Errorf("ensure parent directories: %w", err)
	}
	return id, nil
}

// CommitFile atomically publishes a new immutable file version and transfers
// the namespace reference from the previous head.
func (d *DB) CommitFile(ctx context.Context, path string, mtime int64, placements []ChunkPlacement) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin file commit: %w", err)
	}
	defer tx.Rollback()
	id, err := publishFileVersion(ctx, tx, path, mtime, placements)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit file version: %w", err)
	}
	return id, nil
}

// GCChunks reclaims every chunk whose reference count has fallen to zero,
// implementing the spec's GCChunk action. A refcount of zero means the chunk is
// reachable from no live file head and no active snapshot, so collecting it
// cannot make any reachable version unreadable. It returns the placements of the
// collected chunks so a caller can later reclaim their log space via compaction.
func (d *DB) GCChunks(ctx context.Context) ([]ChunkPlacement, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, "SELECT hash, vlog_id, vaddr_offset, logical_len, compressed_len FROM chunk WHERE refcount <= 0")
	if err != nil {
		return nil, err
	}
	var collected []ChunkPlacement
	for rows.Next() {
		var p ChunkPlacement
		if err := rows.Scan(&p.Hash, &p.VlogID, &p.VaddrOffset, &p.LogicalLen, &p.CompressedLen); err != nil {
			rows.Close()
			return nil, err
		}
		collected = append(collected, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if _, err := tx.ExecContext(ctx, "DELETE FROM chunk WHERE refcount <= 0"); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return collected, nil
}

// FileVersionChunks returns the ordered chunk placements of a file version: each
// chunk's content hash, its logical length, and where its bytes live in a vlog.
// It is the read side of the splice path -- the base placement list the write
// cache overlays its pending modifications onto. A zero fileID (a path with no
// committed version yet) yields an empty list.
func (d *DB) FileVersionChunks(ctx context.Context, fileID int64) ([]ChunkPlacement, error) {
	if fileID == 0 {
		return nil, nil
	}
	var blob []byte
	if err := d.db.QueryRowContext(ctx, "SELECT chunks FROM file WHERE id = ?", fileID).Scan(&blob); err != nil {
		return nil, err
	}
	var out []ChunkPlacement
	for i := 0; i+19 <= len(blob); i += 19 {
		hash := append([]byte(nil), blob[i:i+15]...)
		p := ChunkPlacement{Hash: hash, LogicalLen: int(binary.LittleEndian.Uint32(blob[i+15 : i+19]))}
		err := d.db.QueryRowContext(ctx, "SELECT vlog_id, vaddr_offset, compressed_len FROM chunk WHERE hash = ?", hash).Scan(&p.VlogID, &p.VaddrOffset, &p.CompressedLen)
		if err != nil {
			return nil, fmt.Errorf("placement for chunk %x: %w", hash, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// ChunkByHash looks up the placement of an already-stored chunk by its content
// hash. It backs dedup during the splice: a freshly recomputed chunk whose hash
// is already present reuses that placement instead of writing its bytes again.
func (d *DB) ChunkByHash(ctx context.Context, hash []byte) (ChunkPlacement, bool, error) {
	p := ChunkPlacement{Hash: append([]byte(nil), hash...)}
	err := d.chunkByHashStmt.QueryRowContext(ctx, hash).Scan(&p.VlogID, &p.VaddrOffset, &p.LogicalLen, &p.CompressedLen)
	if err == sql.ErrNoRows {
		return ChunkPlacement{}, false, nil
	}
	if err != nil {
		return ChunkPlacement{}, false, err
	}
	return p, true, nil
}

func (d *DB) GetFileSize(ctx context.Context, path string) (int64, error) {
	var chunks []byte
	err := d.db.QueryRowContext(ctx, `SELECT file.chunks FROM file_head
		JOIN file ON file.id = file_head.file_id WHERE file_head.path = ?`, path).Scan(&chunks)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var size int64
	for i := 0; i+19 <= len(chunks); i += 19 {
		size += int64(binary.LittleEndian.Uint32(chunks[i+15 : i+19]))
	}
	return size, nil
}

func (d *DB) CreateSnapshot(ctx context.Context, name string, createdAt int64) (uint64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, "INSERT INTO snapshot (name, created_at) VALUES (?, ?)", name, createdAt)
	if err != nil {
		return 0, fmt.Errorf("create snapshot: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT path, file_id FROM file_head")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		var fileID int64
		if err := rows.Scan(&path, &fileID); err != nil {
			return 0, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO snapshot_file (snapshot_id, path, file_id) VALUES (?, ?, ?)", id, path, fileID); err != nil {
			return 0, err
		}
		chunks, err := fileChunks(ctx, tx, fileID)
		if err != nil {
			return 0, err
		}
		if err := adjustChunkRefs(ctx, tx, chunks, 1); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return uint64(id), nil
}

func (d *DB) DeleteSnapshot(ctx context.Context, snapshotID uint64) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT file_id FROM snapshot_file WHERE snapshot_id = ?", snapshotID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var fileID int64
		if err := rows.Scan(&fileID); err != nil {
			return err
		}
		chunks, err := fileChunks(ctx, tx, fileID)
		if err != nil {
			return err
		}
		if err := adjustChunkRefs(ctx, tx, chunks, -1); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM snapshot WHERE id = ?", snapshotID); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) OpenSnapshotFile(ctx context.Context, snapshotID uint64, path string) (int64, error) {
	var fileID int64
	err := d.db.QueryRowContext(ctx, "SELECT file_id FROM snapshot_file WHERE snapshot_id = ? AND path = ?", snapshotID, path).Scan(&fileID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return fileID, err
}

func (d *DB) UnlinkFile(ctx context.Context, path string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var fileID int64
	err = tx.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", path).Scan(&fileID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	chunks, err := fileChunks(ctx, tx, fileID)
	if err != nil {
		return err
	}
	if err := adjustChunkRefs(ctx, tx, chunks, -1); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM file_head WHERE path = ?", path); err != nil {
		return err
	}
	return tx.Commit()
}

// RenameFile moves a file or directory from oldPath to newPath.  A file move is
// O(1); a directory move is a transactional prefix rewrite of every file_head
// and dir row under the subtree (O(descendants) -- a true O(1) rename would need
// the explicit dir-inode model deferred in this cut).
func (d *DB) RenameFile(ctx context.Context, oldPath, newPath string) error {
	oldPath, newPath = cleanPath(oldPath), cleanPath(newPath)
	if oldPath == newPath {
		return nil
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var oldID int64
	err = tx.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", oldPath).Scan(&oldID)
	if err == sql.ErrNoRows {
		// Not a file: it may be a directory subtree.
		var isDir int
		if derr := tx.QueryRowContext(ctx, "SELECT 1 FROM dir WHERE path = ?", oldPath).Scan(&isDir); derr == sql.ErrNoRows {
			return sql.ErrNoRows
		} else if derr != nil {
			return derr
		}
		if err := renameDirSubtree(ctx, tx, oldPath, newPath); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}

	var replacedID int64
	err = tx.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", newPath).Scan(&replacedID)
	if err == nil {
		chunks, err := fileChunks(ctx, tx, replacedID)
		if err != nil {
			return err
		}
		if err := adjustChunkRefs(ctx, tx, chunks, -1); err != nil {
			return err
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	parent, name := splitPath(newPath)
	if _, err := tx.ExecContext(ctx, `INSERT INTO file_head (path, file_id, parent, name) VALUES (?, ?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET file_id = excluded.file_id, parent = excluded.parent, name = excluded.name`,
		newPath, oldID, parent, name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM file_head WHERE path = ?", oldPath); err != nil {
		return err
	}
	if err := ensureDirs(ctx, tx, newPath, time.Now().UnixNano()); err != nil {
		return err
	}
	return tx.Commit()
}

// renameDirSubtree relocates the directory at oldPath, and every file and
// subdirectory beneath it, to newPath within tx.  Paths are rewritten by
// replacing the oldPath prefix; parent/name are recomputed for each moved row.
func renameDirSubtree(ctx context.Context, tx *sql.Tx, oldPath, newPath string) error {
	if err := ensureDirs(ctx, tx, newPath, time.Now().UnixNano()); err != nil {
		return err
	}
	// Rewrite the moved directory itself plus every descendant directory.
	if err := rewriteSubtreePaths(ctx, tx, "dir", oldPath, newPath); err != nil {
		return err
	}
	// Rewrite every file beneath the subtree.
	if err := rewriteSubtreePaths(ctx, tx, "file_head", oldPath, newPath); err != nil {
		return err
	}
	return nil
}

// rewriteSubtreePaths moves the row at oldPath (if any) and all rows whose path
// is prefixed by oldPath+"/" to the corresponding path under newPath in the
// named namespace table (dir or file_head), recomputing parent/name.
func rewriteSubtreePaths(ctx context.Context, tx *sql.Tx, table, oldPath, newPath string) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT path FROM `+table+` WHERE path = ? OR path LIKE ? ESCAPE '\'`,
		oldPath, escapeLike(oldPath)+`/%`)
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
	for _, p := range paths {
		np := newPath + p[len(oldPath):]
		parent, name := splitPath(np)
		if _, err := tx.ExecContext(ctx,
			`UPDATE `+table+` SET path = ?, parent = ?, name = ? WHERE path = ?`,
			np, parent, name, p); err != nil {
			return err
		}
	}
	return nil
}

// escapeLike escapes the LIKE metacharacters in a literal path prefix so a
// directory name containing %, _, or \ does not widen the subtree match.
func escapeLike(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}
