package meta

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
)

// MakeVlog creates a new vlog with the specified protection scheme.
func (d *DB) MakeVlog(ctx context.Context, protectionScheme string, dataShards, parityShards int32) (uint32, error) {
	res, err := d.db.ExecContext(ctx, "INSERT INTO vlog (protection_scheme, data_shards, parity_shards) VALUES (?, ?, ?)", protectionScheme, dataShards, parityShards)
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
}

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
	rows, err := d.db.QueryContext(ctx, "SELECT id, length, protection_scheme, data_shards, parity_shards FROM vlog ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []VlogInfo
	for rows.Next() {
		var info VlogInfo
		if err := rows.Scan(&info.ID, &info.Length, &info.ProtectionScheme, &info.DataShards, &info.ParityShards); err != nil {
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

func (d *DB) SetVlogLength(ctx context.Context, vlogID uint32, length int64) error {
	_, err := d.db.ExecContext(ctx, "UPDATE vlog SET length = ? WHERE id = ?", length, vlogID)
	return err
}

// AssignPlogToVlog maps a plog to a shard of a vlog.
func (d *DB) AssignPlogToVlog(ctx context.Context, vlogID uint32, shardIdx int, plogID uint32) error {
	_, err := d.db.ExecContext(ctx, "INSERT INTO vlog_plog (vlog_id, shard_idx, plog_id) VALUES (?, ?, ?)", vlogID, shardIdx, plogID)
	return err
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

// AddChunk records immutable chunk placement. References are adjusted only when
// a file version is published, unlinked, or captured/released by a snapshot.
func (d *DB) AddChunk(ctx context.Context, hash []byte, vlogID uint32, vaddrOffset int64, logicalLen int, compressedLen int) error {
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO chunk (hash, refcount, vlog_id, vaddr_offset, logical_len, compressed_len)
		VALUES (?, 0, ?, ?, ?, ?)
		ON CONFLICT(hash) DO NOTHING
	`, hash, vlogID, vaddrOffset, logicalLen, compressedLen)
	return err
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

// CommitFile atomically publishes a new immutable file version and transfers
// the namespace reference from the previous head.
func (d *DB) CommitFile(ctx context.Context, path string, mtime int64, chunks []byte) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin file commit: %w", err)
	}
	defer tx.Rollback()

	var oldID int64
	err = tx.QueryRowContext(ctx, "SELECT file_id FROM file_head WHERE path = ?", path).Scan(&oldID)
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

	if err := adjustChunkRefs(ctx, tx, chunks, 1); err != nil {
		return 0, fmt.Errorf("increment new chunk refs: %w", err)
	}
	res, err := tx.ExecContext(ctx, "INSERT INTO file (path, mtime, chunks) VALUES (?, ?, ?)", path, mtime, chunks)
	if err != nil {
		return 0, fmt.Errorf("insert file: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO file_head (path, file_id) VALUES (?, ?)
		ON CONFLICT(path) DO UPDATE SET file_id = excluded.file_id`, path, id); err != nil {
		return 0, fmt.Errorf("publish file head: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit file version: %w", err)
	}
	return id, nil
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

type FileChunkInfo struct {
	VlogID      uint32
	VaddrOffset int64
	LogicalLen  int
}

func (d *DB) GetFileChunks(ctx context.Context, fileID int64) ([]FileChunkInfo, error) {
	var chunksBlob []byte
	err := d.db.QueryRowContext(ctx, "SELECT chunks FROM file WHERE id = ?", fileID).Scan(&chunksBlob)
	if err != nil {
		return nil, err
	}

	var chunks []FileChunkInfo
	for i := 0; i+19 <= len(chunksBlob); i += 19 {
		hash := chunksBlob[i : i+15]
		logicalLen := binary.LittleEndian.Uint32(chunksBlob[i+15 : i+19])

		var info FileChunkInfo
		info.LogicalLen = int(logicalLen)

		err = d.db.QueryRowContext(ctx, "SELECT vlog_id, vaddr_offset FROM chunk WHERE hash = ?", hash).Scan(&info.VlogID, &info.VaddrOffset)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, info)
	}
	return chunks, nil
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

func (d *DB) RenameFile(ctx context.Context, oldPath, newPath string) error {
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
	if _, err := tx.ExecContext(ctx, `INSERT INTO file_head (path, file_id) VALUES (?, ?)
		ON CONFLICT(path) DO UPDATE SET file_id = excluded.file_id`, newPath, oldID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM file_head WHERE path = ?", oldPath); err != nil {
		return err
	}
	return tx.Commit()
}
