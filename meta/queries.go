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

// AssignPlogToVlog maps a plog to a shard of a vlog.
func (d *DB) AssignPlogToVlog(ctx context.Context, vlogID uint32, shardIdx int, plogID uint32) error {
	_, err := d.db.ExecContext(ctx, "INSERT INTO vlog_plog (vlog_id, shard_idx, plog_id) VALUES (?, ?, ?)", vlogID, shardIdx, plogID)
	return err
}

// OpenFile looks up the latest file ID for a path. If it does not exist, returns 0.
func (d *DB) OpenFile(ctx context.Context, path string) (int64, error) {
	var id int64
	err := d.db.QueryRowContext(ctx, "SELECT id FROM file WHERE path = ? ORDER BY id DESC LIMIT 1", path).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// AddChunk adds a chunk mapping or increments its refcount if it already exists.
func (d *DB) AddChunk(ctx context.Context, hash []byte, vlogID uint32, vaddrOffset int64, logicalLen int, compressedLen int) error {
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO chunk (hash, refcount, vlog_id, vaddr_offset, logical_len, compressed_len)
		VALUES (?, 1, ?, ?, ?, ?)
		ON CONFLICT(hash) DO UPDATE SET refcount = refcount + 1
	`, hash, vlogID, vaddrOffset, logicalLen, compressedLen)
	return err
}

// CommitFile creates a new file entry acting as a snapshot.
func (d *DB) CommitFile(ctx context.Context, path string, mtime int64, chunks []byte) (int64, error) {
	res, err := d.db.ExecContext(ctx, "INSERT INTO file (path, mtime, chunks) VALUES (?, ?, ?)", path, mtime, chunks)
	if err != nil {
		return 0, fmt.Errorf("insert file: %w", err)
	}
	id, err := res.LastInsertId()
	return id, err
}

func (d *DB) GetFileSize(ctx context.Context, path string) (int64, error) {
	var chunks []byte
	err := d.db.QueryRowContext(ctx, "SELECT chunks FROM file WHERE path = ? ORDER BY id DESC LIMIT 1", path).Scan(&chunks)
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
