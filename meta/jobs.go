package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Job is one entry in the durable maintenance work stream.
type Job struct {
	ID         int64
	Kind       string
	State      string
	TargetVlog uint32
	DestVlog   uint32
	TargetDisk uint32
	DestDisk   uint32
}

const (
	JobCompact   = "compact"
	JobDrain     = "drain"
	JobReprotect = "reprotect"
	JobReplace   = "replace"
	JobRebalance = "rebalance"
	JobRunning   = "running"
	JobDone      = "done"
	JobCancelled = "cancelled"
)

const jobColumns = "id, kind, state, target_vlog, dest_vlog, target_disk, dest_disk"

func scanJob(row interface{ Scan(...any) error }) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Kind, &j.State, &j.TargetVlog, &j.DestVlog, &j.TargetDisk, &j.DestDisk)
	return j, err
}

// GetOrCreateCompactionJob returns the running compaction job for a vlog,
// creating one if none exists. Reusing an in-flight job is what lets a crashed
// rewrite resume against the same destination vlog instead of orphaning it.
func (d *DB) GetOrCreateCompactionJob(ctx context.Context, targetVlog uint32) (Job, error) {
	j, err := scanJob(d.db.QueryRowContext(ctx,
		"SELECT "+jobColumns+" FROM job WHERE kind = ? AND state = ? AND target_vlog = ?",
		JobCompact, JobRunning, targetVlog))
	if err == nil {
		return j, nil
	}
	if err != sql.ErrNoRows {
		return Job{}, err
	}
	res, err := d.db.ExecContext(ctx,
		"INSERT INTO job (kind, state, target_vlog, created_at) VALUES (?, ?, ?, ?)",
		JobCompact, JobRunning, targetVlog, time.Now().UnixNano())
	if err != nil {
		return Job{}, fmt.Errorf("create compaction job: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Job{}, err
	}
	return Job{ID: id, Kind: JobCompact, State: JobRunning, TargetVlog: targetVlog}, nil
}

// GetOrCreateDrainJob returns the running drain job for a disk, creating one if
// none exists. Like compaction, reusing an in-flight job is what lets a crashed
// drain resume rather than restart: the disk is already draining and its
// not-yet-migrated plogs are still listed on it.
func (d *DB) GetOrCreateDrainJob(ctx context.Context, targetDisk uint32) (Job, error) {
	return d.getOrCreateDiskJob(ctx, JobDrain, targetDisk, 0)
}

// GetOrCreateReprotectJob returns the running reprotect job for a failed disk,
// creating one if none exists. Like drain it is keyed by target_disk, so a crash
// mid-reprotect resumes from the shards still mapped to the failed disk's plogs
// rather than restarting or stranding the vlogs degraded.
func (d *DB) GetOrCreateReprotectJob(ctx context.Context, targetDisk uint32) (Job, error) {
	return d.getOrCreateDiskJob(ctx, JobReprotect, targetDisk, 0)
}

// GetOrCreateReplaceJob returns the running replace job moving targetDisk's
// shards onto destDisk, creating one if none exists. The pinned destination is
// persisted so a crash mid-replace resumes onto the same freshly added disk.
func (d *DB) GetOrCreateReplaceJob(ctx context.Context, targetDisk, destDisk uint32) (Job, error) {
	return d.getOrCreateDiskJob(ctx, JobReplace, targetDisk, destDisk)
}

// GetOrCreateRebalanceJob records the single cluster-wide rebalance pass.
func (d *DB) GetOrCreateRebalanceJob(ctx context.Context) (Job, error) {
	return d.getOrCreateDiskJob(ctx, JobRebalance, 0, 0)
}

// getOrCreateDiskJob is the shared get-or-create for disk-maintenance jobs keyed
// by target_disk. destDisk is 0 for jobs that pick destinations dynamically
// (drain, reprotect) and the pinned destination for replace.
func (d *DB) getOrCreateDiskJob(ctx context.Context, kind string, targetDisk, destDisk uint32) (Job, error) {
	j, err := scanJob(d.db.QueryRowContext(ctx,
		"SELECT "+jobColumns+" FROM job WHERE kind = ? AND state = ? AND target_disk = ?",
		kind, JobRunning, targetDisk))
	if err == nil {
		return j, nil
	}
	if err != sql.ErrNoRows {
		return Job{}, err
	}
	res, err := d.db.ExecContext(ctx,
		"INSERT INTO job (kind, state, target_disk, dest_disk, created_at) VALUES (?, ?, ?, ?, ?)",
		kind, JobRunning, targetDisk, destDisk, time.Now().UnixNano())
	if err != nil {
		return Job{}, fmt.Errorf("create %s job: %w", kind, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return Job{}, err
	}
	return Job{ID: id, Kind: kind, State: JobRunning, TargetDisk: targetDisk, DestDisk: destDisk}, nil
}

func (d *DB) SetJobDest(ctx context.Context, jobID int64, destVlog uint32) error {
	_, err := d.db.ExecContext(ctx, "UPDATE job SET dest_vlog = ? WHERE id = ?", destVlog, jobID)
	return err
}

func (d *DB) MarkJobDone(ctx context.Context, jobID int64) error {
	_, err := d.db.ExecContext(ctx, "UPDATE job SET state = ? WHERE id = ?", JobDone, jobID)
	return err
}

// CancelRunningReprotect cancels the running reprotect job for a disk, if any,
// and reports whether one was cancelled. A node returning to working calls this
// to abandon a reprotect that the (now transient) loss made unnecessary: the
// disk's bytes are intact again, so the not-yet-regenerated shards still resolve
// to it. A cancelled job is excluded from RunningJobs, so a later restart does
// not resume it.
func (d *DB) CancelRunningReprotect(ctx context.Context, targetDisk uint32) (bool, error) {
	res, err := d.db.ExecContext(ctx,
		"UPDATE job SET state = ? WHERE kind = ? AND state = ? AND target_disk = ?",
		JobCancelled, JobReprotect, JobRunning, targetDisk)
	if err != nil {
		return false, fmt.Errorf("cancel reprotect of disk %d: %w", targetDisk, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// RunningJobs lists jobs to resume after a restart.
func (d *DB) RunningJobs(ctx context.Context) ([]Job, error) {
	rows, err := d.db.QueryContext(ctx,
		"SELECT "+jobColumns+" FROM job WHERE state = ? ORDER BY id", JobRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// GetJob returns a durable maintenance job by id.
func (d *DB) GetJob(ctx context.Context, id int64) (Job, error) {
	j, err := scanJob(d.db.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM job WHERE id = ?", id))
	if err == sql.ErrNoRows {
		return Job{}, fmt.Errorf("job %d not found", id)
	}
	return j, err
}

// ChunkLoc identifies a live chunk and where its bytes currently live.
type ChunkLoc struct {
	Hash        []byte
	VaddrOffset int64
	LogicalLen  int
}

// LiveChunksInVlog returns the still-referenced chunks stored in a vlog, the
// ones compaction must copy forward.
func (d *DB) LiveChunksInVlog(ctx context.Context, vlogID uint32) ([]ChunkLoc, error) {
	rows, err := d.db.QueryContext(ctx,
		"SELECT hash, vaddr_offset, logical_len FROM chunk WHERE vlog_id = ? AND refcount > 0 ORDER BY vaddr_offset", vlogID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkLoc
	for rows.Next() {
		var c ChunkLoc
		if err := rows.Scan(&c.Hash, &c.VaddrOffset, &c.LogicalLen); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RelocateChunk atomically repoints a chunk at its rewritten copy. The bytes
// must already be durable in destVlog; until this commits the chunk still
// resolves to its old location, so a crash mid-compaction is safe to resume.
func (d *DB) RelocateChunk(ctx context.Context, hash []byte, destVlog uint32, newOffset int64) error {
	_, err := d.db.ExecContext(ctx,
		"UPDATE chunk SET vlog_id = ?, vaddr_offset = ? WHERE hash = ?", destVlog, newOffset, hash)
	return err
}

// RetireVlog drops a fully-drained vlog: any remaining (dead) chunk rows, its
// shard mappings, and the vlog row itself. It returns the plogs that backed it
// so the caller can unmount and delete their files. It fails loudly if any live
// chunk still points at the vlog, which would mean compaction left data behind.
func (d *DB) RetireVlog(ctx context.Context, vlogID uint32) ([]PlogInfo, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var liveRemaining int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM chunk WHERE vlog_id = ? AND refcount > 0", vlogID).Scan(&liveRemaining); err != nil {
		return nil, err
	}
	if liveRemaining > 0 {
		return nil, fmt.Errorf("refusing to retire vlog %d with %d live chunks", vlogID, liveRemaining)
	}

	rows, err := tx.QueryContext(ctx, `SELECT p.id, p.disk_id, p.length FROM plog p
		JOIN vlog_plog vp ON vp.plog_id = p.id WHERE vp.vlog_id = ? ORDER BY p.id`, vlogID)
	if err != nil {
		return nil, err
	}
	var plogs []PlogInfo
	for rows.Next() {
		var p PlogInfo
		if err := rows.Scan(&p.ID, &p.DiskID, &p.Length); err != nil {
			rows.Close()
			return nil, err
		}
		plogs = append(plogs, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	if _, err := tx.ExecContext(ctx, "DELETE FROM chunk WHERE vlog_id = ?", vlogID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM vlog_plog WHERE vlog_id = ?", vlogID); err != nil {
		return nil, err
	}
	for _, p := range plogs {
		if _, err := tx.ExecContext(ctx, "DELETE FROM plog WHERE id = ?", p.ID); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM vlog WHERE id = ?", vlogID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return plogs, nil
}

// GetVlog returns one vlog's protection configuration.
func (d *DB) GetVlog(ctx context.Context, vlogID uint32) (VlogInfo, error) {
	var info VlogInfo
	err := d.db.QueryRowContext(ctx,
		"SELECT id, length, protection_scheme, data_shards, parity_shards FROM vlog WHERE id = ?", vlogID).
		Scan(&info.ID, &info.Length, &info.ProtectionScheme, &info.DataShards, &info.ParityShards)
	return info, err
}
