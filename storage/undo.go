package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const undoHeaderSize = 56

var undoMagic = []byte("ROSEUND1")

// Replaced only in serial fault-injection tests. The actual directory sync is
// shared by journal creation, retirement, and their retry paths.
var undoSyncDirectory = syncParent

var undoSyncFile = func(file *os.File) error { return file.Sync() }

// Set only by subprocess crash tests before opening any files. Keeping the
// checkpoints in the real I/O path exercises ordering without a second model of
// journal implementation. Production never installs a callback.
var undoCrashHook func(string)

func undoCheckpoint(name string) {
	if undoCrashHook != nil {
		undoCrashHook(name)
	}
}

type undoRecord struct {
	identity    [sha256.Size]byte
	start, size int64
	file        *os.File
}

func openUndo(path string) (undoRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return undoRecord{}, err
	}
	fail := func(err error) (undoRecord, error) { f.Close(); return undoRecord{}, err }
	info, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if info.Size() < undoHeaderSize+sha256.Size {
		return fail(fmt.Errorf("short undo journal"))
	}
	header := make([]byte, undoHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return fail(err)
	}
	start, size := int64(binary.LittleEndian.Uint64(header[8:16])), int64(binary.LittleEndian.Uint64(header[16:24]))
	if !bytes.Equal(header[:8], undoMagic) || start < 0 || size < start || size-start != info.Size()-undoHeaderSize-sha256.Size {
		return fail(fmt.Errorf("invalid undo journal geometry"))
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, io.NewSectionReader(f, 0, info.Size()-sha256.Size)); err != nil {
		return fail(err)
	}
	stored := make([]byte, sha256.Size)
	if _, err := f.ReadAt(stored, info.Size()-sha256.Size); err != nil {
		return fail(err)
	}
	if !bytes.Equal(digest.Sum(nil), stored) {
		return fail(fmt.Errorf("undo journal checksum mismatch"))
	}
	var identity [sha256.Size]byte
	copy(identity[:], header[24:56])
	return undoRecord{identity: identity, start: start, size: size, file: f}, nil
}

func syncParent(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// saveUndo durably preserves the suffix before a destructive append/truncate.
// Repeated calls retain the original baseline. Extending the protected range
// combines the still-unchanged prefix with the already saved original suffix.
// Copying is streamed, so a large rollback does not require a same-sized buffer.
func saveUndo(data *os.File, start int64) error {
	path := data.Name() + ".undo"
	old, err := openUndo(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		defer old.file.Close()
		if start >= old.start {
			identity, err := undoIdentity(data, old.start)
			if err != nil {
				return err
			}
			if identity != old.identity {
				return fmt.Errorf("undo journal belongs to different file identity")
			}
			// A preceding attempt may have renamed this journal and then
			// failed to sync the directory. Presence alone is not durability.
			return undoSyncDirectory(path)
		}
	}
	info, err := data.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if old.file != nil {
		size = old.size
	}
	if start < 0 || start > size {
		return fmt.Errorf("undo start %d outside baseline size %d", start, size)
	}
	temp := path + ".tmp"
	f, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { f.Close(); os.Remove(temp) }()
	digest := sha256.New()
	writer := io.MultiWriter(f, digest)
	header := make([]byte, undoHeaderSize)
	copy(header, undoMagic)
	binary.LittleEndian.PutUint64(header[8:16], uint64(start))
	binary.LittleEndian.PutUint64(header[16:24], uint64(size))
	identity, err := undoIdentity(data, start)
	if err != nil {
		return err
	}
	if old.file != nil && old.identity != identity {
		return fmt.Errorf("undo journal belongs to different file identity")
	}
	copy(header[24:56], identity[:])
	if _, err := writer.Write(header); err != nil {
		return err
	}
	end := size
	if old.file != nil {
		end = old.start
	}
	if _, err := io.CopyN(writer, io.NewSectionReader(data, start, end-start), end-start); err != nil {
		return err
	}
	if old.file != nil {
		if _, err := io.CopyN(writer, io.NewSectionReader(old.file, undoHeaderSize, old.size-old.start), old.size-old.start); err != nil {
			return err
		}
	}
	if _, err := f.Write(digest.Sum(nil)); err != nil {
		return err
	}
	undoCheckpoint("save-written")
	if err := undoSyncFile(f); err != nil {
		return err
	}
	undoCheckpoint("save-synced")
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	undoCheckpoint("save-renamed")
	if err := undoSyncDirectory(path); err != nil {
		return err
	}
	undoCheckpoint("save-durable")
	return nil
}

// restoreUndo validates the entire journal before modifying data. The journal
// remains intact through restore+truncate+sync, making interrupted replay
// repeatable. A bad journal is an error, never permission to accept torn bytes.
func restoreUndo(data *os.File) error {
	path := data.Name() + ".undo"
	record, err := openUndo(path)
	if os.IsNotExist(err) {
		// A prior replay may have unlinked the journal but failed its directory
		// sync. Complete retirement before returning a writable file whose
		// identity may subsequently be rebound during relocation.
		return undoSyncDirectory(path)
	}
	if err != nil {
		return err
	}
	defer record.file.Close()
	identity, err := undoIdentity(data, record.start)
	if err != nil {
		return err
	}
	if identity != record.identity {
		return fmt.Errorf("undo journal belongs to different file identity")
	}
	if _, err := io.CopyN(io.NewOffsetWriter(data, record.start), io.NewSectionReader(record.file, undoHeaderSize, record.size-record.start), record.size-record.start); err != nil {
		return err
	}
	undoCheckpoint("restore-copied")
	if err := data.Truncate(record.size); err != nil {
		return err
	}
	undoCheckpoint("restore-truncated")
	if err := undoSyncFile(data); err != nil {
		return err
	}
	undoCheckpoint("restore-synced")
	if err := os.Remove(path); err != nil {
		return err
	}
	undoCheckpoint("restore-removed")
	if err := undoSyncDirectory(path); err != nil {
		return err
	}
	undoCheckpoint("restore-durable")
	return nil
}

// commitUndo retires the rollback baseline only after syncing replacement data.
// A caller must not publish the new prefix until this returns successfully.
func commitUndo(data *os.File) error {
	if err := undoSyncFile(data); err != nil {
		return err
	}
	undoCheckpoint("commit-synced")
	path := data.Name() + ".undo"
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			// Retry after unlink succeeded but its directory sync failed.
			// Without a new sync the old journal could return after power loss
			// and roll back bytes this retry just acknowledged.
			return undoSyncDirectory(path)
		}
		return err
	}
	undoCheckpoint("commit-removed")
	if err := undoSyncDirectory(path); err != nil {
		return err
	}
	undoCheckpoint("commit-durable")
	return nil
}

func undoIdentity(data *os.File, start int64) ([sha256.Size]byte, error) {
	prefix := make([]byte, min(start, int64(SectorSize)))
	if _, err := data.ReadAt(prefix, 0); err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(prefix), nil
}

// A temporary replacement is never the authoritative journal. Only discard it
// after successful replay (or confirmation that no journal exists), on writable
// open. Read-only inspection must preserve it as evidence of interrupted work.
func discardUndoTemp(data *os.File) error {
	path := data.Name() + ".undo.tmp"
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return undoSyncDirectory(path)
}
