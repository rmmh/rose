package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestPlogFailedAppendThenCommitDiscardsPhysicalTail(t *testing.T) {
	for _, length := range []int{100, dataPerBlock} {
		for _, retryAppend := range []bool{false, true} {
			t.Run(fmt.Sprintf("prefix=%d/retry=%v", length, retryAppend), func(t *testing.T) {
				p, path := tempPlog(t, "plog")
				want := bytes.Repeat([]byte{0x5a}, length)
				if _, err := p.Write(0, want); err != nil {
					t.Fatal(err)
				}
				if err := p.Commit(); err != nil {
					t.Fatal(err)
				}
				p.writeAt = func(data []byte, offset int64) (int, error) {
					// Persist several sectors, then report a short write. This
					// leaves a tail beyond even the old open trailer.
					n, err := p.file.WriteAt(data[:len(data)-SectorSize], offset)
					if err != nil {
						return n, err
					}
					return n, io.ErrShortWrite
				}
				if _, err := p.Write(int64(length), make([]byte, 8*SectorSize)); !errors.Is(err, io.ErrShortWrite) {
					t.Fatalf("partial write error=%v", err)
				}
				if p.LogicalLength() != int64(length) {
					t.Fatal("failed append changed cursor")
				}
				p.writeAt = nil
				if retryAppend {
					// A smaller subsequent append must not inherit the rejected
					// request's longer physical suffix either.
					next := bytes.Repeat([]byte{0xc3}, 17)
					if _, err := p.Write(int64(length), next); err != nil {
						t.Fatal(err)
					}
					want = append(want, next...)
				}
				if err := p.Commit(); err != nil {
					t.Fatal(err)
				}
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := OpenExistingPlog(path, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				got, err := reopened.Read(0, len(want))
				if err != nil || !bytes.Equal(got, want) || reopened.LogicalLength() != int64(len(want)) {
					t.Fatalf("failed physical tail survived commit: length=%d want=%d err=%v", reopened.LogicalLength(), len(want), err)
				}
				if err := reopened.Verify(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestPlogCommitShortWritePreservesRecovery(t *testing.T) {
	for _, failCall := range []int{1, 2} {
		t.Run(fmt.Sprint(failCall), func(t *testing.T) {
			p, path := tempPlog(t, "plog")
			prefix := bytes.Repeat([]byte{0x5a}, 100)
			if _, err := p.Write(0, prefix); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Write(100, []byte("pending")); err != nil {
				t.Fatal(err)
			}
			calls := 0
			p.writeAt = func(data []byte, offset int64) (int, error) {
				calls++
				if calls == failCall {
					return p.file.WriteAt(data[:len(data)/2], offset)
				}
				return p.file.WriteAt(data, offset)
			}
			if err := p.Commit(); !errors.Is(err, io.ErrShortWrite) {
				t.Fatalf("commit short write=%v", err)
			}
			if _, err := os.Stat(path + ".undo"); err != nil {
				t.Fatalf("failed commit retired recovery journal: %v", err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenExistingPlog(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Read(0, len(prefix))
			if err != nil || !bytes.Equal(got, prefix) || reopened.LogicalLength() != 100 {
				t.Fatalf("failed commit lost prefix: %v", err)
			}
		})
	}
}

func TestPlogDataSyncFailureRetainsJournal(t *testing.T) {
	p, path := tempPlog(t, "plog")
	prefix := bytes.Repeat([]byte{0x5a}, 100)
	if _, err := p.Write(0, prefix); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write(100, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected data sync failure")
	original := undoSyncFile
	t.Cleanup(func() { undoSyncFile = original })
	undoSyncFile = func(file *os.File) error {
		if file.Name() == path {
			return injected
		}
		return original(file)
	}
	for retry := 0; retry < 2; retry++ {
		if err := p.Commit(); !errors.Is(err, injected) {
			t.Fatalf("commit sync failure=%v", err)
		}
		if _, err := os.Stat(path + ".undo"); err != nil {
			t.Fatalf("failed sync retired journal: %v", err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	// Replay's own data sync must also succeed before it retires the journal.
	if reopened, err := OpenExistingPlog(path, 1); !errors.Is(err, injected) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatalf("replay bypassed data sync failure: %v", err)
	}
	if _, err := os.Stat(path + ".undo"); err != nil {
		t.Fatalf("failed replay retired journal: %v", err)
	}
	undoSyncFile = original
	reopened, err := OpenExistingPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read(0, len(prefix))
	if err != nil || !bytes.Equal(got, prefix) || reopened.LogicalLength() != 100 {
		t.Fatalf("sync retry lost prefix: %v", err)
	}
}

func TestPlogJournalSyncFailurePreventsDataWrite(t *testing.T) {
	p, path := tempPlog(t, "plog")
	prefix := bytes.Repeat([]byte{0x5a}, 100)
	if _, err := p.Write(0, prefix); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected journal sync failure")
	original := undoSyncFile
	t.Cleanup(func() { undoSyncFile = original })
	undoSyncFile = func(file *os.File) error {
		if file.Name() == path+".undo.tmp" {
			return injected
		}
		return original(file)
	}
	for retry := 0; retry < 2; retry++ {
		if _, err := p.Write(100, make([]byte, SectorSize)); !errors.Is(err, injected) {
			t.Fatalf("journal sync failure=%v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, baseline) || p.LogicalLength() != 100 {
			t.Fatalf("failed journal sync changed prefix: %v", err)
		}
		if _, err := os.Stat(path + ".undo"); !os.IsNotExist(err) {
			t.Fatalf("unsynced journal was published: %v", err)
		}
	}
}
