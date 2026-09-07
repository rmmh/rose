package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func failUndoDirectorySync(t *testing.T) (*bool, *int, error) {
	t.Helper()
	failing, calls := true, 0
	injected := errors.New("injected directory sync failure")
	original := undoSyncDirectory
	undoSyncDirectory = func(path string) error {
		calls++
		if failing {
			return injected
		}
		return original(path)
	}
	t.Cleanup(func() { undoSyncDirectory = original })
	return &failing, &calls, injected
}

func TestPlogJournalCreationRetryRequiresDirectorySync(t *testing.T) {
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
	failing, calls, injected := failUndoDirectorySync(t)
	appendix := bytes.Repeat([]byte{0xc3}, SectorSize)
	for retry := 0; retry < 2; retry++ {
		if _, err := p.Write(100, appendix); !errors.Is(err, injected) {
			t.Fatalf("retry %d bypassed failed journal directory sync: %v", retry, err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, baseline) || p.LogicalLength() != 100 {
			t.Fatalf("retry %d modified data without a durable journal: %v", retry, err)
		}
	}
	if *calls != 2 {
		t.Fatalf("sync attempts=%d, want 2", *calls)
	}
	*failing = false
	if _, err := p.Write(100, appendix); err != nil {
		t.Fatal(err)
	}
	corruptByte(t, path, CalcPhysical(99))
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
		t.Fatalf("successful retry did not protect original prefix: %v", err)
	}
}

func TestPlogCommitRetryRequiresDurableJournalRemoval(t *testing.T) {
	p, path := tempPlog(t, "plog")
	prefix := bytes.Repeat([]byte{0x5a}, 100)
	if _, err := p.Write(0, prefix); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	// Finish the block exactly. A retry has no open trailer to rewrite and
	// therefore creates no fresh journal whose directory sync could hide this bug.
	want := append(prefix, bytes.Repeat([]byte{0xc3}, dataPerBlock-100)...)
	if _, err := p.Write(100, want[100:]); err != nil {
		t.Fatal(err)
	}
	failing, calls, injected := failUndoDirectorySync(t)
	for retry := 0; retry < 2; retry++ {
		if err := p.Commit(); !errors.Is(err, injected) {
			t.Fatalf("retry %d acknowledged without durable journal removal: %v", retry, err)
		}
		if _, err := os.Stat(path + ".undo"); !os.IsNotExist(err) {
			t.Fatalf("expected unlink before failed directory sync: %v", err)
		}
	}
	if *calls != 2 {
		t.Fatalf("sync attempts=%d, want 2", *calls)
	}
	*failing = false
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if *calls != 3 {
		t.Fatalf("successful retry omitted directory sync: calls=%d", *calls)
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
		t.Fatalf("committed prefix lost: %v", err)
	}
}

func TestPlogReplayRetryRequiresDurableJournalRemoval(t *testing.T) {
	p, path := tempPlog(t, "plog")
	prefix := bytes.Repeat([]byte{0x5a}, 100)
	if _, err := p.Write(0, prefix); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write(100, make([]byte, SectorSize)); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	failing, calls, injected := failUndoDirectorySync(t)
	for retry := 0; retry < 2; retry++ {
		reopened, err := OpenExistingPlog(path, 1)
		if reopened != nil {
			reopened.Close()
		}
		if !errors.Is(err, injected) {
			t.Fatalf("retry %d reopened before durable journal retirement: %v", retry, err)
		}
	}
	if *calls != 2 {
		t.Fatalf("sync attempts=%d, want 2", *calls)
	}
	*failing = false
	reopened, err := OpenExistingPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read(0, len(prefix))
	if err != nil || !bytes.Equal(got, prefix) || reopened.LogicalLength() != 100 {
		t.Fatalf("replay retry lost original prefix: %v", err)
	}
}
