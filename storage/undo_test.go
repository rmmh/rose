package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func undoFixture(t *testing.T) (*os.File, []byte) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "plog"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	data := bytes.Repeat([]byte("acknowledged-prefix"), 1024)
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	return f, data
}

func TestUndoRestoresTornSuffixAndTruncatesAppend(t *testing.T) {
	f, want := undoFixture(t)
	if err := saveUndo(f, 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xee}, len(want)), 4096); err != nil {
		t.Fatal(err)
	}
	if err := restoreUndo(f); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(f.Name())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("restored size=%d err=%v", len(got), err)
	}
	if err := restoreUndo(f); err != nil {
		t.Fatal(err)
	}
}

func TestUndoExtensionPreservesOriginalBaseline(t *testing.T) {
	f, want := undoFixture(t)
	if err := saveUndo(f, 8192); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(bytes.Repeat([]byte{0xab}, 8192), 8192); err != nil {
		t.Fatal(err)
	}
	if err := saveUndo(f, 4096); err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(5000); err != nil {
		t.Fatal(err)
	}
	if err := restoreUndo(f); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(f.Name())
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("extended journal saved modified bytes as the original suffix")
	}
}

func TestUndoRejectsCorruptEvidenceBeforeRestore(t *testing.T) {
	f, _ := undoFixture(t)
	if err := saveUndo(f, 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("changed"), 4096); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(f.Name())
	journal, err := os.OpenFile(f.Name()+".undo", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.WriteAt([]byte{0xff}, undoHeaderSize); err != nil {
		t.Fatal(err)
	}
	journal.Close()
	if err := restoreUndo(f); err == nil {
		t.Fatal("corrupt undo journal accepted")
	}
	after, _ := os.ReadFile(f.Name())
	if !bytes.Equal(before, after) {
		t.Fatal("failed validation partially restored data")
	}
}

func TestUndoCommitPreventsLaterRollback(t *testing.T) {
	f, _ := undoFixture(t)
	if err := saveUndo(f, 4096); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("new committed bytes"), 4096); err != nil {
		t.Fatal(err)
	}
	if err := commitUndo(f); err != nil {
		t.Fatal(err)
	}
	want, _ := os.ReadFile(f.Name())
	if err := restoreUndo(f); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(f.Name())
	if !bytes.Equal(got, want) {
		t.Fatal("committed bytes rolled back")
	}
}

func TestPlogUndoRestoresAcknowledgedPrefix(t *testing.T) {
	for _, length := range []int{100, SectorSize + 37, dataPerBlock - 17, dataPerBlock + 29} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			p, path := tempPlog(t, "plog")
			want := bytes.Repeat([]byte{0x5a}, length)
			if _, err := p.Write(0, want); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			baseline, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Write(int64(length), bytes.Repeat([]byte{0xc3}, 2*SectorSize)); err != nil {
				t.Fatal(err)
			}
			// Simulate a torn rewrite of the previous acknowledged ragged sector.
			corruptByte(t, path, CalcPhysical(int64(length-1)))
			before, _ := os.ReadFile(path)
			_, _ = InspectPlog(path, 1)
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) {
				t.Fatal("read-only inspection replayed journal")
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenExistingPlog(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			got, err := reopened.Read(0, length)
			if err != nil || !bytes.Equal(got, want) || reopened.LogicalLength() != int64(length) {
				t.Fatalf("acknowledged prefix not restored: length=%d err=%v", reopened.LogicalLength(), err)
			}
			restored, _ := os.ReadFile(path)
			if !bytes.Equal(restored, baseline) {
				t.Fatal("physical baseline not restored exactly")
			}
		})
	}
}

func TestPlogUndoTruncationCommitBoundary(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprint(commit), func(t *testing.T) {
			p, path := tempPlog(t, "plog")
			want := twoBlockPayload()
			if _, err := p.Write(0, want); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := p.TruncateTo(100); err != nil {
				t.Fatal(err)
			}
			if commit {
				if err := p.Commit(); err != nil {
					t.Fatal(err)
				}
				want = want[:100]
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
				t.Fatalf("truncate commit=%v: length=%d err=%v", commit, reopened.LogicalLength(), err)
			}
		})
	}
}

func TestPlogUndoRejectsChangedIdentity(t *testing.T) {
	p, path := tempPlog(t, "plog")
	if _, err := p.Write(0, bytes.Repeat([]byte{7}, 100)); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write(100, make([]byte, SectorSize)); err != nil {
		t.Fatal(err)
	}
	if err := p.RebindDiskUID([]byte("different disk")); err == nil {
		t.Fatal("rebound identity with pending journal")
	}
	corruptByte(t, path, 30)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if reopened, err := OpenExistingPlog(path, 1); err == nil {
		reopened.Close()
		t.Fatal("replayed journal against different superblock")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed identity check modified data")
	}
}
