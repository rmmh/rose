package storage

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUndoCrashChild(t *testing.T) {
	point := os.Getenv("ROSE_UNDO_CRASH_POINT")
	if point == "" {
		return
	}
	undoCrashHook = func(name string) {
		if name == point {
			// No defers, Close, or test-framework shutdown: the OS closes the
			// process's descriptors at the specified real persistence boundary.
			os.Exit(73)
		}
	}
	path := os.Getenv("ROSE_UNDO_CRASH_PATH")
	p, err := OpenExistingPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Write(p.LogicalLength(), bytes.Repeat([]byte{0xc3}, 2*SectorSize)); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(point, "extend-") {
		// The first append already protected the final block. Moving the
		// truncation into an earlier block replaces that existing journal.
		undoCrashHook = func(name string) {
			if "extend-"+name == point {
				os.Exit(73)
			}
		}
		err = p.TruncateTo(10)
	} else if strings.HasPrefix(point, "restore-") {
		// Persist torn acknowledged bytes as well as the longer uncommitted
		// tail; recovery must use the journal rather than hash this new state.
		if _, err := p.file.WriteAt([]byte{0xff}, CalcPhysical(99)); err != nil {
			t.Fatal(err)
		}
		if err := p.file.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = OpenExistingPlog(path, 1)
	} else {
		err = p.Commit()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Fatalf("crash point %q was not reached", point)
}

func TestPlogUndoProcessCrashBoundaries(t *testing.T) {
	for _, point := range []string{
		"save-written", "save-synced", "save-renamed", "save-durable",
		"commit-synced", "commit-removed", "commit-durable",
		"restore-copied", "restore-truncated", "restore-synced", "restore-removed", "restore-durable",
		"extend-save-written", "extend-save-synced", "extend-save-renamed", "extend-save-durable",
	} {
		t.Run(point, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "plog")
			p, err := OpenPlog(path, 1)
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0x5a}, 100)
			if strings.HasPrefix(point, "extend-") {
				want = bytes.Repeat([]byte{0x5a}, dataPerBlock+100)
			}
			if _, err := p.Write(0, want); err != nil {
				t.Fatal(err)
			}
			if err := p.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestUndoCrashChild$")
			cmd.Env = append(os.Environ(), "ROSE_UNDO_CRASH_POINT="+point, "ROSE_UNDO_CRASH_PATH="+path)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("child did not crash at %s: %v\n%s", point, err, output)
			}
			// A process crash after unlink sees the replacement data. Before
			// unlink it rolls back. Power loss before directory sync may permit
			// either outcome; modeling cache loss remains a separate obligation.
			if point == "commit-removed" || point == "commit-durable" {
				want = append(want, bytes.Repeat([]byte{0xc3}, 2*SectorSize)...)
			}
			for reopen := 0; reopen < 2; reopen++ {
				p, err := OpenExistingPlog(path, 1)
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := p.Read(0, len(want))
				length := p.LogicalLength()
				verifyErr := p.Verify()
				p.Close()
				if readErr != nil || verifyErr != nil || length != int64(len(want)) || !bytes.Equal(got, want) {
					t.Fatalf("reopen %d length=%d want=%d read=%v verify=%v", reopen, length, len(want), readErr, verifyErr)
				}
				for _, suffix := range []string{".undo", ".undo.tmp"} {
					if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
						t.Fatalf("recovery left sidecar %s: %v", suffix, err)
					}
				}
			}
		})
	}
}

func TestRemovePlogFilesCleansRecoveryEvidence(t *testing.T) {
	p, path := tempPlog(t, "plog")
	if _, err := p.Write(0, make([]byte, SectorSize)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".undo.tmp", []byte("interrupted replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for repeat := 0; repeat < 2; repeat++ {
		if err := RemovePlogFiles(path); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{"", ".undo", ".undo.tmp"} {
			if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
				t.Fatalf("surviving %s: %v", suffix, err)
			}
		}
	}
}
