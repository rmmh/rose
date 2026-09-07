package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
)

func publicationCrashBytes(seed int64) []byte {
	data := make([]byte, 65537)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

func openPublicationCrashServer(t *testing.T, dir string, disks int) (*Server, func()) {
	t.Helper()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	roots := make(map[uint32]string)
	for disk := 1; disk <= disks; disk++ {
		roots[uint32(disk)] = filepath.Join(dir, fmt.Sprintf("disk-%d", disk))
	}
	s := NewServerWithDiskRoots(db, roots)
	if err := s.Recover(context.Background()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	s.StopMaintenanceDriver()
	return s, func() { s.CloseStorage(); db.Close() }
}

func writePublicationCrashVersion(t *testing.T, s *Server) {
	t.Helper()
	ctx := context.Background()
	h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "crash-write"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: publicationCrashBytes(22)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
}

func TestServerPublicationCrashChild(t *testing.T) {
	stage := os.Getenv("ROSE_SERVER_PUBLICATION_STAGE")
	if stage == "" {
		return
	}
	disks := 1
	if os.Getenv("ROSE_SERVER_PUBLICATION_DISKS") == "2" {
		disks = 2
	}
	s, _ := openPublicationCrashServer(t, os.Getenv("ROSE_SERVER_PUBLICATION_DIR"), disks)
	s.publicationFault = func(at string) error {
		if at == stage {
			os.Exit(73)
		}
		return nil
	}
	writePublicationCrashVersion(t, s)
	t.Fatal("server publication crash point not reached")
}

func assertPublicationCrashRead(t *testing.T, s *Server, snapshot uint64, want []byte) {
	t.Helper()
	ctx := context.Background()
	var handle int64
	if snapshot == 0 {
		h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file"})
		if err != nil {
			t.Fatal(err)
		}
		handle = h.Handle
	} else {
		h, err := s.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: snapshot, Path: "bucket/file"})
		if err != nil {
			t.Fatal(err)
		}
		handle = h.Handle
	}
	defer s.Close(ctx, &pb.CloseRequest{Handle: handle})
	read, err := s.Read(ctx, &pb.ReadRequest{Handle: handle, Length: int64(len(want) + 1)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.Buffer, want) {
		t.Fatalf("snapshot=%d recovered different bytes or length: got=%d want=%d", snapshot, len(read.Buffer), len(want))
	}
}

func TestServerPublicationProcessCrashRecovery(t *testing.T) {
	for _, disks := range []int{1, 2} {
		for _, stage := range []string{"shards-synced", "prefix-recorded", "placements-verified", "namespace-published"} {
			t.Run(fmt.Sprintf("disks=%d/%s", disks, stage), func(t *testing.T) {
				dir := t.TempDir()
				s, closeServer := openPublicationCrashServer(t, dir, disks)
				old := publicationCrashBytes(11)
				auditWrite(t, s, "bucket/file", old)
				snap, err := s.CreateSnapshot(context.Background(), &pb.CreateSnapshotRequest{Name: "before-crash"})
				if err != nil {
					t.Fatal(err)
				}
				closeServer()
				cmd := exec.Command(os.Args[0], "-test.run=^TestServerPublicationCrashChild$")
				cmd.Env = append(os.Environ(), "ROSE_SERVER_PUBLICATION_STAGE="+stage, "ROSE_SERVER_PUBLICATION_DIR="+dir, fmt.Sprintf("ROSE_SERVER_PUBLICATION_DISKS=%d", disks))
				output, err := cmd.CombinedOutput()
				if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
					// Server startup logs contain keys. Keep child output on disk,
					// not in routine test failure output.
					log := filepath.Join(dir, "child.log")
					_ = os.WriteFile(log, output, 0600)
					t.Fatalf("child did not reach %s: %v; details in %s", stage, err, log)
				}
				s, closeServer = openPublicationCrashServer(t, dir, disks)
				want := old
				if stage == "namespace-published" {
					want = publicationCrashBytes(22)
				}
				assertPublicationCrashRead(t, s, 0, want)
				assertPublicationCrashRead(t, s, snap.SnapshotId, old)
				writePublicationCrashVersion(t, s)
				assertPublicationCrashRead(t, s, 0, publicationCrashBytes(22))
				assertPublicationCrashRead(t, s, snap.SnapshotId, old)
				closeServer()
				// A second clean recovery must preserve the retry result too.
				s, closeServer = openPublicationCrashServer(t, dir, disks)
				defer closeServer()
				assertPublicationCrashRead(t, s, 0, publicationCrashBytes(22))
				assertPublicationCrashRead(t, s, snap.SnapshotId, old)
			})
		}
	}
}

func TestServerPublicationErrorRetry(t *testing.T) {
	for _, stage := range []string{"shards-synced", "prefix-recorded", "placements-verified", "namespace-published"} {
		t.Run(stage, func(t *testing.T) {
			s, closeServer := openPublicationCrashServer(t, t.TempDir(), 2)
			defer closeServer()
			ctx := context.Background()
			old := publicationCrashBytes(11)
			auditWrite(t, s, "bucket/file", old)
			h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/file", OperationKey: "returned-error"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: publicationCrashBytes(22)}); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected publication boundary error")
			s.publicationFault = func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			}
			if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); !errors.Is(err, injected) {
				t.Fatalf("boundary error not returned: %v", err)
			}
			want := old
			if stage == "namespace-published" {
				want = publicationCrashBytes(22)
			}
			assertPublicationCrashRead(t, s, 0, want)
			s.publicationFault = nil
			if _, err := s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
				t.Fatal(err)
			}
			assertPublicationCrashRead(t, s, 0, publicationCrashBytes(22))
			if len(s.pinnedChunks) != 0 {
				t.Fatalf("successful retry retained %d pin owners", len(s.pinnedChunks))
			}
		})
	}
}
