package fuse_test

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	rosefuse "github.com/rmmh/rose/fuse"
	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/server"
)

// retryNoSys retries an op a few times while macFUSE returns ENOSYS. macFUSE
// intermittently emits a macFUSE-private opcode go-fuse does not implement during
// the create+truncate+write sequence; the kernel surfaces it as a spurious
// ENOSYS on an unrelated syscall. The retry isolates our FS logic from that
// platform artifact (the deterministic coverage lives in the meta and server
// tests).
func retryNoSys(t *testing.T, what string, op func() error) {
	t.Helper()
	var err error
	for i := 0; i < 20; i++ {
		err = op()
		if !errors.Is(err, syscall.ENOSYS) && !errors.Is(err, syscall.ENOTSUP) && !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// mountRose mounts a fresh Rose filesystem at a temp dir, skipping the test if
// the platform cannot establish a FUSE mount (e.g. macFUSE not installed in CI).
func mountRose(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srv := server.NewServerWithDataDir(db, filepath.Join(dir, "plogs"))

	mnt := filepath.Join(dir, "mnt")
	if err := os.MkdirAll(mnt, 0755); err != nil {
		t.Fatal(err)
	}
	root := rosefuse.NewRoseRoot(srv)
	fuseServer, err := gofuse.Mount(mnt, root, &gofuse.Options{
		MountOptions: fuse.MountOptions{
			FsName: "rose-test",
			// macFUSE otherwise probes AppleDouble (._*) sidecars and xattrs on
			// every op, emitting macFUSE-private opcodes go-fuse does not implement
			// (surfacing as spurious ENOSYS). These are no-ops on Linux.
			Options: []string{"noappledouble", "noapplexattr"},
		},
	})
	if err != nil {
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	// Wait for the kernel INIT handshake to finish; operations issued before it
	// completes get spurious ENOSYS/ENOTSUP on macFUSE.
	if err := fuseServer.WaitMount(); err != nil {
		t.Skipf("FUSE mount did not settle: %v", err)
	}
	t.Cleanup(func() {
		if err := fuseServer.Unmount(); err != nil {
			// Best effort; lazy-unmount so a stuck handle does not wedge the suite.
			_ = fuseServer.Unmount()
		}
	})
	return mnt
}

func TestFuseMkdirWriteReadList(t *testing.T) {
	mnt := mountRose(t)

	// mkdir, then a file inside it written through the mount.
	bucket := filepath.Join(mnt, "bucket")
	retryNoSys(t, "mkdir", func() error { return os.Mkdir(bucket, 0755) })
	want := []byte("hello rose over fuse")
	retryNoSys(t, "write", func() error { return os.WriteFile(filepath.Join(bucket, "a.txt"), want, 0644) })

	// Read it back.
	var got []byte
	retryNoSys(t, "read", func() (err error) {
		got, err = os.ReadFile(filepath.Join(bucket, "a.txt"))
		return
	})
	if string(got) != string(want) {
		t.Fatalf("read = %q, want %q", got, want)
	}

	// A subdirectory shows up in listings alongside the file.
	retryNoSys(t, "mkdir sub", func() error { return os.Mkdir(filepath.Join(bucket, "sub"), 0755) })
	var ents []os.DirEntry
	retryNoSys(t, "readdir", func() (err error) {
		ents, err = os.ReadDir(bucket)
		return
	})
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	if len(names) != 2 || names[0] != "a.txt" || names[1] != "sub" {
		t.Fatalf("listing = %v, want [a.txt sub]", names)
	}

	// Stat distinguishes file from dir.
	var fi, di os.FileInfo
	retryNoSys(t, "stat file", func() (err error) {
		fi, err = os.Stat(filepath.Join(bucket, "a.txt"))
		return
	})
	if fi.IsDir() || fi.Size() != int64(len(want)) {
		t.Fatalf("stat file = %+v", fi)
	}
	retryNoSys(t, "stat dir", func() (err error) {
		di, err = os.Stat(filepath.Join(bucket, "sub"))
		return
	})
	if !di.IsDir() {
		t.Fatalf("stat dir = %+v", di)
	}

	// Root lists the bucket.
	var rootEnts []os.DirEntry
	retryNoSys(t, "readdir root", func() (err error) {
		rootEnts, err = os.ReadDir(mnt)
		return
	})
	if len(rootEnts) != 1 || rootEnts[0].Name() != "bucket" {
		t.Fatalf("root listing = %v, want [bucket]", rootEnts)
	}
}
