package fuse_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	rosefuse "github.com/rmmh/rose/fuse"
	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/server"
	"golang.org/x/sys/unix"
)

func TestFuseRenameRejectsUnsupportedFlagsBeforeMutation(t *testing.T) {
	root := rosefuse.NewRoseRoot(nil)
	if errno := root.Rename(context.Background(), "source", root, "dest", 1); errno != syscall.EINVAL {
		t.Fatalf("Rename with unsupported flags = %v, want EINVAL", errno)
	}
}

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
// the platform cannot establish a FUSE mount unless ROSE_REQUIRE_FUSE=1.
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
	ttl := time.Duration(0)
	mountOptions := fuse.MountOptions{FsName: "rose-test"}
	if runtime.GOOS == "darwin" {
		// Suppress macFUSE's private AppleDouble and xattr probes. These
		// options must not be passed to Linux fusermount.
		mountOptions.Options = []string{"noappledouble", "noapplexattr"}
	}
	fuseServer, err := gofuse.Mount(mnt, root, &gofuse.Options{
		MountOptions:    mountOptions,
		EntryTimeout:    &ttl,
		AttrTimeout:     &ttl,
		NegativeTimeout: &ttl,
	})
	if err != nil {
		if os.Getenv("ROSE_REQUIRE_FUSE") == "1" {
			t.Fatalf("required FUSE mount unavailable: %v", err)
		}
		t.Skipf("FUSE mount unavailable: %v", err)
	}
	t.Cleanup(func() {
		if err := fuseServer.Unmount(); err != nil {
			// Best effort; lazy-unmount so a stuck handle does not wedge the suite.
			_ = fuseServer.Unmount()
		}
	})
	// Register cleanup before checking the handshake, including its failure path.
	if err := fuseServer.WaitMount(); err != nil {
		if os.Getenv("ROSE_REQUIRE_FUSE") == "1" {
			t.Fatalf("required FUSE mount did not settle: %v", err)
		}
		t.Skipf("FUSE mount did not settle: %v", err)
	}
	return mnt
}

func TestFuseTouchStyleCreateAndSetTimesBeforeClose(t *testing.T) {
	mnt := mountRose(t)

	bucket := filepath.Join(mnt, "bucket")
	retryNoSys(t, "mkdir", func() error { return os.Mkdir(bucket, 0755) })
	path := filepath.Join(bucket, "foo")
	want := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

	var f *os.File
	retryNoSys(t, "create", func() (err error) {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
		return err
	})
	tv := []unix.Timeval{
		unix.NsecToTimeval(want.UnixNano()),
		unix.NsecToTimeval(want.UnixNano()),
	}
	retryNoSys(t, "futimes", func() error { return unix.Futimes(int(f.Fd()), tv) })
	retryNoSys(t, "close", f.Close)

	var fi os.FileInfo
	retryNoSys(t, "stat", func() (err error) { fi, err = os.Stat(path); return })
	if fi.Size() != 0 {
		t.Fatalf("size = %d, want 0", fi.Size())
	}
	if !fi.ModTime().Equal(want) {
		t.Fatalf("mtime = %v, want %v", fi.ModTime().UTC(), want)
	}
}

func TestFuseCopyAppearsInImmediateList(t *testing.T) {
	mnt := mountRose(t)

	bucket := filepath.Join(mnt, "bucket")
	retryNoSys(t, "mkdir", func() error { return os.Mkdir(bucket, 0755) })
	src := filepath.Join(t.TempDir(), "copied.txt")
	want := []byte("copied through cp\n")
	if err := os.WriteFile(src, want, 0644); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(bucket, "copied.txt")
	retryNoSys(t, "cp", func() error {
		return exec.Command("cp", src, dst).Run()
	})

	var ents []os.DirEntry
	retryNoSys(t, "readdir after cp", func() (err error) {
		ents, err = os.ReadDir(bucket)
		return err
	})
	if len(ents) != 1 || ents[0].Name() != "copied.txt" {
		t.Fatalf("listing = %v, want [copied.txt]", ents)
	}
	var got []byte
	retryNoSys(t, "read copied", func() (err error) {
		got, err = os.ReadFile(dst)
		return err
	})
	if string(got) != string(want) {
		t.Fatalf("copied content = %q, want %q", got, want)
	}
}

func TestFuseShellRedirectionWritesAfterDupClose(t *testing.T) {
	mnt := mountRose(t)

	bucket := filepath.Join(mnt, "bucket")
	retryNoSys(t, "mkdir", func() error { return os.Mkdir(bucket, 0755) })
	dst := filepath.Join(bucket, "redir.txt")
	retryNoSys(t, "shell redirect", func() error {
		return exec.Command("sh", "-c", "echo test > \"$1\"", "sh", dst).Run()
	})

	var got []byte
	retryNoSys(t, "read redirected", func() (err error) {
		got, err = os.ReadFile(dst)
		return err
	})
	if string(got) != "test\n" {
		t.Fatalf("redirected content = %q, want %q", got, "test\n")
	}
}

func TestFuseFileTimes(t *testing.T) {
	mnt := mountRose(t)

	bucket := filepath.Join(mnt, "bucket")
	retryNoSys(t, "mkdir", func() error { return os.Mkdir(bucket, 0755) })
	f := filepath.Join(bucket, "a.txt")
	retryNoSys(t, "write", func() error { return os.WriteFile(f, []byte("hi"), 0644) })

	// A freshly written file reports a sane, non-zero mtime (the bug was every
	// node reporting the 1970 epoch because FUSE never filled out.Mtime).
	var fi os.FileInfo
	retryNoSys(t, "stat", func() (err error) { fi, err = os.Stat(f); return })
	if fi.ModTime().Year() < 2000 {
		t.Fatalf("file mtime = %v, want a recent time", fi.ModTime())
	}

	// utimes round-trips: setting a specific mtime is visible on the next stat.
	want := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	retryNoSys(t, "chtimes", func() error { return os.Chtimes(f, want, want) })
	retryNoSys(t, "restat", func() (err error) { fi, err = os.Stat(f); return })
	if !fi.ModTime().Equal(want) {
		t.Fatalf("file mtime after Chtimes = %v, want %v", fi.ModTime().UTC(), want)
	}

	// Directories carry an mtime too.
	var di os.FileInfo
	retryNoSys(t, "stat dir", func() (err error) { di, err = os.Stat(bucket); return })
	if di.ModTime().Year() < 2000 {
		t.Fatalf("dir mtime = %v, want a recent time", di.ModTime())
	}
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
