package meta

import (
	"context"
	"testing"
)

// commitEmptyFile publishes a zero-chunk file head at path, which is enough to
// exercise the namespace (parent/name + ancestor dir creation).
func commitEmptyFile(t *testing.T, db *DB, path string) {
	t.Helper()
	if _, err := db.CommitFile(context.Background(), path, 1, nil); err != nil {
		t.Fatalf("commit %q: %v", path, err)
	}
}

func entryNames(entries []DirEntry) (dirs, files []string) {
	for _, e := range entries {
		if e.IsDir {
			dirs = append(dirs, e.Name)
		} else {
			files = append(files, e.Name)
		}
	}
	return
}

func TestListDirReturnsImmediateChildrenOnly(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	commitEmptyFile(t, db, "bucket/a.txt")
	commitEmptyFile(t, db, "bucket/sub/b.txt")
	commitEmptyFile(t, db, "bucket/sub/deep/c.txt")
	commitEmptyFile(t, db, "other/d.txt")

	entries, err := db.ListDir(ctx, "bucket")
	if err != nil {
		t.Fatal(err)
	}
	dirs, files := entryNames(entries)
	if len(files) != 1 || files[0] != "a.txt" {
		t.Fatalf("bucket files = %v, want [a.txt]", files)
	}
	if len(dirs) != 1 || dirs[0] != "sub" {
		t.Fatalf("bucket dirs = %v, want [sub] (immediate children only)", dirs)
	}

	// Root lists the two top-level buckets as directories, no files.
	rootEntries, err := db.ListDir(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	rootDirs, rootFiles := entryNames(rootEntries)
	if len(rootFiles) != 0 {
		t.Fatalf("root files = %v, want none", rootFiles)
	}
	if len(rootDirs) != 2 || rootDirs[0] != "bucket" || rootDirs[1] != "other" {
		t.Fatalf("root dirs = %v, want [bucket other]", rootDirs)
	}
}

func TestCommitFileCreatesAncestorDirs(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	commitEmptyFile(t, db, "x/y/z/file.txt")
	for _, dir := range []string{"x", "x/y", "x/y/z"} {
		e, ok, err := db.StatPath(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || !e.IsDir {
			t.Fatalf("ancestor %q: ok=%v isDir=%v, want a directory", dir, ok, e.IsDir)
		}
	}
}

func TestMkdirRmdir(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Mkdir(ctx, "empty/dir", 5); err != nil {
		t.Fatal(err)
	}
	e, ok, err := db.StatPath(ctx, "empty/dir")
	if err != nil || !ok || !e.IsDir {
		t.Fatalf("StatPath(empty/dir) = (%+v, %v, %v)", e, ok, err)
	}
	// The empty dir shows up in its parent's listing (marker row).
	entries, err := db.ListDir(ctx, "empty")
	if err != nil {
		t.Fatal(err)
	}
	if dirs, _ := entryNames(entries); len(dirs) != 1 || dirs[0] != "dir" {
		t.Fatalf("empty/ dirs = %v, want [dir]", dirs)
	}

	// Non-empty rmdir is refused.
	commitEmptyFile(t, db, "empty/dir/f.txt")
	if err := db.Rmdir(ctx, "empty/dir"); err == nil {
		t.Fatal("rmdir of non-empty dir should fail")
	}

	// After unlinking the child, rmdir succeeds.
	if err := db.UnlinkFile(ctx, "empty/dir/f.txt"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rmdir(ctx, "empty/dir"); err != nil {
		t.Fatalf("rmdir empty dir: %v", err)
	}
	if _, ok, _ := db.StatPath(ctx, "empty/dir"); ok {
		t.Fatal("dir still present after rmdir")
	}
}

func TestMkdirRejectsFilePath(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	commitEmptyFile(t, db, "a/file")
	if err := db.Mkdir(context.Background(), "a/file", 1); err == nil {
		t.Fatal("mkdir over an existing file should fail")
	}
}

func TestRenameDirectorySubtree(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	commitEmptyFile(t, db, "proj/src/a.go")
	commitEmptyFile(t, db, "proj/src/sub/b.go")
	if err := db.Mkdir(ctx, "proj/src/empty", 1); err != nil {
		t.Fatal(err)
	}

	if err := db.RenameFile(ctx, "proj/src", "proj/lib"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}

	// Old paths are gone, new paths resolve.
	if _, ok, _ := db.StatPath(ctx, "proj/src"); ok {
		t.Fatal("old dir still present")
	}
	for _, p := range []string{"proj/lib", "proj/lib/sub", "proj/lib/empty"} {
		if e, ok, err := db.StatPath(ctx, p); err != nil || !ok || !e.IsDir {
			t.Fatalf("StatPath(%q) = (%+v, %v, %v)", p, e, ok, err)
		}
	}
	for _, p := range []string{"proj/lib/a.go", "proj/lib/sub/b.go"} {
		if e, ok, err := db.StatPath(ctx, p); err != nil || !ok || e.IsDir {
			t.Fatalf("StatPath(%q) = (%+v, %v, %v), want a file", p, e, ok, err)
		}
	}
	// The renamed subtree is correctly parented under the new name.
	entries, err := db.ListDir(ctx, "proj/lib")
	if err != nil {
		t.Fatal(err)
	}
	dirs, files := entryNames(entries)
	if len(files) != 1 || files[0] != "a.go" {
		t.Fatalf("proj/lib files = %v", files)
	}
	if len(dirs) != 2 || dirs[0] != "empty" || dirs[1] != "sub" {
		t.Fatalf("proj/lib dirs = %v, want [empty sub]", dirs)
	}
}

func TestRenameFileSetsParent(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	commitEmptyFile(t, db, "d1/a.txt")
	if err := db.RenameFile(ctx, "d1/a.txt", "d2/b.txt"); err != nil {
		t.Fatal(err)
	}
	entries, err := db.ListDir(ctx, "d2")
	if err != nil {
		t.Fatal(err)
	}
	if _, files := entryNames(entries); len(files) != 1 || files[0] != "b.txt" {
		t.Fatalf("d2 files = %v, want [b.txt]", files)
	}
}

func TestBackfillNamespace(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	// Simulate a pre-migration namespace: a file head with no parent/name and no
	// ancestor dir rows.
	res, err := db.db.ExecContext(ctx, "INSERT INTO file (path, mtime, chunks) VALUES ('legacy/deep/f.txt', 1, '')")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := res.LastInsertId()
	if _, err := db.db.ExecContext(ctx, "INSERT INTO file_head (path, file_id, parent, name) VALUES ('legacy/deep/f.txt', ?, '', '')", id); err != nil {
		t.Fatal(err)
	}

	if err := backfillNamespace(db.db); err != nil {
		t.Fatal(err)
	}

	for _, dir := range []string{"legacy", "legacy/deep"} {
		if e, ok, err := db.StatPath(ctx, dir); err != nil || !ok || !e.IsDir {
			t.Fatalf("backfilled ancestor %q = (%+v, %v, %v)", dir, e, ok, err)
		}
	}
	entries, err := db.ListDir(ctx, "legacy/deep")
	if err != nil {
		t.Fatal(err)
	}
	if _, files := entryNames(entries); len(files) != 1 || files[0] != "f.txt" {
		t.Fatalf("legacy/deep files = %v, want [f.txt]", files)
	}
}

func TestSplitPath(t *testing.T) {
	cases := []struct{ in, parent, name string }{
		{"a", "", "a"},
		{"a/b", "a", "b"},
		{"a/b/c", "a/b", "c"},
		{"/a/b", "a", "b"},
		{"", "", ""},
	}
	for _, c := range cases {
		p, n := splitPath(c.in)
		if p != c.parent || n != c.name {
			t.Errorf("splitPath(%q) = (%q, %q), want (%q, %q)", c.in, p, n, c.parent, c.name)
		}
	}
}
