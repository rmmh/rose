package webdav_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/server"
	rosewebdav "github.com/rmmh/rose/webdav"
	"golang.org/x/net/webdav"
)

// newDAV stands up a Rose server behind a WebDAV HTTP handler and returns its
// base URL.
func newDAV(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	srv := server.NewServerWithDataDir(db, filepath.Join(dir, "plogs"))

	h := &webdav.Handler{
		FileSystem: rosewebdav.New(srv),
		LockSystem: webdav.NewMemLS(),
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts.URL
}

func do(t *testing.T, method, url string, body io.Reader, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func TestWebDAVMkcolPutGetPropfind(t *testing.T) {
	base := newDAV(t)

	// MKCOL creates a directory.
	resp := do(t, "MKCOL", base+"/bucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("MKCOL status = %d, want 201", resp.StatusCode)
	}

	// PUT writes a file into it.
	want := "hello rose over webdav"
	resp = do(t, "PUT", base+"/bucket/a.txt", strings.NewReader(want), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 201/204", resp.StatusCode)
	}

	// GET reads it back.
	resp = do(t, "GET", base+"/bucket/a.txt", nil, nil)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	if string(got) != want {
		t.Fatalf("GET body = %q, want %q", got, want)
	}

	// PROPFIND lists the directory's immediate children.
	resp = do(t, "PROPFIND", base+"/bucket", nil, map[string]string{"Depth": "1"})
	bodyBytes, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d, want 207", resp.StatusCode)
	}
	if !strings.Contains(string(bodyBytes), "a.txt") {
		t.Fatalf("PROPFIND body missing a.txt:\n%s", bodyBytes)
	}

	// DELETE removes the whole subtree.
	resp = do(t, "DELETE", base+"/bucket", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE status = %d, want 204/200", resp.StatusCode)
	}
	resp = do(t, "GET", base+"/bucket/a.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
}

func TestWebDAVMoveAndEmptyPut(t *testing.T) {
	base := newDAV(t)

	// An empty PUT still creates a file.
	resp := do(t, "PUT", base+"/empty.txt", strings.NewReader(""), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT empty status = %d", resp.StatusCode)
	}
	resp = do(t, "GET", base+"/empty.txt", nil, nil)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(got) != 0 {
		t.Fatalf("GET empty: status=%d body=%q", resp.StatusCode, got)
	}

	// MOVE renames it.
	resp = do(t, "MOVE", base+"/empty.txt", nil, map[string]string{"Destination": base + "/renamed.txt"})
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("MOVE status = %d", resp.StatusCode)
	}
	resp = do(t, "GET", base+"/renamed.txt", nil, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET renamed status = %d, want 200", resp.StatusCode)
	}
}
