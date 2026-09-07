package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
)

func TestCatalogCheckEmitsScopedReportAndFailsOnIssues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rose.db")
	db, err := meta.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var out bytes.Buffer
	if err := runCatalogCheck(context.Background(), path, &out); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Scope  string                  `json:"scope"`
		Issues []meta.ConsistencyIssue `json:"issues"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || report.Scope != "catalog" || len(report.Issues) != 0 {
		t.Fatalf("report=%s err=%v", out.Bytes(), err)
	}
	if _, err := db.GetDB().Exec("INSERT INTO chunk(hash,refcount,vlog_id,vaddr_offset,logical_len,compressed_len) VALUES(zeroblob(15),0,99,0,1,1)"); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := runCatalogCheck(context.Background(), path, &out); err == nil {
		t.Fatal("inconsistent catalog returned success")
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil || len(report.Issues) != 1 || report.Issues[0].Code != "missing_vlog" {
		t.Fatalf("report=%s err=%v", out.Bytes(), err)
	}
}
