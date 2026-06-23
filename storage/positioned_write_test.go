package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestPlogEnsureAppendRetriesPartialRange(t *testing.T) {
	p, err := OpenPlog(filepath.Join(t.TempDir(), "plog"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	data := bytes.Repeat([]byte("retry-safe"), 900)
	if _, err := p.Write(1, data[:4096]); err != nil {
		t.Fatal(err)
	}
	if err := p.EnsureAppend(0, data); err != nil {
		t.Fatal(err)
	}
	if err := p.EnsureAppend(0, data); err != nil {
		t.Fatal(err)
	}
	if got := p.LogicalLength(); got != int64(len(data)) {
		t.Fatalf("length = %d, want %d", got, len(data))
	}
	got, err := p.Read(0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("retried bytes differ")
	}
}

func TestVlogEnsureWriteRetriesPartialReplicaFanout(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenPlog(filepath.Join(dir, "a"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenPlog(filepath.Join(dir, "b"), 2)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	v, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{a}, plogClientAdapter{b}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("fanout"), 1200)
	if _, err := a.Write(1, data); err != nil {
		t.Fatal(err)
	} // simulate a prior partial fan-out
	if err := v.EnsureWrite(context.Background(), 0, [][]byte{data}); err != nil {
		t.Fatal(err)
	}
	if err := v.EnsureWrite(context.Background(), v.Length(), [][]byte{data}); err != nil {
		t.Fatal(err)
	}
	if got := v.Length(); got != int64(2*len(data)) {
		t.Fatalf("length = %d, want %d", got, 2*len(data))
	}
	got, err := v.Read(context.Background(), 0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("vlog content differs")
	}
}
