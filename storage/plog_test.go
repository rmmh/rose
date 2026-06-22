package storage

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func tempPlog(t *testing.T, name string) (*Plog, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	p, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatalf("open plog: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, path
}

// corruptByte flips one byte at a physical file offset behind a plog's back.
func corruptByte(t *testing.T, path string, physOffset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open for corruption: %v", err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, physOffset); err != nil {
		t.Fatalf("read for corruption: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, physOffset); err != nil {
		t.Fatalf("write corruption: %v", err)
	}
}

// twoBlockPayload returns deterministic data spanning more than one full
// hash-protected block so that early sectors have on-disk hash sectors.
func twoBlockPayload() []byte {
	data := make([]byte, dataPerBlock+5*SectorSize+123)
	rng := rand.New(rand.NewSource(99))
	rng.Read(data)
	return data
}

func TestPlogRoundTripAcrossBlocks(t *testing.T) {
	p, _ := tempPlog(t, "plog")
	data := twoBlockPayload()
	if _, err := p.Write(0, data); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	// Unaligned read spanning a block + hash-sector boundary.
	got, err := p.Read(dataPerBlock-10, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[dataPerBlock-10:dataPerBlock-10+4096]) {
		t.Fatalf("unaligned read mismatch")
	}
	full, err := p.Read(0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(full, data) {
		t.Fatalf("full read mismatch")
	}
}

func TestPlogDetectsBitrotOnRead(t *testing.T) {
	p, path := tempPlog(t, "plog")
	data := twoBlockPayload()
	if _, err := p.Write(0, data); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	// Sector 0 lives in a completed block, so its hash is durably on disk.
	corruptByte(t, path, 100)
	if _, err := p.Read(0, 256); !errors.Is(err, ErrBitrot) {
		t.Fatalf("read of corrupt sector = %v, want ErrBitrot", err)
	}
	// A sector in a later, intact block still reads cleanly.
	if _, err := p.Read(dataPerBlock+10, 256); err != nil {
		t.Fatalf("read of intact sector failed: %v", err)
	}
}

func TestPlogScrubReportsCorruption(t *testing.T) {
	p, path := tempPlog(t, "plog")
	data := twoBlockPayload()
	if _, err := p.Write(0, data); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if res, err := p.Scrub(); err != nil || !res.Healthy() {
		t.Fatalf("scrub of healthy plog: res=%+v err=%v", res, err)
	}
	corruptByte(t, path, dataPerBlock-50) // last data sector of block 0
	res, err := p.Scrub()
	if err != nil {
		t.Fatal(err)
	}
	if len(res.CorruptSectors) != 1 {
		t.Fatalf("scrub corrupt sectors = %v, want exactly one", res.CorruptSectors)
	}
	wantOffset := int64((HashesPerBlock - 1) * SectorSize)
	if res.CorruptSectors[0] != wantOffset {
		t.Fatalf("corrupt sector offset = %d, want %d", res.CorruptSectors[0], wantOffset)
	}
}

// TestPlogRaggedEdgeAcrossCommits exercises the case that previously misaligned
// the layout: many small writes each followed by Commit, then a reopen.
func TestPlogRaggedEdgeAcrossCommits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog")
	p, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	var want []byte
	offsets := make([]int64, 0)
	pieces := [][]byte{[]byte("first version"), []byte("second version"), []byte("third, somewhat longer, version")}
	for _, piece := range pieces {
		off, err := p.Write(0, piece)
		if err != nil {
			t.Fatal(err)
		}
		offsets = append(offsets, off)
		want = append(want, piece...)
		if err := p.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	_ = p.Close()

	reopened, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for i, piece := range pieces {
		got, err := reopened.Read(offsets[i], len(piece))
		if err != nil {
			t.Fatalf("reopened read piece %d: %v", i, err)
		}
		if !bytes.Equal(got, piece) {
			t.Fatalf("reopened piece %d = %q, want %q", i, got, piece)
		}
	}
	// Appending after recovery must keep sectors aligned and readable.
	if _, err := reopened.Write(0, []byte("appended after restart")); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Commit(); err != nil {
		t.Fatal(err)
	}
	full, err := reopened.Read(0, len(want)+len("appended after restart"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(full, append(want, []byte("appended after restart")...)) {
		t.Fatalf("post-recovery full read mismatch: %q", full)
	}
}

// TestPlogTrailingBlockVerifiableAcrossRestart writes a sub-block payload (sealed
// sectors but no completed hash sector), commits, and reopens. A sector that rots
// while the plog is closed must be caught on read after the restart, which is only
// possible because the open block's sealed hashes were persisted to the sidecar.
func TestPlogTrailingBlockVerifiableAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog")
	p, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Five full sectors plus a ragged tail: well under a full block, so no hash
	// sector is emitted and the sealed sectors are protected only by the sidecar.
	payload := make([]byte, 5*SectorSize+200)
	rand.New(rand.NewSource(7)).Read(payload)
	if _, err := p.Write(0, payload); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()

	// The sidecar exists and exactly covers the five sealed sectors.
	persisted, err := os.ReadFile(path + OpenHashesSuffix)
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if len(persisted) != 5*HashSize {
		t.Fatalf("sidecar holds %d bytes, want %d (5 sealed sectors)", len(persisted), 5*HashSize)
	}

	// Rot a byte in the second sealed sector while the plog is closed.
	corruptByte(t, path, SectorSize+100)

	reopened, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Read(0, 3*SectorSize); !errors.Is(err, ErrBitrot) {
		t.Fatalf("read of rotted trailing sector after restart = %v, want ErrBitrot", err)
	}

	// Without the sidecar the loader recomputes the hash from the (corrupt) bytes
	// and cannot tell -- the gap the sidecar closes.
	if err := os.Remove(path + OpenHashesSuffix); err != nil {
		t.Fatal(err)
	}
	blind, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer blind.Close()
	if _, err := blind.Read(0, 3*SectorSize); err != nil {
		t.Fatalf("without sidecar the corruption should be undetected, got %v", err)
	}
}

// TestPlogSidecarClearedWhenBlockCompletes checks the sidecar is removed once the
// open block fills and flushes its hash sector into the main file, so a later
// restart never mistakes a stale hash list for a fresh open block.
func TestPlogSidecarClearedWhenBlockCompletes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog")
	p, err := OpenPlog(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// A few sealed sectors: the sidecar is present.
	short := make([]byte, 3*SectorSize)
	rand.New(rand.NewSource(8)).Read(short)
	if _, err := p.Write(0, short); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + OpenHashesSuffix); err != nil {
		t.Fatalf("sidecar should exist for an open block: %v", err)
	}

	// Fill out exactly one full block: its hash sector goes into the main file and
	// the open block becomes empty, so the sidecar must be cleared.
	rest := make([]byte, dataPerBlock-3*SectorSize)
	rand.New(rand.NewSource(9)).Read(rest)
	if _, err := p.Write(0, rest); err != nil {
		t.Fatal(err)
	}
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + OpenHashesSuffix); !os.IsNotExist(err) {
		t.Fatalf("sidecar should be cleared after the block completed, stat err = %v", err)
	}
}

// plogClientAdapter wraps a *Plog as a committing PlogClient for vlog tests.
type plogClientAdapter struct{ p *Plog }

func (a plogClientAdapter) Write(_ context.Context, txnID int64, data []byte) (int64, error) {
	return a.p.Write(txnID, data)
}

func (a plogClientAdapter) EnsureAppend(_ context.Context, offset int64, data []byte) error {
	return a.p.EnsureAppend(offset, data)
}
func (a plogClientAdapter) Read(_ context.Context, offset int64, length int) ([]byte, error) {
	return a.p.Read(offset, length)
}
func (a plogClientAdapter) Commit(_ context.Context, txnID int64) error { return a.p.Commit() }

func TestDuplicateVlogSurvivesBitrot(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a")
	pathB := filepath.Join(dir, "b")
	plogA, err := OpenPlog(pathA, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer plogA.Close()
	plogB, err := OpenPlog(pathB, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer plogB.Close()

	vlog, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{plogA}, plogClientAdapter{plogB}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := twoBlockPayload()
	if _, err := vlog.Write(ctx, 1, data); err != nil {
		t.Fatal(err)
	}
	if err := vlog.Commit(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// Corrupt the first replica; the duplicate must still serve correct data.
	corruptByte(t, pathA, 100)
	if _, err := plogA.Read(0, 256); !errors.Is(err, ErrBitrot) {
		t.Fatalf("replica A read = %v, want ErrBitrot", err)
	}
	got, err := vlog.Read(ctx, 0, 256)
	if err != nil {
		t.Fatalf("duplicate vlog read after bitrot: %v", err)
	}
	if !bytes.Equal(got, data[:256]) {
		t.Fatalf("duplicate vlog returned wrong data after bitrot")
	}
}
