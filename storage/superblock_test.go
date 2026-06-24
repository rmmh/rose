package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/uid"
)

func sampleHeader(id uint32) *pb.PlogHeader {
	cluster := uid.New()
	self := uid.New()
	sib := uid.New()
	return &pb.PlogHeader{
		ClusterUid:      cluster[:],
		PlogUid:         self[:],
		PlogId:          id,
		DiskId:          7,
		VlogId:          42,
		ShardIndex:      1,
		ProtectionScheme: "EC",
		DataShards:      4,
		ParityShards:    2,
		SiblingPlogUids: [][]byte{self[:], sib[:]},
	}
}

func TestSuperblockRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog-1")
	hdr := sampleHeader(1)
	p, err := OpenPlog(path, 1, WithHeader(hdr))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := OpenExistingPlog(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	got := reopened.Header()
	if got == nil {
		t.Fatal("nil header after reopen")
	}
	if !bytes.Equal(got.ClusterUid, hdr.ClusterUid) ||
		!bytes.Equal(got.PlogUid, hdr.PlogUid) ||
		got.PlogId != hdr.PlogId || got.DiskId != hdr.DiskId ||
		got.VlogId != hdr.VlogId || got.ShardIndex != hdr.ShardIndex ||
		got.ProtectionScheme != hdr.ProtectionScheme ||
		got.DataShards != hdr.DataShards || got.ParityShards != hdr.ParityShards {
		t.Fatalf("header mismatch:\n got %+v\nwant %+v", got, hdr)
	}
	if len(got.SiblingPlogUids) != 2 ||
		!bytes.Equal(got.SiblingPlogUids[0], hdr.SiblingPlogUids[0]) ||
		!bytes.Equal(got.SiblingPlogUids[1], hdr.SiblingPlogUids[1]) {
		t.Fatalf("sibling uids mismatch: %v", got.SiblingPlogUids)
	}
}

func TestOpenExistingRejectsHeaderless(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy")
	// A non-empty file with no superblock magic stands in for a legacy plog.
	if err := os.WriteFile(path, make([]byte, SectorSize*2), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := OpenExistingPlog(path, 1)
	if !errors.Is(err, ErrUnrecognizedPlogFormat) {
		t.Fatalf("got %v, want ErrUnrecognizedPlogFormat", err)
	}
}

func TestSuperblockCorruptionRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog-1")
	p, err := OpenPlog(path, 1, WithHeader(sampleHeader(1)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	p.Close()
	// Flip a byte inside the protobuf payload; the HMAC must catch it.
	corruptByte(t, path, plogHeaderPrefix+2)
	_, err = OpenExistingPlog(path, 1)
	if !errors.Is(err, ErrPlogHeaderCorrupt) {
		t.Fatalf("got %v, want ErrPlogHeaderCorrupt", err)
	}
}

// TestGeometrySurvivesHeader writes data spanning multiple hash blocks, reopens,
// and reads it back, proving the superblock offset round-trips through
// CalcPhysical/CalcLogical and that the first data byte is not the superblock.
func TestGeometrySurvivesHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plog-1")
	p, err := OpenPlog(path, 1, WithHeader(sampleHeader(1)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Just over two full blocks so we exercise interposed hash sectors.
	data := make([]byte, 2*DataPerBlock+1234)
	for i := range data {
		data[i] = byte(i*31 + 7)
	}
	if _, err := p.Write(1, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := p.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	p.Close()

	reopened, err := OpenExistingPlog(path, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	for _, span := range [][2]int{{0, 100}, {DataPerBlock - 50, 200}, {len(data) - 500, 500}} {
		off, n := span[0], span[1]
		got, err := reopened.Read(int64(off), n)
		if err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
		if !bytes.Equal(got, data[off:off+n]) {
			t.Fatalf("data mismatch at offset %d", off)
		}
	}
	// Logical length must exclude the superblock sector.
	if reopened.logicalLength != int64(len(data)) {
		t.Fatalf("logical length = %d, want %d", reopened.logicalLength, len(data))
	}
}
