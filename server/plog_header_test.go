package server

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

// TestPlogSuperblockMembership provisions a DUPLICATE vlog and asserts every
// member plog's on-disk superblock is fully self-describing: cluster identity,
// the plog's own UID/id, its disk UID, and the complete vlog membership including
// the sibling plog UIDs indexed by shard.
func TestPlogSuperblockMembership(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	roots := map[uint32]string{}
	for id := uint32(1); id <= 3; id++ {
		roots[id] = filepath.Join(dir, "disk-"+string(rune('0'+id)))
	}
	db, err := meta.Open(filepath.Join(dir, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewServerWithDiskRoots(db, roots)
	if err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}

	s.vlogMu.Lock()
	vlogID, _, err := s.provisionVlogLocked(ctx, "DUPLICATE", 1, 0)
	s.vlogMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	clusterUID, _, _, err := db.ClusterInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	vinfo, err := db.GetVlog(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	members, err := db.ListVlogPlogs(ctx, vlogID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 mirror plogs, got %d", len(members))
	}

	// Map plog -> disk and gather the expected sibling UID set ordered by shard.
	plogInfos, err := db.ListPlogs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	diskOf := map[uint32]uint32{}
	for _, p := range plogInfos {
		diskOf[p.ID] = p.DiskID
	}
	wantSiblings := make([][]byte, len(members))
	for _, m := range members {
		pu, err := db.PlogUID(ctx, m.PlogID)
		if err != nil {
			t.Fatal(err)
		}
		wantSiblings[m.ShardIndex] = append([]byte(nil), pu[:]...)
	}

	// Close the server's open handles so we read the durable files fresh.
	s.CloseStorage()

	for _, m := range members {
		diskID := diskOf[m.PlogID]
		p, err := storage.OpenExistingPlog(s.plogPath(diskID, m.PlogID), m.PlogID)
		if err != nil {
			t.Fatalf("open plog %d: %v", m.PlogID, err)
		}
		h := p.Header()
		if h == nil {
			t.Fatalf("plog %d has no header", m.PlogID)
		}
		wantPlogUID, _ := db.PlogUID(ctx, m.PlogID)
		wantDiskUID, _ := db.DiskUID(ctx, diskID)
		if !bytes.Equal(h.ClusterUid, clusterUID[:]) {
			t.Errorf("plog %d cluster uid mismatch", m.PlogID)
		}
		if !bytes.Equal(h.PlogUid, wantPlogUID[:]) {
			t.Errorf("plog %d plog uid mismatch", m.PlogID)
		}
		if h.PlogId != m.PlogID {
			t.Errorf("plog %d header plog_id = %d", m.PlogID, h.PlogId)
		}
		if !bytes.Equal(h.DiskUid, wantDiskUID[:]) {
			t.Errorf("plog %d disk uid mismatch", m.PlogID)
		}
		if h.VlogId != vlogID || !bytes.Equal(h.VlogUid, vinfo.UID[:]) {
			t.Errorf("plog %d vlog identity mismatch: id=%d", m.PlogID, h.VlogId)
		}
		if int(h.ShardIndex) != m.ShardIndex {
			t.Errorf("plog %d shard index = %d, want %d", m.PlogID, h.ShardIndex, m.ShardIndex)
		}
		if h.ProtectionScheme != "DUPLICATE" {
			t.Errorf("plog %d scheme = %q", m.PlogID, h.ProtectionScheme)
		}
		if len(h.SiblingPlogUids) != len(wantSiblings) {
			t.Fatalf("plog %d sibling count = %d, want %d", m.PlogID, len(h.SiblingPlogUids), len(wantSiblings))
		}
		for i := range wantSiblings {
			if !bytes.Equal(h.SiblingPlogUids[i], wantSiblings[i]) {
				t.Errorf("plog %d sibling[%d] mismatch", m.PlogID, i)
			}
		}
		_ = p.Close()
	}

	// Sanity: the marker files exist on each disk root.
	for id := range roots {
		if _, err := os.Stat(filepath.Join(roots[id], diskUIDMarker)); err != nil {
			t.Errorf("disk %d missing rose_disk_uid marker: %v", id, err)
		}
	}
}
