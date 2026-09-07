package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/rmmh/rose/uid"
)

var publicationStages = []string{"version-written", "operation-written", "leases-released", "committed"}

func publicationFaultPlacements() []ChunkPlacement {
	p := ChunkPlacement{Hash: bytes.Repeat([]byte{2}, 15), VlogID: 1, VaddrOffset: 100, LogicalLen: 7}
	return []ChunkPlacement{p, p} // occurrence multiplicity matters for refcounts
}

func publicationFaultFixture(t *testing.T) (*DB, string, int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CommitFile(ctx, "bucket/file", 1, []ChunkPlacement{{Hash: bytes.Repeat([]byte{1}, 15), VlogID: 1, LogicalLen: 3}}); err != nil {
		t.Fatal(err)
	}
	op, err := db.CreateWriteOp(ctx, "pending", "bucket/file")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ClaimVlogLease(ctx, 1, op.ID, 0); err != nil {
		t.Fatal(err)
	}
	return db, path, op.ID
}

// Read raw rows and independently encode the expected ordered extents. Avoid the
// publication/refcount helpers so a shared bookkeeping bug cannot satisfy both
// sides of the assertion.
func assertPublicationOutcome(t *testing.T, db *DB, committed bool) int64 {
	t.Helper()
	var state string
	var opFile, headFile, offset int64
	var leases, versions int
	var chunks []byte
	if err := db.db.QueryRow(`SELECT state, file_id, acknowledged_offset FROM write_op WHERE idempotency_key='pending'`).Scan(&state, &opFile, &offset); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(`SELECT h.file_id, f.chunks FROM file_head h JOIN file f ON f.id=h.file_id WHERE h.path='bucket/file'`).Scan(&headFile, &chunks); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM vlog_lease`).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRow(`SELECT COUNT(*) FROM file`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	wantState, wantLeases, wantVersions, wantOffset := WriteOpPrepared, 1, 1, int64(0)
	count, hashByte, logicalLen := 1, byte(1), uint32(3)
	wantRefs := map[byte]int{1: 1}
	if committed {
		wantState, wantLeases, wantVersions, wantOffset = WriteOpCommitted, 0, 2, 14
		count, hashByte, logicalLen = 2, 2, 7
		wantRefs = map[byte]int{1: 0, 2: 2}
		if opFile != headFile {
			t.Fatalf("committed op=%d head=%d", opFile, headFile)
		}
	} else if opFile != 0 {
		t.Fatalf("prepared op acquired file id %d", opFile)
	}
	if state != wantState || leases != wantLeases || versions != wantVersions || offset != wantOffset {
		t.Fatalf("torn publication: state=%s leases=%d versions=%d offset=%d", state, leases, versions, offset)
	}
	wantChunks := make([]byte, count*19)
	for i := 0; i < count; i++ {
		copy(wantChunks[i*19:], bytes.Repeat([]byte{hashByte}, 15))
		binary.LittleEndian.PutUint32(wantChunks[i*19+15:], logicalLen)
	}
	if !bytes.Equal(chunks, wantChunks) {
		t.Fatal("head extent order/content differs")
	}
	rows, err := db.db.Query(`SELECT hash, refcount FROM chunk`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var refs int
		if err := rows.Scan(&hash, &refs); err != nil {
			t.Fatal(err)
		}
		want, ok := wantRefs[hash[0]]
		if !ok || refs != want {
			t.Fatalf("unexpected chunk references: hash=%x refs=%d", hash, refs)
		}
		delete(wantRefs, hash[0])
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(wantRefs) != 0 {
		t.Fatal("missing expected chunk rows")
	}
	return headFile
}

func TestPublicationTransactionErrorsAndLostReply(t *testing.T) {
	for _, stage := range publicationStages {
		t.Run(stage, func(t *testing.T) {
			db, _, opID := publicationFaultFixture(t)
			injected := errors.New("injected publication failure")
			db.publicationFault = func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			}
			if _, err := db.CommitWriteOpVersion(context.Background(), opID, "bucket/file", 2, publicationFaultPlacements()); !errors.Is(err, injected) {
				t.Fatalf("fault not reached: %v", err)
			}
			assertPublicationOutcome(t, db, stage == "committed")
			db.publicationFault = nil
			for retry := 0; retry < 2; retry++ {
				id, err := db.CommitWriteOpVersion(context.Background(), opID, "bucket/file", 2, publicationFaultPlacements())
				if err != nil {
					t.Fatal(err)
				}
				if want := assertPublicationOutcome(t, db, true); id != want {
					t.Fatalf("retry id=%d want=%d", id, want)
				}
			}
		})
	}
}

func TestPublicationCrashChild(t *testing.T) {
	stage := os.Getenv("ROSE_PUBLICATION_CRASH_STAGE")
	if stage == "" {
		return
	}
	db, err := Open(os.Getenv("ROSE_PUBLICATION_CRASH_PATH"))
	if err != nil {
		t.Fatal(err)
	}
	db.publicationFault = func(at string) error {
		if at == stage {
			os.Exit(73)
		}
		return nil
	}
	op, err := db.WriteOpByKey(context.Background(), "pending")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.CommitWriteOpVersion(context.Background(), op.ID, "bucket/file", 2, publicationFaultPlacements())
	t.Fatalf("crash point not reached: %v", err)
}

func TestPublicationTransactionProcessCrash(t *testing.T) {
	for _, stage := range publicationStages {
		t.Run(stage, func(t *testing.T) {
			db, path, opID := publicationFaultFixture(t)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestPublicationCrashChild$")
			cmd.Env = append(os.Environ(), "ROSE_PUBLICATION_CRASH_STAGE="+stage, "ROSE_PUBLICATION_CRASH_PATH="+path)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 73 {
				t.Fatalf("unexpected child result: %v\n%s", err, output)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertPublicationOutcome(t, reopened, stage == "committed")
			id, err := reopened.CommitWriteOpVersion(context.Background(), opID, "bucket/file", 2, publicationFaultPlacements())
			if err != nil {
				t.Fatal(err)
			}
			if want := assertPublicationOutcome(t, reopened, true); id != want {
				t.Fatalf("retry id=%d want=%d", id, want)
			}
		})
	}
}
