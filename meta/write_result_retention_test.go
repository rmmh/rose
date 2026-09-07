package meta

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestRetryResultRootSurvivesUnlinkGCAndRestartUntilExpiry(t *testing.T) {
	db, path := checkerFixture(t)
	ctx := context.Background()
	op, err := db.CreateWriteOp(ctx, "retained", "retry-file")
	if err != nil {
		t.Fatal(err)
	}
	p := ChunkPlacement{Hash: bytes.Repeat([]byte{1}, 15), VlogID: 1, LogicalLen: 3, CompressedLen: 3}
	fileID, err := db.CommitWriteOpVersionWithRetention(ctx, op.ID, "retry-file", 10, []ChunkPlacement{p, p}, 200)
	if err != nil {
		t.Fatal(err)
	}
	again, err := db.CommitWriteOpVersionWithRetention(ctx, op.ID, "retry-file", 10, []ChunkPlacement{p, p}, 999)
	if err != nil || again != fileID {
		t.Fatalf("retry result=%d err=%v", again, err)
	}
	var refs, deadline int64
	if err := db.db.QueryRow("SELECT refcount FROM chunk").Scan(&refs); err != nil || refs != 8 {
		t.Fatalf("root multiplicity refs=%d err=%v", refs, err)
	}
	if err := db.db.QueryRow("SELECT expires_at FROM write_result_root").Scan(&deadline); err != nil || deadline != 200 {
		t.Fatalf("retry extended deadline=%d err=%v", deadline, err)
	}
	for _, name := range []string{"retry-file", "dir/file"} {
		if err := db.UnlinkFile(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	var snapshot uint64
	if err := db.db.QueryRow("SELECT id FROM snapshot").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteSnapshot(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if n, err := db.ExpireWriteResults(ctx, 199); err != nil || n != 0 {
		t.Fatalf("early expiry=%d %v", n, err)
	}
	if removed, err := db.GCChunks(ctx, nil); err != nil || len(removed) != 0 {
		t.Fatalf("GC reclaimed retained result: %v %v", removed, err)
	}
	if issues, err := db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
		t.Fatalf("retained catalog issues=%v err=%v", issues, err)
	}
	// A failure after state/refcount changes must roll the whole expiry back.
	if _, err := db.db.Exec("CREATE TRIGGER reject_expiry BEFORE DELETE ON write_result_root BEGIN SELECT RAISE(ABORT,'injected'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExpireWriteResults(ctx, 200); err == nil {
		t.Fatal("expiry ignored failed root deletion")
	}
	got, err := db.WriteOpByKey(ctx, "retained")
	if err != nil || got.State != WriteOpCommitted {
		t.Fatalf("failed expiry changed state=%v err=%v", got, err)
	}
	if issues, err := db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
		t.Fatalf("failed expiry damaged references: %v %v", issues, err)
	}
	if _, err := db.db.Exec("DROP TRIGGER reject_expiry"); err != nil {
		t.Fatal(err)
	}
	if n, err := db.ExpireWriteResults(ctx, 200); err != nil || n != 1 {
		t.Fatalf("expiry=%d %v", n, err)
	}
	if n, err := db.ExpireWriteResults(ctx, 201); err != nil || n != 0 {
		t.Fatalf("repeat expiry=%d %v", n, err)
	}
	got, err = db.CreateWriteOp(ctx, "retained", "retry-file")
	if err != nil || got.State != WriteOpExpired || got.ID != op.ID {
		t.Fatalf("expired key revived: %v %v", got, err)
	}
	if _, err := db.CommitWriteOpVersionWithRetention(ctx, op.ID, "retry-file", 10, []ChunkPlacement{p, p}, 999); err == nil {
		t.Fatal("expired operation published")
	}
	if removed, err := db.GCChunks(ctx, nil); err != nil || len(removed) != 1 {
		t.Fatalf("expired result not reclaimed: %v %v", removed, err)
	}
	if issues, err := db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
		t.Fatalf("expired catalog issues=%v err=%v", issues, err)
	}
}

func TestRetryRootPublicationIsAtomicWithResult(t *testing.T) {
	for _, stage := range []string{"version-written", "operation-written", "leases-released", "committed"} {
		t.Run(stage, func(t *testing.T) {
			db, _ := checkerFixture(t)
			ctx := context.Background()
			op, err := db.CreateWriteOp(ctx, "retained", "retry-file")
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("publication fault")
			db.publicationFault = func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			}
			p := ChunkPlacement{Hash: bytes.Repeat([]byte{1}, 15), VlogID: 1, LogicalLen: 3, CompressedLen: 3}
			if _, err := db.CommitWriteOpVersionWithRetention(ctx, op.ID, "retry-file", 10, []ChunkPlacement{p, p}, 200); !errors.Is(err, injected) {
				t.Fatalf("fault result=%v", err)
			}
			var roots, refs int
			if err := db.db.QueryRow("SELECT COUNT(*) FROM write_result_root").Scan(&roots); err != nil {
				t.Fatal(err)
			}
			if err := db.db.QueryRow("SELECT refcount FROM chunk").Scan(&refs); err != nil {
				t.Fatal(err)
			}
			wantRoots, wantRefs := 0, 4
			if stage == "committed" {
				wantRoots, wantRefs = 1, 8
			}
			if roots != wantRoots || refs != wantRefs {
				t.Fatalf("partial publication: roots=%d refs=%d", roots, refs)
			}
			if issues, err := db.CheckCatalog(ctx); err != nil || len(issues) != 0 {
				t.Fatalf("publication issues=%v err=%v", issues, err)
			}
		})
	}
}
