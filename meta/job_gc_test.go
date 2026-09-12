package meta

import (
	"context"
	"github.com/rmmh/rose/uid"
	"testing"
)

func TestTerminalVlogJobCollectionPreservesOwners(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	source, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	dest, err := db.MakeVlog(ctx, uid.New(), "NONE", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	var preserve, collect []int64
	for _, tc := range []struct {
		kind, state string
		dest        uint32
		keep        bool
	}{
		{JobCompact, JobRunning, 0, true}, {JobPromote, JobRunning, 999, true},
		{JobScrubRepair, JobRunning, 0, true}, {JobCompact, JobDone, dest, true},
		{JobPromote, JobCancelled, dest, true}, {JobDrain, JobDone, 0, true},
		{JobReplace, JobDone, 0, true}, {JobReprotect, JobDone, 0, true}, {JobRebalance, JobDone, 0, true},
		{JobCompact, JobDone, 0, false}, {JobPromote, JobDone, 999, false},
		{JobScrubRepair, JobDone, 0, false}, {JobScrubRepair, JobCancelled, 0, false},
	} {
		res, err := db.db.Exec("INSERT INTO job(kind,state,target_vlog,dest_vlog,created_at) VALUES(?,?,?,?,0)", tc.kind, tc.state, source, tc.dest)
		if err != nil {
			t.Fatal(err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if tc.keep {
			preserve = append(preserve, id)
		} else {
			collect = append(collect, id)
		}
	}
	if _, err := db.GCTerminalVlogJobs(ctx, 0); err == nil {
		t.Fatal("unbounded job GC accepted")
	}
	for _, want := range []int{2, 2, 0} {
		if n, err := db.GCTerminalVlogJobs(ctx, 2); err != nil || n != want {
			t.Fatalf("batch=%d want=%d err=%v", n, want, err)
		}
		for _, id := range preserve {
			if _, err := db.GetJob(ctx, id); err != nil {
				t.Fatalf("protected job %d removed: %v", id, err)
			}
		}
	}
	for _, id := range preserve {
		if _, err := db.GetJob(ctx, id); err != nil {
			t.Fatalf("protected job %d removed: %v", id, err)
		}
	}
	for _, id := range collect {
		var n int
		if err := db.db.QueryRow("SELECT count(*) FROM job WHERE id=?", id).Scan(&n); err != nil || n != 0 {
			t.Fatalf("obsolete job %d retained: %v", id, err)
		}
	}

	// Once the output is retired, its old terminal owners can be collected too.
	if _, err := db.RetireVlog(ctx, dest); err != nil {
		t.Fatal(err)
	}
	if n, err := db.GCTerminalVlogJobs(ctx, 10); err != nil || n != 2 {
		t.Fatalf("retired outputs=%d %v", n, err)
	}
	// New work must not reuse a collected identity.
	running, err := db.GetOrCreateCompactionJob(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MarkJobDone(ctx, running.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GCTerminalVlogJobs(ctx, 10); err != nil {
		t.Fatal(err)
	}
	next, err := db.GetOrCreateCompactionJob(ctx, source)
	if err != nil || next.ID <= collect[len(collect)-1] {
		t.Fatalf("job identity reused: %v %v", next, err)
	}
}
func TestTerminalJobCollectionRollsBackBatch(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 3; i++ {
		if _, err := db.db.Exec("INSERT INTO job(kind,state,target_vlog,created_at) VALUES('scrubrepair','done',1,0)"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.db.Exec("CREATE TRIGGER fail_job_gc BEFORE DELETE ON job WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT,'injected failure'); END"); err != nil {
		t.Fatal(err)
	}
	if n, err := db.GCTerminalVlogJobs(context.Background(), 3); err == nil || n != 0 {
		t.Fatalf("failed GC=%d %v", n, err)
	}
	var n int
	if err := db.db.QueryRow("SELECT count(*) FROM job").Scan(&n); err != nil || n != 3 {
		t.Fatalf("partial deletion=%d %v", n, err)
	}
}
