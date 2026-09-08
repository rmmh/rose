package meta

import (
	"context"
	"sync"
	"testing"
)

func TestConcurrentVlogJobCreationConverges(t *testing.T) {
	for _, kind := range []string{JobCompact, JobPromote, JobScrubRepair} {
		t.Run(kind, func(t *testing.T) {
			db, err := OpenEphemeral()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			makeJob := map[string]func(context.Context, uint32) (Job, error){
				JobCompact:     db.GetOrCreateCompactionJob,
				JobPromote:     db.GetOrCreatePromoteJob,
				JobScrubRepair: db.GetOrCreateScrubRepairJob,
			}[kind]
			ctx := context.Background()
			type result struct {
				job Job
				err error
			}
			results := make(chan result, 12)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for n := 0; n < 12; n++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					j, err := makeJob(ctx, 7)
					results <- result{j, err}
				}()
			}
			close(start)
			wg.Wait()
			close(results)
			var id int64
			for r := range results {
				if r.err != nil {
					t.Fatal(r.err)
				}
				if id == 0 {
					id = r.job.ID
				}
				if r.job.ID != id {
					t.Fatalf("concurrent callers acquired different job IDs: %d and %d", id, r.job.ID)
				}
			}
			other, err := makeJob(ctx, 8)
			if err != nil || other.ID == id {
				t.Fatalf("independent source=%v err=%v", other, err)
			}
			if err := db.MarkJobDone(ctx, id); err != nil {
				t.Fatal(err)
			}
			next, err := makeJob(ctx, 7)
			if err != nil || next.ID == id {
				t.Fatalf("new pass=%v err=%v", next, err)
			}
			var count int
			if err := db.db.QueryRow("SELECT COUNT(*) FROM job").Scan(&count); err != nil || count != 3 {
				t.Fatalf("catalog jobs=%d err=%v", count, err)
			}
		})
	}
}

func TestRunningVlogJobUniquenessConstraint(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, kind := range []string{JobCompact, JobPromote, JobScrubRepair} {
		insert := func() error {
			_, err := db.db.Exec("INSERT INTO job(kind,state,target_vlog,created_at) VALUES(?,'running',7,0)", kind)
			return err
		}
		if err := insert(); err != nil {
			t.Fatal(err)
		}
		if err := insert(); err == nil {
			t.Fatalf("duplicate running %s ownership accepted", kind)
		}
		if _, err := db.db.Exec("UPDATE job SET state='done' WHERE kind=?", kind); err != nil {
			t.Fatal(err)
		}
		if err := insert(); err != nil {
			t.Fatalf("completed history prevented new %s job: %v", kind, err)
		}
	}
}

func TestCheckerReportsDuplicateVlogJobOwners(t *testing.T) {
	db, _ := checkerFixture(t)
	if _, err := db.db.Exec("DROP INDEX idx_job_running_vlog"); err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 2; n++ {
		if _, err := db.db.Exec("INSERT INTO job(kind,state,target_vlog,created_at) VALUES('compact','running',1,0)"); err != nil {
			t.Fatal(err)
		}
	}
	issues, err := db.CheckCatalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, issue := range issues {
		if issue.Code == "duplicate_job_owner" {
			return
		}
	}
	t.Fatalf("checker missed duplicate job owners: %v", issues)
}
