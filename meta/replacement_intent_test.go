package meta

import (
	"context"
	"sync"
	"testing"
)

func TestReplacementIntentHasOneImmutableDestination(t *testing.T) {
	db, err := OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	type result struct {
		dest uint32
		job  Job
		err  error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, dest := range []uint32{2, 3} {
		wg.Add(1)
		go func(dest uint32) {
			defer wg.Done()
			<-start
			job, err := db.GetOrCreateReplaceJob(ctx, 1, dest)
			results <- result{dest, job, err}
		}(dest)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner result
	wins := 0
	for r := range results {
		if r.err == nil {
			wins++
			winner = r
		}
	}
	if wins != 1 {
		t.Fatalf("conflicting replacement intents had %d successes, want one", wins)
	}
	if winner.job.DestDisk != winner.dest {
		t.Fatal("winner received another request's destination")
	}
	again, err := db.GetOrCreateReplaceJob(ctx, 1, winner.dest)
	if err != nil || again.ID != winner.job.ID {
		t.Fatalf("identical retry changed job: %v %v", again, err)
	}
	var count int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM job").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rejected intent changed catalog: count=%d err=%v", count, err)
	}
	for _, pair := range [][2]uint32{{0, 2}, {1, 0}, {1, 1}} {
		if _, err := db.GetOrCreateReplaceJob(ctx, pair[0], pair[1]); err == nil {
			t.Fatalf("invalid replacement %v accepted", pair)
		}
	}
	if err := db.db.QueryRow("SELECT COUNT(*) FROM job").Scan(&count); err != nil || count != 1 {
		t.Fatalf("invalid intent left a job: count=%d err=%v", count, err)
	}
}
