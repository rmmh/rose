package server_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
)

func TestChaosAbandonedWriteReleasesPreparation(t *testing.T) {
	c := newChaosCluster(t, 4, 2)
	defer c.close()
	c.server().StopMaintenanceDriver()
	ctx := context.Background()
	client := c.client()
	writeFile(t, client, "/mirror/existing", []byte("published before failure"))
	if err := c.server().SetDiskState(ctx, 1, meta.DiskFailed); err != nil {
		t.Fatal(err)
	}
	w := newWorkload(t, c, newOracle())
	w.degraded = func() bool { return true }
	w.doWrite(ctx, client, 0, rand.New(rand.NewSource(1)))
	if w.stats.writesDegraded.Load() != 1 {
		t.Fatal("fixture did not reject the write during degradation")
	}
	ops, err := c.server().GetDB().ListPreparedWriteOps(ctx)
	if err != nil || len(ops) != 0 {
		t.Fatalf("abandoned workload retained preparations: %v %v", ops, err)
	}
}

func TestChaosDiskCompletionWaitsForDurableJob(t *testing.T) {
	c := newChaosCluster(t, 4, 2)
	defer c.close()
	c.server().StopMaintenanceDriver()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	job, err := c.server().GetDB().GetOrCreateReprotectJob(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	i := newChaosInjector(t, c, newWorkload(t, c, newOracle()), 1)
	steps := 0
	err = i.finishDiskMaintenance(ctx, 1, meta.JobReprotect, func() error {
		steps++
		if steps == 1 {
			return nil
		} // accepted/deferred is not completion
		return c.server().GetDB().MarkJobDone(ctx, job.ID)
	})
	if err != nil || steps != 2 {
		t.Fatalf("completion steps=%d err=%v", steps, err)
	}
}
