package server_test

// The fault injector for TestChaos.  It deliberately completes (and checks)
// one topology transition before starting the next one: the workload remains
// concurrent, but this keeps a failing seed useful instead of making several
// unrelated maintenance jobs race for the same shard.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
)

type chaosInjector struct {
	t       *testing.T
	cluster *chaosCluster
	work    *workload
	rng     *rand.Rand
	active  atomic.Bool // transient outage/restart: workload errors are retried
	faults  atomic.Int64
}

func newChaosInjector(t *testing.T, c *chaosCluster, w *workload, seed int64) *chaosInjector {
	i := &chaosInjector{t: t, cluster: c, work: w, rng: rand.New(rand.NewSource(seed ^ 0x51a7e))}
	w.degraded = i.active.Load
	return i
}

func (i *chaosInjector) run(ctx context.Context) {
	// Give the workers time to create real vlogs before selecting a plog to
	// corrupt or a disk to migrate.
	t := time.NewTimer(1200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			i.inject(ctx)
			t.Reset(1200 * time.Millisecond)
		}
	}
}

func (i *chaosInjector) inject(ctx context.Context) {
	i.active.Store(true)
	defer i.active.Store(false)
	// All of these faults retain at least an EC read quorum: node outages are
	// one at a time; disk maintenance is completed before the next injection.
	var err error
	switch i.rng.Intn(5) {
	case 0:
		err = i.nodeOutage(ctx)
	case 1:
		err = i.failAndReprotect(ctx)
	case 2:
		err = i.drainAndReplace(ctx)
	case 3:
		err = i.bitrotAndRepair(ctx)
	case 4:
		i.cluster.restart()
	}
	if ctx.Err() != nil {
		return // run deadline interrupted a maintenance pass; final sweep is strict
	}
	if err != nil {
		i.t.Errorf("chaos fault failed: %v", err)
		return
	}
	i.faults.Add(1)
	i.assertInvariants(ctx)
}

func (i *chaosInjector) nodeOutage(ctx context.Context) error {
	node := uint32(i.rng.Intn(i.cluster.nodes) + 1)
	s := i.cluster.server()
	if err := s.SetNodeState(ctx, node, meta.NodeFailed); err != nil {
		return err
	}
	// Keep the outage long enough for concurrent reads to exercise EC/mirror
	// reconstruction, then restore the original plogs before another event.
	time.Sleep(100 * time.Millisecond)
	return s.SetNodeState(ctx, node, meta.NodeWorking)
}

func (i *chaosInjector) failAndReprotect(ctx context.Context) error {
	disk, err := i.diskWithPlogs(ctx)
	if err != nil {
		return err
	}
	s := i.cluster.server()
	if err := s.SetDiskState(ctx, disk, meta.DiskFailed); err != nil {
		return err
	}
	if err := s.ReprotectDisk(ctx, disk); err != nil {
		return err
	}
	// The old disk no longer owns any shard after reprotect. Returning it to the
	// active pool supplies capacity for later faults without weakening placement.
	return s.SetDiskState(ctx, disk, meta.DiskActive)
}

func (i *chaosInjector) drainAndReplace(ctx context.Context) error {
	old, err := i.diskWithPlogs(ctx)
	if err != nil {
		return err
	}
	node := i.cluster.nodeFor(old)
	newID := i.cluster.nextDiskID()
	root := filepath.Join(filepath.Dir(i.cluster.rootFor(old)), fmt.Sprintf("disk-%d", newID))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	s := i.cluster.server()
	if err := s.AttachDiskOnNode(ctx, newID, node, root, 0); err != nil {
		return err
	}
	i.cluster.addDisk(newID, node, root)
	return s.ReplaceDiskWith(ctx, old, newID)
}

func (i *chaosInjector) bitrotAndRepair(ctx context.Context) error {
	disk, plog, err := i.anyPlog(ctx)
	if err != nil {
		return err
	}
	path := filepath.Join(i.cluster.rootFor(disk), fmt.Sprintf("plog-%d", plog))
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	// Offset 100 is inside the first hash-protected data sector for every plog
	// created by this workload (writes are at least 1 KiB).
	b := []byte{0}
	if _, err = f.ReadAt(b, 100); err != nil {
		return err
	}
	b[0] ^= 0xff
	if _, err = f.WriteAt(b, 100); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	res, err := i.cluster.server().ScrubAndRepair(ctx)
	if err != nil {
		return err
	}
	if len(res.Unrepairable) != 0 {
		return fmt.Errorf("bitrot left %d unrepaired shards", len(res.Unrepairable))
	}
	if res.ShardsRepaired == 0 {
		return fmt.Errorf("bitrot scrub repaired no shards")
	}
	return nil
}

func (i *chaosInjector) diskWithPlogs(ctx context.Context) (uint32, error) {
	states := i.cluster.server().DiskStates()
	var ids []uint32
	for id, state := range states {
		if state != meta.DiskActive {
			continue
		}
		ps, err := i.cluster.server().GetDB().PlogsOnDisk(ctx, id)
		if err != nil {
			return 0, err
		}
		if len(ps) != 0 {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return 0, fmt.Errorf("no active disk contains a plog")
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	return ids[i.rng.Intn(len(ids))], nil
}

func (i *chaosInjector) anyPlog(ctx context.Context) (uint32, uint32, error) {
	disk, err := i.diskWithPlogs(ctx)
	if err != nil {
		return 0, 0, err
	}
	ps, err := i.cluster.server().GetDB().PlogsOnDisk(ctx, disk)
	if err != nil {
		return 0, 0, err
	}
	return disk, ps[i.rng.Intn(len(ps))].PlogID, nil
}

func (i *chaosInjector) assertInvariants(ctx context.Context) {
	placements := map[uint32]map[uint32]bool{}
	err := i.cluster.server().GetDB().AllShardPlacements(ctx, func(p meta.ShardPlacement) {
		seen := placements[p.VlogID]
		if seen == nil {
			seen = map[uint32]bool{}
			placements[p.VlogID] = seen
		}
		node := i.cluster.nodeFor(p.DiskID)
		if seen[node] {
			i.t.Errorf("placement invariant: vlog %d has two shards on node %d", p.VlogID, node)
		}
		seen[node] = true
	})
	if err != nil {
		i.t.Errorf("placement audit: %v", err)
	}
	res, err := i.cluster.server().ScrubAndRepair(ctx)
	if err != nil {
		i.t.Errorf("post-fault scrub: %v", err)
	} else if len(res.Unrepairable) != 0 {
		i.t.Errorf("post-fault scrub has %d unrepaired shards", len(res.Unrepairable))
	}
}

func (c *chaosCluster) nodeFor(disk uint32) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.diskNode[disk]
}
func (c *chaosCluster) rootFor(disk uint32) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roots[disk]
}
func (c *chaosCluster) nextDiskID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var max uint32
	for id := range c.roots {
		if id > max {
			max = id
		}
	}
	return max + 1
}
func (c *chaosCluster) addDisk(id, node uint32, root string) {
	c.mu.Lock()
	c.roots[id], c.diskNode[id] = root, node
	c.mu.Unlock()
}

// TestChaos is the end-to-end, real-data control-plane test. It is explicitly
// opt-in because it uses RAM-disk space and intentionally churns files. A seed
// is printed so failures reproduce exactly with ROSE_CHAOS_SEED.
func TestChaos(t *testing.T) {
	if os.Getenv("ROSE_CHAOS") != "1" {
		t.Skip("set ROSE_CHAOS=1 to run chaos")
	}
	seed := chaosSeed(1)
	duration := chaosDuration(30 * time.Second)
	t.Logf("chaos seed=%d duration=%s", seed, duration)
	c := newChaosCluster(t, 4, 2)
	defer c.close()
	w := newWorkload(t, c, newOracle())
	i := newChaosInjector(t, c, w, seed)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); i.run(ctx) }()
	w.run(ctx, 2, seed)
	<-done
	// All injected faults have settled before this strict durability sweep.
	i.assertInvariants(context.Background())
	w.verifyAll(context.Background())
	t.Logf("chaos faults=%d %s", i.faults.Load(), w.stats.summary())
	if i.faults.Load() == 0 || w.stats.writes.Load() == 0 || w.stats.reads.Load() == 0 {
		t.Fatal("chaos did not exercise faults, writes, and reads")
	}
}
