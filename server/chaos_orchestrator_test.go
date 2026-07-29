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
	"github.com/rmmh/rose/storage"
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
	choice := i.rng.Intn(5)
	names := [...]string{"node-outage", "fail-reprotect", "drain-replace", "bitrot-repair", "restart"}
	i.t.Logf("chaos fault %d: %s", i.faults.Load()+1, names[choice])
	switch choice {
	case 0:
		err = i.nodeOutage(ctx)
	case 1:
		err = i.failAndReprotect(ctx)
	case 2:
		err = i.drainAndReplace(ctx)
	case 3:
		err = i.bitrotAndRepair(ctx)
	case 4:
		i.work.restartMu.Lock()
		i.cluster.restart()
		i.work.restartMu.Unlock()
	}
	if ctx.Err() != nil {
		return // run deadline interrupted a maintenance pass; final sweep is strict
	}
	if err != nil {
		i.t.Errorf("chaos fault failed: %v", err)
		return
	}
	i.faults.Add(1)
	// The workload deadline may expire between the check above and this audit.
	// Give a fault that already completed its own bounded window to verify the
	// resulting state instead of turning cancellation midway through scrub into
	// a false corruption report.
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	i.assertInvariants(auditCtx)
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
	// DUPLICATE vlogs occupy every active disk, so losing one leaves no legal
	// reprotection destination unless replacement capacity is added first.
	// Provision a same-node spare, then leave the failed disk failed: the spare
	// replaces its active capacity after reprotection.
	node := i.cluster.nodeFor(disk)
	spare := i.cluster.nextDiskID()
	root := filepath.Join(filepath.Dir(i.cluster.rootFor(disk)), fmt.Sprintf("disk-%d", spare))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	s := i.cluster.server()
	if err := s.AttachDiskOnNode(ctx, spare, node, root, 0); err != nil {
		return err
	}
	i.cluster.addDisk(spare, node, root)
	if err := s.SetDiskState(ctx, disk, meta.DiskFailed); err != nil {
		return err
	}
	return s.ReprotectDisk(ctx, disk)
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
	for attempt := 0; attempt < 5; attempt++ {
		disk, plog, err := i.anyPlog(ctx)
		if err != nil {
			return err
		}
		path := filepath.Join(i.cluster.rootFor(disk), fmt.Sprintf("plog-%05d", plog))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			continue // concurrent relocation won after selection
		}
		// Logical offset 100 is inside the first hash-protected data sector.
		// Convert it to a physical offset so the leading superblock is untouched.
		b := []byte{0}
		offset := storage.CalcPhysical(100)
		if _, err = f.ReadAt(b, offset); err == nil {
			b[0] ^= 0xff
			_, err = f.WriteAt(b, offset)
		}
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err != nil {
			continue
		}
		if closeErr != nil {
			return closeErr
		}
		// Repair deliberately defers a vlog with an active client operation.
		// Race-enabled workers hold those references longer, so give that one
		// vlog a few chances to quiesce before deciding scrub missed the damage.
		for scrubAttempt := 0; scrubAttempt < 10; scrubAttempt++ {
			res, err := i.cluster.server().ScrubAndRepair(ctx)
			if err != nil {
				return err
			}
			if len(res.Unrepairable) != 0 {
				return fmt.Errorf("bitrot left %d unrepaired shards: %v", len(res.Unrepairable), res.Unrepairable)
			}
			if res.ShardsRepaired != 0 {
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(time.Millisecond)
		}

		// Explicit maintenance can move the chosen plog between selection and
		// scrub. If it no longer maps to this disk (or the byte was overwritten),
		// the injected fault hit a stale file; retry instead of reporting a false
		// scrub miss.
		mapped := false
		plogs, err := i.cluster.server().GetDB().PlogsOnDisk(ctx, disk)
		if err != nil {
			return err
		}
		for _, p := range plogs {
			if p.PlogID == plog {
				mapped = true
				break
			}
		}
		current := []byte{0}
		check, openErr := os.Open(path)
		if openErr == nil {
			_, openErr = check.ReadAt(current, offset)
			_ = check.Close()
		}
		if mapped && openErr == nil && current[0] == b[0] {
			return fmt.Errorf("bitrot scrub repaired no shards")
		}
	}
	return fmt.Errorf("bitrot injection repeatedly raced plog relocation")
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
	type candidate struct {
		disk uint32
		plog uint32
	}
	states := i.cluster.server().DiskStates()
	var disks []uint32
	for disk, state := range states {
		if state == meta.DiskActive {
			disks = append(disks, disk)
		}
	}
	sort.Slice(disks, func(a, b int) bool { return disks[a] < disks[b] })
	var sealed []candidate
	for _, disk := range disks {
		ps, err := i.cluster.server().GetDB().PlogsOnDisk(ctx, disk)
		if err != nil {
			return 0, 0, err
		}
		for _, p := range ps {
			path := filepath.Join(i.cluster.rootFor(disk), fmt.Sprintf("plog-%05d", p.PlogID))
			opened, err := storage.OpenExistingPlog(path, p.PlogID)
			if err != nil {
				continue // a concurrent topology transition may have moved it
			}
			shardLength := opened.LogicalLength()
			if err := opened.Close(); err != nil {
				return 0, 0, err
			}
			// The open ragged-edge sector has no durable sector hash until it
			// fills. Inspect the physical plog rather than its vlog's published
			// cursor: a valid slow non-quorum mirror may be shorter than that
			// cursor. Choose only an actually sealed first sector so corruption
			// is guaranteed to be visible to Scrub.
			if shardLength >= storage.SectorSize {
				sealed = append(sealed, candidate{disk: disk, plog: p.PlogID})
			}
		}
	}
	if len(sealed) == 0 {
		return 0, 0, fmt.Errorf("no active plog has a sealed sector")
	}
	pick := sealed[i.rng.Intn(len(sealed))]
	return pick.disk, pick.plog, nil
}

func (i *chaosInjector) assertInvariants(ctx context.Context) {
	placements := map[uint32]map[uint32]bool{}
	err := i.cluster.server().GetDB().AllShardPlacements(ctx, func(p meta.ShardPlacement) {
		seen := placements[p.VlogID]
		if seen == nil {
			seen = map[uint32]bool{}
			placements[p.VlogID] = seen
		}
		if seen[p.DiskID] {
			i.t.Errorf("placement invariant: vlog %d has two shards on disk %d", p.VlogID, p.DiskID)
		}
		seen[p.DiskID] = true
	})
	if err != nil {
		i.t.Errorf("placement audit: %v", err)
	}
	res, err := i.cluster.server().ScrubAndRepair(ctx)
	if err != nil {
		i.t.Errorf("post-fault scrub: %v", err)
	} else if len(res.Unrepairable) != 0 {
		i.t.Errorf("post-fault scrub has %d unrepaired shards: %v", len(res.Unrepairable), res.Unrepairable)
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
