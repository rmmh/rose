package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
)

// virtualScaleCluster exercises the control-plane catalog with logical plog
// extents only. It deliberately never creates a data file: a 4 GiB vlog is one
// metadata row plus its EC shard rows, not 4 GiB of test output.
type virtualScaleCluster struct {
	t              testing.TB
	db             *meta.DB
	nodes          int
	disksPerNode   int
	diskNode       map[uint32]uint32
	diskState      map[uint32]string
	nodeState      map[uint32]string
	plogBytes      map[uint32]int64
	workers        []*Server
	mu             sync.Mutex
	placementScans int
}

// scaleTracef writes immediately instead of waiting for Go's verbose test-log
// flush. Keep it opt-in so ordinary test output remains concise.
func scaleTracef(format string, args ...any) {
	if os.Getenv("ROSE_SCALE_TRACE") == "1" {
		fmt.Fprintf(os.Stderr, "[scale] "+format+"\n", args...)
	}
}

func seconds(d time.Duration) string { return fmt.Sprintf("%.6fs", d.Seconds()) }

// scaleFillPercent controls how much of a profile's target capacity is
// populated. Each virtual vlog remains 99%-full; lowering this value creates
// fewer vlogs rather than smaller ones.
func scaleFillPercent(t testing.TB) float64 {
	t.Helper()
	percent := 100.0
	if raw := os.Getenv("ROSE_SCALE_FILL_PERCENT"); raw != "" {
		var err error
		percent, err = strconv.ParseFloat(raw, 64)
		if err != nil || percent <= 0 || percent > 100 {
			t.Fatalf("ROSE_SCALE_FILL_PERCENT must be a number in (0, 100], got %q", raw)
		}
	}
	return percent
}

func newVirtualScaleCluster(t testing.TB, nodes, disksPerNode int) *virtualScaleCluster {
	t.Helper()
	dir := t.TempDir()
	db, err := meta.OpenEphemeral()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c := &virtualScaleCluster{t: t, db: db, nodes: nodes, disksPerNode: disksPerNode, diskNode: map[uint32]uint32{}, diskState: map[uint32]string{}, nodeState: map[uint32]string{}, plogBytes: map[uint32]int64{}}
	ctx := context.Background()
	for n := 1; n <= nodes; n++ {
		if err := db.RegisterNode(ctx, uint32(n)); err != nil {
			t.Fatal(err)
		}
		c.nodeState[uint32(n)] = meta.NodeWorking
		roots := make(map[uint32]string, disksPerNode)
		for d := 0; d < disksPerNode; d++ {
			id := uint32((n-1)*disksPerNode + d + 1)
			if err := db.RegisterDiskWithCapacity(ctx, id, uint32(n), 8_000_000_000_000); err != nil {
				t.Fatal(err)
			}
			c.diskNode[id], c.diskState[id] = uint32(n), meta.DiskActive
			roots[id] = filepath.Join(dir, fmt.Sprintf("node-%d-disk-%d", n, d))
		}
		// These are real independent Server instances sharing the metadata master.
		// The virtual catalog below supplies their fake disks.
		s := NewServerWithDiskRoots(db, roots)
		s.SetMaintenanceInterval(0)
		if err := s.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.StopMaintenanceDriver)
		c.workers = append(c.workers, s)
	}
	return c
}

func (c *virtualScaleCluster) disk(node, ordinal int) uint32 {
	return uint32((node-1)*c.disksPerNode + ordinal + 1)
}

// populate creates count 2+1 EC vlogs, each with a single 99%-full virtual
// extent. Disk choice rotates within each node to make the initial layout even.
func (c *virtualScaleCluster) populate(count int, logicalBytes int64) {
	c.populateEC(count, 2, 1, logicalBytes)
}

func (c *virtualScaleCluster) populateEC(count, dataShards, parityShards int, logicalBytes int64) {
	c.populateECProgress(count, dataShards, parityShards, logicalBytes, nil)
}

// populateECProgress emits coarse-grained progress after each completed batch.
// The full-scale benchmark uses this instead of logging each vlog, which would
// itself dominate a multi-million-vlog run.
func (c *virtualScaleCluster) populateECProgress(count, dataShards, parityShards int, logicalBytes int64, progress func(done int)) {
	c.t.Helper()
	ctx := context.Background()
	shardBytes := (logicalBytes + int64(dataShards) - 1) / int64(dataShards)
	batch := count / 100
	if batch < 1 {
		batch = 1
	}
	for i := 0; i < count; i++ {
		vlogID, err := c.db.MakeVlog(ctx, "EC", int32(dataShards), int32(parityShards))
		if err != nil {
			c.t.Fatal(err)
		}
		if err := c.db.SetVlogLength(ctx, vlogID, logicalBytes); err != nil {
			c.t.Fatal(err)
		}
		for shard := 0; shard < dataShards+parityShards; shard++ {
			diskID := c.disk(shard+1, i%c.disksPerNode)
			plogID, err := c.db.MakePlog(ctx, diskID)
			if err != nil {
				c.t.Fatal(err)
			}
			if err := c.db.SetPlogLength(ctx, plogID, shardBytes); err != nil {
				c.t.Fatal(err)
			}
			if err := c.db.AssignPlogToVlog(ctx, vlogID, shard, plogID); err != nil {
				c.t.Fatal(err)
			}
			c.plogBytes[plogID] = shardBytes
		}
		if progress != nil && ((i+1)%batch == 0 || i+1 == count) {
			progress(i + 1)
		}
	}
}

func (c *virtualScaleCluster) failDisk(diskID uint32) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.db.SetDiskState(context.Background(), diskID, meta.DiskFailed); err != nil {
		c.t.Fatal(err)
	}
	c.diskState[diskID] = meta.DiskFailed
}

func (c *virtualScaleCluster) failNode(nodeID uint32) {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.db.SetNodeState(context.Background(), nodeID, meta.NodeFailed); err != nil {
		c.t.Fatal(err)
	}
	c.nodeState[nodeID] = meta.NodeFailed
}

func (c *virtualScaleCluster) readiness(vlogID uint32) (commit, readable bool) {
	c.t.Helper()
	info, err := c.db.GetVlog(context.Background(), vlogID)
	if err != nil {
		c.t.Fatal(err)
	}
	shards, err := c.db.VlogShardDisks(context.Background(), vlogID)
	if err != nil {
		c.t.Fatal(err)
	}
	live := 0
	for _, shard := range shards {
		if c.diskState[shard.DiskID] == meta.DiskActive && c.nodeState[c.diskNode[shard.DiskID]] == meta.NodeWorking {
			live++
		}
	}
	return live == len(shards), live >= int(info.DataShards)
}

// audit walks every persisted vlog/shard mapping and checks the TLA
// NodeLevelDurability invariant. It deliberately uses catalog queries rather
// than the in-memory builder state, so its timing exposes metadata lookup cost.
func (c *virtualScaleCluster) audit() (vlogs, plogs, colocated int) {
	ctx := context.Background()
	infos, err := c.db.ListVlogs(ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	for _, info := range infos {
		vlogs++
		shards, err := c.db.VlogShardDisks(ctx, info.ID)
		if err != nil {
			c.t.Fatal(err)
		}
		seen := map[uint32]bool{}
		for _, shard := range shards {
			plogs++
			node := c.diskNode[shard.DiskID]
			if seen[node] {
				colocated++
			}
			seen[node] = true
		}
	}
	return vlogs, plogs, colocated
}

// readinessAll evaluates commit and read gates across all vlogs. Unlike the
// small-profile helper above, it derives the EC thresholds from each catalog
// row so it also covers the 5+2 full-scale profile.
func (c *virtualScaleCluster) readinessAll() (commitReady, readable int) {
	ctx := context.Background()
	infos, err := c.db.ListVlogs(ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	for _, info := range infos {
		shards, err := c.db.VlogShardDisks(ctx, info.ID)
		if err != nil {
			c.t.Fatal(err)
		}
		live := 0
		for _, shard := range shards {
			if c.diskState[shard.DiskID] == meta.DiskActive && c.nodeState[c.diskNode[shard.DiskID]] == meta.NodeWorking {
				live++
			}
		}
		if live == len(shards) {
			commitReady++
		}
		if live >= int(info.DataShards) {
			readable++
		}
	}
	return commitReady, readable
}

func (c *virtualScaleCluster) scanDiskPlogs() int {
	count := 0
	for disk := uint32(1); disk <= uint32(c.nodes*c.disksPerNode); disk++ {
		ps, err := c.db.PlogsOnDisk(context.Background(), disk)
		if err != nil {
			c.t.Fatal(err)
		}
		count += len(ps)
	}
	return count
}

func scalePostPopulate(t *testing.T, profile string, c *virtualScaleCluster) {
	t.Helper()
	started := time.Now()
	plogs := c.scanDiskPlogs()
	t.Logf("profile=%s op=scan-disk-plogs disks=%d plogs=%d elapsed=%s", profile, c.nodes*c.disksPerNode, plogs, seconds(time.Since(started)))

	started = time.Now()
	vlogs, mappedPlogs, colocated := c.audit()
	t.Logf("profile=%s op=audit-placement vlogs=%d plogs=%d colocated=%d elapsed=%s", profile, vlogs, mappedPlogs, colocated, seconds(time.Since(started)))
	if colocated != 0 {
		t.Fatalf("profile=%s has %d node-colocated shards", profile, colocated)
	}

	started = time.Now()
	commitReady, readable := c.readinessAll()
	t.Logf("profile=%s op=readiness-all commit-ready=%d readable=%d elapsed=%s", profile, commitReady, readable, seconds(time.Since(started)))

	started = time.Now()
	moves := c.rebalance(8, 10<<30)
	t.Logf("profile=%s op=rebalance max-moves=8 moves=%d placement-scans=%d elapsed=%s", profile, moves, c.placementScans, seconds(time.Since(started)))
}

// reprotectDisk is the logical-extent repair transition. It uses the same
// placement rule as the real repair path, but virtual reconstruction only
// carries the shard's length rather than materializing its bytes.
func (c *virtualScaleCluster) reprotectDisk(diskID uint32) int {
	ctx := context.Background()
	plogs, err := c.db.PlogsOnDisk(ctx, diskID)
	if err != nil {
		c.t.Fatal(err)
	}
	repaired := 0
	for _, lost := range plogs {
		shards, err := c.db.VlogShardDisks(ctx, lost.VlogID)
		if err != nil {
			c.t.Fatal(err)
		}
		occupied := map[uint32]bool{}
		for _, shard := range shards {
			if shard.DiskID != diskID {
				occupied[c.diskNode[shard.DiskID]] = true
			}
		}
		var dest uint32
		for candidate := uint32(1); candidate <= uint32(c.nodes*c.disksPerNode); candidate++ {
			if c.diskState[candidate] == meta.DiskActive && c.nodeState[c.diskNode[candidate]] == meta.NodeWorking && !occupied[c.diskNode[candidate]] {
				dest = candidate
				break
			}
		}
		if dest == 0 {
			c.t.Fatalf("reprotect vlog %d: no placement-allowed destination", lost.VlogID)
		}
		newPlog, err := c.db.MakePlog(ctx, dest)
		if err != nil {
			c.t.Fatal(err)
		}
		if err := c.db.SetPlogLength(ctx, newPlog, c.plogBytes[lost.PlogID]); err != nil {
			c.t.Fatal(err)
		}
		if err := c.db.ReplaceShardPlog(ctx, lost.VlogID, lost.ShardIndex, lost.PlogID, newPlog); err != nil {
			c.t.Fatal(err)
		}
		c.plogBytes[newPlog] = c.plogBytes[lost.PlogID]
		delete(c.plogBytes, lost.PlogID)
		repaired++
	}
	return repaired
}

// rebalance is the logical-extent equivalent of Rebalance: it intentionally
// performs the same catalog placement check, but changes only metadata. It is
// used to count control-plane work independently of physical copy bandwidth.
func (c *virtualScaleCluster) rebalance(maxMoves int, minSkew int64) int {
	ctx := context.Background()
	usage := make(map[uint32]int64, c.nodes*c.disksPerNode)
	byDisk := make(map[uint32][]meta.PlogOnDisk, len(usage))
	for disk := uint32(1); disk <= uint32(c.nodes*c.disksPerNode); disk++ {
		ps, err := c.db.PlogsOnDisk(ctx, disk)
		if err != nil {
			c.t.Fatal(err)
		}
		byDisk[disk] = ps
		for _, p := range ps {
			usage[disk] += c.plogBytes[p.PlogID]
		}
	}
	extreme := func(high bool) uint32 {
		var selected uint32 = 1
		for disk := uint32(1); disk <= uint32(c.nodes*c.disksPerNode); disk++ {
			if (high && usage[disk] > usage[selected]) || (!high && usage[disk] < usage[selected]) {
				selected = disk
			}
		}
		return selected
	}
	moves := 0
	for moves < maxMoves {
		src, dst := extreme(true), extreme(false)
		if usage[src]-usage[dst] <= minSkew {
			break
		}
		moved := false
		for _, p := range byDisk[src] {
			shards, err := c.db.VlogShardDisks(ctx, p.VlogID)
			if err != nil {
				c.t.Fatal(err)
			}
			c.placementScans += len(shards)
			occupied := false
			for _, shard := range shards {
				if shard.DiskID != src && c.diskNode[shard.DiskID] == c.diskNode[dst] {
					occupied = true
					break
				}
			}
			if occupied || c.plogBytes[p.PlogID] > usage[src]-usage[dst] {
				continue
			}
			if err := c.db.MovePlogToDisk(ctx, p.PlogID, dst); err != nil {
				c.t.Fatal(err)
			}
			usage[src] -= c.plogBytes[p.PlogID]
			usage[dst] += c.plogBytes[p.PlogID]
			moved, moves = true, moves+1
			break
		}
		if !moved {
			break
		}
	}
	return moves
}

func TestControlPlaneScaleSmoke(t *testing.T) {
	c := newVirtualScaleCluster(t, 3, 8)
	// Ten fake vlogs per server: normal go test validates the distributed shape
	// without attempting to fill its virtual disks.
	c.populate(30, MaxVlogBytes*99/100)
	if got := len(c.workers); got != 3 {
		t.Fatalf("workers = %d, want 3", got)
	}

	shards, err := c.db.VlogShardDisks(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[uint32]bool{}
	for _, shard := range shards {
		n := c.diskNode[shard.DiskID]
		if seen[n] {
			t.Fatalf("vlog 1 has colocated shards on node %d", n)
		}
		seen[n] = true
	}

	// A failed disk loses one of three shards: reads remain possible but the EC
	// commit gate closes. With only three nodes there is no placement-allowed
	// replacement node, which is the expected TLA-conformant degraded state.
	c.failDisk(c.disk(1, 0))
	commit, readable := c.readiness(1)
	if commit || !readable {
		t.Fatalf("after one disk loss: commit=%v readable=%v, want false/true", commit, readable)
	}
}

func TestVirtualRebalanceRespectsMoveCap(t *testing.T) {
	c := newVirtualScaleCluster(t, 4, 2) // one spare node makes 2+1 relocation legal
	c.populate(20, MaxVlogBytes*99/100)
	ps, err := c.db.PlogsOnDisk(context.Background(), c.disk(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) == 0 {
		t.Fatal("missing source plogs")
	}
	// A logical skew above the policy band; the virtual store never allocates it.
	c.plogBytes[ps[0].PlogID] += 32 << 30
	if err := c.db.SetPlogLength(context.Background(), ps[0].PlogID, c.plogBytes[ps[0].PlogID]); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if got := c.rebalance(1, 10<<30); got != 1 {
		t.Fatalf("moves = %d, want 1", got)
	}
	t.Logf("op=rebalance max-moves=1 min-skew-gib=10 moves=1 placement-scans=%d elapsed=%s", c.placementScans, seconds(time.Since(started)))
	if c.placementScans == 0 {
		t.Fatal("rebalance did not inspect placement metadata")
	}
}

func TestVirtualReprotectTiming(t *testing.T) {
	c := newVirtualScaleCluster(t, 4, 2) // node 4 is the repair destination
	c.populate(20, MaxVlogBytes*99/100)
	victim := c.disk(1, 0)
	c.failDisk(victim)
	started := time.Now()
	repaired := c.reprotectDisk(victim)
	t.Logf("op=reprotect failed-disk=%d repaired-shards=%d elapsed=%s", victim, repaired, seconds(time.Since(started)))
	commit, readable := c.readiness(1)
	if !commit || !readable {
		t.Fatalf("after reprotect: commit=%v readable=%v, want true/true", commit, readable)
	}
}

type scaleProfile struct {
	name                       string
	env                        string
	nodes, disks, data, parity int
	targetBytes                int64
}

func prepareScaleProfile(t *testing.T, p scaleProfile) (*virtualScaleCluster, int) {
	t.Helper()
	if os.Getenv(p.env) != "1" {
		t.Skipf("set %s=1 to run the %s virtual scale profile", p.env, p.name)
	}
	fillPercent := scaleFillPercent(t)
	chunkBytes := MaxVlogBytes * 99 / 100
	targetBytes := int64(float64(p.targetBytes) * fillPercent / 100)
	vlogs := int(targetBytes / chunkBytes)
	if vlogs == 0 {
		vlogs = 1
	}
	scaleTracef("profile=%s phase=bootstrap servers=%d disks=%d ec=%d+%d fill-percent=%.3f target-logical-bytes=%d vlogs=%d", p.name, p.nodes, p.nodes*p.disks, p.data, p.parity, fillPercent, targetBytes, vlogs)
	started := time.Now()
	c := newVirtualScaleCluster(t, p.nodes, p.disks)
	t.Logf("profile=%s phase=recover servers=%d elapsed=%s", p.name, p.nodes, seconds(time.Since(started)))
	started = time.Now()
	c.populateECProgress(vlogs, p.data, p.parity, chunkBytes, func(done int) {
		scaleTracef("profile=%s phase=populate progress=%d%% vlogs=%d/%d logical-tb=%.2f elapsed=%s", p.name, done*100/vlogs, done, vlogs, float64(done)*float64(chunkBytes)/1e12, seconds(time.Since(started)))
	})
	t.Logf("profile=%s phase=populate-complete fill-percent=%.3f vlogs=%d plogs=%d logical-tb=%.2f elapsed=%s", p.name, fillPercent, vlogs, vlogs*(p.data+p.parity), float64(vlogs)*float64(chunkBytes)/1e12, seconds(time.Since(started)))
	scalePostPopulate(t, p.name, c)
	return c, vlogs
}

func logAllReadiness(t *testing.T, profile, operation string, c *virtualScaleCluster) (int, int) {
	t.Helper()
	started := time.Now()
	commitReady, readable := c.readinessAll()
	t.Logf("profile=%s op=%s commit-ready=%d readable=%d elapsed=%s", profile, operation, commitReady, readable, seconds(time.Since(started)))
	return commitReady, readable
}

func TestControlPlaneScale100TB(t *testing.T) {
	c, _ := prepareScaleProfile(t, scaleProfile{
		name: "100TB", env: "ROSE_SCALE_100TB", nodes: 3, disks: 8, data: 2, parity: 1, targetBytes: 100_000_000_000_000,
	})

	started := time.Now()
	c.failDisk(c.disk(1, 0))
	commit, readable := c.readiness(1)
	scaleTracef("profile=100TB phase=disk-loss disk=1 commit-ready=%t readable=%t elapsed=%s", commit, readable, seconds(time.Since(started)))
	t.Logf("profile=100TB phase=disk-loss disk=1 commit-ready=%t readable=%t elapsed=%s", commit, readable, seconds(time.Since(started)))
	if commit || !readable {
		t.Fatalf("disk loss readiness = %t/%t, want false/true", commit, readable)
	}
	_, _ = logAllReadiness(t, "100TB", "readiness-all-after-disk-loss", c)

	// Restore the independent disk transition, then measure a node failure from
	// the same fully populated metadata state.
	if err := c.db.SetDiskState(context.Background(), c.disk(1, 0), meta.DiskActive); err != nil {
		t.Fatal(err)
	}
	c.diskState[c.disk(1, 0)] = meta.DiskActive
	started = time.Now()
	c.failNode(1)
	commit, readable = c.readiness(1)
	scaleTracef("profile=100TB phase=node-loss node=1 commit-ready=%t readable=%t elapsed=%s", commit, readable, seconds(time.Since(started)))
	t.Logf("profile=100TB phase=node-loss node=1 commit-ready=%t readable=%t elapsed=%s", commit, readable, seconds(time.Since(started)))
	if commit || !readable {
		t.Fatalf("node loss readiness = %t/%t, want false/true", commit, readable)
	}
	_, _ = logAllReadiness(t, "100TB", "readiness-all-after-node-loss", c)
}

func TestControlPlaneScale10PB(t *testing.T) {
	// 9*60*32 TB at 90% fill under 5+2 EC is >10 PB. This profile has
	// enough distinct node domains for the seven EC shards.
	c, vlogs := prepareScaleProfile(t, scaleProfile{
		name: "10PB", env: "ROSE_FULL_SCALE", nodes: 9, disks: 60, data: 5, parity: 2, targetBytes: 10_000_000_000_000_000,
	})
	started := time.Now()
	c.failNode(1)
	commitReady, readableCount := c.readinessAll()
	t.Logf("profile=10PB phase=node-loss node=1 commit-ready=%d/%d readable=%d/%d elapsed=%s", commitReady, vlogs, readableCount, vlogs, seconds(time.Since(started)))
	if commitReady == vlogs || readableCount != vlogs {
		t.Fatalf("node loss readiness = %d/%d commit-ready, %d/%d readable", commitReady, vlogs, readableCount, vlogs)
	}
}
