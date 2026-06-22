package server_test

// Workload engine and reference oracle for the chaos test. The oracle records
// the last committed content digest per path (and frozen copies per snapshot);
// the workload runs concurrent clients issuing a weighted mix of
// write/read-verify/delete/rename/snapshot ops and checks every read against the
// oracle. Read-after-write equality is the Go reflection of RoseStorage's
// Durability invariant (committed => Readable): anything the oracle says is
// committed must read back byte-identical.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

// fileState is the oracle's record of a committed file: the digest and length of
// its last successfully committed content.
type fileState struct {
	sum [32]byte
	len int
}

func digestOf(data []byte) fileState {
	return fileState{sum: sha256.Sum256(data), len: len(data)}
}

// oracle is the source of truth the workload verifies reads against. It records
// the current committed content per path and a frozen copy of that map per
// snapshot id. All methods are safe for concurrent use, but the workload further
// partitions the path keyspace per worker so a given path is only mutated by one
// goroutine — the oracle never has to arbitrate competing writers.
type oracle struct {
	mu    sync.Mutex
	files map[string]fileState
	snaps map[uint64]map[string]fileState
}

func newOracle() *oracle {
	return &oracle{
		files: make(map[string]fileState),
		snaps: make(map[uint64]map[string]fileState),
	}
}

func (o *oracle) put(path string, st fileState) {
	o.mu.Lock()
	o.files[path] = st
	o.mu.Unlock()
}

func (o *oracle) get(path string) (fileState, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	st, ok := o.files[path]
	return st, ok
}

func (o *oracle) remove(path string) {
	o.mu.Lock()
	delete(o.files, path)
	o.mu.Unlock()
}

func (o *oracle) rename(oldPath, newPath string) {
	o.mu.Lock()
	if st, ok := o.files[oldPath]; ok {
		o.files[newPath] = st
		delete(o.files, oldPath)
	}
	o.mu.Unlock()
}

// freezeSnapshot records the current committed state under a snapshot id. The
// caller must ensure no commit is in flight (the workload holds its snapshot
// write-lock), so the frozen map matches what the server's snapshot captured.
func (o *oracle) freezeSnapshot(id uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	frozen := make(map[string]fileState, len(o.files))
	for p, st := range o.files {
		frozen[p] = st
	}
	o.snaps[id] = frozen
}

func (o *oracle) dropSnapshot(id uint64) {
	o.mu.Lock()
	delete(o.snaps, id)
	o.mu.Unlock()
}

func (o *oracle) snapshotState(id uint64) (map[string]fileState, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	frozen, ok := o.snaps[id]
	return frozen, ok
}

// allFiles returns a copy of the current committed path->state map for a final
// verification sweep.
func (o *oracle) allFiles() map[string]fileState {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[string]fileState, len(o.files))
	for p, st := range o.files {
		out[p] = st
	}
	return out
}

// workloadStats tallies what the workload did, for an end-of-run summary and for
// assertions. readMismatch and opError are the hard-failure counters; a degraded
// write rejected by the commit gate is expected and counted separately.
type workloadStats struct {
	writes         atomic.Int64
	writesDegraded atomic.Int64
	reads          atomic.Int64
	readMissing    atomic.Int64 // committed file unexpectedly unreadable
	readMismatch   atomic.Int64 // read content disagreed with the oracle
	deletes        atomic.Int64
	renames        atomic.Int64
	snapCreates    atomic.Int64
	snapVerifies   atomic.Int64
	snapDeletes    atomic.Int64
	opErrors       atomic.Int64 // unexpected (non-degradation) errors
}

func (s *workloadStats) summary() string {
	return fmt.Sprintf("writes=%d (degraded=%d) reads=%d (missing=%d mismatch=%d) deletes=%d renames=%d snaps=%d/verify=%d/del=%d opErrors=%d",
		s.writes.Load(), s.writesDegraded.Load(), s.reads.Load(), s.readMissing.Load(), s.readMismatch.Load(),
		s.deletes.Load(), s.renames.Load(), s.snapCreates.Load(), s.snapVerifies.Load(), s.snapDeletes.Load(), s.opErrors.Load())
}

// workload drives a steady op mix against the cluster. Each worker owns a
// disjoint slice of the path keyspace (key index % workers == worker id), so the
// oracle has a single writer per path; snapMu serializes snapshot creation
// against in-flight commits so a snapshot's oracle copy matches the server's.
type workload struct {
	t       *testing.T
	cluster *chaosCluster
	oracle  *oracle
	stats   workloadStats

	buckets    []string
	keysPerWkr int // distinct object keys each worker cycles through, per bucket

	snapMu  sync.RWMutex // RLock: a commit is in flight; Lock: taking a snapshot
	maintMu sync.Mutex   // at most one GC+compaction pass at a time
	snaps   struct {
		mu  sync.Mutex
		ids []uint64
	}

	// degraded reports whether a vlog-degraded error is currently expected
	// (chaos has disks/nodes down). With no chaos it always returns false, so any
	// degraded write is then a real failure.
	degraded func() bool
}

func newWorkload(t *testing.T, c *chaosCluster, o *oracle) *workload {
	return &workload{
		t:          t,
		cluster:    c,
		oracle:     o,
		buckets:    []string{bucketEC, bucketMirror},
		keysPerWkr: 24,
		degraded:   func() bool { return false },
	}
}

// path returns the object path a worker uses for (bucket, key) within its own
// keyspace partition.
func (w *workload) path(bucket string, worker, key int) string {
	return fmt.Sprintf("/%s/w%d-obj%04d", bucket, worker, key)
}

// run launches `workers` goroutines, each looping the op mix against its own
// client until ctx is cancelled, and returns when all have stopped.
func (w *workload) run(ctx context.Context, workers int, seed int64) {
	var wg sync.WaitGroup
	for id := 0; id < workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := w.cluster.client()
			rng := rand.New(rand.NewSource(seed + int64(workerID)*1_000_003))
			for ctx.Err() == nil {
				w.step(ctx, client, workerID, rng)
			}
		}(id)
	}
	wg.Wait()
}

// step performs one weighted random operation.
func (w *workload) step(ctx context.Context, client pb.RoseClient, workerID int, rng *rand.Rand) {
	switch r := rng.Intn(100); {
	case r < 38:
		w.doWrite(ctx, client, workerID, rng)
	case r < 78:
		w.doRead(ctx, client, workerID, rng)
	case r < 86:
		w.doDelete(ctx, client, workerID, rng)
	case r < 92:
		w.doRename(ctx, client, workerID, rng)
	case r < 93:
		w.doSnapshotCreate(ctx, client, workerID)
	case r < 96:
		w.doSnapshotVerify(ctx, client, rng)
	case r < 97:
		w.doSnapshotDelete(ctx, client)
	default:
		w.doMaintenance(ctx)
	}
}

// doMaintenance reclaims space and exercises the GC + compaction paths under
// load: row-level GC drops chunks left unreferenced by overwrites/deletes, then
// compaction physically rewrites the wastiest vlogs and retires the old ones. It
// is what keeps a long soak's footprint bounded on a finite RAM disk, and it
// runs against the live Server (compaction is internally crash-safe and locked).
func (w *workload) doMaintenance(ctx context.Context) {
	// Only one maintenance pass at a time: two concurrent Compact passes can both
	// select the same wastiest vlog from a stale usage snapshot, and the second
	// then races to compact a vlog the first already retired.
	if !w.maintMu.TryLock() {
		return
	}
	defer w.maintMu.Unlock()
	// Use a detached context so a compaction in flight when the run deadline
	// fires still finishes cleanly rather than aborting mid-rewrite.
	_ = ctx
	mctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv := w.cluster.server()
	if _, err := srv.GC(mctx); err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	// A low dead-bytes floor so the modest writes of this workload still trigger
	// reclamation on the RAM disk.
	policy := server.CompactionPolicy{MinWasteRatio: 0.25, MinDeadBytes: 1 << 20, MaxJobs: 4}
	if _, err := srv.Compact(mctx, policy); err != nil {
		w.recordOpErr(ctx, err)
	}
}

// randomSize returns a write size: usually small for IOPS, occasionally larger
// to span several chunks and roll vlogs. Capped well under the RAM-disk budget so
// a bounded keyspace plus periodic compaction keeps the footprint stable.
func randomSize(rng *rand.Rand) int {
	if rng.Intn(100) < 80 {
		return 1024 + rng.Intn(63*1024) // 1 KiB .. 64 KiB
	}
	return 128*1024 + rng.Intn(640*1024) // 128 KiB .. ~768 KiB
}

func (w *workload) doWrite(ctx context.Context, client pb.RoseClient, workerID int, rng *rand.Rand) {
	bucket := w.buckets[rng.Intn(len(w.buckets))]
	path := w.path(bucket, workerID, rng.Intn(w.keysPerWkr))
	data := make([]byte, randomSize(rng))
	rng.Read(data)

	// Hold the snapshot read-lock across the whole commit so a concurrent
	// snapshot freezes a consistent before/after, never a half-applied write.
	w.snapMu.RLock()
	defer w.snapMu.RUnlock()

	open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	if _, err := client.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	_, err = client.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle()})
	if err != nil {
		// A degraded-vlog rejection leaves the prior committed version readable;
		// it is expected while chaos has shards down, and the oracle keeps the old
		// state. Anything else is a real failure.
		if isDegraded(err) {
			w.stats.writesDegraded.Add(1)
			if !w.degraded() {
				w.recordOpErr(ctx, fmt.Errorf("unexpected degraded write with no faults injected: %w", err))
			}
			return
		}
		w.recordOpErr(ctx, err)
		return
	}
	w.oracle.put(path, digestOf(data))
	w.stats.writes.Add(1)
}

func (w *workload) doRead(ctx context.Context, client pb.RoseClient, workerID int, rng *rand.Rand) {
	path := w.path(w.buckets[rng.Intn(len(w.buckets))], workerID, rng.Intn(w.keysPerWkr))
	st, ok := w.oracle.get(path)
	if !ok {
		return // never committed (or deleted): nothing to verify
	}
	open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		w.readFailure(ctx, path, err)
		return
	}
	read, err := client.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: int64(st.len)})
	if err != nil {
		w.readFailure(ctx, path, err)
		return
	}
	w.stats.reads.Add(1)
	if sha256.Sum256(read.GetBuffer()) != st.sum || len(read.GetBuffer()) != st.len {
		w.stats.readMismatch.Add(1)
		w.t.Errorf("read mismatch at %s: got %d bytes (sum %x), want %d bytes (sum %x)",
			path, len(read.GetBuffer()), sha256.Sum256(read.GetBuffer()), st.len, st.sum)
	}
}

// readFailure classifies a failed read of a committed file. While chaos has the
// vlog below its read threshold a transient failure is tolerable, but with no
// faults a committed file must always read, so it is a hard error.
func (w *workload) readFailure(ctx context.Context, path string, err error) {
	if ctx.Err() != nil {
		return // run is ending; a cancelled read is expected shutdown
	}
	w.stats.readMissing.Add(1)
	if !w.degraded() {
		w.t.Errorf("committed file %s unreadable with no faults injected: %v", path, err)
	}
}

func (w *workload) doDelete(ctx context.Context, client pb.RoseClient, workerID int, rng *rand.Rand) {
	path := w.path(w.buckets[rng.Intn(len(w.buckets))], workerID, rng.Intn(w.keysPerWkr))
	if _, ok := w.oracle.get(path); !ok {
		return
	}
	w.snapMu.RLock()
	defer w.snapMu.RUnlock()
	if _, err := client.Unlink(ctx, &pb.UnlinkRequest{Path: path}); err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	w.oracle.remove(path)
	w.stats.deletes.Add(1)
}

func (w *workload) doRename(ctx context.Context, client pb.RoseClient, workerID int, rng *rand.Rand) {
	bucket := w.buckets[rng.Intn(len(w.buckets))]
	oldPath := w.path(bucket, workerID, rng.Intn(w.keysPerWkr))
	newPath := w.path(bucket, workerID, rng.Intn(w.keysPerWkr))
	if oldPath == newPath {
		return
	}
	if _, ok := w.oracle.get(oldPath); !ok {
		return
	}
	w.snapMu.RLock()
	defer w.snapMu.RUnlock()
	if _, err := client.Rename(ctx, &pb.RenameRequest{OldPath: oldPath, NewPath: newPath}); err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	w.oracle.rename(oldPath, newPath)
	w.stats.renames.Add(1)
}

func (w *workload) doSnapshotCreate(ctx context.Context, client pb.RoseClient, workerID int) {
	// Exclusive against in-flight commits so the server snapshot and the oracle
	// freeze capture the same committed set.
	w.snapMu.Lock()
	defer w.snapMu.Unlock()
	name := fmt.Sprintf("snap-w%d-%d", workerID, w.stats.snapCreates.Load())
	resp, err := client.CreateSnapshot(ctx, &pb.CreateSnapshotRequest{Name: name})
	if err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	id := resp.GetSnapshotId()
	w.oracle.freezeSnapshot(id)
	w.snaps.mu.Lock()
	w.snaps.ids = append(w.snaps.ids, id)
	w.snaps.mu.Unlock()
	w.stats.snapCreates.Add(1)
}

func (w *workload) doSnapshotVerify(ctx context.Context, client pb.RoseClient, rng *rand.Rand) {
	id, ok := w.randomSnapshot(rng)
	if !ok {
		return
	}
	frozen, ok := w.oracle.snapshotState(id)
	if !ok {
		return
	}
	// Verify a handful of random paths from the frozen epoch (RetainedSnapshots-
	// Readable): each must read back its snapshot-time content.
	paths := make([]string, 0, len(frozen))
	for p := range frozen {
		paths = append(paths, p)
	}
	if len(paths) == 0 {
		w.stats.snapVerifies.Add(1)
		return
	}
	for i := 0; i < 4 && i < len(paths); i++ {
		path := paths[rng.Intn(len(paths))]
		st := frozen[path]
		open, err := client.OpenSnapshot(ctx, &pb.OpenSnapshotRequest{SnapshotId: id, Path: path})
		if err != nil {
			if ctx.Err() == nil && !w.degraded() {
				w.t.Errorf("snapshot %d path %s unreadable: %v", id, path, err)
			}
			continue
		}
		read, err := client.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: int64(st.len)})
		if err != nil {
			if ctx.Err() == nil && !w.degraded() {
				w.t.Errorf("snapshot %d path %s read failed: %v", id, path, err)
			}
			continue
		}
		if sha256.Sum256(read.GetBuffer()) != st.sum {
			w.stats.readMismatch.Add(1)
			w.t.Errorf("snapshot %d path %s content drifted from its epoch", id, path)
		}
	}
	w.stats.snapVerifies.Add(1)
}

func (w *workload) doSnapshotDelete(ctx context.Context, client pb.RoseClient) {
	w.snaps.mu.Lock()
	if len(w.snaps.ids) <= 1 { // keep at least one around for verification
		w.snaps.mu.Unlock()
		return
	}
	id := w.snaps.ids[0]
	w.snaps.ids = w.snaps.ids[1:]
	w.snaps.mu.Unlock()

	if _, err := client.DeleteSnapshot(ctx, &pb.DeleteSnapshotRequest{SnapshotId: id}); err != nil {
		w.recordOpErr(ctx, err)
		return
	}
	w.oracle.dropSnapshot(id)
	w.stats.snapDeletes.Add(1)
}

func (w *workload) randomSnapshot(rng *rand.Rand) (uint64, bool) {
	w.snaps.mu.Lock()
	defer w.snaps.mu.Unlock()
	if len(w.snaps.ids) == 0 {
		return 0, false
	}
	return w.snaps.ids[rng.Intn(len(w.snaps.ids))], true
}

// recordOpErr flags an unexpected op error, unless the run context is already
// done — an op interrupted by the run deadline failing with a cancellation is
// expected shutdown, not a fault.
func (w *workload) recordOpErr(ctx context.Context, err error) {
	if ctx.Err() != nil || w.degraded() {
		return
	}
	w.stats.opErrors.Add(1)
	w.t.Errorf("workload op error: %v", err)
}

// verifyAll reads back every currently committed file and asserts byte-identity
// with the oracle — the final Durability sweep after the workload (and chaos)
// settle. It uses a fresh client and assumes the cluster is healthy (all faults
// recovered), so every committed file must read.
func (w *workload) verifyAll(ctx context.Context) {
	client := w.cluster.client()
	files := w.oracle.allFiles()
	for path, st := range files {
		open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
		if err != nil {
			w.t.Errorf("final sweep: open %s: %v", path, err)
			continue
		}
		read, err := client.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: int64(st.len)})
		if err != nil {
			w.t.Errorf("final sweep: read %s: %v", path, err)
			continue
		}
		if sha256.Sum256(read.GetBuffer()) != st.sum || len(read.GetBuffer()) != st.len {
			w.t.Errorf("final sweep: %s content mismatch (got %d bytes, want %d)", path, len(read.GetBuffer()), st.len)
		}
	}
	w.t.Logf("final sweep verified %d committed files", len(files))
}

// isDegraded reports whether an error is the commit gate's read-only-degradation
// rejection (too few live shards to durably commit), as opposed to a real fault.
func isDegraded(err error) bool {
	return err != nil && strings.Contains(err.Error(), "degraded")
}

// TestChaosWorkloadNoFaults runs the workload engine against a healthy cluster
// (no chaos) for a bounded number of ops, then sweeps. It validates the engine
// and oracle before the fault injector is layered on: with no faults injected
// every read must verify and no write may be degraded.
func TestChaosWorkloadNoFaults(t *testing.T) {
	c := newChaosCluster(t, 4, 1)
	defer c.close()

	o := newOracle()
	w := newWorkload(t, c, o)

	ctx, cancel := context.WithTimeout(context.Background(), chaosDuration(3*time.Second))
	defer cancel()
	w.run(ctx, 2, chaosSeed(1))

	w.verifyAll(context.Background())
	t.Logf("workload: %s", w.stats.summary())

	if w.stats.writes.Load() == 0 || w.stats.reads.Load() == 0 {
		t.Fatal("workload did no writes or reads")
	}
}
