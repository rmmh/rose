# TODO

## Deterministic simulation

- Make virtual disk, metadata, and transaction state cheaply cloneable with
  copy-on-write snapshots or an undo log.
- Hash canonical simulator state and memoize already-explored states.
- Add partial-order reduction for independent disk operations and symmetry
  reduction for equivalent disks.
- Branch crash, disk failure, repair, and compaction decisions from each
  runnable multi-transaction scheduler state.
- Emit and replay minimized deterministic failure traces.

## Durability runtime

- Add framed prepared-record recovery, tail validation/truncation, and
  persistent transaction staging.
- Persist disk roots, shard placement, and vlog cursors; reconstruct plogs and
  vlogs on server restart.
- Implement multi-disk placement, strict read-only degradation, repair, and
  compaction.

## Bitrot and scrubbing (done)

- Plog now keeps 4KB-aligned, immutable sealed sectors with a ragged-edge open
  sector overwritten in place across commits, fixing logical/physical drift.
- Reads verify each sealed sector against its recorded hash and fail with
  ErrBitrot instead of returning corrupt bytes; DUPLICATE/EC vlogs fall through
  to a surviving copy.
- Plog.Scrub validates every completed block (sector hashes + per-block HMAC);
  Vlog.Scrub and Server.Scrub aggregate per-shard results for bulk integrity
  passes.

## Bitrot follow-ups

- Persist sealed sector hashes for the trailing open block so sub-1MB data is
  verifiable after a restart (currently trusted only within the writing
  session).
- Add a Scrub RPC and a repair pass that rewrites corrupt shards from surviving
  redundancy (DUPLICATE copy / EC reconstruct).

## Chunk GC and compaction (done)

- Chunk rows are inserted inside the publish transaction, so a committed chunk
  is never durably visible at refcount 0 (closes the spec's pin window).
- DB.GCChunks / Server.GC reclaim refcount-0 chunk rows (RoseSnapshotGC GCChunk).
- VlogUsages accounts live/dead bytes; CompactionPolicy selects candidates by
  waste ratio + dead-byte floor, most-wasteful-first, capped by MaxJobs.
- Server.CompactVlog rewrites live chunks into a fresh vlog and retires the old
  one via a durable `job` row; crash-safe and resumed by Recover (mirrors the
  RoseStorage job_* work stream and RoseTxnCommit ReclaimOrphan: copy live
  records elsewhere before removing the source).

## Compaction follow-ups

- Drive Compact from a background goroutine on an interval / dead-space trigger
  instead of only on explicit call; expose a Compact/GC RPC.
- Stream large chunks instead of buffering whole chunks in memory during the
  rewrite. (Destination commits are now batched: CompactVlog copies every live
  chunk, fsyncs the destination once, then repoints all the chunk rows -- one
  fsync per job instead of per chunk, preserving the bytes-durable-before-repoint
  crash-safety invariant.)
- Reclaim duplicate bytes left in the destination when a crash re-copies a chunk
  whose row was not yet relocated (currently dead space until the next compaction).
- Retire the source vlog's plog files transactionally with the metadata so a
  crash between RetireVlog and os.Remove cannot leak plog files on disk.
- Track per-vlog logical vs compressed bytes once compression lands, so dead-space
  accounting stays correct under EC overhead and compression ratios.

## Storage control plane (disk lifecycle + commit gating: done)

- Disk lifecycle (active/draining/failed/detached) is now a durable catalog
  column (`disk.state`), mirroring RoseStorage's disk_state. Configured disks are
  registered active on Recover, which re-adopts any persisted non-active state so
  a disk that was draining/failed before a restart stays out of placement.
- Server.SetDiskState transitions a disk and persists it under vlogMu;
  activeDiskIDs (the placement source) now returns only active disks, so new
  shards never land on a draining/failed/detached disk.
- CommitReady/Readable gate on live shard counts per protection scheme
  (commitThreshold: NONE=1, DUPLICATE=min(MinCopies, copies), EC=all shards;
  readThreshold: data_shards for EC, 1 otherwise). Close refuses to acknowledge a
  write when its vlog lacks enough live shards (strict read-only degradation).

## Disk drain/remove job (done)

- Server.DrainDisk evacuates every shard off a disk and detaches it
  (StartRemove -> DrainStep* -> FinishJob), run under a durable `job` row
  (kind=drain, target_disk) so a crash mid-drain is resumed from Recover off the
  shards still on the disk. The disk moves to draining immediately (out of
  placement) and to detached only once empty (NoDetachedData).
- Each shard is relocated with compaction's copy-then-repoint discipline: copy
  the plog file to the destination disk and fsync, atomically flip plog.disk_id
  (MovePlogToDisk), remove the source, then re-mount the owning vlog so in-memory
  clients resolve to the relocated file.
- pickDrainDestination enforces PlacementAllowed: the destination must be an
  active disk not already holding another shard/copy of the same vlog, so a move
  never collapses two shards onto one disk. A drain with no legal destination
  fails and leaves the disk draining (stuck until capacity is added).

## Disk reprotect job (done)

- Server.ReprotectDisk regenerates every shard lost with a failed disk onto
  healthy disks (StartReprotect -> ReprotectStep* -> FinishJob), under a durable
  `job` row (kind=reprotect, target_disk) so a crash mid-reprotect resumes from
  the shards still mapped to the failed disk's plogs. The disk stays failed:
  reprotect restores durability, it does not repair hardware.
- Each lost shard is rebuilt from surviving redundancy, never from the failed
  disk: a sibling mirror's full logical bytes for DUPLICATE (healing bitrot in
  passing), a reed-solomon reconstruct over the equal-length surviving shard
  streams for EC (storage.ReconstructECShard). NONE has no redundancy and errors
  loudly as data loss.
- The regenerated bytes are made durable in a fresh plog on a placement-allowed
  disk (pickDrainDestination's PlacementAllowed reused) before the shard mapping
  is atomically flipped (meta.ReplaceShardPlog: repoint vlog_plog + drop the lost
  plog row in one txn), then the vlog is re-mounted. A crash before the flip
  leaves the shard referencing the failed disk and the step re-runs; the
  guard-on-old-plog in ReplaceShardPlog makes the repoint idempotent.

## Disk replace job (done)

- Server.AttachDisk brings fresh local capacity online: it configures a new disk
  root and registers it active in the catalog, making it eligible for placement
  and as a replace destination. (AddDisk/ReplaceDisk are reserved by the gRPC
  surface for the future RPC driver, so the control-plane verbs are AttachDisk
  and ReplaceDiskWith.)
- Server.ReplaceDiskWith is drain with a pinned destination: every shard on the
  old disk is relocated onto one freshly added disk (the swap-in-place an operator
  expects when retiring a disk), then the old disk is detached. It runs under a
  durable `job` row (kind=replace, target_disk=old, dest_disk=new) so a crash
  mid-replace resumes onto the same destination; the pinned dest_disk is a new
  job column. Each shard reuses drain's copy-then-repoint discipline and is
  PlacementAllowed-checked against the destination first.

## Disk rebalance job (done)

- Server.Rebalance evens disk-space usage (bytes, not shard count -- shards vary
  widely in size) across active disks (RoseStorage RebalanceStep) as a bounded,
  best-effort pass: it moves the largest fitting shard off the busiest disk onto
  the lightest PlacementAllowed disk, reusing drain's copy-then-repoint
  migratePlog. Each move picks a shard no larger than the byte gap so it shrinks
  the spread without overshooting/bouncing. Unlike drain/reprotect/replace it
  carries no durable job row - every move is individually crash-safe and a partial
  pass just leaves a less-even (but valid) cluster the next pass continues from.
- RebalancePolicy makes the aggressiveness configurable so it does not chase
  perfect balance: MinSkewBytes is a hysteresis band (no move unless the busiest
  disk holds more than MinSkewBytes over the idlest, and it stops once the byte
  spread falls back within the band), MaxMovesPerPass caps the IO per pass, and
  Cooldown is the backoff between passes that actually moved something. Defaults:
  tolerate a 10 GiB spread, eight moves/pass, five-minute cooldown.

## Node failure and fault domains (done)

- Node liveness mirrors RoseStorage's node_state (working/failed) as a durable
  catalog column (`node.state`), loaded on Recover. SetNodeState transitions a
  node under vlogMu; a failed node's disks drop out of the live set via
  diskLiveLocked (DiskLive = disk active /\ node working) without changing their
  disk_state, so commit/read gating and placement react to a node going offline
  and the loss reverses when it returns. Disks map to node fault domains via
  diskNodes (default: each disk its own node, the bounded model's shape;
  SetDiskNode groups several disks onto one node).
- PlacementAllowed now enforces NodeLevelDurability: provisionVlogLocked places
  one shard per distinct node (distinctNodeDisksLocked), and every relocation
  destination check (drain/reprotect pickDrainDestination, replace
  ensurePlacementAllowed, rebalanceOne) is keyed by node via occupiedNodesLocked,
  so no two shards/copies of a vlog ever share a node. EC/DUPLICATE provisioning
  fails loudly when there are too few distinct nodes for the scheme.
- A node returning to working cancels any reprotect its outage triggered: SetNodeState
  marks running reprotect jobs for the node's failed disks cancelled (a new job
  state excluded from RunningJobs, so a later restart won't resume them) and
  restores those disks to active, since their bytes survived and the
  not-yet-regenerated shards resolve to them again.

## Storage control plane follow-ups (from RoseStorage.tla, not yet implemented)

- Automatic repair/driver: a background scheduler that detects failed disks and
  drives reprotect, runs rebalance on the cooldown tick, and re-admits writes once
  every published object is fully protected again; plus the gRPC handlers
  (AddDisk/RemoveDisk/ReplaceDisk/StartReprotect/StartRebalance) wired to the
  control-plane methods.
- Make Recover tolerate a failed disk whose plog files are actually gone (it
  currently OpenPlogs every catalog plog by path, so a true file loss fails
  startup before reprotect can run); reconstruct or stub the missing shard's
  client so reprotect can resume from a cold start, not just a live server.
  This is also what limits the node-return reprotect-cancel: reverting a disk
  failed -> active relies on its original plogs still being openable, which holds
  for a live server (handles never closed) but not across a cold restart while
  the offline node's files are unreachable. Until Recover tolerates absent files,
  cancel-on-return is only sound for a running server.
- Reclaim the orphan plog files a reprotect leaves on the returning disk when a
  node-return cancels it: for shards regenerated before the cancel, ReplaceShardPlog
  deleted the old plog row but the file physically remains on the returned disk
  (catalog no longer references it). Dead space, not dead metadata; sweep it from
  the GC/compaction pass (same class as the stray-file items below).
- Reclaim the regenerated plog left behind when a crash re-runs a reprotect step
  whose ReplaceShardPlog had not yet committed (same duplicate-bytes-on-crash
  caveat compaction has).
- Retire a drained disk's stray source files on resume (a crash after the
  metadata flip can leave a copied-from file on the disk being removed).
