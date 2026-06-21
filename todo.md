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
- Batch destination commits (one fsync per job, not per chunk) and stream large
  chunks instead of buffering whole chunks in memory during the rewrite.
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

## Storage control plane follow-ups (from RoseStorage.tla, not yet implemented)

- Implement the last maintenance job on the same durable `job` machinery:
  rebalance (RebalanceStep across live disks to even out shard distribution).
- Make Recover tolerate a failed disk whose plog files are actually gone (it
  currently OpenPlogs every catalog plog by path, so a true file loss fails
  startup before reprotect can run); reconstruct or stub the missing shard's
  client so reprotect can resume from a cold start, not just a live server.
- Reclaim the regenerated plog left behind when a crash re-runs a reprotect step
  whose ReplaceShardPlog had not yet committed (same duplicate-bytes-on-crash
  caveat compaction has).
- Model node failure (node_state working/failed) and the one-disk-per-node fault
  domain; fold node liveness into DiskLive so a failed node's disks drop out, and
  honor NodeLevelDurability fault domains in PlacementAllowed.
- Automatic repair that re-admits writes once every published object is fully
  protected again, plus an RPC/background driver for disk add/drain/replace.
- Retire a drained disk's stray source files on resume (a crash after the
  metadata flip can leave a copied-from file on the disk being removed).
