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

## Storage control plane follow-ups (from RoseStorage.tla, not yet implemented)

- Model node failure (node_state working/failed) and the one-disk-per-node fault
  domain; fold node liveness into DiskLive so a failed node's disks drop out.
- Implement the remaining maintenance jobs as durable work-stream entries
  (reusing the meta `job` machinery): drain/remove, replace, reprotect, rebalance
  (DrainStep/ReprotectStep/RebalanceStep), driving disks to detached only once
  empty (NoDetachedData) and never writing to a draining disk (NoWritesToDraining).
- Enforce the full PlacementAllowed for EC during maintenance moves: never
  collapse two distinct EC shards (or duplicate copies) onto one disk/node;
  honor NodeLevelDurability fault domains.
- Automatic repair that re-admits writes once every published object is fully
  protected again, plus an RPC/background driver for disk add/drain/replace.
