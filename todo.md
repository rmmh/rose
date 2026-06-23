# TODO

## Client-facing IOPS (namespace + mounts)

The directory-aware namespace (parent-keyed `file_head` + `dir` marker rows),
`ListDir`/`Mkdir`/`Rmdir` RPCs, a directory-tree FUSE mount, and a WebDAV adapter
(`--webdav`) are in place. Remaining work:

- O(1) directory rename. `RenameFile` rewrites every descendant path/parent row
  under the old prefix (O(descendants)); the README's "fast directory renames"
  wants the explicit dir-inode model (rename relinks one row) deferred in this cut.
- Snapshot-aware `ListDir`. ListDir/StatPath read the live head only; the
  `OpenSnapshot` path has no directory listing yet.
- WebDAV random-access / partial writes. PUT is whole-file sequential (append),
  committed on Close; range/partial PUT and in-place rewrite are unsupported.
- FUSE truncate of existing data. `RoseFile.Setattr` accepts the size O_TRUNC
  sets but the append-only write path cannot shrink committed data; only
  truncate-on-create round-trips.
- macFUSE emits spurious ENOSYS/ENOTSUP/EINTR on private opcodes; the FUSE test
  retries to isolate FS logic from the platform. Revisit if go-fuse gains
  handling for those darwin opcodes.

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

## EC at the vlog level (write path + promotion done)

Erasure coding is off the per-chunk path (which padded every chunk up to a
dataShards multiple and fanned every tiny chunk across all d+m shards) and onto
the vlog byte stream as coarse column striping, with EC deferred behind a
replicated staging vlog and a maintenance-pass promotion step.

- Geometry: stripe column width C = dataPerBlock (255*4096), so each column maps
  to exactly one hash-protected plog block. Logical offset o -> row o/(C*d),
  column (o%(C*d))/C, position o%C. Data column j of row r lives in data plog j
  at plog offset r*C; parity column k in parity plog d+k at r*C. Whole-stream
  reconstruct still works because each row is exactly C per shard, so parity at
  global position p is a function of the data shards at global position p.
- Complete rows only: the EC vlog only ever receives complete C*d stripe rows,
  so data AND parity are append-only and seal normally -- no mutable parity, no
  plog format change. (A partial trailing row would force parity to keep changing
  after its block is already full-and-sealed, since columns fill left-to-right;
  that collision is why we do not put a partial row in the EC vlog.)
- (done) Write path: an EC bucket writes through a replicated (m+1) DUPLICATE
  staging vlog tagged with the target EC scheme (vlog.target_{data,parity}_shards,
  VlogInfo.IsStaging). leasedVlogForWrite routes EC policies to staging via
  vlogMatchesPolicy/provisionForPolicyLocked, so chunks are protected by
  replication until promoted.
- (done) Promotion: Server.PromoteStaging runs in the maintenance pass (and a
  durable JobPromote keyed by the staging vlog so a crash resumes from Recover).
  promotablePrefix packs whole live chunks into complete stripe rows, choosing the
  prefix whose cumulative size lands closest below a row boundary so the single
  padded final row stores the fewest zeros; the sub-row remainder stays replicated
  in staging to coalesce with later writes. Coded rows are made durable in the EC
  vlog before any chunk is reparented (RelocateChunk), mirroring compaction's
  crash-safety. Chunks are content-addressed/unsplittable, so promotion reparents
  whole chunks by hash.
- (done) staging cleanup -- PromoteStagingVlog now retires the staging vlog the
  moment it holds no live chunks: when a whole-row promotion drains it completely
  (count == len(live)), and when a sub-row remainder is later deleted (count == 0
  with no live chunks left). retireVlogLocked (shared with compaction) drops the
  catalog rows, unmounts and deletes the plog files, and clears it as any bucket's
  active vlog, so the empty replica does not linger until the dead-byte floor
  makes it a compaction candidate. Sub-row data that never completes a row still
  stays replicated (long-tail cost) until deleted or coalesced.
- TODO (task #3, remaining): the EC-vlog-too-full edge case is not yet reachable
  -- each promote job provisions its own fresh EC vlog and a single promotion's
  rows are bounded by the staging cap, so promotion never has to seal a full EC
  vlog and retarget the remainder. Revisit if promotion is changed to fill an EC
  vlog across multiple jobs. A coalescing pass that packs orphaned sub-row
  remainders from superseded staging vlogs is also still open.
- (done) EC compaction -- CompactVlog routes EC vlogs to compactECVlogLocked,
  which repacks the live chunks into complete stripe rows (writeChunksAsRows,
  shared with promotion; paddedRowLen sizes the all-chunks final-row padding)
  instead of the per-chunk dest.Write an EC dest rejects, then retires the
  wasteful vlog. Same copy-then-repoint crash-safety as the mirror path: rows are
  durable before any chunk is reparented, so a crash re-runs from the chunks still
  in the source.
- TODO (perf): block data cache -- keep recently written/promoted column data in
  memory so promotion and reconstruct avoid re-reading data plogs.
- TODO (perf): fan out per-row shard writes concurrently (the new path encodes
  and writes rows sequentially for clarity first).

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

- (done) The open block's sealed sector hashes now ride inline at the tail of the
  plog: Commit writes them in an HMAC-protected trailer sector immediately after
  the ragged-edge sector, and continued writes overwrite it as the block grows
  (the block's real hash sector replaces it once the block completes). reload()
  recovers the exact committed length and verified hashes from a valid trailer, so
  a sub-1MB trailing block stays verifiable across restarts instead of being
  recomputed (and thus trusted) from the very sectors it protects. A torn/absent
  trailer falls back to recomputing the sectors, the original behavior. No sidecar
  file, and one fsync per Commit.
- Close the torn-write fallback gap without trusting the bytes: when reload finds
  no valid trailer (a crash that overwrote the old trailer before the next
  Commit), the open block's sealed sector hashes are currently recomputed from the
  possibly-rotted sectors. Instead, recover them from the chunk rows in the
  metadata DB whose vaddr range covers those sectors -- validate each covered
  chunk's bytes against its stored content hash, and recompute the sector hashes
  only from chunk content that checks out. The chunks were durably committed in
  the DB before/independently of the lost trailer, so there is no true
  verification gap on the trailing block, only a fallback that needs the catalog.
- Anchor each plog's integrity metadata outside the file: store the expected
  sector hashes (or a root over them) per shard in the metadata DB -- the README's
  promised "hash tree roots for each log", not yet built. In-file HMACs are
  self-consistent but unanchored, so block-boundary truncation or rollback to an
  older consistent state still self-verifies, and (until keys are secret) targeted
  tampering is undetectable. A DB-anchored root also gives an authoritative
  committed length per shard, resolving (not just bounding) the post-crash length
  ambiguity the open-block trailer leaves.
- Add a Scrub RPC and a repair pass that rewrites corrupt shards from surviving
  redundancy (DUPLICATE copy / EC reconstruct).

## Encryption (not yet implemented)

- The per-block bitrot HMAC is keyed by a single hardcoded global placeholder
  (storage.bitrotKey = "rose-bitrot-key-todo"), so the hash-of-hashes is only a
  bitrot checksum, not tamper-evident: anyone with the binary can rewrite a
  sector, its hash, and re-derive the HMAC. Fold this key into a general
  encryption story -- per-server and per-bucket keys (encryption on by default?)
  -- so the integrity HMAC becomes a genuine authenticator and data can be
  encrypted at rest. Decide the default (encrypt-by-default vs opt-in) and key
  custody/rotation.

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

- The maintenance driver now drives GC then Compact each pass (after
  reprotect/rebalance), so dead-space reclamation runs on the interval instead of
  only on an explicit call; SetCompactionPolicy is the operator knob. Still TODO:
  a dead-space trigger that runs ahead of the tick, and a Compact/GC RPC on the
  gRPC surface.
- Stream large chunks instead of buffering whole chunks in memory during the
  rewrite. (Destination commits are now batched: CompactVlog copies every live
  chunk, fsyncs the destination once, then repoints all the chunk rows -- one
  fsync per job instead of per chunk, preserving the bytes-durable-before-repoint
  crash-safety invariant.)
- Reclaim duplicate bytes left in the destination when a crash re-copies a chunk
  whose row was not yet relocated (currently dead space until the next compaction).
  This is the benign window: those rows are complete and row-aligned, referenced
  by nobody, counted dead by VlogUsages, and reclaimed by re-compacting that dest
  -- reads stay correct throughout.
- (done) Closed the sharper resume window where a plog ends up longer than its
  vlog. A vlog's length is restored authoritatively from the DB (mountVlogLocked
  uses info.Length), but each backing plog restored its own length from its file
  (OpenPlog -> calcLogical / reload). If a crash landed after writeChunksAsRows
  sealed rows to the plog files but before Commit/SetVlogLength, the DB still had
  the old length while the plog reported the inflated physical length (the new
  write overwrote the previous open-block trailer, so reload fell back to
  trust-the-bytes). An unreconciled EC/DUPLICATE append then landed at the inflated
  plog cursor while reads resolved against the smaller vlog length -- a correctness
  bug, not reclaimable dead space. Fix: mountVlogLocked now calls
  Vlog.ReconcileShardLengths after mount, which truncates each backing plog down to
  the committed per-shard length (length for DUPLICATE/NONE, length/dataShards for
  EC) via Plog.TruncateTo. The DB length is authoritative, so the orphan tail is
  dropped rather than mistaken for committed data, realigning the plog cursor with
  where the vlog expects the next append. Offline shards (unreachable disks) are
  skipped. Covered by TestRecoverTruncatesOrphanPlogTail (crash injected between the
  dest row Commit and SetVlogLength; Recover asserts vlog/plog lengths agree, reads
  resolve, and a subsequent write lands where reads look).
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
- (done) Recover now tolerates missing/inaccessible plog backing storage, split by
  granularity, and closes the repair loop for a lost disk:
  - A disk whose entire backing directory is gone or inaccessible (the disk
    unplugged/unmounted) is marked failed durably (setDiskStateLocked) by a
    boot-time pre-scan over s.diskRoot, before anything is opened. It leaves
    placement, its shards mount offline, and the maintenance driver's next pass
    (RunMaintenanceOnce already reprotects DiskFailed disks) regenerates every
    shard it held onto a healthy disk -- no operator action.
  - An individual missing plog file inside an otherwise-accessible directory is
    deliberately NOT a disk failure: the disk stays active, serving its other
    shards, and that shard is stubbed offline. The maintenance pass's
    RepairOfflineShards then regenerates it from surviving redundancy onto a
    placement-allowed disk and clears the stub -- restoring full protection at
    shard granularity without condemning the disk. (A genuine teardown goes
    through RetireVlog, which deletes the plog catalog rows, so a cataloged-but-
    missing file is a real loss to repair, not an out-of-band teardown to ignore.)
  The root cause was OpenPlog's O_CREATE silently resurrecting a missing shard as
  an empty file (which then either tripped ReconcileShardLengths' "target beyond
  length" or, for an empty vlog, mounted a bogus zero-length shard reads would
  trust). Fix: a non-creating storage.OpenExistingPlog (errors.Is(err,
  fs.ErrNotExist)-detectable) plus the disk-granular pre-scan; reopenNodePlogsLocked
  also uses OpenExistingPlog so a returned node's genuinely-lost file fails the
  durability gate as its comment always intended (O_CREATE had been defeating it
  too). Covered by TestRecoverStubsSingleMissingPlogFile (disk persists active),
  TestRecoverFailsWhollyMissingDisk, and TestRecoverFailedDiskGetsReprotected
  (end-to-end: boot-failed disk reprotected onto a spare in one maintenance pass).
- (done) An individual offline-stubbed shard (single out-of-band file loss) now
  has an automatic path back to full redundancy. RepairOfflineShards (run every
  maintenance pass, after reprotect) discovers offline shards from the catalog
  (offlineShardsLocked over ListVlogPlogs x s.offlinePlogs) and regenerates each
  from surviving redundancy via the same repairVlogShardsLocked path scrub-repair
  uses -- onto a placement-allowed disk, clearing the offlinePlogs stub. It is
  catalog-driven (no full byte scrub) so it is cheap to run on the interval and a
  no-op when nothing is offline. offlinePlogClient no longer implements Scrub, so
  Vlog.Scrub skips an offline shard (an availability, not integrity, condition)
  instead of aborting the whole vlog; a deep ScrubAndRepair also folds offline
  shards into its repair set via mergeShards. Covered by
  TestRecoverStubbedShardGetsRepaired (end-to-end: a stubbed shard regenerated
  onto a spare in one maintenance pass, disk stays active).
- Still open: reverting a *failed* disk back to active across a cold restart
  relies on its original plogs still being openable; a disk failed for a genuinely
  gone directory cannot be reactivated (correctly -- the bytes are lost), only
  replaced. Node-return cancel-on-return remains the path for a transiently
  offline node whose files survived.
- (done) SweepStrayPlogFiles, run at the end of each maintenance pass, reclaims
  plog files no catalog row references on their disk: the orphan a reprotect
  leaves on a returning disk, a drained disk's stray source file, and the
  duplicate a crash leaves after a relocation's catalog flip but before os.Remove.
  Conservative (only files with no plog row for that (disk, id)) and skips
  unreachable disks (failed disk / failed node).
- Reclaim the regenerated plog left behind when a crash re-runs a reprotect step
  whose ReplaceShardPlog had not yet committed (same duplicate-bytes-on-crash
  caveat compaction has).
- (done) Reap abandoned prepared write ops so their orphan vlog bytes are actually
  reclaimable. A write op spills/seals chunks into a leased vlog as it goes
  (SetVlogLength persists the inflated length pre-Close), and on a crash before
  Close those bytes are durable-but-unpublished -- no chunk rows, dead space by
  design (meta/db.go "orphan vlog bytes are reclaimed by compaction"). But
  CompactVlog and PromoteStagingVlog skip any *leased* vlog (compaction.go ~L92,
  promotion.go ~L52), and the lease is only released on a successful Close
  (CommitWriteOpVersion) or by AbandonWriteOp -- which has no production caller.
  Recover does no per-op recovery (server.go ~L370) on purpose, so a client can
  resume by re-Opening the same operation_key. The gap: if the client never
  resumes, the op stays `prepared` forever, its vlog_lease dangles, and the vlog
  is pinned out of compaction indefinitely -- the bytes leak instead of being
  reclaimed. Needs a reaper (maintenance pass or startup sweep) that AbandonWriteOps
  prepared ops past some age with no active handle and releases their leases. The
  age threshold must not be so short it kills a client mid-reconnect (the resume
  contract). Not the vlog being truncated -- it mounts at full recorded length;
  purely a reclamation gap.
- (done) A drained disk's stray source file -- left when a crash lands after
  migratePlog's disk_id flip but before os.Remove -- is now reclaimed by
  SweepStrayPlogFiles, which sees the plog row moved to the destination disk and
  removes the copy left behind on the old disk.
