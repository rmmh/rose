# Correctness audit and implementation plan

Audit date: 2026-09-07. Code baseline: `fc6a6d134badbb94bcd9d2b86a77d49d3e13748c`.

The highest-priority problems are silent corruption during partial-sector recovery
and publication of chunk references that do not point to adequately protected,
readable data. Four executable reproductions confirm these problems. The existing
TLA+ models are useful design sketches, but they neither refine the current
implementation nor all pass their checked-in configurations.

The findings and baseline results below describe the original audit. Implementation
has since started: see [the normative contract](correctness-contract.md) and
[implementation progress](correctness-progress.md) for current status and remaining
acceptance criteria. The original reproductions now run in the ordinary suite,
split across `server/correctness_audit_test.go`, `server/publication_test.go`, and
`server/recovery_integrity_test.go`.

## Scope and evidence

Reviewed publication, deduplication, reference accounting, snapshot operations,
handle pins and leases, plog/vlog persistence and recovery, maintenance ordering,
disk/node lifecycle, filesystem adapters, and all four handwritten TLA+ modules
and their eight configurations. This is a targeted source audit with reproductions,
not a proof of every code path or a complete security audit. Generated protobuf
code and third-party implementations were not comprehensively audited.

Validation performed:

| Check | Result |
| --- | --- |
| Baseline `go test ./...`, with writable GOCACHE and `ROSE_NO_RAMDISK=1` | All packages except WebDAV passed; WebDAV initially could not bind a loopback listener in the sandbox |
| `go test ./webdav` with loopback access | Passed |
| `go test -race ./meta ./storage ./server ./durability` | Passed |
| `go test ./fuse -v -count=1` | Adapter flag test passed; mount-dependent tests skipped because FUSE mounting was unavailable |
| `go vet ./...` | Failed: protobuf messages containing mutexes copied in `server/control_plane_test.go:90,92` and `server/writecache_test.go:606` |
| Four `TestAudit…` regression specifications | All failed with the intended assertions below |

The long chaos workload is opt-in (`ROSE_CHAOS=1`) and was not run. Passing race
tests only covers the schedules exercised; it does not establish crash safety.

## Confirmed runtime bugs

Priorities: P0 = silent corruption; P1 = broken publication/protection or major
filesystem behavior; P2 = lifecycle, compatibility, or resource-exhaustion defect.

### B1 — P0: missing-trailer recovery trusts corrupted ragged bytes

Evidence: `storage/plog.go:386` (`rebuildOpenBlock`), `:1037` (`RecoverHashes`),
`server/chunk_crypto.go:78` (`readChunkPayload`). Reproduction:
`TestAuditRecoveryMustRejectCorruptRaggedPayload`.

Commit a 100-byte file, remove its plog trailer and flip one ciphertext payload
byte, then recover. Recovery succeeds and `Read` returns different plaintext
without an error. A missing trailer is a supported interrupted-append condition;
the test also introduces corruption in the retained committed prefix.

`rebuildOpenBlock` loads the partial sector and clears `bufCorrupt`.
`RecoverHashes` only handles sealed sectors and returns immediately when there
are none. Ordinary chunk reads decrypt ranges without validating the content hash.
Thus the database's authoritative length does not establish the integrity of those
bytes. The same missing validation needs examination for partial tails following
sealed sectors.

Fix: authenticate all committed bytes, including the ragged sector, before they
become readable or sources for repair. Track verification state explicitly; unknown
must not mean healthy. Validate against catalog chunk hashes or independently
anchored integrity metadata. On failure, use a verified replica or return an
integrity error. Do not turn unknown bytes into trusted bytes by writing new hashes.

### B2 — P1: deduplication bypasses bucket protection

Evidence: `server/api.go:1457` (`planChunk`), `server/pin.go:42`,
`meta/db.go:59`, `meta/queries.go:339`. Reproduction:
`TestAuditDedupMustRespectBucketProtection`.

Write bytes to a `NONE` bucket, then the identical bytes to an `EC 2+1` bucket.
The second publication references the first bucket's unprotected vlog. The dedup
lookup is global by content hash and returns before destination policy selection.
The schema allows only one location per hash, so merely adding a policy check to
the lookup is insufficient: the publication upsert can still retain the old row.

Fix: choose the dedup domain explicitly. The simplest match to the README's
bucket-scoped dedup is `(bucket identity, content hash)` plus placements suitable
for that bucket. Alternatively, separate content identity from placement identity
and require shared placements to satisfy every referencing policy. Specify policy
changes and cross-bucket rename semantics as part of this choice.

### B3 — P1: dedup-only Close acknowledges an unreadable new file

Evidence: `server/api.go:1457`, `:1548` (`sealChunks`), `:1136`
(`finishHandle`), `server/pin.go:42`. Reproduction:
`TestAuditDedupMustNotPublishUnreadableData`.

Write a file on one disk, mark that disk failed, then write identical bytes to a
new path. `Close` succeeds. A live metadata reference is enough for dedup;
readability is not checked. There are no new pending chunks or leases, so neither
the sealing gate nor the commit loop validates the reused storage.

Fix: publication must validate protection for the entire final extent list,
including unchanged base chunks and dedup hits. Bind the validated locations to
an ownership/placement generation through the metadata commit. A preflight check
alone leaves a check-to-publication race with topology changes. When strict global
degradation is selected, also enforce admission across existing published data.

### B4 — P1: publishing fresh bytes resurrects a dead placement

Evidence: `meta/queries.go:339` (`upsertChunkRefs`) and
`server/api.go:1457`. Reproduction:
`TestAuditFreshWriteMustReplaceDeadChunkPlacement`.

Write a `NONE` file, unlink it without running GC, fail its disk, and write the
same bytes to a new path using a healthy spare. Dedup correctly ignores the
zero-reference old row and writes fresh bytes. But `ON CONFLICT(hash) DO UPDATE
SET refcount = refcount + 1` discards the new address and revives the old address
on the failed disk. `Close` still succeeds.

Fix: resolve content identity and placement ownership transactionally. Replacing a
zero-reference location needs to account for open-reader pins and concurrent
publication, and must preserve readable old versions. For conflicting live rows,
validate that the retained placement is suitable before discarding a fresh one.
Return the canonical committed placements to the caller rather than assuming the
prepublication placement list won the upsert.

### B5 — P1: FUSE cached nodes retain paths after rename

Source-confirmed; a mounted reproduction was not run. Evidence:
`fuse/fs.go:59,150,191,251` (`RoseDir`, `Rename`, `RoseFile`, `Open`).

Nodes store their construction-time path. `Rename` moves the server namespace
but never updates the moved node or cached descendants. A subsequent operation
on a cached inode resolves its old path. Since server `Open` permits an absent
path, this can look like an empty file; a subsequent write can recreate the old
name. Server retargeting of already-open handles does not fix future opens on
the cached FUSE inode.

Fix: use stable namespace identities for adapter nodes, or implement coherent,
synchronized path resolution from the current inode tree. Test cached file and
directory descendants, replacement rename, recreation of the old name, open
handles, and path-based truncate after rename.

### B6 — P2: FUSE has no fsync implementation

Source-confirmed: neither `RoseFile` nor `roseHandle` implements the go-fuse
fsync interfaces. The pinned go-fuse v2.9.0 `fs/bridge.go` returns `ENOTSUP` in
this case. `Flush` publishes, but it is a distinct kernel operation and does not
supply an explicit fsync contract. Wire fsync to durable publication and test
write → fsync → crash → reopen, including error propagation and subsequent writes.

### B7 — P2: disconnected clients can pin data indefinitely

Source-confirmed lifecycle gap: `server/reaper.go:30` treats every registered
handle's operation as active, regardless of request activity. Handles are removed
by explicit close/abort paths; there is no client-session expiry attached to them.
A client that disappears without closing leaves a prepared operation exempt from
reaping. Leases prevent compaction, and reader pins can retain unlinked data too.

Fix: define renewable client/session leases, a reconnect grace period, and atomic
expiration versus request admission. Expiration must fence old handles before
releasing pins. A durable idempotency record can outlive its connection without
keeping all connection-local resources forever.

### B8 — P1: provisioning rollback deletes files before catalog ownership

Found during implementation cleanup review, after the original baseline audit.
`server/server.go`, `provisionVlogCoreLocked` deferred cleanup, removed physical
plogs before calling `DiscardEmptyVlog` and `DiscardUnassignedPlog` with the
request context. Cancellation or a catalog error could therefore leave durable
placement rows pointing at deleted files. This is a source-confirmed failure
ordering; it was not one of the four original executable reproductions.

The implementation now removes catalog ownership first using
`context.WithoutCancel`, retains physical evidence on a metadata cleanup error,
and deletes retired data and undo sidecars together. The partial-provisioning
regression passes. Injection of cancellation and ambiguous SQLite outcomes at
these exact boundaries remains required for complete crash-safety evidence.

### B9 — P1: failed append leaves a physical tail after successful commit

Found during persistence fault injection after the initial audit. A partial
batched `WriteAt` failure restores `Plog`'s in-memory cursor and buffers, but can
leave a longer physical file. Committing the old prefix, or a shorter replacement
append, previously synced that longer file without trimming it. Reopen could then
derive its geometry from the failed tail instead of the acknowledged prefix.

`TestPlogFailedAppendThenCommitDiscardsPhysicalTail` reproduces the problem for
ragged and complete-block prefixes, with and without a smaller replacement append.
Commit now truncates to the exact data/trailer geometry before syncing and retiring
the undo journal. Removing that truncation in a compiler-overlay mutation produces
the expected regression assertion. These tests establish process-level recovery;
they do not substitute for power-loss simulation of the complete publication path.

### B10 — P2: committed publication retry leaks preparation pins

Found while checking the lost-publication-reply boundary. If the catalog commits
but the server returns before releasing its operation pins, the next successful
Close takes the already-committed branch. That branch previously released handle
pins but omitted the operation's deduplication pins, preventing later reclamation
even after its namespace references disappeared.

The common successful-publication path now releases preparation pins for both
first publication and committed retries. The server regression constructs the
catalog-committed/handle-active state and checks both pin owners after Close.
Removing the release with a compiler overlay reproduces the expected pin leak.

### B11 — P1: staging retirement frees an unlinked reader's bytes

Promotion retired staging logs based only on persistent chunk reference counts.
An open reader can pin a chunk whose last namespace reference was removed; that
chunk has refcount zero but its bytes are still required. Both empty-staging
retirement and retirement after moving the remaining live chunks could delete
its catalog location and physical files. Compaction's separate pin check did not
protect these promotion paths.

The shared retirement boundary now holds the pin lock while checking chunk
ownership and retiring catalog state. Promotion defers retirement while those
pins remain and retries after they clear. The regression covers an entirely dead
staging log and one whose live chunks are promoted to EC, checking that an
unlinked reader remains readable and the source is retired after reader Close.
Removing the shared guard reproduces the premature-retirement failure.

### B12 — P1: maintenance destination assignment can overwrite ownership

`SetJobDest` previously updated any running job unconditionally and marked the
destination maintenance-owned. It accepted replacing a job's established
destination, assigning the same destination to two jobs, and assigning the source
as its own destination. Such states undermine the exclusive append ownership and
stable recovery target assumed by compaction and promotion.

Assignment now atomically requires an unset or identical destination, rejects
another job's ownership (including a completed job's output), and rejects a
self-reference or zero destination. Rejected assignments roll back both catalog
changes. Tests assert stable job rows and ownership markers, plus successful
identical retries and independent destinations. This is destination ownership
fencing, not a substitute for the outstanding disk-placement generation protocol.

### B13 — P1: abandoned file tails indefinitely defer reprotection

File writes can append beyond the published catalog prefix and then be aborted
or expired. Although cancellation releases their durable leases, relocation used
the mounted length alone to infer an active writer and deferred forever. A
failed disk could therefore keep referenced data degraded and freeze new
publication even after all affected clients had abandoned their operations.

Relocation now uses durable lease ownership for scoped file vlogs. The raw RPC
path retains its extra-tail guard because raw writes have no file-operation
lease. Remount likewise preserves a scoped tail only while a lease owns it;
otherwise it reconciles to the catalog prefix that repair actually regenerated.
The regression proves active file leases still defer repair, abort permits repair
without restart, old plaintext survives, new publication resumes, and an
uncommitted raw tail remains protected. Restoring the previous relocation guard
makes the regression fail at the deferred-repair assertion.

### B14 — P2: FUSE Create leaves the new name unpublished until close

`RoseDir.Create` registered an open write operation but deferred its initial
publication until Flush/Close. Path-based operations could not find the file
while its creating descriptor remained open. The mounted timestamp regression
exposed this as EIO from `futimes`; a mount-independent test reproduces the missing
name and dispatches a node-only timestamp update explicitly.

Create now publishes the initial file through `FlushHandle` before returning,
retaining the same handle for subsequent writes. Publication failure aborts the
preparation and returns an error. Regressions verify immediate visibility,
node-only timestamps surviving close, and failed Create leaving no visible name
or prepared operation. Required mounted FUSE tests passed with the race detector.
This does not resolve the separate cross-adapter namespace/handle identity work.

### B15 — P1: committed retries lose their result to namespace changes and GC

Committed operations stored a historical file ID without retaining its chunks.
Overwrite/unlink followed by GC or compaction could make retry validation fail.
Retry Open also built its cache from the current path head, rather than the
winning operation's version.

Explicit client keys now acquire a durable retry-result root at publication,
with a 24-hour default and a configurable positive retention period. Retry Open
loads and pins the committed version. Expiry atomically releases the root and
fences the key; Open and Close enforce expiry independently of maintenance.
Tests cover namespace removal, GC, compaction, restart, compatible/conflicting
retries, non-resurrection, and active-reader survival across expiry. Anonymous
writes retain session semantics. Bounded expired-key generations and historical
file-row reclamation remain architectural requirements.

### B16 — P2: existing retry handles bypass deadline admission on mutation

After retention was enabled, Open/Close and maintenance enforced expiry, but an
already-open retry handle could still acknowledge identical Write bytes after
the deadline if no expiry sweep had run. Handle Truncate and timestamp updates
also accepted expired requests and could mutate their local caches.

Mutation admission now checks state and the retained deadline from one SQL
snapshot. It rejects expired Write, Truncate, and timestamp updates before
changing handle state, without releasing valid read pins. Exact-deadline tests
run without a reaper; removing the deadline check makes all three operations
incorrectly succeed. Root reclamation remains a separate maintenance/admission
transaction, so rejecting a mutation does not itself invalidate an open reader.

### B17 — P2: reprotection creates idle jobs and reuses stale completion

The background driver continues scanning failed disks after their shards have
been reprotected. Each empty pass previously created and completed another job,
so terminal metadata grew even with no new workload. Separately, StartReprotect
returned the most recent completed job without checking whether the disk had
returned, acquired new shards, and failed again.

An empty pass now completes an existing running job if necessary and otherwise
returns without creating a row. The RPC reuses a completed result only when the
source has no mapped shards; a new failure with shards starts new work. Tests
cover repeated idle passes, interrupted finalization, two failure cycles, stable
retries of the current completion, and readable data across both cycles. The
previous implementation fails both new regressions. This avoids idle metadata
growth but does not replace terminal-job retention or placement generations.

### B18 — P2: replacement retries can acknowledge a different destination

The RPC checked the destination before calling the catalog, but
`GetOrCreateReplaceJob` returned an existing running job without comparing its
destination with the request. Concurrent RPCs could both pass their preliminary
checks and both succeed despite naming different replacement disks. The direct
replacement path also silently preferred the existing job's destination.

The catalog transaction now rejects destination mismatches, so preliminary RPC
checks are not the ownership authority. Identical retries retain the original
job. Zero and self-replacement arguments are rejected before inserting a job.
A concurrent regression requires exactly one destination to win, checks stable
identical retries, and verifies rejected requests add no catalog rows. The
previous implementation acknowledges both conflicting requests. Placement epochs
and delayed storage-completion fencing remain separate requirements.

### B19 — P2: vlog job lookup and creation permit duplicate ownership

The catalog helpers for compaction, promotion, and scrub repair performed their
lookup and insert as separate statements. The single-connection pool serializes
statements, but concurrent callers can both observe no job and then insert
different running owners. Production maintenance usually holds the broader vlog
lock; the catalog boundary did not independently provide its get-or-create
guarantee.

The three helpers now share a transaction covering lookup and creation. A partial
unique index enforces one running job per kind/source vlog while permitting
completed history and later passes. The checker independently reports duplicate
owners in catalogs without that constraint. Concurrent, direct-insertion, and
checker regressions pass; the previous implementation both accepted duplicate
rows and returned distinct job IDs to concurrent callers. Cross-kind maintenance
interactions and placement-generation fencing remain separate obligations.

### B20 — P1: a rewrite can retire another running job's destination

After destination assignment, a failed or interrupted maintenance pass leaves a
running job holding that vlog ID. Compaction could treat the destination as an
ordinary source, relocate its chunks, and retire it. Promotion could likewise
retire an empty staging destination. The original job then repeatedly fails to
resume because its persisted destination no longer exists. The broad vlog lock
serializes individual passes but does not preserve ownership between passes.

Compaction and promotion now defer when their proposed source is a running job's
destination. Catalog retirement independently rejects deletion of such a vlog in
the deletion transaction. Terminal jobs release this hold; shard repair remains
allowed because restoring the destination in place can be necessary for the
owning job to complete. Regressions reproduce deletion in both rewrite paths on
the previous code, preserve catalog and physical destination files on repeated
passes, reject direct catalog deletion, resume the original job, and reclaim its
output after completion. This does not establish placement-generation fencing or
exclude every interaction between jobs sharing a source.

### B21 — P2: retired sources leave obsolete scrub-repair jobs running

A rewrite can drain and retire a vlog while an interrupted in-place repair job
still targets it. Repair recovery requires a mounted vlog, so that job would fail
on every restart and remain running indefinitely even though its source no
longer needs repair. Source retirement now cancels running scrub-repair jobs in
the deletion transaction. Cancellation distinguishes superseded work from an
actual completed repair. A failure to persist cancellation rolls back retirement;
repeated retirement preserves the terminal result. Metadata fault injection and
server compaction regressions cover both the atomic boundary and recovery's job
list. This does not reclaim historical job rows or resolve unrepairable live data.

## Verification defects found while implementing CI

- The FUSE helper passed macFUSE-only options to Linux and skipped all mount or
  handshake errors. Options are now platform-specific, and required-mount mode
  fails instead of skipping. Cleanup is registered before the handshake check.
  Successful mounted execution still requires a capable host.
- Chaos candidate selection opened live plogs writable. Following introduction
  of undo recovery, this could replay a live writer's pending journal and
  truncate its file behind the mounted handle. Selection now uses read-only
  `InspectPlog`; storage regressions assert that inspection cannot replay undo.
- The initially enabled 30-second race-enabled chaos run with seed 1 failed.
  After the inspection fix, that observed run verified 19 committed
  files with zero read mismatches, but reported 98 unexpected degraded-write
  errors and one bitrot injection with no observed repair. `ReprotectDisk` can return nil
  after deferring leased/busy vlogs, while the injector treats return as complete
  and clears its active-fault flag. Failed Close attempts also retain retryable
  writers. Following B13, the harness now explicitly aborts abandoned writes,
  waits for source-shard removal and durable job completion, completes admitted
  recovery within its own bounded context, and drains overlapping workload
  requests before clearing the fault flag. Seed 1 then passed with 11 faults,
  59 committed files verified, and zero read mismatches or operation errors.
  The strict publication gate and healthy-state error assertions remain intact.
  This is bounded sampled evidence, not exhaustive concurrency verification;
  network abort/expiry histories and deterministic replay remain open.
- After correcting platform-specific mount options, a full suite executed real
  FUSE mounts under escalated execution and exposed B14. The corrected required
  mount run passes; earlier sandbox skips did not validate this behavior.

## Architectural mismatches and follow-up risks

1. **There is no single executable commit protocol shared with simulation.**
   `durability.Coordinator` is only referenced by its own tests. Production uses
   `finishHandle` and the storage methods directly. The claim in
   `docs/deterministic-simulation-design.md` that the coordinator is shared is
   currently false. Testing a separate ideal coordinator cannot catch B1–B4.

2. **Strict durability has conflicting definitions.**
   `RoseTxnCommit` and `transaction-commit-design.md` require all configured shards
   and global read-only degradation. Runtime DUPLICATE commits require
   `min(minCopies, provisioned copies)`, normally two, and admission is local to
   newly written vlogs. `RoseStorage` models another threshold-based policy.
   Persist desired protection, achieved protection, and failure domains; make the
   default and the semantics of changing it explicit. A placement being reachable
   is also different from its bytes being verified and durable.

3. **Snapshot architecture is proposed, not implemented.**
   Publication creates no automatic snapshot. `CreateSnapshot` copies every live
   file-head row and adjusts every referenced chunk occurrence in one transaction.
   There are no persistent COW namespace/extent trees or retention scheduler.
   Empty directories are not captured in `snapshot_file`. Distinguish explicit
   file snapshots from a complete namespace snapshot contract before implementing
   automatic snapshots: doing the current full copy on every publication would
   amplify metadata work severely.

4. **Version and retry metadata grow without a reclamation policy.**
   Old `file` rows survive overwrite/unlink, terminal `write_op` rows survive, and
   the `writeOps` mutex map has no eviction. Chunk GC does not reclaim these.
   Committed retry records refer to historical file IDs without themselves
   retaining the corresponding chunks. Decide how long byte-for-byte retry
   validation remains supported after overwrite/unlink and GC; either retain the
   necessary data or store an adequate immutable request/result digest and an
   explicit expiry contract.

5. **Recovery performs maintenance before the final hash-recovery pass.**
   `server/server.go:482` resumes durable jobs before calling `RecoverHashes`.
   `readPlainChunkRecord` checks header geometry but does not recompute the full
   payload hash. Investigate whether repair/compaction can copy unauthenticated
   fallback bytes and give them fresh valid checksums. B1 is already reproduced;
   this amplification schedule remains to be tested. Verification should precede
   admitting a source for maintenance.

6. **Persisting length is separate from freezing the committed prefix.**
   `CommitVlog` commits vlogs, then samples their lengths in a later loop.
   `WriteVlog` releases `vlogMu` after resolving a vlog; an already-admitted write
   can still run while `CommitVlog` holds that lock. Investigate the schedule in
   which an append runs after that vlog's sync but before its sampled length is
   persisted. `finishHandle` also samples `v.Length()` separately from `Commit`.
   This is a source-derived race risk, not a reproduced finding. Return an exact
   durable high-water mark from commit and publish only that value under a
   generation/ownership check.

7. **The storage layer is not physically append-only at the durability boundary.**
   A later append/commit rewrites a sector containing an already-committed ragged
   prefix and overwrites integrity trailers. The model only appends independent
   records. Model torn overwrites, short writes, ENOSPC and fsync failure, including
   corruption of previously acknowledged prefixes. Choose an explicit atomic-write
   assumption or preserve old durable sectors with copy-on-write/journaling.

8. **Concurrency ownership is spread across many mechanisms.**
   `namespaceMu`, `vlogMu`, `pinMu`, handle/operation locks, `maintRunMu`, durable
   leases, and `activeVlogOps` collectively establish safety. Namespace publication
   holds a global lock across chunk finalization and disk sync; maintenance holds
   topology locks across bulk I/O. Document lock order and ownership first, then
   move expensive work outside broad locks using generation-checked transitions.
   Exposed raw plog/vlog RPCs must obey the same ownership contract as file writes.

9. **Distribution and encryption claims need precise boundaries.**
   Nodes currently represent fault domains in one server owning local disk roots,
   with one SQLite authority, not independently failing metadata replicas.
   Encryption now exists despite stale TODOs. CTR stream identity uses only 64
   hash bits, and repeated content in one vlog can encrypt differing per-record
   headers with the same stream. Review nonce uniqueness, record authentication,
   key exposure/custody, and integrity anchoring separately before promising
   adversarial tamper resistance. This audit does not establish cryptographic
   security.

## Existing TLA+ results and gaps

Ran copies of the eight configurations with the checked-in `tla2tools.jar`, Java
26, two workers, a 1 GiB heap, and a 45-second limit per configuration. The
`-deadlock` flag matches `tla/Makefile` and disables deadlock checking.

| Configuration | Result | Distinct states on completed successful search |
| --- | --- | ---: |
| RoseMetadata | Passed | 185,848 |
| RoseSnapshotGC | Passed | 2,180 |
| RoseSnapshotGCRetention | Passed | 60,687 |
| RoseTxnCommit | Passed | 398,188 |
| RoseStorageMultiNode | **NoNodeColocation violated**, depth 15 | — |
| RoseStorageDisk | Timed out; not verified | — |
| RoseStorageEC | Timed out; not verified | — |
| RoseStorageReplica | Timed out; not verified | — |

### M1: existing multi-node counterexample

The failing configuration puts `d1` and `d2` on `n1`. After opening/buffering `o1`,
creating its DUPLICATE vlog and `d1` plog, the critical actions are:

```text
WriteVlog(o1)
WritePlog(d1, o1, 0)
CommitPlog(d1, o1, 0)
RetryPlog(d1, o1, 0)
SendPlogAck(d1, o1, 0)
ReceivePlogAck(d1, o1, 0)   // leaves an old request in pending[d1]
StartRebalance(j1)
RebalanceStep(j1, d1, d3, o1, 0)
RebalanceStep(j1, d3, d2, o1, 0)
CommitPlog(d1, o1, 0)      // delayed request recreates the copy on n1
```

Now both `d1` and `d2` store the object. `PlacementAllowed` considers pending
object/shard pairs but misses request messages left after acknowledgement;
`CommitPlog` can consume the surviving request after relocation.
This is a confirmed **model** bug/counterexample, not evidence that the runtime's
lease and active-I/O guards have the identical defect. Preserve the trace as a
regression, model outstanding operations and placement epochs, and map the
runtime guards explicitly. Do not simply remove the invariant or forbid retries.

Replay from `tla/` (TLC writes state/trace files in the working directory):

```sh
java -Xmx1g -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock \
  -config RoseStorageMultiNode.cfg RoseStorage.tla
```

### Why the passing models do not establish implementation correctness

| Model | Missing or misleading property | Required change |
| --- | --- | --- |
| RoseMetadata | Chunks are sets, not ordered extents; counts collapse repeated occurrences and multiple files in a snapshot. Live handles follow the live head rather than pinning the opened version. `Read` can simply become disabled after GC. `SnapshotsImmutable` only checks that chunks belong to the chunk universe. | Ordered extents with multiplicity; explicit opened versions and pins; assert open-reader readability; capture immutable snapshot contents in history variables and check equality. |
| RoseSnapshotGC | Models a future two-leaf COW tree. Refcounts are recalculated with the same helper checked by the invariant. Pins have no owners. Retention config uses `NoPins`; MaxTime=4 and snapshot time>=1 mean age never exceeds DailyWindow=3 or WeeklyWindow=6. | First model actual row/refcount updates, then separately refine a COW design. Owner-counted pins, independent reachability oracle, and enough logical time/snapshot slots for daily-only, weekly-only, expiry, and ID reuse scenarios. |
| RoseTxnCommit | `StrictModeIsReadOnlyWhenDegraded` follows directly from the definition of WritesAllowed. Publish does not require WritesAllowed, so it cannot enforce that all existing transactions stop publishing during global degradation. Automatic snapshots are modeled but absent in code. Repair can put distinct shards of one transaction on one disk. | Check actual admission/publication transitions with historical protection state; add placement uniqueness to repair and invariants; align snapshots and retry semantics with the selected runtime contract. |
| RoseStorage | One object/job in each checked configuration; mixed RPC markers and data reservations; no byte offsets, torn sectors, dedup, leases, snapshot pins, or staged-EC promotion. | Separate messages, reservations, durable bytes and published placements. Add epochs and stale-completion fencing; check two writers and interacting jobs in reduced configurations. |
| All | No refinement mapping to production; no meaningful liveness/fairness properties. Crash actions do not capture the full runtime restart protocol. | Map each action to code and persistent tables, check crash transitions at actual boundaries, and add conditional progress properties under stated fairness/capacity assumptions. |

## Implementation sequence and acceptance criteria

### Phase 1: establish the contract and fix reproduced safety defects

1. Write a short normative contract for `Write`, `Flush`, `Close`, fsync, retry,
   overwrite conflicts, snapshot visibility, dedup domains, strict/degraded writes,
   disk versus node loss, and client lease expiry. Distinguish volatile Write's
   acknowledged offset from durable publication. Retain single-master SQLite as
   the explicit authority for this work.
2. Fix B1, including partial tails with and without sealed sectors and with healthy
   redundant copies. Move verification ahead of recovery-time maintenance.
3. Fix B2–B4 together at the content/placement/publication boundary. Define a
   prepared publication containing final extents, required protection, canonical
   locations/generations, and exact durable high-water marks. Ensure dedup and
   fresh-data conflicts obey the same validation and reference rules.
4. Promote each opt-in reproduction into the ordinary suite when its fix lands;
   add one-disk-loss and GC/compaction variants. Add deterministic scheduling hooks
   around validation and metadata publication to test topology changes there.

Exit: all four reproductions pass as normal tests; every successful publication
references verified bytes with the selected protection; corruption is either
recovered from verified redundancy or reported, never silently returned.

### Phase 2: make the runtime state machine testable

1. Extract the **actual** publication workflow behind small disk, catalog, clock,
   and scheduling interfaces. Either use `durability.Coordinator` for real work
   after extending it, or replace it; do not maintain two competing protocols.
2. Separate append cursor, synced prefix, published prefix, and placement epoch.
   Persist the latter states where recovery needs them. Specify the crash outcome
   of every metadata mutation and filesystem create/sync/repoint/delete boundary.
3. Define ownership for file leases, raw storage RPCs, readers, maintenance, and
   late asynchronous completions. Add generation checks for returning shards and
   delayed writes. Document lock order and identify where ownership replaces a
   long-held lock.
4. Implement B5–B7, retry/version metadata retirement, and the explicit snapshot
   contract. Implement COW trees only if automatic scalable snapshots remain a
   requirement; they are not a prerequisite to repairing publication safety.

Exit: production and deterministic tests execute the same transition code; every
resource has an owner and release/expiry rule; crash recovery is idempotent at
each boundary; adapter fsync and cached rename tests run without relying solely
on mount-capable environments.

### Phase 3: build a small compositional model with runtime refinement

Prefer three interacting bounded layers over one enormous storage model:

| Layer | State and actions | Core invariants |
| --- | --- | --- |
| Publication and namespace | Two keys/writers, two paths/buckets, ordered extents, open versions, replay, flush/close, unlink/rename, snapshots, reference deltas, owner pins, lease expiry | Atomic visibility; retry stability; exact reference multiplicity; open-reader and snapshot immutability; no reachable/pinned reclamation; placement meets every reference's policy |
| Persistence and recovery | Reserved offsets, volatile/synced/published prefixes, sector verification state, partial writes, torn overwrite, fsync, catalog commit, restart | Published length never exceeds durable verified bytes; append preserves the committed prefix; unknown/corrupt data never becomes a trusted repair source |
| Placement and maintenance | Disk/node state, epochs, pending requests, replica completeness, staged EC, copy/sync/repoint/retire jobs, reader I/O holds | Distinct failure domains; no late completion revives old placement; durable copy before repoint; no retirement with references/holds; reads survive the configured loss budget |

Use abstract payload identities and a few sector positions, not real megabytes.
Include two pin owners of the same chunk and repeated chunk occurrences. Model
both a process crash and destructive disk loss; distinguish outage/return from
replacement with a new identity. State whether the fault budget is cumulative or
rolling after repair, and test repair followed by another permitted failure.

Maintain a refinement table mapping every abstract action to concrete functions,
SQL transactions, lock/lease preconditions, and fault injection points. Express
reference correctness using an independent graph traversal, not the update helper.
Use history variables for published results and snapshot contents. Add conditional
liveness: recoverable jobs eventually finish with sufficient healthy capacity;
abandoned intents eventually release resources; healthy acknowledged requests
eventually return under fair scheduling. Never require progress during an
unbounded outage.

Exit: M1 is explained and fixed with a regression; all bounded configurations
finish; invariants fail under deliberate mutations such as removing a pin, skipping
fsync, reviving a stale placement, and dropping a reference decrement. Model
counterexamples replay against the runtime where the refinement says they apply.

### Phase 4: continuous verification and operational invariants

Build a deterministic explorer over the shared production transitions with
cloneable state, canonical hashing, and minimized replay traces. Prioritize
two-writer histories mixing repeated content, policies, unlink/rename, snapshots,
GC, promotion, repair, and client disconnects. Only then add partial-order and
symmetry reductions, checking that reductions preserve the relevant observations.

Add subprocess kill/restart tests over real SQLite/files at each persistence
boundary. Model short writes, torn sectors, ENOSPC, failed sync, stale returned
shards and delayed RPC completion separately; a clean CloseStorage/reopen cycle
does not emulate a power failure. Keep a byte-array namespace/snapshot oracle and
track the distinction between successful, failed-before-publish, and ambiguous
post-publish client outcomes.

CI should run ordinary tests, race tests, vet, fast complete TLC configurations,
and deterministic regression traces. Run larger complete models, chaos histories,
and mount integration tests on scheduled capable workers. Archive tool versions,
configuration, seed, state counts, elapsed time, and counterexamples. Timeouts and
skips must remain visible and must not be labeled verification success.

Provide a read-only consistency checker for exact chunk references, extent bounds,
canonical placements, protection, lease/job ownership, and catalog/disk agreement.
Keep this separate from repair so the checker can report evidence without changing
the state it is diagnosing. Correct the stale README/TODO/design claims as the
contract and implementation converge.
