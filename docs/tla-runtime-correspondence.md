# TLA+ and runtime correspondence

This is a correspondence and gap table, not a refinement proof. The implementation
must eventually execute the same transitions as the deterministic explorer, and
those transitions need an explicit abstraction into the models. Neither a passing
TLC configuration nor the table below establishes that refinement today.

## Transaction publication model

`RoseTxnCommit` models two immutable transaction identities, independently durable
shard records, atomic metadata visibility, explicit snapshot capture, permanent disk
failure, repair, and orphan reclamation. It does not model ordered file extents,
byte offsets, encryption, deduplication domains, namespace replacement, owner pins,
operation keys, terminal-result expiry, or placement generations.

| Model action or predicate | Runtime correspondence | Remaining gap |
| --- | --- | --- |
| `StartTxn` | `Server.Open`, `DB.CreateWriteOp`, lazy `ensureWriteOperation` | The model gates admission globally; runtime may admit volatile staging during degradation and gates publication. Align the final admission contract and shared transition code. |
| `PrepareShard` | `Vlog.writeWithin`, positioned physical appends | The model adds one record, while runtime splits chunks across sectors/EC rows and has asynchronous completion, leases, and offsets. |
| `FsyncShard` | `Plog.Commit`, `Vlog.CommitPrefix` | The runtime must protect an old acknowledged prefix against torn overwrite. A set of durable records does not capture this failure mode. |
| `Publish` | `durability.Coordinator` via `preparedPublication`, `DB.CommitWriteOpVersion` | The production stepper orders exact prefix sync/recording, canonical extent verification, and atomic publication. Runtime checks scoped content and all required shards. The model omits these representations and SQLite failure/ambiguous-reply boundaries. |
| `WritesAllowed` / `NoDegradedPublication` | `publishedTopologyReadyLocked`, `requiredPlacementReadyLocked` | The model sees all record loss. Runtime knows topology loss but still needs durable integrity-health evidence and a complete strict admission policy. The monitor records the pre-publication degraded state independently of the guard. |
| `FailDisk` | `SetDiskState`, offline plog clients | Model loss is permanent and bounded. Runtime also supports node failure and returned stale disks; generation fencing remains necessary. |
| `RepairShard` | reprotection, scrub repair, metadata repoint | Model repair is atomic, requires a readable source, excludes disks already holding a shard, and replaces the failed shard's canonical mapping. Runtime copy/sync/repoint/retire boundaries require independent crash and delayed-completion checks. |
| `CaptureSnapshot` | `CreateSnapshot` | Snapshot creation is explicit. This small model records captured transaction membership only; full directory/file metadata, snapshot identity, reference multiplicity, retention, and open readers belong in the namespace/retention model. |
| `Crash`, `RecoverOrAbandon`, `ReclaimOrphan` | recovery, handle reaper, GC/compaction | Runtime has durable prepared intents, restart grace, leases, pins, and resumable jobs absent from the model. |

`NoColocatedShards` examines actual volatile/durable records rather than the
placement constructor. `CanonicalShardPlacement` checks that repair does not leave
two metadata mappings for one shard. `NoDegradedPublication` uses a monotonic
history monitor: removing the publication guard must make it false on a reachable
schedule. The older `StrictModeIsReadOnlyWhenDegraded` predicate is only a check on
the definition of admission; it is not evidence that publication obeys admission.

## Configuration and mutation evidence

The original transaction configuration has three disks and requires three shards.
After a disk fails it cannot construct another fully protected transaction, which
can conceal a missing global gate. `RoseTxnCommitGlobal.cfg` has four disks and two
required shards, so it can prepare a healthy transaction while an earlier published
transaction is degraded. It also permits repair onto a spare disk.

Run positive checks with `make -C tla test-txn`. Run positive checks followed by
mutation checks with `make -C tla mutations`. The mutation runner creates separate
model/config copies, preserves TLC logs and counterexamples, and emits JSON. A
syntax error, timeout, or violation of an unexpected invariant is a failed mutation
check, not a detected regression. The runner itself only tests mutation sensitivity;
it does not replace the positive checks.

Verified on 2026-09-07 using the repository TLC jar (revision `932a781`):

| Configuration or mutation | Result |
| --- | --- |
| `RoseTxnCommit.cfg` | 2,575,064 generated / 396,292 distinct states; depth 19; complete, no invariant error |
| `RoseTxnCommitGlobal.cfg` | 816,305 generated / 147,520 distinct states; depth 15; complete, no invariant error |
| Remove global publication guard | `NoDegradedPublication` violated |
| Allow repair onto an occupied disk | `NoColocatedShards` violated |

These runs check safety in finite configurations. They do not establish conditional
liveness, arbitrary cluster sizes, or implementation correctness. The full plan
still requires independent namespace/refcount oracles, realistic retention horizons,
placement epochs, runtime trace replay, and compositional refinement.

## Snapshot/GC model scope and retention coverage

`RoseSnapshotGC` remains a bounded COW design model, not a representation of the
current SQLite row-based namespace. Its publication action still combines a tree
update and snapshot creation. The runtime uses explicit snapshot creation. A
compositional model of actual ordered row extents, immutable snapshot history,
metadata transitions, and runtime refinement is still required.

The model now applies incremental reference changes on publication, expiration,
and node GC. `NodeRefsCorrect` and `ChunkRefsCorrect` independently recount the
resulting graph; the recount helpers are used only for initialization and checking,
not to maintain the state under test. Pins are sets owned by named owners, and GC
consults their union. `PinnedChunksReadable` checks their survival separately from
namespace reachability.

The retention configuration advances to time 10, past its weekly window of 6.
Expired bounded snapshot slots can be reused once, with a generation counter.
This abstracts allocation of a new snapshot identity; it does not authorize the
runtime to reuse an externally visible SQLite snapshot ID. Public IDs remain
monotonic even when a deleted snapshot name is reused.

`make -C tla snapshot-coverage` checks negated coverage predicates and requires an
expected counterexample for each. Witnesses now cover the daily-only age tier,
weekly-only age tier, an active candidate past the weekly window, expiration, and
slot reuse. Those witnesses establish reachability only. Run the positive safety
configurations with `make -C tla test-snapshots`; `snapshot-mutations` runs those
checks before testing sensitivity to missing root references and unsafe pinned
chunk collection. Logs and JSON reports distinguish unexpected failures and
timeouts from expected witnesses or mutation detections.

Verified owner-pin configuration: 87,025 generated / 10,400 distinct states,
depth 14, complete with no invariant error. Both refcount/pin mutations were
caught by the expected invariants. The final time-10 retention configuration also
completed with no invariant error: 55,454,827 generated / 8,211,719 distinct states,
depth 22, 18m43s. Conditional liveness and full snapshot immutability history remain
unverified; neither finite run establishes runtime refinement.

## Prefix persistence and recovery model

`RosePrefixRecovery` models one plog's journal ordering with two successive
nonempty acknowledged prefixes. Its `published` variable is the prefix whose
plog Commit has returned successfully; SQLite namespace publication follows that
boundary in the server and is modeled separately. Versions abstract correct,
successively longer byte prefixes. `intact=FALSE` represents a torn overwrite
that can damage previously acknowledged bytes.

| Model action/state | Runtime correspondence | Abstraction or remaining gap |
| --- | --- | --- |
| `Prepare`, `SyncJournal`, `RenameJournal` | `saveUndo` streams the baseline, syncs the temporary journal, and renames it | Verified journal contents are abstracted as one prefix version. Partial journal writes and checksum failures are covered by runtime tests, not represented as readable model states. |
| `InstallJournal`, `FailInstallSync` | `saveUndo` directory sync, including reuse after an error | Presence of a journal does not imply durable installation. The retry mutation removes this distinction and fails. |
| `WriteData`, `TornWrite` | `Plog.writeLocked` and Commit's in-place ragged/trailer writes | One record abstracts data and integrity metadata. Sector geometry, truncation, range extension, and file identity are not modeled here. |
| `SyncData`, `RemoveJournal`, `RetireJournal`, `RetryCommit` | `commitUndo` file sync, unlink, directory sync, and retry | No storage acknowledgement may precede durable journal retirement. |
| `Publish` | Successful plog Commit return | This is not the complete SQLite file-publication transaction. |
| `FlushData`, `FlushDirectory` | Writes reaching media before an explicit sync | Data and directory persistence are independent; explicit sync establishes ordering rather than being the only possible persistence event. |
| `Crash`, `ProcessCrash` | Power-loss abstraction; abrupt process exit | Power loss restores only the modeled durable state. A process exit preserves current kernel state. Actual filesystem power-loss testing remains required. |
| `Replay`, `ReplaySync`, `ReplayRemove`, `ReplayRetire` | `restoreUndo`, including durable retirement on writable reopen | Replay retains its durable baseline until restored data is synced. Journal identity changes and corrupt evidence remain outside this model. |

`PublishedRecoverable` requires a durable correct file prefix or a durable journal
capable of restoring it. `ReadyPrefixValid` checks the prefix exposed in Idle.
`RollbackNotOlderThanPublished` prevents a surviving journal from rolling back
an acknowledged prefix, even when the replacement file itself is already durable.

`make -C tla prefix-mutations` runs the positive configuration and four sensitivity
checks: missing journal directory sync, missing data sync, missing retirement
directory sync, and a retry that bypasses journal installation sync. All produce
their expected invariant failures. The positive run completed with 934 generated /
183 distinct states, depth 34. No fairness/liveness, replica loss, physical sector
refinement, or composition with namespace publication is established by this run.

### Conditional prefix progress

`RosePrefixRecoveryLive` extends the same safety actions rather than implementing
another protocol. It adds a cumulative budget of two faults across power loss,
process exit, torn writes, and directory-sync failures. Once that budget is
exhausted, successful protocol steps are weakly fair; early data/directory flushes
remain unrestricted. The two modeled prefix requests remain offered until
acknowledged. These are assumptions about eventual service and caller demand,
not guarantees that hardware failures stop or clients keep submitting work.

Under those conditions, TLC checks that every non-Idle phase eventually reaches
Idle (`RecoveryCompletes`) and the two offered prefixes are eventually
acknowledged (`PrefixesEventuallyAcknowledged`). The run completed with 1,255
generated / 315 distinct states, depth 34. The unrestricted base safety model is
unchanged and still allows arbitrarily many crashes and retry failures.

`make -C tla prefix-liveness` checks both positive configurations and three
sensitivity cases. Blocking replay retirement violates recovery completion;
removing fair completion or allowing unlimited faults violates eventual
acknowledgement. Each negative case selects only its expected temporal property
and rejects safety violations, parser errors, and timeouts as successful evidence.
This proves conditional progress only for the bounded plog abstraction. Runtime
scheduling refinement, maintenance/lease-expiry liveness, and composition with the
catalog and placement models remain open.

## Retry-result retention

`RoseRetryRetention` models the newly implemented retry-root layer separately
from physical durability and namespace-tree design. It has two immutable ordered
versions (one repeats a chunk), one path head, one snapshot, two reader owners,
and three clock ticks. Publication and expiry update reference counts
incrementally; `ExactReferences` independently recounts each root's extent
occurrences. The requested result identity is recorded separately from the
version actually pinned by Open, exposing a retry that incorrectly opens the
current head.

| Model action/state | Runtime boundary |
| --- | --- |
| `Publish`, result root, head and references | `CommitWriteOpVersionWithRetention` transaction; physical bytes are assumed verified and durable before this action |
| `Unlink`, `Snapshot`, `DropSnapshot` | Namespace/snapshot root changes and exact chunk-occurrence reference transfer |
| `Expire` | `ExpireWriteResults`: decrement references, remove the result root, retain an expired key tombstone atomically |
| `OpenRetry`, `RetryMutation` | Deadline admission plus winning-version pinning; state/deadline are read together for mutations |
| `pins`, `Close`, `Crash` | Handle-owned chunk pins, release/expiry, and loss of volatile ownership on process exit |
| `GC` | Reclaim only zero-reference chunks with no reader/preparation owner |

Safety has no fairness assumptions. It checks exact references, root/result
agreement, readable namespace/snapshot/result roots and pinned versions, correct
retry result identity, no expired admission, and no reuse of an expired key.
The separate `LiveSpec` adds weak fairness for clock progress and each expiry
action. Under these assumptions every retained result root eventually expires.
Clients need not release handles, so eventual physical reclamation is not claimed.
`Close` models ownership release only; keyed result/mutation admission is the
separate `RetryMutation` action.

`make -C tla retry-mutations` runs both positive configurations and sensitivity
checks: omit a result root, lose a repeated expiry reference, ignore reader pins,
omit either deadline guard, reuse an expired key, open the current head on retry,
or remove either clock/expiry fairness. Parser errors and timeouts do not count
as detected violations.

This is a bounded abstraction, not a runtime refinement proof. It assumes atomic
SQLite transactions and prior physical durability; it omits append preparation,
partial-byte retries, range geometry, multiple paths, mutable namespace identity,
placement/repair, wall-clock rollback, and the unfinished bounded-key generation
protocol. Existing prefix and placement models remain separate obligations.

## Maintenance destination and I/O ownership

`RoseMaintenance` is a bounded common rewrite layer: compaction and promotion
jobs, three never-reused vlog identities, one published content identity, and two
independent in-flight reader holds. It complements prefix durability and retry
roots rather than claiming to model their internal transitions.

| Action / state | Runtime correspondence | Abstraction boundary |
| --- | --- | --- |
| `Begin` / `dest` | Destination provisioning, `SetJobDest`, rewrite admission's `VlogIsRunningDestination` | Assignment is atomic here; runtime process-crash and ambiguous-assignment tests cover its multiple physical/catalog boundaries. |
| `Copy` / `durable` | Compaction copy or promotion row encoding, sync, and protection verification | One successful durable copy; partial/torn writes and disk loss are delegated to lower-layer models/tests. |
| `Repoint` / `location` | Canonical live-chunk enumeration and `RelocateChunk` | One content identity replaces ordered extent batches. A source with no remaining live content does not repoint stale work. |
| `Finish` | `MarkJobDone` / finish-on-recovery helpers | Job 1 completes after source retirement, as compaction does; job 2 may complete first, as promotion does. |
| `Retire` | `retireVlogLocked`, catalog `RetireVlog`, physical cleanup | No published content, I/O reader, or running destination owner may remain; cleanup is atomic here. |
| `ReadBegin` / `ReadEnd` | `resolveVlog` / `activeVlogOps` / `endVlogOp` | Short physical I/O holds, not indefinite open-handle pins; namespace/snapshot roots are represented by the single canonical content location. |
| `Crash` | Process loss of volatile I/O owners with durable job/catalog state retained | No torn persistence or destructive disk loss is introduced at this layer. |

Safety checks published/reader readability, destination preservation, distinct
job outputs, and absence of self-rewrites. The model allows two rewrite kinds to
share a source and permits a later rewrite of a completed job's destination.

Conditional liveness requires weak fairness for copy, repoint, job completion,
reader release, and retirement. Under those assumptions, started jobs finish and
unused vlogs eventually retire. No fairness is imposed on starting a job or on
allocating unavailable capacity. The finite identities are never reused; this is
not the unfinished placement-generation/stale-return protocol. Unbounded reader
holds or unfair maintenance scheduling intentionally defeat reclamation liveness.

`make -C tla maintenance-mutations` checks both positive configurations and seven
sensitivity cases: omit destination or reader retirement guards, publish without
both durable copy and its validation, repoint stale source work, or remove finish,
reader-release, or retirement fairness. Each liveness mutation selects its named
property; parser errors, safety failures, and timeouts do not count as the expected
temporal counterexample. Four safety and three temporal counterexamples are
required. This is a refinement map with explicit omissions, not a proof that all
runtime maintenance paths implement the abstraction.

## Repair generations and completion

`RoseRepairEpoch` models one serialized repair with at most two fresh destination
identities and two lifecycle events. Generations do not wrap; each new attempt
allocates a different plog. It checks the catalog completion boundary, not a
byte-level refinement or a license to release `vlogMu` during physical I/O.

| Action / state | Runtime correspondence | Abstraction boundary |
| --- | --- | --- |
| `Begin` / `capturedSource` | `regenerateShardLocked` reads `GetVlog.PlacementEpoch`, then allocates its destination | Reconstruction and allocation are abstracted; raw/source ownership and actual bytes remain runtime prerequisites. |
| `Capture` / `capturedDest` | `RepairDestinationEpoch` checks an unassigned plog on an active disk and working node before writes | A single destination with exclusive ownership; general placement geometry is checked separately by runtime tests. |
| `SourceChange` / `DestinationChange` | `installPlacementEpochTriggers` advances epochs on source/destination identity, ownership, or lifecycle changes | Two events may represent outage and return, or identity changes with unchanged availability. Ghost freshness flags independently record invalidation; they are not runtime fields. |
| `Write` / `durable` | Destination `Write`, `Commit`, and verified source reconstruction | Successful durable bytes are atomic here; torn persistence, corruption, and ENOSPC are outside this model. |
| `Commit` | `ReplaceShardPlog` compares captured source/destination epochs and rechecks availability in the repoint transaction | `acceptedSafely` records completion-time validity independently of the guards. Later failures must not retroactively invalidate a historically valid completion. |
| `Interrupt` | Cancellation/error before publication or an ambiguous response after it | A live invocation reaches cleanup. Process exit and recovery use durable `repair_owned` intent and are checked separately by B26's subprocess regressions. |
| `Cleanup` | `DiscardUnassignedPlog` protects a committed mapping before removing unpublished destination files | SQL and physical cleanup are combined here. No guarantee of cleanup after a crash or failed deletion is inferred. |

Safety forbids stale or unsynced publication, removal of a published destination,
loss of the old source on rejection, and destination leaks at idle/terminal
boundaries. Weak fairness for capture, write, commit, and cleanup makes each
started attempt terminate. This does not promise successful repair under outage,
unlimited retries, concurrent repairs, remote completion, or eventual cleanup
after a process crash. Separate runtime tests cover epoch overflow and reopen.

## Repair process recovery and physical reclamation

`RoseRepairRecovery` complements `RoseRepairEpoch` with a source plog, a legitimate
unassigned raw plog, and a fresh repair destination. The catalog set and physical
file set are distinct. At most two process crashes and two transient outages are
allowed; identities are never reused. This checks another bounded protocol layer,
not a mechanically proved composition of the repair models.

| Action / state | Runtime correspondence | Abstraction boundary |
| --- | --- | --- |
| `Allocate` / `owned` | `MakeRepairPlog` inserts the row and `repair_owned` together | Atomic SQLite statement; a separate late owner update would violate recovery's assumptions. |
| `Copy` | Verified reconstruction plus destination write/commit | Durable bytes are atomic; no unsynced data is modeled as recoverable here. |
| `Publish` / `mapped` | `ReplaceShardPlog` replaces the mapping and deletes the old source row | Successful epoch/placement admission is assumed from the companion model and runtime checks. Physical source deletion has not yet happened. |
| `Reject` / `Cleanup` | Rejected completion, then `DiscardUnassignedPlog` | Catalog retirement only. The model has no caller-held raw destination; its raw plog is unrelated to the repair. |
| `Crash` / `Recover` | Process exit, then `RetireUnassignedRepairPlogs` before mounting/resuming work | Catalog ownership survives; recovery removes only owned unassigned rows. Subprocess regressions cover pre/post-repoint and a second crash at `repair-catalog-retired` before physical deletion. |
| `Sweep` | Catalog-first `RemovePlogFiles`, or `SweepStrayPlogFiles` after interruption | Each physical deletion is separate from catalog retirement and consults current ownership. Data and undo-journal ordering remains a lower-layer obligation. |
| `Fail` / `Return` | Unreachable disk/node and subsequent return | Temporary unavailability only; no destructive media loss. |

Safety preserves published catalog/file membership, preserves the unrelated raw
plog, and forbids an abandoned repair catalog row after recovery/cleanup finishes.
Conditional liveness checks eventual termination and eventual catalog/file
agreement under weak fairness for the protocol, recovery, media return, and
sweeping. No liveness claim applies to permanent outage or unfair scheduling.
Witnesses reach crashes before physical copy, after publication, after catalog
retirement but before unlink, and successful cleanup following restart.

## Relocation outcomes (`RoseRelocationOutcome.tla`)

This companion model separates catalog disk placement, mounted clients, and the
physical disk copies of one plog ID. `mounted = 2` means no mounted client; disks
are 0 and 1. One serialized relocation attempt runs under topology ownership,
with at most two lifecycle events and two process crashes. Copy abstracts fully
verified, synced bytes of the acknowledged prefix. Independent freshness flags
record lifecycle events; they do not control admission, and detect stale
resolution even when a mutation removes the epoch increment itself.

| Action/state | Runtime correspondence | Boundary |
| --- | --- | --- |
| `Copy` / `files` | `copyFile`, destination verification, `RebindDiskUID` | Complete durable copy is atomic; no short writes, torn sectors, or UID/crypto representation. |
| `Commit` / `catalog`, `after` | `MovePlogToDisk` and attempted post-trigger generations | Applied and rejected transactions can both return uncertain outcomes. Atomic persistence is assumed; driver/VFS internals are omitted. |
| `Change` / `epoch`, `rollbackFresh` | Source/vlog and destination disk generation fences | Original-disk changes after repoint can invalidate rollback without changing the moved plog epoch. Physical bytes survive these events. |
| `Resolve`, `ResolveFailure` | Clean-session `ResolvePlogRelocation`, or failed reads | Exact source/destination generations resolve; stale/failed resolution quarantines. Session cleanup is a runtime assumption, covered in part by deferred-constraint tests. |
| `Remount`, `Rollback` | Client replacement, `remountVlogLocked`, fenced reverse `MovePlogToDisk` | Either can fail independently. An uncertain rollback can have applied or not. Partial remount changes within storage clients are not represented. |
| `quarantine` / mounted sentinel | `quarantineRelocationLocked` | No mounted access. A running invocation excludes outside I/O until cleanup or quarantine; reader holds are not modeled. |
| `Cleanup`, `Sweep` | Conditional candidate removal and `SweepStrayPlogFiles` | Per-disk ownership matters despite a shared plog ID. Sweep cannot interleave with an invocation holding topology ownership. |
| `Crash`, `Recover` | Process exit; quiescent `Recover` reconstructs authoritative mounts | Catalog and files persist; mounted clients disappear. Recovery is atomic here, and physical cleanup remains a separate action. |

Safety checks authoritative-file preservation, coherent accessible mounts,
quarantine fencing, served-file presence, and freshness of every resolved result.
Conditional liveness requires eventual recovered access and reclamation under
weak fairness for protocol steps, recovery, and sweeping, with finite crashes
and lifecycle changes. It does not promise availability during failed recovery.

Eleven mutations cover missing copy, unconditional destination deletion, skipped
remount, missing source/destination epoch checks or increments, missing fences,
unsafe sweeping, and unfair recovery/sweep. Six witnesses cover applied/rejected
uncertain commits, stale destination quarantine, independently invalidated rollback
tokens, applied-but-uncertain rollback, and recovery followed by stray cleanup.

This is a bounded protocol check for B30, not a mechanical refinement proof or
composition with the repair/maintenance models. Runtime tests cover the major
forward-result paths, rejected rollback, and restart, but applied-but-error
rollback, partial remount failures, every resolution/deletion crash boundary,
active I/O, online retry, and low-level persistence failures remain incomplete.
