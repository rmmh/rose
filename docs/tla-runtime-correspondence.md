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
