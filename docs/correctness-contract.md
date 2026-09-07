# Correctness contract

This is the normative destination for the implementation of
[the correctness plan](correctness-plan.md). Implementation and verification
status is recorded separately in [correctness-progress.md](correctness-progress.md);
this contract alone is not evidence that a property has been implemented.

## Authority, visibility, and retries

One server owns the namespace and its SQLite catalog. SQLite uses WAL and FULL
synchronous commits. A node is a storage fault domain, not a metadata voter.
Replicated metadata and independent multi-master writes are outside this contract.

An open file handle sees its immutable opened version plus its own staged writes.
Write and truncate change that handle's staged state. A successful Write reports
the handle's logical size, not a durable recovery offset. Until publication, a
reconnecting client must replay its writes. Reads on another ordinary handle do
not observe that staging.

Flush and fsync publish a complete new version and retain a usable handle. Close
publishes and releases the handle. A later write on a flushed handle starts a new
operation. Publication linearizes in the transaction that updates the immutable
file version, namespace head, chunk references, terminal write result, and leases.
Acknowledgement follows that transaction's durable commit. A lost reply after
commit is ambiguous to the client, but must not create a second publication.

A stable operation key binds a write intent and its terminal result. Compatible
retries return that result; conflicting payload, length, or explicit mtime retries
are rejected. Same-key handles reconcile with the winning result. Separate keys
opened on the same old version use last-publication-wins semantics; they do not
implicitly merge concurrent edits. Namespace operations serialize with publication:
unlink cancels a pending name, rename retargets its open intents, and a delayed
Close must not recreate an explicitly removed name.

The retry retention period must be explicit. Byte-level retry validation requires
retaining the referenced version's bytes for that period. Expired keys cannot be
silently reused as new operations. Session expiration must fence a handle before
releasing its pins or leases; an operation's retry record may outlive its session.

New results for client-supplied `Open.operation_key` values retain their exact
version for 24 hours after publication by default. `-retry-retention` (or
`SetRetryRetention`) accepts a positive duration for future publications; changing
it does not alter stored deadlines. Anonymous writes use session semantics and
do not acquire a durable retry-result root. Flush ends the keyed operation; later
writes on that handle start a new anonymous operation as before.

Maintenance, Open, and Close expire elapsed roots using server time, including
the exact deadline. Expired keys are fenced even when maintenance has not run.
A retry Open pins the winning version rather than the current path head. Those
pins keep its bytes readable after root expiry until the handle is released or
reaped, but Close cannot return an expired keyed result. A successful historical
retry does not restore an overwritten or unlinked namespace entry. Expired-key
tombstones currently remain indefinitely; bounded key generations are still
required. The configured policy must be supplied again on restart for future
results; existing results keep their persisted absolute deadlines.

`OpenResponse.retry_expires_at_ns` and `CloseResponse.retry_expires_at_ns` expose
the stored exclusive deadline in Unix nanoseconds. Zero means the response has
no retained committed result, including a newly prepared Open or an anonymous
operation. The first keyed Close advertises the committed deadline; later retry
Open and Close responses return that same value, including a Close retry without
the original handle. A policy change does not extend the returned deadline.

Write, handle Truncate, and handle timestamp updates also check operation state
and deadline before mutating or acknowledging a keyed request. This admission
check does not depend on an expiry sweep. It reads state and deadline from one
catalog snapshot, rejecting requests admitted at or after the deadline while
leaving the already-open version readable through its existing pins.

Network handles become eligible for expiration after one hour without a handle
request by default (configurable by `SetWriteOpExpiry`). Read, Write, Getattr,
Truncate, handle mtime updates, and Flush renew activity. The reaper and requests
serialize under the handle lock: a renewal that wins preserves the handle; once
removed, even a request that previously looked it up must fail. FUSE and WebDAV
explicitly retain local handles until the fd/request ends, so local idle time
alone does not revoke an open file. Startup grace gives recovered prepared
operations a reconnection window. This session rule is separate from the still
required durable retry-result retention policy.

## Content identity and protection

Deduplication is scoped to a bucket and a protection policy. The current format
represents that scope as a fixed 32-byte domain derived from an unambiguous encoding
of the bucket name and policy, then hashes the domain and plaintext together.
File extent lists store the resulting 15-byte scoped content address. Vlog metadata
and plog superblocks persist the domain; promotion, compaction, and shard relocation
preserve it. Ordinary storage vlogs may use the empty domain, but file publication
must not adopt them without rewriting into the file's domain.

Changing a bucket policy affects subsequent file publications. Their complete
extent list, including unchanged base data, must satisfy the new policy. Existing
immutable versions and snapshots retain their original domains. Rename preserves
the existing version and its protection; the next file publication conforms to its
destination bucket. It does not silently reinterpret old ciphertext or hashes.

Strict mode is the required default: every required replica or EC shard must be
durable at publication, on distinct required fault domains. Known degradation of
published data prevents further publication until repaired. Reads need one verified
replica or N verified distinct EC shards. Disk and node fault tolerance are separate
policy choices. Desired protection and achieved placement must be durable metadata,
not values inferred from how many disks happen to be online during recovery.
Each vlog now records `required_shards` at creation; compaction preserves that
count, and publication compares it with distinct reachable disk mappings. Content
verification then checks every mirror or the complete intersecting EC codewords,
including parity. This count is a disk-level requirement; configurable node-loss
policy and durable corruption/achieved-protection state remain separate work.

An explicit degraded-write mode, if exposed, must persist its achieved protection
and repair obligation. Merely lowering a quorum or calling a metadata row live
does not establish readability, integrity, or protection.

## Persistence and recovery

Append position, synced prefix, and published prefix are different values. A sync
returns the exact prefix it made durable while it still owns the append lock.
Metadata records only that prefix; out-of-order metadata updates cannot retract a
newer durable prefix. A chunk's complete record must fit within its published
vlog prefix. Every publication resolves and validates the canonical placement of
all final extents, including dedup hits. Fresh bytes must not be discarded in favor
of an unreferenced, obsolete location.

Relocation copies and verifies a destination, syncs it, then repoints metadata.
It retires source storage only after references and active I/O ownership permit it.
Raw storage RPCs have a separate ownership boundary: raw plog commits cover only
unmapped plogs; raw vlog writes/commits cover only unleased, unscoped vlogs. They
cannot append to or advance file-owned storage. File ownership survives lease
release because its persisted domain remains attached to the vlog. Maintenance
owns its destinations durably, including after job completion, and recovery uses
that ownership to restore the all-copy quorum. Unfinished rewrite sources cannot
admit new raw or file appends. Claiming the destination and recording it on the
running job occur in one catalog transaction.

Late requests and completions are fenced by ownership/generation so they cannot
recreate retired placements. Durable job state permits idempotent restart at every
copy, sync, metadata update, and deletion boundary.

Missing integrity metadata means unknown, not healthy. Recovery verifies retained
committed bytes before serving them or using them as maintenance sources. This
includes partial sectors. It must not authenticate bytes by hashing the same
unverified bytes, or seal them into a newly trusted block. A verified old prefix
can retain its evidence when an unreferenced longer tail is trimmed.

The fault model must include a process crash, lost writes before sync, short writes,
failed sync, and torn overwrites of sectors that include an acknowledged prefix.
Preserving that prefix requires either an explicit, tested atomic-write assumption
or an implementation that retains an independent durable copy. A normal storage
shutdown is not proof of power-failure behavior. Unknown/corrupt data must cause
verified-redundancy fallback or an error, never successful corrupted plaintext.

## Snapshots, references, and reclamation

Snapshots are explicit namespace operations; a successful Close does not implicitly
create a snapshot. Each snapshot captures the complete namespace, including empty
directories and metadata, at one metadata transaction boundary. Retained snapshots
are immutable and read-only. Automatic scheduling can invoke the same operation;
it is not a separate commit protocol. ListDir and Getattr accept `snapshot_id`
to read the frozen namespace, including empty directories. Getattr rejects a
request that combines a snapshot ID with a handle. Snapshot deletion removes its
namespace rows, while already-open readers retain their pinned file version.

Reference counts represent ordered extent occurrences reachable through live heads
and retained snapshots, plus any durable retained-version roots. Reader and worker
pins have distinct owners. Releasing one owner's pin must not release another's.
GC may reclaim a chunk only when it has neither a reference nor a pin. Repointing
content-preserving placements must keep pinned readers usable.

Snapshot retention is opt-in and durable. Without a policy, explicit snapshots
remain roots until explicitly deleted. A configured policy retains every snapshot
within its continuous window, then the newest representative per fixed 24-hour
bucket within the daily window, then per fixed 7-day bucket within the weekly
window. Buckets are UTC intervals anchored at the Unix epoch. Younger retained
snapshots also represent their daily/weekly buckets. Boundaries are inclusive;
timestamp ties choose the larger snapshot ID. Future timestamps survive backward
clock steps. Selection and reference release occur in one SQLite transaction,
and open-reader pins remain separate roots. The maintenance scheduler expires
snapshots before GC. `--snapshot-retention=24h,720h,8760h` persists example windows;
`off` disables expiration; omitting the option preserves the existing policy.
In-process `ListSnapshots` discovers retained snapshot IDs, newest first.

Retention, session expiry, terminal retry records, obsolete file versions, operation
locks, and maintenance jobs each need a bounded lifetime or explicit retained root.
A read-only consistency checker must independently traverse metadata and compare
references, extent bounds, placement, protection, ownership, and disk identity.

FUSE cached nodes must resolve their current namespace identity after rename;
already-open handles retain their server handle identity. Fsync must reach durable
publication, propagate failures, and allow subsequent writes on the same handle.
Adapter correctness tests must exercise these paths without requiring a kernel mount.

## Evidence required

Production and deterministic simulation must execute the same transition code.
TLA+ actions must map to actual code, catalog transactions, ownership preconditions,
and crash boundaries. Check invariants with independent oracles, including reference
multiplicity and snapshot history. Check conditional progress under fair scheduling
and sufficient healthy capacity, not during indefinite outages.

Complete bounded model runs, mutation tests, minimized replay traces, real-process
crash tests, race tests, and mount integration tests supply different evidence.
Timeouts and skips do not count as a successful verification of their missing scope.
The integrity contract concerns accidental corruption; it does not claim resistance
to an attacker who can rewrite the catalog, keys, or checksum metadata.

## Read-only inspection

`rose --check-catalog --metadir <dir>` checks an existing catalog without starting
services, initializing schemas, backfilling indexes, or repairing data. It emits
JSON with `scope: "catalog"` and a deterministically sorted issue list; findings
produce a nonzero exit status. The checker independently decodes ordered extent
occurrences and traverses namespace/snapshot roots. It checks reference counts,
extent bounds, mapping geometry, lease/job ownership, namespace indexes, and
foreign keys. This command does not yet validate physical disk bytes, disk
identity, or volatile reader/worker pins; a clean catalog result does not prove
storage integrity or the complete correctness contract.

`rose --check-storage --metadir <dir> --datadirs <disk1,disk2,...>` adds physical
inspection and emits `scope: "catalog-and-files"`. It uses the same numeric disk
ordering as startup, compares disk markers and plog identity/domain with one
catalog snapshot, verifies stored sector/trailer integrity evidence, checks the
required physical prefixes, and reports uncataloged plog files. Missing or
unverifiable evidence is reported; the inspector does not reconstruct it or
repair the files. Every plog opens with `O_RDONLY`.

Run physical inspection against quiescent files. A catalog read transaction does
not freeze another process's physical writes, so concurrent mutation prevents a
coherent cross-file conclusion. This inspection does not yet compare plaintext
content hashes across all logical references, fully validate protection policy,
or inspect volatile reader/worker owners.
