# Concurrency and resource ownership

This describes the current single-server/SQLite implementation and the ownership
that the shared publication stepper requires. It is an implementation contract
for changes, not a proof that every interleaving has been explored. Durable
placement generations now fence shard repair's source and destination at its
catalog commit; remote completion fencing and a complete simulated scheduler
remain unfinished.

## Lock order

The following are permitted nested acquisitions in the audited publication,
handle, maintenance, and reclamation paths. A path may omit intermediate locks.

| Outer owner | Inner acquisition | Concrete paths |
| --- | --- | --- |
| `maintRunMu` | `namespaceMu`, `vlogMu`, or `pinMu` | `RunMaintenanceOnce`, `GC`, `Compact` |
| `namespaceMu` | handle `stateMu`, `vlogMu`, or `pinMu` | `finishHandle`, rename/unlink, Open, expiry, bucket policy updates |
| handle `stateMu` | operation lock, write-cache mutex | Write, Read, Truncate, Getattr, Close/Flush |
| operation lock | write-cache mutex, `vlogMu`, `pinMu` | finalization, publication, committed-retry refresh |
| write-cache mutex | `vlogMu` during base reads | cache read-through to `readChunksAt` / `resolveVlog` |
| `vlogMu` | `pinMu`, storage locks, short key-cache locks | publication, retirement, repair, full scrub |
| `pinMu` | catalog transaction | Open/pin admission, GC, retry expiry, retirement |

Do not acquire a handle state lock while holding `handlesMu`. Lookup copies a
pointer and releases the registry lock before waiting for handle state. Once
state is held, `handleStillRegistered` rechecks identity under `handlesMu`; this
prevents a request paused after lookup from reviving a closed/reaped handle.
Namespace scans similarly copy handle pointers before locking individual handles.

`writeOpsMu` protects only registry membership and holder/waiter counts. Increment
before releasing the registry lock and waiting on the operation mutex. Release
the operation mutex before decrementing/removing its registry entry. Never wait
for an operation while holding `writeOpsMu`, and never take a second operation
lock while holding the first.

`maintenanceMu` protects driver lifecycle fields. `StopMaintenanceDriver` copies
and clears them, releases that mutex, then cancels and waits. Do not call it while
holding locks needed by a maintenance pass, including `maintRunMu`, `namespaceMu`,
`vlogMu`, or `pinMu`. `CloseStorage` stops the driver before acquiring `vlogMu`.

Key-cache locks cover only cache accesses. Release them before catalog lookup or
any acquisition of a broader server lock. SQLite transactions do not call back
into server locking paths. Fault hooks invoked inside an owned interval must not
re-enter public methods that acquire the same locks; subprocess exit and bounded
error injection are supported uses.

## Ownership intervals and release

| Resource | Owner and admission | Release or expiry |
| --- | --- | --- |
| Handle state/cache | Registered handle, serialized by `stateMu`; each request rechecks registration and renews use time | Close/Abort removes registration; reaper fences idle network handles under the same state lock |
| Local adapter handle | `RetainHandle` increments `localOwners` under handle state | Returned once-only release; an idle mounted fd is not a disconnected network client |
| Open version bytes | Handle pin owner holds content identities | Close/Abort/reaper releases pins; Flush replaces the set with the newly opened version |
| Prepared dedup bytes | Operation pin owner, admitted before resolving a live chunk | Publication or durable cancellation/abandonment cleanup; failed cancellation retains operation pins for retry |
| Append cursor | Durable `vlog_lease` for a prepared operation; raw appends obey separate ownership checks | Publication/cancellation/abandonment removes leases; committed prefix remains catalog authority |
| In-flight file I/O | `resolveVlog` chooses canonical placement and increments `activeVlogOps` under `vlogMu` | `readChunksAt` calls `endVlogOp` after the read, including errors; maintenance defers relocation while active |
| Rewrite destination | Durable running job's destination ID; `maintenance_owned` prevents file allocation | Running destination hold ends when job becomes terminal; ordinary retirement still requires reference/pin checks |
| Repair destination | `plog.repair_owned` is set atomically at allocation; the live repair retains topology ownership | Local failure discards only an unassigned plog. Quiescent recovery transactionally removes marked unassigned rows before file deletion; published mappings protect their destinations. Raw unassigned plogs have no marker and survive. |
| Retry result | Durable `write_result_root` with exclusive deadline and exact reference multiplicity | Expiry transaction releases refs and marks the operation expired; existing reader pins remain independent |
| Snapshot data | Durable snapshot entries and their reference multiplicities | Snapshot deletion/expiry removes refs; open snapshot readers retain their own pins |

A pin protects content from reclamation; it does not freeze a physical address.
Readers resolve the current canonical location under topology ownership and then
hold the mounted I/O resource. A running destination hold similarly prevents
rewrite/retirement, but permits shard repair in place. Scrub now retains `vlogMu`
through inspection: copying mounted pointers without a hold would not keep their
plog clients alive while maintenance retires files.

## Publication and retirement boundaries

`finishHandle` holds namespace, handle state, and operation ownership while
finalizing cache data. `publishPreparedVersion` then holds `vlogMu` followed by
`pinMu` across the complete shared coordinator sequence: admission, each exact
sync/recorded prefix, canonical placement validation, and atomic namespace/result
publication. The placement check cannot become stale through topology changes or
GC inside that interval. A hook error ends the invocation; a later request
resolves durable operation state instead of continuing an old in-memory stepper.

Open holds namespace ownership and `pinMu` from version lookup through pin
registration. GC holds `pinMu` across snapshotting pins and deleting zero-ref
chunk rows. Retirement holds `vlogMu` and `pinMu` through the catalog deletion;
its transaction rejects live chunks and running destination ownership, and cancels
scrub jobs superseded by source removal. Physical cleanup follows catalog removal.
These orderings close lookup-to-pin and check-to-delete gaps.

GC also deletes a bounded batch of unowned immutable file-version rows. Its single
SQL deletion rechecks namespace/snapshot/retry roots and prepared/committed
operation references. It does not change chunk refcounts: root removal already
did so. Handles cache their extents and mtime; removing an expired version row
does not remove their independent content pins or require taking handle locks.

## Preconditions for reducing broad lock scope

Each vlog has a durable positive `placement_epoch`. Catalog triggers advance it
atomically when its mapping, leases, identity, prefix, protection geometry, or
mapped disk/node identity or availability changes. Returning to a previous state
does not restore an earlier generation. Overflow fails the mutation transaction.
Repair captures the epoch before reconstruction and compares it in the same
transaction that repoints the shard and deletes the old plog row. Rejection
discards the unpublished destination; a retry captures a fresh source epoch.

An unassigned destination has its own durable `plog.placement_epoch`, captured
before physical writes. Mapping ownership, plog identity/location/recorded length,
and disk/node lifecycle changes advance this epoch even without a source mapping.
Disk/node removal and reinsertion cannot revive an old completion while the
destination plog exists. The repoint transaction compares both generations and
requires the destination disk active and its node working. Plog IDs are never
reused by catalog allocation; arbitrary catalog rewrites are outside this contract.
Repair still holds `vlogMu` across I/O: remote request generations and complete I/O
holds are prerequisites for shortening that interval. The fence does not undo
physical writes or protect clients from being closed during reconstruction.

Bulk publication sync, repair, compaction, and scrub currently retain topology
ownership across substantial I/O. Replacing these locks requires all of:

1. Capture a placement generation and acquire an explicit I/O hold before
   releasing topology ownership, covering both the mounted object and its files.
2. Revalidate the generation and source/destination job ownership before applying
   completion or catalog publication. A returned/replaced disk must not validate
   an old request merely because its numeric ID is reused.
3. Release holds on success, failure, cancellation, and process recovery; retain
   durable intent when publication outcome is ambiguous.
4. Replay stale-completion, deletion, expiry, and repeated-failure schedules through
   production transitions with independent content and reference oracles.

The existing `activeVlogOps` reader interval is a starting point, not a persisted
generation protocol. This document does not authorize moving sync outside the
publication lock interval without the missing ownership and validation steps.
