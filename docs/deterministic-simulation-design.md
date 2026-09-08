# Deterministic durability simulation

## Implemented production boundary

`durability.Coordinator` drives the actual file publication path through
`server.preparedPublication`. The former independent shard-record coordinator
has been replaced; its idealized interleaving counts are not runtime evidence.
The adapter exposes these effects:

```go
type Publication interface {
    Admit(context.Context) ([]uint32, error)
    Sync(context.Context, uint32) (int64, error)
    RecordPrefix(context.Context, uint32, int64) error
    Verify(context.Context) error
    Publish(context.Context) error
}
```

Admission checks referenced topology and obtains the operation's vlog leases.
For each leased vlog, the stepper captures `CommitPrefix`'s exact return value and
records that value in the catalog. Verification resolves canonical dedup winners,
checks bucket domains and extent bounds, and verifies the required protected
bytes. Publication atomically updates the namespace, version, references,
operation result, optional retry root, and leases through the existing SQLite
transaction. Empty and dedup-only publications still perform admission and
verification, without requiring a fresh vlog sync.

The caller retains namespace, operation, vlog, and pin ownership across the
entire sequence. This extraction does not relax locks or permit topology changes
between verification and publication. Append preparation happens before this
boundary; client reply delivery and handle cleanup happen after it.

`Transaction` stores control state and performs one effect per `Step`. The
existing production hooks follow sync, prefix recording, placement verification,
and namespace publication. A returned error stops the invocation, including an
ambiguous error after namespace publication. Retrying uses durable operation
state and a fresh invocation; it does not resume a failed in-memory stepper.
Copying control state alone does not clone the adapter or its ownership.

## Evidence and limits

The protocol tests inject failure into every effect for zero, one, and two leased
vlogs, check the durable-prefix and publication ordering independently of the
adapter, and cover every hook including an ambiguous published result. Their
adapter deliberately does not reject unsafe ordering on behalf of the stepper.
A cursor advance after sync checks that the captured prefix is used.

The real server publication tests exercise this same coordinator with SQLite and
plog files, including subprocess crashes and response-loss retries at the retained
hooks. These tests establish bounded histories, not exhaustive filesystem or
multi-writer correctness. Separate TLA+ prefix and retry models retain their
explicit abstraction limits.

## Remaining explorer work

1. Extract cloneable disk, catalog, clock, and ownership state behind the actual
   effects, including preparation, namespace changes, and maintenance. The current
   adapter still performs real I/O directly; effect-test state is not a full server
   simulator.
2. Define canonical state hashes and replayable traces; branch at production
   transitions with an independent byte-array namespace/snapshot oracle. Cover
   two writers, repeated extents, pins, retries, topology changes, and GC.
3. Model torn writes, fsync/SQLite ambiguity, outage/return, disk replacement,
   placement generations, and conditional repair progress. Refine model actions to
   these effects without hiding lower-layer failures inside atomic assumptions.
4. Add trace minimization, then justify symmetry and partial-order reductions by
   comparing observations with unreduced exploration. Archive replay traces and
   run them in CI alongside process-crash and seeded chaos coverage.
