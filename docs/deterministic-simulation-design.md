# Deterministic durability simulation

Rose will test transaction correctness through the production transaction
coordinator against abstract metadata, disk, and scheduling interfaces. The
same coordinator is used with operating-system disks and SQLite in production,
and with virtual disks and metadata in deterministic simulation.

## Contract boundary

```go
type Disk interface {
    ID() string
    Prepare(ctx context.Context, record PreparedRecord) error
    Sync(ctx context.Context) error
}

type Metadata interface {
    Begin(ctx context.Context, txnID string) error
    Publish(ctx context.Context, txnID string, placement []Placement) error
    Abandon(ctx context.Context, txnID string) error
}
```

The coordinator calls a scheduler hook immediately before and after each
observable boundary: transaction begin, every prepared append, every disk
sync, metadata publication, and client acknowledgement. A hook may inject a
crash. The acknowledgement boundary follows successful `Metadata.Publish`.

## Simulator fault model

Virtual disks have distinct volatile and durable record sets. A crash removes
volatile records; a disk failure removes or hides its durable records. Metadata
publication is atomic. The deterministic recovery oracle abandons unpublished
transactions and requires every published transaction to retain its required
verified fragments. This is not a consensus read quorum: a replicated chunk
needs one hash-verified copy, while EC needs `N` verified distinct shards.

The first explorer exhaustively checks every crash barrier and every
permutation of disk preparation/sync order for one strict EC transaction. It
also checks the ambiguous post-publish/pre-ack crash: metadata may be
published, but the caller receives an error and must retry with the same
idempotency key. The coordinator is step-driven, and the current suite also
enumerates all 184,756 interleavings of two strict three-shard transactions.

## Expansion path

1. Add cloneable disk and metadata state with state hashing, so a crash can
   branch from every concurrent transaction schedule without replaying each
   complete prefix.
2. Add disk failure, repair, and compaction decisions at every yield point.
3. Apply symmetry reduction to equivalent disks and partial-order reduction to
   independent operations.
4. Run seeded long histories against real files and SQLite. These test the
   implementation and filesystem/WAL behavior, but are not exhaustive.

Every failed simulation emits a trace containing the policy, disk permutation,
and decision sequence. The same trace must replay without randomness.
