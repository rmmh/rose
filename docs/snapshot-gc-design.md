# Snapshot and garbage-collection design

Every successful metadata publication creates a global, read-only snapshot.
The snapshot captures the namespace root produced by that transaction; it does
not copy file chunk maps or chunk data. A snapshot therefore represents a
consistent system state at a completed metadata-transaction boundary, never an
in-flight write buffer or partially published file.

## Persistent metadata roots

Namespace and file extent maps are immutable copy-on-write trees:

```text
snapshot -> namespace root -> path tree -> file extent root -> chunk IDs
```

Changing a small range of a large file copies only the affected extent leaf and
its ancestors. Unmodified subtrees remain shared with the prior file and
namespace roots. Creating a snapshot stores a generation, timestamp, and
namespace-root reference, so its direct metadata cost is constant.

The first bounded formal model, `tla/RoseSnapshotGC.tla`, represents this with
a root and two extent leaves. A publish allocates one new root and one changed
leaf while sharing the untouched leaf. This is the safety-relevant structural
sharing behavior without modeling an unbounded B-tree.

## Retention

Retention is evaluated from logical time using policy constants:

| Snapshot age | Retained representatives |
| --- | --- |
| Within the continuous window | Every snapshot |
| Within the daily window | Newest snapshot in each daily bucket |
| Within the weekly window | Newest snapshot in each weekly bucket |
| Older than the weekly window | None |

Buckets use fixed UTC-like logical periods. The newest representative is
deterministic; snapshot creation advances the generation time so ties do not
occur in the model. Real policy values are configuration, not protocol
constants.

## GC and pins

Roots, active readers, and repair/rebalance workers pin immutable metadata and
data. Expiring a snapshot only releases its root reference. A background GC
worker then reclaims zero-reference metadata nodes incrementally, decrementing
their direct child-node and chunk references. A chunk is collectible only when
its metadata reference count is zero and it has no reader or maintenance pin.

This avoids work proportional to file size or historical snapshot count at
snapshot creation and prevents unlink, retention compaction, or a concurrent
maintenance operation from collecting readable data.

## Required invariants

- Retained snapshots resolve only to committed/readable chunks.
- Snapshot roots and their reachable immutable nodes cannot change while the
  snapshot is retained.
- Reference counts match the persistent metadata graph.
- GC cannot collect a node or chunk that is reachable from the live root, a
  retained snapshot, or a pin.
