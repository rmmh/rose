# Single-metadata-master transaction protocol

Rose currently assumes one authoritative metadata server backed by SQLite. Its
SQLite commit is the linearization and acknowledgement point for a file write:
metadata consensus is intentionally outside this design.

## Ordering

1. Create a durable `OPEN` transaction record with an idempotency key.
2. Allocate unique shard placements on active disks (and nodes when required).
3. Append framed `PREPARED` shard records containing transaction ID, chunk ID,
   shard index, offset, length, and checksum.
4. Flush and fsync each prepared shard; persist durable acknowledgements.
5. In one SQLite `synchronous=FULL` transaction, persist exact chunk/shard
   placement, publish the immutable file version and namespace root, create the
   automatic snapshot root, update references, and mark the transaction
   `PUBLISHED`.
6. Acknowledge the caller only after that metadata transaction commits.

A crash before step 5 leaves only unreachable prepared data. A crash after
step 5 cannot expose a torn file because every referenced shard was durable
first. Recovery abandons unpublishable `OPEN`/`PREPARED` transactions and
reclaims their records asynchronously.

## Admission during degradation

Strict mode is the default: a write requires every configured duplicate or EC
shard to be durable before publication. If a published object is degraded, or
the active disk set cannot satisfy full placement, the server admits reads but
becomes read-only until repair restores full protection.

An explicit future `AllowDegradedWrites` policy may publish with fewer durable
fragments.
That policy must persist achieved protection and schedule reprotection; it is
not the default for personal deployments.

## Formal model

`tla/RoseTxnCommit.tla` verifies that only fsynced records are published,
every published transaction has an automatic snapshot, crashes discard only
volatile records, published data retains its required verified fragments after
the configured loss, and strict mode disables write admission while data is
degraded. These fragments are not a consensus read quorum: replication reads
one hash-verified copy; EC reads `N` hash-verified distinct shards.
