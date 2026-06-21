# Recovery and orphan-log semantics

On open, Rose distinguishes incomplete bytes from complete but unpublished
records.

- A torn header, invalid record checksum, or partial record at a plog tail is
  discarded by truncating that tail to the last complete framed record.
- A complete `PREPARED` record that was fsynced but whose transaction never
  reached the SQLite `PUBLISHED` commit is retained as an unreachable hole.

Valid orphan records must not be reused or overwritten in place: immutable
chunk addresses, snapshots, and concurrent recovery all depend on append-only
offsets. They are reclaimed only by segment compaction, which copies live
records to replacement plogs, atomically updates published placement metadata,
and then retires the old segment.

## Recovery sequence

1. Open SQLite with full synchronous WAL semantics.
2. Scan each plog's framed records until the first invalid/torn tail; truncate
   only that tail.
3. Load `PUBLISHED` shard placement from SQLite and validate each referenced
   record's transaction ID, length, and checksum.
4. If a published placement lacks the required verified fragments, start in degraded
   read-only mode (or fail startup if it is unreadable).
5. Mark `OPEN` and `PREPARED` transactions abandoned unless an idempotent
   caller resumes them. Their complete durable records are orphan holes.
6. Mount active plogs/vlogs from persisted placement and schedule repair or
   compaction as needed.

The transaction model represents this with `Crash`, `RecoverOrAbandon`, and
`ReclaimOrphan`. `ReclaimOrphan` represents post-recovery segment compaction,
not an in-place write into the original hole.
