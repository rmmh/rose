# TODO

## Deterministic simulation

- Make virtual disk, metadata, and transaction state cheaply cloneable with
  copy-on-write snapshots or an undo log.
- Hash canonical simulator state and memoize already-explored states.
- Add partial-order reduction for independent disk operations and symmetry
  reduction for equivalent disks.
- Branch crash, disk failure, repair, and compaction decisions from each
  runnable multi-transaction scheduler state.
- Emit and replay minimized deterministic failure traces.

## Durability runtime

- Add framed prepared-record recovery, tail validation/truncation, and
  persistent transaction staging.
- Persist disk roots, shard placement, and vlog cursors; reconstruct plogs and
  vlogs on server restart.
- Implement multi-disk placement, strict read-only degradation, repair, and
  compaction.

## Bitrot and scrubbing (done)

- Plog now keeps 4KB-aligned, immutable sealed sectors with a ragged-edge open
  sector overwritten in place across commits, fixing logical/physical drift.
- Reads verify each sealed sector against its recorded hash and fail with
  ErrBitrot instead of returning corrupt bytes; DUPLICATE/EC vlogs fall through
  to a surviving copy.
- Plog.Scrub validates every completed block (sector hashes + per-block HMAC);
  Vlog.Scrub and Server.Scrub aggregate per-shard results for bulk integrity
  passes.

## Bitrot follow-ups

- Persist sealed sector hashes for the trailing open block so sub-1MB data is
  verifiable after a restart (currently trusted only within the writing
  session).
- Add a Scrub RPC and a repair pass that rewrites corrupt shards from surviving
  redundancy (DUPLICATE copy / EC reconstruct).
