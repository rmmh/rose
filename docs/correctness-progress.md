# Correctness implementation progress

Objective: implement all phases of [correctness-plan.md](correctness-plan.md).
The audit findings remain historical evidence. This checklist tracks the current
implementation and does not replace or reduce the plan's acceptance criteria.

## Implemented in the working tree

- B1: unauthenticated open-block bytes fail reads and cannot be appended to or
  re-signed. Catalog recovery covers the ragged sector, preserves independently
  verified prefixes on truncation, and runs before recovery-time maintenance.
  Maintenance additionally validates each decrypted chunk's scoped content hash.
  `TestRecoveryVerifiesRaggedPayloadWithoutTrailer` covers healthy/corrupt tails,
  with/without sealed sectors, and healthy mirror fallback.
- B2: bucket/policy-scoped content addresses; persisted vlog and superblock
  domains; domain-preserving promotion/compaction/repair; complete-version
  rehoming on the next publication after policy changes. Tests cover cross-bucket
  isolation, same-domain dedup, policy upgrades of unchanged data, and old snapshots.
- B3: publication resolves the complete final list and checks its canonical
  placement, committed bounds, readiness, and content before the metadata commit.
  Topology and GC are locked out through commit. Dedup-only failed-disk publication
  now fails. Broader strict-mode and achieved-protection work remains below.
- Publication protection: scoped file vlogs write/sync all backing copies; each
  publication verifies every mirror or all intersecting EC rows, including parity
  consistency, against independently content-verified bytes. Vlogs persist an
  immutable required shard count. Missing mappings, unavailable shards, and
  same-disk colocation cannot lower that requirement. Referenced file data with
  known topology degradation blocks unrelated file publication, including empty
  files, until repaired. Tests cover underprotected dedup, repair/retry, unrelated
  publication, mapping loss, inconsistent copies, and corrupt EC parity.
- Compaction preserves the source's shard requirement and EC staging target.
  Namespace publication, compaction, and EC promotion use the same complete-chunk
  and all-shard verifier before publishing/repointing references. Compaction cannot
  reduce a three-copy promise to two copies merely because one disk is down.
- B4: zero-reference upserts install fresh locations. Reads follow a live canonical
  placement even while the old vlog is mounted. Regression tests cover lost old
  storage and a pinned unlinked reader following the replacement.
- Durable prefix race: `Vlog.CommitPrefix` returns the length under the append
  lock; production publication, raw vlog commits, promotion, and compaction use it.
  Catalog lengths advance monotonically. A deterministic two-vlog barrier test
  verifies that a later unsynced append cannot enter the persisted prefix.
- B5: cached FUSE node paths move together on local rename, including duplicate
  lookup instances and descendants. Replaced/unlinked nodes reject new path
  operations; open handles retain their version. Forget releases bookkeeping.
  Mount-independent tests cover directory moves, old-name recreation, replacement,
  open readers, truncation, unlink, and duplicate cached nodes. Namespace changes
  through a different adapter still need a mount notification/identity mechanism.
- B6: FUSE fsync publishes without closing. A mount-independent adapter test covers
  storage error propagation, retry, subsequent writes, and reading from a fresh
  server without releasing the original handle.
- B7: idle network handles expire under the same lock that renews and fences
  requests. Reaping drops reader pins and abandons unowned prepared writes and
  their leases. FUSE/WebDAV retain explicit local ownership until release/close;
  cleanup also releases ownership on errors. Handle Getattr renews activity and
  rejects expired IDs. Deterministic clock tests cover renewal, stale lookup
  fencing, reader pins, write leases, multiple same-key owners, and local release.
- M1: storage placement and relocation account for outstanding request/ack retry
  messages as ownership. The original multi-node configuration now completes with
  no invariant errors: 85,194,101 generated / 6,125,160 distinct states, depth 37.
  The disk-failure configuration also completed: 112,501,891 generated /
  6,777,152 distinct states, depth 37. These are model repairs, not demonstrated
  runtime refinements.
- Raw storage ownership: WriteVlog rejects scoped/file-leased logs; CommitVlog
  skips them, and CommitPlog skips vlog-owned plogs. Raw transactions cannot
  publish a file operation's storage prefix. Scoped ownership persists after
  lease release. A regression test exercises staging, raw commit, file commit,
  and attempted raw append after publication. Generations for delayed maintenance
  and returning shards remain separate outstanding work.
- Durable maintenance ownership: claiming a job destination atomically marks its
  vlog as maintenance-owned. The marker survives completion and job reclamation;
  remount restores an all-copy quorum even for unscoped raw-data destinations.
  Running rewrite sources and all maintenance destinations reject new raw writes;
  file allocation excludes them too. Tests cover claim rollback, completion,
  remount, and failed-copy writes after remount.
- Complete explicit snapshot namespace: snapshot creation captures directory rows
  and mtimes, including empty directories, in the file-reference transaction.
  ListDir and Getattr accept snapshot IDs for immutable metadata reads. Tests cover
  live rename/removal/new directories, same-name retry, invalid/deleted snapshots,
  deletion cascades, retained open readers, and catalog reopen.
- Persisted snapshot retention: opt-in continuous/daily/weekly windows, atomic
  root expiration, deterministic ties and fixed UTC buckets, future timestamp
  preservation, and snapshot discovery through the in-process API. Maintenance
  runs expiration before GC. The executable exposes `--snapshot-retention`;
  omission preserves saved policy and `off` disables expiration. Tests cover
  tiers, boundaries, ties, policy reopen/disable, reference release, and open
  readers surviving expiration/GC. Remote discovery/configuration tooling remains
  to be reconciled with the final operational interface.
- Operation-lock reclamation: write-operation mutexes count holders and waiters
  and leave the registry after their last user exits. A same-key contention race
  test checks mutual exclusion through repeated lock creation/reclamation and
  verifies that completed distinct operation IDs do not grow the registry.
- Independent catalog checker: ordered extent decoding and head/snapshot traversal
  check reference multiplicity, missing rows, logical lengths and durable bounds,
  shard indexes/counts/colocation, lease/job ownership, foreign keys, and namespace
  parent/name consistency. `--check-catalog --metadir <dir>` opens an existing
  catalog in SQLite read-only mode, emits scoped JSON, and fails on findings.
  Mutation tests inject each class of corruption and check that inspection neither
  repairs data nor creates an absent database. Physical disk agreement and volatile
  owners remain outside this catalog-only scope.
- Catalog path escaping: filesystem paths containing URI metacharacters are now
  escaped before opening SQLite. The checker fixture exposed that `?` previously
  caused the requested filename to be interpreted as URI parameters.
- Transaction model safety: publication now obeys the global gate; a separate
  history monitor catches degraded publication. Repair excludes occupied disks
  and replaces the old shard mapping. Snapshot capture is explicit. Both the
  original and a new spare-disk configuration finish, and automated mutations
  demonstrate that removing either safety guard violates its named invariant.
  [Runtime correspondence](tla-runtime-correspondence.md) records the mapping and
  gaps without claiming refinement.
- Snapshot/GC model: mutation actions update refcounts incrementally and invariants
  independently recount the graph. Pins now have named owners. The retention
  horizon extends past weekly expiry, and bounded snapshot slots have generations
  and reuse. Coverage witnesses exercise daily/weekly tiers, past-weekly age,
  expiration, and reuse; refcount and pinned-GC mutations are detected. This is
  still a COW design model, not refinement of the runtime's SQLite namespace.
- Read-only catalog/disk inspection: `--check-storage` combines a single catalog
  snapshot with supplied quiescent disk roots. It checks identity markers,
  superblock identity/domain, authenticated stored bytes, required physical
  prefixes, and uncataloged plog files. Files open with `O_RDONLY`; no recovery,
  trailer reconstruction, truncation, marker creation, or repair runs. Mutation
  tests cover payload/header corruption, missing trailer/file, wrong disk UID,
  extra files, and short prefixes while checking that evidence is unchanged.
  This does not inspect volatile owners or prove plaintext content and protection
  policy beyond the catalog and stored integrity evidence.
- Existing protobuf mutex-copy vet findings have been corrected.
- [correctness-contract.md](correctness-contract.md) states the normative contract
  and distinguishes it from implementation evidence. Scoped addresses use plog
  format version 4; no migration from the research baseline is provided.

## Still required

- Phase 1: finish strict admission policy across file and raw APIs, durable
  corruption health/repair tracking and achieved-protection evidence, and explicit
  configured disk/node protection policy. File publication now enforces the
  persisted shard requirement and global known topology gate, but those checks
  do not establish all desired/achieved protection semantics. Add publication-
  boundary topology/GC scheduling regressions.
- Phase 2: shared production/simulator transition code; persisted placement epochs
  and delayed-completion fencing; cross-adapter namespace
  identity/notifications; bounded retry/version/job reclamation;
  final snapshot operational interface; documented lock ordering and
  reduced broad-lock I/O where ownership safely replaces it.
- Persistence: undo journals now protect the physical prefix during append and
  truncation. Verify error/restart outcomes at each actual filesystem and SQLite
  boundary, including journal creation/replacement/removal, interrupted replay,
  directory sync failure, and catalog/physical publication crash interactions.
- Phase 3: preserve the repaired M1 regression; implement the compositional models and refinement
  table; ordered extents and independent refcount/snapshot oracles; owner pins;
  realistic retention horizons; conditional liveness; mutation testing; complete
  every checked-in configuration, including the earlier timed-out storage models.
- Phase 4: deterministic state explorer with cloneable state, canonical hashing,
  minimized trace replay and checked reductions; subprocess crash tests; broad
  mixed-operation histories; extend inspection to volatile owners and complete logical-content/protection
  evidence; CI and scheduled model,
  chaos and mount verification with archived evidence.
- Reconcile all README/TODO/design descriptions with the completed runtime and
  selected contract. Review encryption/nonce/key handling without claiming an
  adversarial authentication guarantee from the current checksum format.

## Verification so far

- `go test ./meta ./storage ./server`: passed after the publication/domain changes
  and the explicit unverified-open-block write/commit fence.
- `go test -race ./meta ./storage ./server`: passed after the scoped-publication
  changes; repeat after remaining concurrent lifecycle changes.
- `go test ./fuse -run TestFsync -count=1`: passed without a kernel mount.
- `go test -race ./server ./fuse -run
  'TestIdleHandle|TestLocalOwnership|TestReap|TestFsync|TestCachedRename' -count=1`:
  passed after the lifecycle changes.
- `go test -race ./meta ./storage ./server ./fuse ./durability -timeout=90s`:
  passed after lifecycle and raw-ownership changes.
- Full suite (`ROSE_NO_RAMDISK=1 go test ./... -timeout=90s`) and `go vet ./...`
  passed after raw-ownership changes. Remaining storage model configurations are
  still being checked; whole-plan completion is not claimed.

### Protection batch

- Full Go suite passed after shared all-shard verification was added to namespace
  publication, compaction, and promotion.
- Broad race suite passed with required shard counts and the global topology gate.
- Focused mirror/EC verification, degraded dedup, compaction, and promotion race
  tests passed. The EC staging-target compaction regression also passed.
- The replica storage model is running to completion; EC remains to be checked.
- Durable health evidence and placement-generation fencing remain part of the full
  plan. Maintenance ownership and remounted quorum behavior are now covered by
  the dedicated lifecycle regression, not inferred from publication tests.

### Maintenance ownership and snapshot namespace batch

- Full Go suite and vet passed after durable ownership and complete snapshot
  namespace reads were added.
- Focused ownership/compaction and snapshot namespace race tests passed.
- Snapshot directory persistence is verified by closing and reopening the catalog.
- The replica TLC process remains live and is being allowed to finish; no reduced
  configuration is substituted for the checked-in configuration.

### Snapshot retention batch

- Full Go suite and vet passed after retention and startup configuration changes.
- Focused race tests cover selection, persistence, and reader-pin/GC interaction.
- The unchanged replica TLC configuration is still running. Its shrinking state
  queue is progress evidence, not a passing result; EC remains to be checked.

### Operation-lock lifetime batch

- Server suite passed after holder/waiter-counted operation locks were introduced.
- Race tests for lock contention, concurrent operations, and retry paths passed.
- Retry-result byte retention remains unimplemented: committed `write_op.file_id`
  is not a durable chunk-reference root. A correct implementation must retain
  its ordered extents until expiry, preserve active reader/retry owners, and fence
  expired keys rather than silently accepting them as new operations. Bounded
  key history also needs a protocol-level expiry/generation rule; deleting rows
  alone cannot preserve the existing arbitrary-string-key semantics.

### Catalog checker batch

- Full Go suite and vet passed after the checker and SQLite path escaping fix.
- Mutation-based catalog checks pass under the race detector; CLI tests verify
  scoped JSON and non-success on findings.
- RoseStorageReplica completed with no invariant error: 263,323,355 generated /
  16,436,268 distinct states, depth 37, 26m26s. RoseStorageEC is now running using
  its unchanged checked-in configuration. Model refinement is still outstanding.

### Transaction model batch

- Original transaction configuration: 396,292 distinct states, depth 19, complete.
- Spare-disk/global-degradation configuration: 147,520 distinct states, depth 15,
  complete. It exposes schedules the original full-disk geometry could hide.
- Both targeted mutations produce the expected invariant violations; parser,
  incomplete-state, unexpected-invariant, and timeout results do not count as
  successful mutation detection.
- `make -C tla test-txn` and `make -C tla mutations` expose the positive and
  sensitivity checks. EC storage TLC remains live; its run has not been replaced.

### Retention model batch

- Owner-pin safety configuration completed: 10,400 distinct states, depth 14.
- Five expected coverage witnesses and both targeted invariant mutations passed.
- The larger time-10 retention safety run remains live, as does EC storage TLC.
  Earlier exploratory retention runs were stopped when the model source changed;
  those partial runs are not counted as passing verification.
- The current-row namespace model, immutable snapshot history, conditional
  liveness, and runtime refinement are still required.

### Physical inspection batch

- Full Go suite and vet passed after read-only physical inspection and CLI wiring.
- Focused corruption/missing-file/identity/prefix inspection tests passed under
  the race detector, including header and lost-trailer cases.
- Quiescence is required for coherent catalog/file comparison; SQLite alone cannot
  freeze another process's physical writes. Reports identify their scope.
- The time-10 retention and EC storage TLC runs remain active and are not reported
  as passing checks.

### Prefix undo-journal batch

- Plog append, commit, and truncation durably save the original physical suffix
  before overwriting it. The journal is checksummed and bound to the superblock;
  writable reopen validates it completely before restoring bytes and the old file
  length. Replay retains the journal until the restored file has been synced.
- Commit syncs the replacement data, removes the journal, and syncs its parent
  directory before reporting success. Extending a rollback range preserves the
  original saved suffix rather than snapshotting already modified bytes.
- Tests tear an acknowledged ragged sector across several block geometries and
  verify exact physical restoration. Tests also cover committed/uncommitted
  truncation, invalid journal checksums, changed identity, and idempotent replay.
  Rebinding disk identity rejects a pending journal.
- Read-only inspection never replays journals and reports pending journal files
  explicitly. Journal data is streamed; large truncations can incur correspondingly
  large journal I/O. The subsequent batch below adds retirement cleanup and
  subprocess crash coverage; fault injection at every ordering boundary remains
  required.
- Full Go suite, vet, and focused storage/inspection race tests passed.
- The final time-10 retention model completed without invariant errors:
  55,454,827 generated / 8,211,719 distinct states, depth 22, 18m43s.
  EC storage TLC remains running; no passing result is claimed for it.

### Journal crash and retirement batch

- Sixteen subprocess crash cases interrupt the actual journal I/O path after
  writes, file sync, rename, directory sync, data sync, replay copy/truncate,
  journal removal, and replacement of an existing journal. Each case reopens
  twice and independently compares the complete expected logical bytes and length
  and verifies stored integrity. Replay cases include persisted corruption of
  acknowledged bytes. Temporary journal files are removed only on writable open
  after successful recovery; read-only inspection still preserves them.
- These are process-exit tests, not power-loss simulations. They do not discard
  the kernel's cache, inject short writes/failed syncs, or establish the filesystem
  and SQLite publication contract at every failure boundary.
- Retirement and failed-copy cleanup now remove plog journals as well as data,
  persisting data-file deletion before discarding recovery evidence. The stray
  sweep recognizes orphan sidecars even when their data file is already absent,
  and retains all sidecars for catalog-owned placements. Regression tests cover
  cleanup, repeated cleanup, and retention of live sidecars.
- Cleanup review found another source-level ordering bug: failed provisioning
  deleted physical files before removing catalog ownership, using the possibly
  cancelled request context for that metadata cleanup. It now retires ownership
  with a context independent of cancellation before physical deletion, and retains
  files if catalog cleanup fails. The partial-provisioning failure regression
  passes; injected ambiguous SQLite outcomes remain to be tested.
- Full Go suite, vet, and focused subprocess/cleanup/provisioning race tests passed.
  The original EC TLC run remains live and is not counted as a completed check.

### Journal directory-sync retry batch

- Fault-injection review found unsafe retry shortcuts in the new journal code.
  After rename succeeded but directory sync failed, a subsequent save accepted
  the journal's presence as sufficient durability. After unlink succeeded but
  directory sync failed, a subsequent commit accepted absence as durable removal.
  The latter could acknowledge a full-block prefix while a power failure could
  still restore an older rollback journal. Writable reopen after replay had the
  same incomplete-retirement issue before a possible disk-identity rebind.
- All three paths now retry directory sync before returning success. Reusing an
  existing journal also verifies that its stored identity matches the data file.
  This adds a directory sync on writable open and on commits without a journal;
  presence/absence alone cannot distinguish a prior failed sync after restart.
- `storage/undo_sync_test.go` injects repeated directory-sync failures and checks
  that writes, commits, and writable reopen continue failing until sync succeeds.
  The write case confirms unchanged physical bytes and logical length; successful
  retries are followed by recovery and full expected-content comparison. The
  commit case ends exactly at a block boundary so a fresh trailer journal cannot
  accidentally mask the missing retirement sync.
- Three compiler-overlay mutations individually removed the retry syncs. Each
  produced its expected regression assertion, rather than a build or unrelated
  test failure; logs are in `/tmp/rose-undo-sync-mutations` for this session.
- Full Go suite, vet, and focused race tests (including all sixteen subprocess
  crash cases) passed. These failures validate directory-sync error handling;
  power-loss cache simulation, short writes/data-sync failures, and SQLite
  publication failure boundaries remain outstanding. The EC TLC run is still live.

### Partial-write and file-sync fault batch

- Found and fixed B9: failed append rolled back the in-memory cursor but left
  partial physical writes beyond it. A subsequent successful commit could preserve
  that tail and make reopening derive incorrect geometry. Commit now truncates
  to its exact data/trailer end before syncing data and retiring the journal.
- `storage/write_fault_test.go` covers short append writes against both ragged and
  full-block prefixes, committing the original prefix or a smaller replacement
  append, and short writes to the ragged sector and open trailer. Reopen compares
  expected bytes and exact length. A compiler-overlay mutation omitting the final
  truncation triggers the intended failure (`/tmp/rose-tail-truncate-mutation`).
- Repeated journal-file sync failures prevent physical data writes. Repeated data
  sync failures during commit and replay preserve the rollback journal until
  recovery can complete. The file-write seam also rejects short writes that fail
  to return an error. Removed the unused `sealSector` method, which had an
  independent physical-write path without journal protection.
- Full Go suite, the complete storage package under the race detector, and vet
  passed. Remaining persistence verification includes power-loss cache simulation,
  truncate/rename/remove failures and ambiguous SQLite publication outcomes; the
  growing set of storage fault tests does not prove the cross-layer protocol.

### Catalog publication crash and lost-reply batch

- Added per-catalog test checkpoints after version/head/reference writes, operation
  state update, lease removal, and successful SQLite commit. Four injected-error
  cases and four subprocess exits exercise those actual transaction boundaries.
  The post-commit error models a lost reply, not an SQLite commit that rolled back.
- Independent raw SQL assertions inspect head/version identity, ordered extent
  bytes, reference multiplicity (including a repeated extent), operation state,
  acknowledged offset, and lease count. Before commit the old state survives;
  after commit the complete new state survives. Retrying returns the same version
  without creating extra versions or incrementing references again.
- Found and fixed B10 in the server: the already-committed Close branch leaked
  operation preparation pins after an ambiguous publication outcome. Successful
  completion now releases those pins in both branches. A server regression
  reconstructs the catalog-committed/handle-active boundary; a compiler-overlay
  mutation removing the release produces the expected leak assertion.
- Catalog crash/error tests and the server pin regression pass under the race
  detector. Full Go tests passed after adding the catalog checkpoints; the server
  suite and vet were rerun after the pin fix. These checks do not inject SQLite
  VFS I/O faults or power-loss cache behavior, and the combined filesystem/catalog
  crash protocol and durable retry-result byte retention remain incomplete.

### Combined storage/catalog publication batch

- Added checkpoints in the production server publication path after shard commit,
  after recording its durable prefix, after complete placement verification, and
  after namespace publication. Eight subprocess cases exercise these boundaries
  with one- and two-disk placements using actual plog files and a persistent
  SQLite catalog. They do not call a separate ideal commit coordinator.
- Recovery checks exact live-file plaintext and length, preserves an older
  snapshot, retries the original operation key, and checks both versions again
  after a second restart. Fresh replacement bytes are deterministic and differ
  from the original; dedup-only success cannot bypass the storage sync checkpoints.
- Four returned-error cases exercise the same boundaries without process restart.
  Before publication the old file remains visible, after publication the new
  file remains visible, and retrying the same handle completes with no leftover
  pin owners. The post-publication case models an ambiguous successful commit.
- Full Go suite, focused combined-path race tests, and vet passed. These tests
  cover the listed process-crash boundaries for mirrored storage; they do not
  establish power-loss behavior, crashes within SQLite VFS calls, inter-shard
  failure schedules, EC promotion/repair crash safety, or shared simulator/runtime
  transition code. Those full-plan obligations remain outstanding.
- The original EC storage TLC handle remains live. Its partial state count is
  progress evidence, not a completed invariant check.

### Shared retirement pin protection

- EC promotion review found B11: staging retirement bypassed compaction's pin
  check and could delete refcount-zero chunks still owned by an unlinked reader.
  The shared retirement function now checks all pin owners and fences pin changes
  through the catalog deletion. Promotion defers retirement when pins remain;
  compaction uses the same guard instead of a separate check.
- The regression exercises both empty staging and staging whose other live chunks
  are moved into EC. The old reader's exact plaintext survives, and releasing its
  final pin allows a subsequent pass to retire the source. A compiler-overlay
  mutation removing the shared guard triggers the expected premature-retirement
  assertion.
- This closes a runtime ownership gap; it does not establish the remaining EC
  promotion/repair crash schedules or durable retry-result roots.

### Commit checkpoint verification

- `ROSE_NO_RAMDISK=1 go test ./... -timeout=180s`: all packages passed.
- `ROSE_NO_RAMDISK=1 go test -race ./... -timeout=180s`: all packages passed,
  including server, storage, metadata, FUSE adapter, and WebDAV tests. Mount-only
  tests still depend on host capabilities; this is not evidence of a mounted
  filesystem or opt-in chaos run.
- `go vet ./...` and staged whitespace checks passed. The shared-retirement
  guard mutation produced its expected regression.
- This is an implementation checkpoint, not completion of the correctness plan.
  The remaining requirements above and the incomplete EC TLC run remain open.
