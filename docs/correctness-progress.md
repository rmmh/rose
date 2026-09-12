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

### EC promotion process-crash batch

- Added six process-exit checkpoints covering destination assignment, EC shard
  commit, durable-prefix recording, individual chunk relocation, job completion,
  and source retirement. The row-writing checkpoints also sit in the coding
  helper shared with EC compaction; this batch directly exercises promotion.
- The fixture uses two distinct records, each exactly twenty stripe rows, in a
  3+1 layout with 64-byte test columns. It checks that both chunks are selected,
  so the first relocation checkpoint interrupts a partially moved source rather
  than an already-complete single-chunk operation.
- Recovery checks both live files and their snapshot, completes residual
  maintenance, requires the staging source and running job to disappear, and
  validates every destination shard/codeword through `verifyProtectedChunk`.
  A second restart checks that both live and snapshot plaintext remain exact.
- Corrected the promotion comments: an interrupted relocation can leave chunks
  split between verified EC destinations and intact staging locations. It does
  not necessarily leave every chunk at its original location.
- Full Go suite, promotion/compaction race tests, vet, and whitespace checks
  passed. These are process-crash tests with small stripe geometry; power-loss,
  inter-shard failures, repair/compaction interruption, and production-size stripe
  coverage remain separate requirements. No completion claim is made for the
  full EC durability protocol or runtime/model refinement.

### Maintenance destination ownership batch

- Found and fixed B12: `SetJobDest` could replace an already-established job
  destination, assign another job's output, or use its own source as destination.
  Assignment now performs an atomic conditional update, permitting identical
  running-job retries while rejecting conflicting, terminal, missing, zero, and
  self-referential claims. Ownership marking rolls back with any rejected claim.
- Independent row assertions check both destination IDs and ownership markers.
  Focused metadata/promotion/compaction race tests passed. Full Go tests and vet
  also passed before this checkpoint was committed.
- A separate provisioning-to-assignment gap remains: failure after creating an
  empty destination but before attaching it to the job can leave an orphan vlog.
  Atomic creation/assignment or resumable cleanup must address that window.
  Stable job destinations do not implement disk/shard placement epochs or bound
  historical job metadata.

### Interrupted maintenance provisioning cleanup

- Maintenance destinations now persist `maintenance_owned` in their initial vlog
  insert. Raw writes and file allocation therefore remain fenced even before
  `SetJobDest` succeeds. Compaction and EC promotion both use this creation mode.
- Writable recovery retires unassigned maintenance destinations before mounting
  plogs. Selection and catalog deletion share a transaction and require zero
  recorded length, no chunk rows, no lease, and no job destination reference.
  Completed jobs still protect their outputs. Physical deletion follows catalog
  deletion; inaccessible or undeletable stray files remain for the existing sweep.
- Provisioning creates each plog row and its shard mapping in one transaction,
  avoiding a process-crash window that left a shard indistinguishable from a raw,
  intentionally unassigned plog. Normal failed-provisioning cleanup still runs
  with cancellation-independent catalog operations.
- Three additional promotion subprocess cases exit after the vlog insert, after
  the first shard mapping, and after full provisioning but before assignment.
  Recovery resumes promotion and leaves exactly one EC destination. Metadata
  tests preserve ordinary, job-owned, nonempty, chunk-bearing, and leased logs;
  raw API tests reject writes to a destination before job assignment.
- Full Go suite, focused metadata/maintenance/provisioning race tests, vet, and
  whitespace checks passed. Returned assignment errors can retain a fenced empty
  destination until restart; immediate bounded cleanup and ambiguous SQLite
  commit outcomes still require work. The tested process-crash gap is now
  reclaimable without exposing its destination to ordinary writers.

### Immediate assignment-error cleanup

- Promotion and both compaction paths now share destination assignment and
  cleanup. On an error, a cancellation-independent transaction conditionally
  retires only that destination if it is still empty, unleased, unreferenced by
  chunks, and unclaimed by any job. Mounted state and files are removed only after
  this catalog decision commits. A successful assignment with a lost reply is
  retained because its durable job reference disqualifies it from cleanup.
- Tests repeat unassigned failures and cancellation five times without restart,
  checking stable catalog/mount counts and deleted physical files. A simulated
  post-assignment lost reply retains mounted files and accepts an identical retry.
  The existing process-crash promotion cases still pass through the shared path.
- Full Go suite, focused metadata/maintenance race tests, vet, and whitespace
  checks passed. Returned assignment errors no longer require restart to reclaim
  ordinary abandoned destinations. Cleanup errors themselves retain evidence or
  leave catalog-free files for the stray sweep; actual SQLite VFS ambiguous errors
  and filesystem failure permutations remain verification obligations.

### Small prefix persistence model

- Added `RosePrefixRecovery`, the first dedicated persistence layer requested by
  the plan. It separates current and durable file/directory state, journal
  installation and retirement, torn overwrites, retry errors, process exits, and
  power-loss abstraction. Journal replay is interruptible. Two successive
  nonempty prefixes expose rollback of previously acknowledged data.
- The bounded run completed: 934 generated / 183 distinct states, depth 34.
  Four targeted mutations remove journal directory sync, data sync, retirement
  directory sync, or the retry installation sync; each fails its expected safety
  invariant. `make -C tla prefix-mutations` runs the positive and negative checks.
- Added an explicit runtime correspondence table and limitations. This model
  checks one plog acknowledgement boundary, not the entire SQLite publication
  transaction. Sector geometry, journal range extension/identity, corruption
  rejection, conditional liveness, and compositional refinement remain required.
- The new make target and mutation runner passed; this batch changes models and
  documentation only. The original large EC placement TLC process remains live
  and is not replaced by this smaller, different-scope model.

### Conditional prefix liveness

- Added a separate liveness configuration extending the exact prefix safety
  actions, with two cumulative faults and weak fairness for successful protocol
  completion. It assumes both modeled prefix requests remain offered. It does
  not constrain the original unrestricted safety configuration.
- `RecoveryCompletes` and `PrefixesEventuallyAcknowledged` pass: 1,255 generated /
  315 distinct states, depth 34. Blocking replay retirement, removing fairness,
  and allowing unbounded faults each produce the expected temporal violation.
- `make -C tla prefix-liveness` passes both positive configurations and all three
  sensitivity checks. Assumptions and scope are recorded in the correspondence
  document. This adds one conditional liveness obligation; maintenance, leases,
  namespace history, and actual scheduler/refinement obligations remain open.

### Reproducible model execution and evidence

- Added `tla/check_models.py` with explicit fast and large configuration sets.
  It copies the exact model/configuration sources and records their hashes, JAR
  hash, Java version, command, seed/fingerprint, state counts, depth, duration,
  exit status, complete logs, and emitted counterexamples. Existing evidence
  directories cannot be overwritten by a new run.
- Passing requires TLC's explicit successful completion message, a nonempty
  complete state graph, zero queued states, and depth/seed data. Parser tests
  reject missing/partial evidence, failures, and timeouts. Reports remain
  incomplete until all selected configurations pass; interruption terminates
  only the runner's own current process and retains its log.
- `make -C tla check-fast` passed its parser tests and all five configurations:
  prefix safety 183 states, prefix liveness 315, transaction 396,292,
  spare-disk transaction 147,520, and snapshot/owner pins 10,400. This batch
  changes verification tooling and documentation only.
- Added [model-verification.md](model-verification.md) with runnable commands,
  artifact interpretation, and scope. The large tier is available for scheduled
  execution; it was not launched alongside the existing live EC run. Repository
  CI scheduling and Go/chaos/mount automation remain unfinished requirements.

### Ordinary verification CI and visible skipped coverage

- Added a main-push, pull-request, and manual GitHub Actions workflow with separate
  ordinary Go, full race, vet, and fast complete TLC jobs. All jobs archive their
  evidence on verification failure as well as success. Permissions are read-only;
  Go uses the module version and model runs use the checked-in JAR.
- Added a local Go runner recording commands, tool version, revision/worktree,
  logs, package outcomes, passed test counts, and explicit failed/skipped test
  identities. Test results cannot pass on empty, malformed, all-skipped, or
  unfinished evidence, even with a zero exit code. Skipped tests are displayed
  in the CI summary and mark coverage partial. Parser regressions cover these
  false-success cases.
- Scheduled large model, heavy chaos/scale, capability-required FUSE mount, and
  deterministic replay automation remain open. The existing large EC TLC run
  remains live; it has not been replaced or declared complete. The workflow has
  been authored locally and has not yet run on GitHub.
- Local validation passed: evidence-parser regressions, ordinary Go and full
  race runs (465 passing test events and nine explicit skips each), vet, and all
  five fast TLC configurations using the workflow's five-minute per-model limit.
  YAML parsing and whitespace checks passed; hosted workflow execution remains
  unverified.

### Required mount and scheduled chaos coverage

- Added weekly/manual runtime jobs for FUSE and opt-in chaos, both under the race
  detector. Chaos records an explicit seed and a 30-second duration. Required
  modes reject skipped tests/packages as well as empty or incomplete execution.
- Corrected the mount test helper: macFUSE options are now restricted to macOS,
  and unmount cleanup is registered before the initialization-handshake check.
  `ROSE_REQUIRE_FUSE=1` turns mount/handshake errors into failures. Ordinary runs
  preserve capability-aware skips.
- Local required-mount execution failed all five mount tests with zero skips,
  as expected on this host without `/dev/fuse`; this proves failure reporting,
  not successful mount coverage. Ordinary FUSE tests passed with capability skips.
  Successful mounted execution and the new hosted jobs remain unverified.
- The first scheduled-command chaos run exposed destructive inspection of live
  plogs: candidate selection used writable open, which can replay pending undo.
  It now uses `InspectPlog`. Storage/FUSE race tests and the four evidence-parser
  regressions passed. The storage undo regression already verifies read-only
  inspection leaves pending writes untouched.
- Both 30-second seed-1 chaos runs failed and are recorded as failures. After
  fixing inspection, the run verified 19 committed files with no read mismatches
  but reported 98 unexpected degraded-write errors and a bitrot repair miss.
  Deferred maintenance completion, abandoned retryable writers, and concurrent
  fault accounting need further investigation; the audit plan records this
  evidence. The new scheduled job exposes this outstanding failure rather than
  treating it as passing or optional coverage.

### Reprotect abandoned file tails

- Fixed B13: lease-free scoped tails no longer indefinitely defer relocation.
  Remount discards abandoned scoped tails by reconciling to the catalog prefix,
  while preserving leased file tails and uncommitted raw tails.
- Added regressions for active lease deferral, abort followed by repair without
  restart, old plaintext preservation, resumed publication, and raw-tail
  protection. Restoring the old relocation guard produces the expected failure.
- The full server suite and focused reprotection/lease/node-return race checks
  passed. The full repository run exposed a separate FUSE timestamp failure
  during an actual mount under escalated execution; that run is failed, not
  passing verification. The audit records the `futimes` EIO for follow-up.
  Chaos ownership cleanup/completion accounting remains unfinished.

### Publish FUSE Create before acknowledging the new name

- Fixed B14 by publishing the initial file through the existing flush protocol
  during Create, while retaining the handle for later writes. A failed initial
  publication aborts its preparation and is returned to the caller.
- The new mount-independent regression fails on the previous implementation
  because Getattr cannot see the new name. It now verifies immediate visibility,
  a timestamp update with no supplied FUSE handle, and timestamp preservation on
  close. A second regression checks rejection during known degradation leaves no
  visible name or prepared operation.
- Required FUSE mounts passed under escalated execution with the race detector,
  including the formerly failing timestamp test. Adapter regressions and vet
  passed, followed by the full repository Go suite. This resolves the observed Create failure; broader cross-adapter
  namespace identity and notifications remain open.

### Explicit chaos abandonment and fault completion

- Failed workload writes now invoke adapter-level abort with a bounded cleanup
  context. The workload retains its restart read lock until cleanup completes;
  successful closed handles make this cleanup a no-op. Cleanup failures fail the
  test even during an injected fault.
- Disk fault completion checks both absence of source shards and absence of a
  running durable job. Accepted/deferred maintenance is retried while workload
  traffic remains concurrent. Each admitted fault has a bounded recovery context
  independent of the run's admission deadline. A final request barrier prevents
  an outage response from being misclassified after the fault flag clears.
- Regressions verify rejected writes leave no prepared operations and a nil
  maintenance result cannot report completion while the durable job remains
  running. Both pass with the race detector.
- The previously failing seed-1 history now passes: 11 completed faults, 107
  successful writes, 59 committed files verified, zero read mismatches, and zero
  operation errors. The history includes reprotection, replacement, outage,
  bitrot repair, and restart. This does not replace deterministic exploration or
  network-disconnect expiry tests; the adapter abort assumption is documented.
- Seed 2 also passed: 13 faults, 117 writes, 62 committed files verified, no
  mismatches or operation errors. Full repository race verification passed with
  476 passing test events and four explicit opt-in skips; heavy chaos was checked
  separately above. Vet and whitespace checks passed.

### Metadata ownership for retained retry results

- Added `write_result_root`, an explicit immutable file-version root with a
  Unix-nanosecond expiry deadline. `CommitWriteOpVersionWithRetention` publishes
  the root, repeated chunk-occurrence references, terminal result, namespace,
  and lease release in the same transaction. A committed retry neither adds a
  second root nor extends the original deadline.
- `ExpireWriteResults` atomically changes the operation to `expired`, releases
  its reference occurrences, and deletes the root. Repeated expiry cannot
  decrement twice. The operation row remains as a key fence; it cannot publish
  again or silently become a new prepared intent.
- The independent catalog checker includes retry roots in its recount and checks
  that each root matches its committed operation's result. Regressions cover
  unlink, snapshot deletion, GC, restart, exact expiry boundaries, repeated
  chunks, idempotent publication, expiry rollback, and publication failure cuts.
- This is metadata support, **not enabled server retention**. Production still
  calls the existing non-retaining entry point. Selecting and exposing the
  retention policy, wiring publication/expiry and active retry pins, enforcing
  expiry at request admission, and bounded expired-key generations remain
  required. The end-to-end historical retry bug is not yet fixed.
- Full metadata race tests, the repository Go suite, vet, and whitespace checks
  passed. Retained-root publication cuts verify rollback before commit and
  retention after an injected lost post-commit reply.

### Server retry-result retention and admission

- Enabled durable result roots for client-supplied operation keys, with a
  24-hour default and positive `SetRetryRetention` / `-retry-retention` settings
  for future publications. Anonymous writes do not promise durable keyed retry
  results. Existing stored deadlines survive restart and policy changes.
- Retry Open now loads and pins the winning operation's historical file instead
  of the current namespace head. Open, Close, and maintenance expire roots and
  fence expired keys. Active handle pins preserve readable bytes across expiry;
  they cannot authorize returning an expired keyed result.
- Added an end-to-end regression spanning overwrite, unlink, GC, compaction,
  restart, matching retries, conflicting bytes, non-resurrection, expiry without
  a maintenance pass, active-reader preservation, and final reclamation. It
  passes; metadata references remain independently checkable.
- Documented B15 and the concrete retention contract. This does not complete
  bounded key generations/tombstone cleanup, obsolete file-row reclamation,
  remote deadline advertisement, or all namespace-history cases.
- Full repository tests, focused retry/publication/handle-expiry race checks,
  vet, and whitespace checks passed. Additional race-tested assertions verify
  the default deadline, rejection of zero retention, unchanged existing deadlines
  after a policy increase, and expiry enforced by Close alone.

### Advertise stored retry deadlines through RPC

- Added `retry_expires_at_ns` to Open and Close responses and regenerated the Go
  protobuf bindings. The value is the stored exclusive Unix-nanosecond deadline;
  zero means no retained committed result. Prepared and anonymous operations do
  not advertise a deadline they have not acquired.
- Close reads the deadline while publication remains serialized, before
  releasing its ownership state. First success, a same-handle retry, and a
  lost-handle keyed retry return the original stored deadline. Retry Open exposes
  the same deadline independently of the current retention configuration.
- Race-tested regressions compare responses with the catalog deadline, verify a
  policy increase cannot extend it, and exercise serialization through real
  in-process gRPC. This completes basic remote deadline advertisement; bounded
  expired-key generations and the wider protocol/model obligations remain open.
- Full repository tests, focused retention/deadline race tests, vet, and
  whitespace checks passed.

### Fence expired mutations on existing retry handles

- Fixed B16: existing keyed handles now reject Write, handle Truncate, and handle
  timestamp updates admitted at or after the stored retry deadline, even when
  maintenance and other Open/Close calls have not expired the root yet.
- Write-operation lookup returns state and deadline from one joined SQL snapshot.
  A concurrent expiry cannot produce an old committed state paired with a
  missing deadline. Rejection leaves handle cache, timestamp, and read pins
  untouched; previously opened immutable bytes remain readable.
- Exact-deadline regressions pass under the race detector. Removing the deadline
  predicate makes all three expired mutations succeed, and the regressions fail
  at their expected assertions. This adds mutation admission coverage; it does
  not complete bounded key generations or namespace identity semantics.
- Full repository tests, full metadata race tests, focused handle-retention race
  tests, vet, and whitespace checks passed.

### Model retry retention, reader ownership, and expiry progress

- Added `RoseRetryRetention`, a bounded model of exact ordered extent references,
  independent retry-result roots, namespace/snapshot roots, tombstones, deadline
  admission, reader pins, GC, and process loss of volatile ownership. An
  independent requested-result record checks that retries pin the winning version.
- A separate liveness configuration requires roots to expire under weakly fair
  clock and expiry scheduling. It imposes no fairness on reader release and makes
  no unconditional physical reclamation claim.
- Added the two complete configurations to fast evidence collection/CI and a
  positive-plus-mutation make target. The CI timeout now accommodates seven
  five-minute configuration limits plus setup. Runtime correspondence and omitted
  layers are explicit; the original large EC run remains live and incomplete.
- Both retention configurations completed: 29,980 generated / 5,531 distinct
  states, depth 13. All seven fast configurations passed with archived source
  hashes and complete TLC evidence. Seven safety mutations and two fairness
  mutations produced their expected invariant or temporal violations.
- The public `make -C tla retry-mutations` target, evidence-parser tests, workflow
  YAML parsing, and whitespace checks passed. No Go implementation changed.

### Reprotect job growth and repeated failure cycles

- Fixed B17: scanning an empty failed disk no longer creates one completed job
  per pass. A running job whose last shard already moved is still finalized;
  subsequent passes preserve that completed row.
- StartReprotect now checks current source mappings before returning an old
  completed job. A disk that returns, gains new shards, and fails again receives
  a new repair job; retries after that repair return its completion.
- Regressions verify zero growth over repeated empty scans, recovery of an
  interrupted final step, distinct jobs for two actual failure cycles, stable
  current-result retries, and retained plaintext. Running the tests against the
  previous implementation exposes both expected failures.
- General terminal-job reclamation and generation-based stale-completion fencing
  remain separate requirements; this fix removes idle work and the observed
  repeated-failure shortcut.
- Full repository tests, focused reprotection/recovery race tests, vet, and
  whitespace checks passed.

### Validate replacement intent in the catalog transaction

- Fixed B18: `GetOrCreateReplaceJob` compares the requested destination with the
  running job inside the transaction. Two callers cannot both succeed with
  different destinations after racing past RPC-level checks. Identical retries
  preserve the original job, and invalid zero/self replacements create no row.
- The concurrent regression passes with the race detector and fails against the
  previous implementation because both conflicting requests are acknowledged.
  This establishes replacement intent equality, not placement-generation or
  late-I/O completion fencing.
- Full repository tests, focused metadata/server replacement and reprotection
  race tests, vet, and whitespace checks passed.

### Atomic vlog-job creation and independent ownership checking

- Fixed B19: compaction, promotion, and scrub-repair job creation now share a
  transaction across lookup and insert. The database additionally enforces one
  running job per kind/source vlog with a partial unique index; completed history
  does not prevent a new pass.
- Concurrent regressions converge on one job, preserve independent sources, and
  permit later passes. Direct duplicate insertion is rejected, while the read-only
  checker reports duplicate owners if the index is absent. The previous code
  accepted duplicate rows and returned different IDs in the concurrent test.
- This strengthens the catalog boundary independently of the server's broader
  vlog lock. Cross-kind conflicts, historical-job reclamation, and placement
  generations remain open requirements.
- Full repository tests, full metadata race tests, focused ownership regressions,
  vet, and whitespace checks passed.

### Preserve running maintenance destinations across other rewrite passes

- Fixed B20: compaction and promotion defer when their proposed source is another
  running job's destination. Catalog deletion checks the same durable ownership
  within its transaction. In-place shard repair remains admissible; completed
  jobs release the rewrite hold.
- Regressions cover both rewrite paths against an assigned empty staging output:
  repeated passes preserve its catalog and physical files, direct deletion is
  rejected, the original job resumes, and its output can be compacted afterward.
  Running the regressions against the previous implementation reproduces deletion
  of the running destination.
- Full repository tests, maintenance/compaction/promotion/reprotection race tests,
  vet, and whitespace checks passed.

### Cancel repair work superseded by source retirement

- Fixed B21: vlog retirement cancels running scrub-repair jobs for that source in
  the same catalog transaction. Recovery no longer repeatedly schedules repair
  of a source that compaction has removed. Historical cancelled rows remain.
- The metadata regression injects cancellation failure and verifies both source
  preservation and unchanged job state, then verifies successful and repeated
  retirement. The server regression verifies compaction removes obsolete work
  from the recovery job list. Both fail against the previous implementation and
  pass with the race detector after the fix.
- Full repository tests, vet, and whitespace checks passed.

### Preserve incomplete repair jobs until actual completion

- Fixed B22 in both scrub and offline-shard repair: per-shard failures keep the
  durable job running even when the aggregate call returns no top-level error.
- Regressions with a temporarily unavailable healthy source verify three failed
  attempts retain one job ID, source return permits completing that job, and
  repaired reads preserve the original payload. Both old paths prematurely
  completed the job; the fixed regressions pass under the race detector.
- Full repository tests, vet, and whitespace checks passed.

### Share the actual publication protocol with scheduled tests

- Replaced the separate ideal shard-record coordinator with the production file
  publication sequence. `publishPreparedVersion` now drives the shared stepper
  through an adapter for admission, exact prefix sync/recording, canonical
  placement verification, and SQLite namespace/result publication.
- Existing namespace/operation/vlog/pin ownership and crash checkpoint names are
  preserved. Failed invocations stop; ambiguous publication errors are resolved
  through durable operation retries rather than resuming stale control state.
- Replaced idealized transaction interleaving tests with independent ordering
  checks at every effect failure and checkpoint, including zero-lease publication,
  an advancing append cursor after sync, and post-publication failure. Real server
  publication and retry tests now execute the same protocol code.
- Rewrote the simulation design to distinguish this implemented boundary from
  remaining cloneable disk/catalog/clock/ownership state, preparation transitions,
  multi-writer exploration, trace minimization, and reduction validation.
- Full repository tests, full race tests (including existing publication process
  crash coverage), vet, and whitespace checks passed. Deliberately skipping
  verification and recording an incorrect prefix each fail the independent
  protocol oracle; both mutations were reverted after checking sensitivity.

### Audit lock order and retain scrub client ownership

- Added `docs/concurrency-ownership.md` covering audited acquisition edges,
  registry lock discipline, handle/pin/lease/job/result ownership, publication
  and retirement intervals, and prerequisites for moving I/O outside broad locks.
- Fixed B23: full scrub holds topology ownership across map traversal and client
  inspection. A paused scrub allows old compaction to retire its backing client;
  the regression now passes under the race detector and verifies maintenance
  resumes after inspection. Pointer snapshots alone would not preserve lifetime.
- Full repository tests, vet, and whitespace checks passed. The lock document is
  an audited implementation contract, not a claim of exhaustive deadlock proof.

### Reclaim unowned immutable file versions

- Added atomic, bounded version collection to ordinary GC, with indexed owner
  lookups for namespace heads, snapshots, retry roots, and prepared/committed
  operations. Each pass removes at most 1,000 rows; deletion does not repeat chunk
  reference decrements already performed when roots were removed.
- Metadata tests cover each owner, expiry, batch limits, repeated collection, and
  transaction rollback on deletion failure. The server retention/restart test now
  verifies an expired unowned version row disappears while an open reader retains
  its original bytes through content pins. Previous GC fails that assertion.
- Legacy committed operation references remain conservative owners. Historical
  operations/jobs and bounded key generations still need retirement policies;
  this change does not silently narrow their existing retry semantics.
- Full repository tests, focused metadata/reader-retention race tests, vet, and
  whitespace checks passed.

### Remove encryption keys from bootstrap logging

- Fixed B24: catalog creation no longer emits the persisted cluster encryption
  key to stderr. The key-stability regression also omits keys from failure output.
- A subprocess test captures bootstrap output and compares it with persisted key
  representations without echoing captured output or key material. The previous
  bootstrap fails this test; the fixed bootstrap and identity tests pass under
  the race detector. Key derivation and persisted identity are unchanged.
- Catalog key custody, historical logs, stream identity/nonce uniqueness, and
  record authentication remain separate review obligations.
- Full repository tests, vet, and whitespace checks passed.

### Check protection geometry and disjoint referenced records

- Extended the independent catalog checker to report unknown protection schemes,
  invalid mirror/EC geometry, incompatible required-shard counts, and malformed
  or underprotected EC staging targets. Legacy unscoped rows with no recorded
  requirement retain their existing interpretation.
- Added a sorted interval check for distinct referenced chunk records, including
  their stored headers. Repeated occurrences of one canonical chunk remain valid;
  distinct adjacent records pass, while equal or overlapping positions fail.
  Unreferenced garbage locations are not treated as live interval owners.
- Corruption regressions fail against the previous checker. Positive geometry
  fixtures and adjacency/multiplicity controls guard against rejecting valid
  catalogs. This covers catalog geometry, not plaintext authentication or achieved
  physical protection under arbitrary disk faults.
- Full repository tests, focused checker race tests, vet, and whitespace checks
  passed.

### Verify referenced plaintext during read-only physical inspection

- Added a read-only plog reader and reconstruction adapter used by physical
  inspection without server recovery, journal replay, repair, or writable file
  opens. Referenced records are read from one catalog snapshot, decrypted with
  persisted cluster/vlog identity, and checked against header lengths and full
  120-bit canonical content addresses. Hashing streams payloads in bounded batches.
- The checker independently implements the scoped hash input and never prints
  decryption keys. Physical files must still be quiescent for a coherent result.
  Existing ciphertext integrity, identity, prefix, journal, and catalog checks
  remain independent evidence.
- Tests cover single-copy, mirror, and a small EC geometry, verify file bytes do
  not change, and detect wrong keys plus full-address changes that preserve the
  64-bit stream selector. The previous checker misses both mismatches.
- This establishes referenced plaintext readability against the trusted catalog;
  it is not cross-shard replica/parity equivalence, volatile-owner inspection,
  adversarial record authentication, or exhaustive destructive-fault coverage.
- Full repository tests, focused physical-inspection race tests, vet, and
  whitespace checks passed. The positive EC fixture uses reduced stripe geometry;
  it does not replace large-layout or exhaustive shard-loss verification.

### Independently inspect stored redundancy

- Physical inspection now compares every mirror over the recorded prefix and
  verifies every complete EC codeword for vlogs holding referenced chunks.
  The traversal is independent of production `Vlog.VerifyAll`, uses read-only
  clients, and requires every copy/shard instead of reconstructing over absence.
- Regressions reseal a divergent mirror or parity shard with valid physical
  integrity metadata. Ordinary plaintext reconstruction still succeeds, but the
  new checker reports lost redundancy. Both previous inspection paths miss this
  condition. Before/after comparisons preserve the inconsistent physical evidence.
- Positive and negative tests use mirror and reduced EC stripe fixtures. This
  supplements plaintext identity and catalog protection counts; it does not
  establish volatile-owner correctness, placement epochs, or exhaustive loss-budget
  histories.
- The mirror fixture uses a validly encrypted provenance-header difference so
  concurrent read-winner selection cannot turn it into a plaintext failure.
  Five repeated focused race runs, the full repository suite, vet, and whitespace
  checks passed with that fixture.

### Model maintenance ownership and conditional completion

- Added a bounded compaction/promotion model with durable destination ownership,
  copy-before-repoint ordering, canonical source rechecking, two independent I/O
  holders, source retirement, and process loss of volatile holds. It represents
  compaction's retire-before-done and promotion's done-before-retire orderings.
- Safety checks reader/published bytes and running destination lifetime. Separate
  conditional liveness requires fair job steps, reader release, and retirement;
  it makes no progress promise under indefinite holds or unavailable allocation.
- Added both configurations to fast evidence collection and CI, plus a public
  positive-and-mutation target. The CI timeout now accommodates nine per-model
  five-minute limits plus setup. The existing large EC run remains separate.
- The runtime correspondence table states which steps abstract successful sync,
  atomic catalog transitions, finite never-reused identities, and one content
  location. It does not substitute for torn-write, placement-generation, full
  namespace, or destructive disk-loss histories.
- Final safety and liveness configurations each completed with 4,303 generated /
  956 distinct states, depth 13. All nine fast configurations passed with archived
  source/tool hashes and complete TLC results. Four safety and three temporal
  mutations produced their expected counterexamples.
- The public maintenance make target, evidence-parser tests, workflow YAML
  parsing, and whitespace checks passed. No Go implementation changed.

### Enforce shard replacement ownership at the catalog boundary

- Fixed B25: replacement rejects zero/self plog IDs, shared source ownership,
  absent/owned destinations, and colocation with another shard. Checks occur in
  the same transaction as repointing and old-row deletion.
- Shared sources are rejected because subsequent server cleanup closes/removes the
  old physical file; keeping only a shared catalog row would not preserve its
  clients. Tests verify failed replacements leave rows/mappings intact, and an
  exclusive valid replacement succeeds after the extra source owner is removed.
- The previous helper accepts the destructive cases. Focused metadata regressions
  pass under the race detector. Source generation fencing is implemented in the
  following checkpoint; the complete placement protocol remains unfinished.

- Persisted source-vlog placement generations and repair completion fencing:
  `vlog.placement_epoch` advances atomically through catalog triggers on mapping,
  lease, identity, availability, prefix, and protection changes. Repair captures
  it before reconstruction and requires equality inside the replacement
  transaction. State changes followed by return to the original state invalidate
  old work. Failed comparisons retain the old mapping and discard the unpublished
  destination; fresh attempts succeed. Positive integer constraints reject epoch
  overflow atomically, and the independent catalog checker reports nonpositive
  epochs. No legacy catalog migration is provided for this research schema.
  Regressions cover six change/return schedules, persistence across reopen,
  transaction rollback, overflow, and an actual repair paused before repointing.
  Removing the generation comparison causes both metadata and server regressions
  to fail. Full ordinary and race suites and `go vet ./...` pass.
  This is a source fence, not permission to release `vlogMu` during repair:
  unassigned destination disk changes do not invalidate the source epoch, and
  remote completion generations, other catalog transitions, and complete I/O
  lifetime holds still need coverage.
- Full repository tests, focused server repair/reprotection/replacement race tests,
  vet, and whitespace checks passed.

### Fence the unassigned repair destination

- Added durable `plog.placement_epoch`, advanced atomically on physical identity,
  location, recorded length, mapping ownership, and disk/node lifecycle changes.
  It covers unassigned destinations, so a disk/node change and return cannot pass
  merely because the source vlog generation is unchanged. Plog allocation never
  reuses IDs. Repair captures the destination epoch before physical writes and
  compares it together with the source epoch in the repoint transaction, which
  also requires an active destination disk and working node.
- Metadata regressions isolate eleven destination lifecycle schedules, including
  disk/node removal and reinsertion, ownership reuse, unavailable destinations,
  and persistence across reopen. An overflow regression checks atomic rollback
  of the underlying disk-state mutation. The catalog checker checks positive
  generations for unassigned as well as assigned plogs.
- Real repair regressions inject destination disk/node return before repointing:
  the source epoch stays unchanged, stale work is rejected, unpublished files
  and catalog rows are removed, and a fresh repair preserves published bytes.
  Removing the destination comparison defeats metadata and server regressions;
  removing the availability predicate permits currently unavailable destinations.
- Full-suite validation exposed an inconsistent virtual scale fixture: absent
  roots and mismatched disk identities caused recovery to persist failed disks
  while the fixture still selected them as active. The fixture now creates
  matching identity markers and supplies every catalog disk root to recovery.
  Its instances exercise the current local catalog authority, not independent
  remote disk ownership. An assertion checks recovered states against the fixture.
- Broad topology locks remain required. Generations do not roll back physical
  writes, prevent clients from closing during I/O, or yet fence other maintenance
  transitions or delayed remote operations. No schema migration is provided.

- Final full repository tests, full race suite, `go vet ./...`, and whitespace
  checks passed after the fixture correction.

### Requested stopping point

- Stopped the original large `RoseStorageEC.cfg` TLC process on user request;
  it exited with signal status 143. The final progress sample at 19:32:13 UTC on
  2026-09-07 reported 2,005,332,770 generated states, 124,736,536 distinct states,
  16,295,768 queued states, and depth 26. This is incomplete evidence, not a pass.
  Its existing source/configuration and log remain at `/tmp/rose-impl-tla`.
- The full correctness plan remains unfinished. Resume from the source and
  destination repair fences, complete I/O ownership and remote generation work,
  and the remaining acceptance criteria recorded above; do not infer completion
  from this checkpoint.

### Resume: model the actual repair generation boundary

- Added `RoseRepairEpoch` safety and conditional attempt-termination configurations
  for source/destination capture, durable copy, generation-checked repoint,
  cancellation/ambiguous response, and conditional cleanup. Two lifecycle events
  permit change/return histories; two fresh destination identities permit retry.
  Independent freshness history detects missing epoch increments as well as
  missing comparisons, avoiding an oracle that merely repeats implementation
  counters. A missing availability admission pair, skipped sync, destructive or
  leaking cleanup, and unfair cleanup also produce counterexamples.
- Both positive configurations completed: 1,250 generated / 597 distinct states,
  depth 12, empty queue. All eleven fast configurations completed with source/tool
  hashes in `/tmp/rose-repair-fast-final/results.json`. Nine mutations and four
  reachability witnesses passed in `/tmp/rose-repair-mutation-final/results.json`.
  Evidence-parser tests passed. The public make target runs positive checks before
  sensitivity checks. CI's fast-model budget now covers eleven per-model timeouts.
- The model covers serialized live-process completion, not physical power loss or
  process-crash cleanup. Tracing that distinction exposed B26: repair allocation
  leaves an unassigned destination indistinguishable from a raw plog if process
  exit bypasses its local cleanup. A temporary diagnostic unwound at the real
  pre-repoint hook, closed storage, and recovered; one unassigned destination
  remained. Durable repair allocation ownership and subprocess regressions remain
  required. No production Go code changed in this checkpoint.

### Recover interrupted repair destinations (B26)

- `MakeRepairPlog` records `repair_owned` in the allocation statement, eliminating
  the crash window between allocation and a separately recorded owner. Published
  destinations keep this marker; their vlog mapping prevents reclamation.
- Before mounting plogs or resuming maintenance, quiescent recovery deletes marked
  unassigned rows with a transactional `DELETE ... RETURNING`. It returns physical
  cleanup candidates only after commit. Raw unassigned plogs are preserved.
  A failed deletion transaction returns no candidates and leaves the row intact;
  repeated successful retirement is idempotent. Physical deletion follows catalog
  retirement; failed/unreachable or interrupted deletion uses the existing stray
  sweep. No legacy schema migration is provided for this research catalog.
- Added subprocess exits after destination allocation, before repointing, and
  after repointing. Recovery removes unpublished catalog rows and files, retains
  the committed replacement and original file contents, and preserves an unrelated
  raw plog byte-for-byte. Removing the durable allocation marker reproduces the
  leaked row/file. Metadata rollback and focused repair tests pass under race.
- The post-repoint fault hook preserves the existing cancellation boundary:
  an ordinary hook error still completes remounting before returning; process exit
  recovers the already-committed mapping. `RoseRepairEpoch` remains a model of
  live-process completion, not a power-loss or process-recovery refinement.
- Full repository tests, full race suite, the additional post-repoint error and
  subprocess regressions, `go vet ./...`, and whitespace checks passed.

### Keep failed repair destinations out of raw RPC admission (B27)

- Auditing B26's durable marker exposed a follow-on admission gap: rejected repair
  plus failed catalog cleanup retains an unassigned mounted destination, and raw
  write/commit previously considered absence of a vlog mapping sufficient. Raw
  bytes acknowledged there would later be reclaimed as abandoned repair data.
- Both raw mutation RPCs now use `RawPlogWritable` under their existing topology
  lock. Admission requires an existing row, no vlog mapping, and no repair marker.
  Repair-owned and missing catalog rows remain excluded even without a mapping.
- The regression runs the real repair path with rejection and cleanup failure,
  checks raw write rejection and unchanged length, and closes the protected plog
  to detect an accidental raw commit. An ordinary raw plog still writes, commits,
  and verifies. Removing the marker predicate triggers both negative assertions.
- Full repository tests, focused metadata/server repair and raw-operation race
  tests, `go vet ./...`, and whitespace checks passed.

### Model repair recovery separately from generation admission

- Added `RoseRepairRecovery`: one source, one raw plog, and one repair destination;
  separate catalog/file sets; atomic ownership at allocation; pre/post-publication
  crash recovery; and catalog retirement before physical deletion. It admits two
  crashes and two temporary outages. The existing generation model remains a
  separate bounded layer, without claiming a mechanically proved composition.
- Safety preserves published and raw plogs and excludes terminal abandoned repair
  rows. Conditional liveness requires finite crashes/outages and weak fairness for
  recovery, media return, and sweeping. It checks eventual completion and eventual
  catalog/file agreement; permanent loss and torn SQLite/filesystem persistence
  remain outside this layer.
- Both new configurations completed with 359 generated / 155 distinct states,
  depth 12. Seven fault mutations and four reachability witnesses are detected;
  these include missing allocation ownership, removal of raw/published data,
  unsafe sweeping, unfair recovery/sweep/return, and a crash between retirement
  and unlink. The public make target includes positive checks before sensitivity.
- All thirteen fast configurations passed with archived tool/source evidence in
  `/tmp/rose-repair-recovery-fast/results.json`; final mutation/witness results
  are in `/tmp/rose-repair-recovery-mutations-final/results.json`. Evidence-parser
  tests, workflow YAML parsing, and whitespace checks passed. CI's fast-model
  timeout now covers thirteen bounded checks. No production Go code changed.

### Fence relocation source placement and reject invalid moves (B28)

- `migratePlogLocked` rejects same/zero disk IDs before physical copy and captures
  the source plog epoch before I/O. The catalog move now compares source disk and
  epoch, rejects missing rows, checks destination disk/node availability, and
  prevents colocation with another shard of any owning vlog.
- The catalog transaction returns its post-trigger epoch. Remount rollback must
  use that generation, so it cannot silently overwrite an intervening source
  change. Draining destinations remain allowed for rollback to the original
  still-readable disk; forward allocation policy stays with the server.
- Regressions cover missing/stale source, missing/failed destination, failed node,
  colocation, return to an old source state, and stale/current rollback. A real
  same-disk relocation test checks unchanged source file bytes and readable
  published content. Removing the pre-copy guard causes source destruction and
  fails that regression. The broad topology lock remains required: this does not
  complete destination generations, source vlog-prefix fencing, or I/O holds.
- Full repository tests, focused relocation/drain/rebalance/replacement race tests,
  `go vet ./...`, and whitespace checks passed.

### Include logical owner generation in relocation admission

- `CapturePlogRelocation` captures the plog and vlog generations in one query and
  requires exactly one owning shard before I/O. `MovePlogToDisk` repeats membership
  and exclusive ownership checks in the repoint transaction. The server remounts
  one owning vlog before closing the old client, so shared sources are rejected.
- Source-prefix and lease changes invalidate relocation even when the physical
  plog generation stays unchanged. The successful move returns both post-trigger
  generations; rollback must use both instead of sampling an intervening state.
- Regressions isolate prefix change/return, lease acquisition/release, shared-source
  admission, and rollback with a fresh plog but stale vlog token. Removing either
  the vlog comparison or exclusive-owner predicates produces expected failures.
  Destination lifecycle fencing, physical I/O holds, and safe handling of all
  ambiguous cleanup outcomes remain required before reducing topology locks.
- Full repository tests, focused relocation/drain/rebalance/replacement race tests,
  `go vet ./...`, and whitespace checks passed.

### Fence relocation destination and rollback disk lifecycles

- Added `disk.placement_epoch` and a one-row persistent `placement_clock`.
  Triggers allocate newer generations on disk insertion, identity/location/state
  changes, and node lifecycle changes. Disk/node deletion and reinsertion cannot
  recreate an earlier generation even with repeated IDs and UIDs. Integer overflow
  aborts the underlying mutation; the clock uses constant catalog space.
- Relocation captures destination and original-disk tokens before copying. The
  copied header uses the UID captured with the destination generation, and the
  catalog repoint compares that generation along with source plog/vlog tokens.
  Rollback uses the original disk token captured before I/O. State change/return
  cannot silently validate old work on either side of the move.
- Metadata regressions isolate destination disk/node return, identity return,
  disk/node deletion and reinsertion, persistence across reopen, overflow rollback,
  and stale/fresh rollback-disk tokens. A real relocation injects destination
  return before repoint, checks old placement and rejected-file cleanup, then
  retries successfully with identical published bytes. Removing the disk comparison
  defeats both metadata and server regressions.
- The read-only checker reports a missing/nonpositive clock and disk generations
  outside its range. No legacy migration is added for this research schema.
  Broad topology locks remain: complete I/O holds, ambiguous completion cleanup,
  and model/runtime composition are still outstanding.
- The initial full race run exposed B29 in `TestChaosClusterSmoke`: another
  promotion pass had already retired a listed candidate and finished its job.
  Promotion treated that stale candidate as an error unless it could newly finish
  a running job. Absent candidates now return no work after attempting intent
  cleanup, preserving real cleanup errors. A deterministic completed-promotion
  regression fails against the previous implementation and passes with the fix;
  the focused chaos smoke test also passes under race.
- Final full repository tests and vet passed. All non-server packages passed the
  initial full race run; the complete server race suite passed on rerun after B29
  was fixed (128.646s). The initial failure remains recorded above.
- Resumed the previously interrupted EC check from its retained checkpoint at
  `/tmp/rose-impl-tla/states/26-09-07-15-10-04.733`, using the original snapshot,
  four workers, fingerprint 65, and seed 8140933882694200072. The snapshot differs
  from current `RoseStorage.tla` only in an explanatory comment ("refines" versus
  "models the required"). TLC reported that checkpoint recovery started; its log
  is `/tmp/rose-impl-tla/ec-resumed.log`. This remains an in-progress check, not
  verification success; do not start another EC run while it is active.

### Exercise a second crash during repair recovery

- Added the `repair-catalog-retired` checkpoint after the recovery deletion
  transaction commits and before any retired file is unlinked. A returned hook
  error stops recovery at that boundary; a subprocess exit bypasses all cleanup.
- The process regression first crashes repair before repointing, then crashes the
  recovering process after catalog retirement. It independently confirms that
  the row is absent while the physical file remains. Restart and the real stray
  sweep remove that file, a repeated sweep removes nothing, and published/raw
  bytes and their catalog files remain intact. This connects the companion
  recovery model's retirement/unlink crash witness to a concrete runtime history.
- The existing EC process successfully recovered 114,924,097 distinct states and
  17,733,754 queued states and resumed exploration. Recovery inputs/tool hashes,
  Java version, seed, fingerprint, and source-comment difference are recorded in
  `/tmp/rose-impl-tla/ec-resume-manifest.json`. Its generated-state counter after
  recovery is not the original run's cumulative generated count. The check is
  still incomplete; continue polling the same process rather than duplicating it.
- Full repository tests, the repair subprocess-crash suite under race, vet, and
  whitespace checks passed.
