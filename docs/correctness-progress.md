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
