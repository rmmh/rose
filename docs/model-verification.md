# Reproducible model checks

Run the fast complete configurations with:

```sh
make -C tla check-fast
```

This checks prefix safety, conditional prefix liveness, both transaction
configurations, and the small snapshot/owner-pin configuration. It also tests the
evidence parser. It does not run the large placement or retention state spaces,
Go tests, or the separate mutation and coverage checks.

For an explicit artifact directory and time limit per configuration:

```sh
python3 tla/check_models.py --tier fast --output-dir /tmp/rose-model-evidence --timeout 300
```

The output directory must be empty. Each configuration gets copies of all local
TLA+ modules and configurations plus its complete TLC log and any counterexample
artifacts emitted by TLC. `results.json` records source/configuration SHA-256
hashes, the checked-in JAR hash, Java version, command, random seed, fingerprint,
generated/distinct/remaining state counts, depth, elapsed seconds, and exit status.
The report is updated after each configuration; an interrupted batch remains
incomplete. Preserve this directory as the model-check artifact.

A check is `passed` only when TLC exits successfully, explicitly reports complete
model checking without errors, reports a nonempty state space with zero states
left on its queue, and provides complete depth/seed information. An observation
of progress, empty output, timeout, invariant failure, or interrupted run cannot
become passing verification evidence. Each report covers only its listed
configurations; it does not establish runtime refinement.

The large tier runs the metadata model, all four storage placement configurations,
and the time-10 retention configuration. It is intended for a separately scheduled
worker with enough time and storage:

```sh
make -C tla check-large
```

That target permits up to twelve hours per configuration. A timeout is still a
failed verification gate, not a smaller substitute run. Do not launch a duplicate
of an already-running configuration merely because an observation timed out.

Sensitivity and coverage checks remain available separately:

```sh
make -C tla mutations
make -C tla snapshot-coverage
make -C tla snapshot-mutations
make -C tla prefix-mutations
make -C tla prefix-liveness
```

## Repository CI

`.github/workflows/correctness.yml` runs on main pushes, pull requests, and manual
dispatch. Separate Go test, full race, vet, and fast-model jobs preserve evidence
even when a verification step fails. Go follows `go.mod`; TLC uses the checked-in
JAR and Temurin Java 21. The workflow has read-only repository permissions.

The Go jobs can also be reproduced locally, using a new output directory for
each invocation:

```sh
python3 scripts/check_go.py test --output-dir /tmp/rose-go-test
python3 scripts/check_go.py race --output-dir /tmp/rose-go-race
python3 scripts/check_go.py vet --output-dir /tmp/rose-go-vet
```

The runner disables RAM disks and opt-in heavy chaos, disables test-result caching,
and gives each Go test binary 180 seconds. It records the command, Go version,
revision, dirty worktree, duration, exit code, package outcomes, test counts,
failed tests, and every skipped test. Raw stdout/stderr and a concise summary are
retained alongside `results.json`. Raw logs can include ephemeral test credentials;
the console summary only prints outcomes and test identities.

A Go test gate requires a zero exit code, valid event data, completed packages and
started tests, and at least one passing test. Skips remain visible in the report
and CI step summary; a passing gate with skips is explicitly partial coverage.
Vet uses its process exit status. Reports start incomplete so an interrupted run
cannot leave an earlier passing result. Evidence directories cannot be reused.
CI artifacts are retained for 14 days; download evidence needed for longer-term
review. CI service cancellation or setup failure can prevent artifact upload and
must not be interpreted as completed verification.

The fast-model job allows five minutes per complete configuration. The large tier
is not substituted into that timeout and is not yet scheduled in CI. Scheduled
large models, chaos/scale tests, capability-required mount tests, and deterministic
trace replay remain requirements of the correctness plan. Adding this workflow
does not establish that GitHub has executed it or that those omitted suites pass.
