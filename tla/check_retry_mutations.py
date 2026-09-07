#!/usr/bin/env python3
"""Check retry retention invariant sensitivity; run positive TLC separately."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    root = Path(__file__).resolve().parent
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-retry-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    source = (root / "RoseRetryRetention.tla").read_text()
    config = (root / "RoseRetryRetention.cfg").read_text()
    live_config = (root / "RoseRetryRetentionLive.cfg").read_text()
    cases = [
        ("omit_result_root", [("!.roots = @ \\cup {v}", "!.roots = @"),
                              ("+ 2 * Occurrences(v,c)", "+ Occurrences(v,c)")], "RootsMatchResults"),
        ("drop_repeated_expiry_reference", [("s.refs[c] - Occurrences(v,c)",
            's.refs[c] - (IF c \\in VersionBytes(v) THEN 1 ELSE 0)')], "ExactReferences"),
        ("ignore_reader_pins", [("    /\\ ~Pinned(c) \\* reader ownership", "    /\\ TRUE")], "PinnedReadable"),
        ("omit_open_deadline", [("    /\\ s.now < s.deadline[v] \\* open admission", "    /\\ TRUE")], "NoExpiredAdmission"),
        ("omit_mutation_deadline", [("    /\\ s.now < s.deadline[s.pins[h]] \\* mutation admission", "    /\\ TRUE")], "NoExpiredAdmission"),
        ("reuse_expired_key", [('s.state[v] = "new"', 's.state[v] \\in {"new", "expired"}')], "NoKeyReuse"),
        ("retry_opens_current_head", [("!.pins[h] = v", "!.pins[h] = s.head")], "PinnedResultIdentity"),
        ("unfair_clock", [("WF_vars(Tick)", "TRUE")], "RetentionEventuallyExpires"),
        ("unfair_expiry", [("(\\A v \\in Versions : WF_vars(Expire(v)))", "TRUE")], "RetentionEventuallyExpires"),
    ]
    results = []
    for name, edits, invariant in cases:
        mutated = source
        for before, after in edits:
            if mutated.count(before) != 1:
                raise RuntimeError(f"{name}: mutation marker is absent or ambiguous")
            mutated = mutated.replace(before, after, 1)
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "RoseRetryRetention.tla").write_text(mutated)
        temporal = invariant == "RetentionEventuallyExpires"
        (folder / "model.cfg").write_text(live_config if temporal else config)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RoseRetryRetention.tla"]
        try:
            run = subprocess.run(command, cwd=folder, capture_output=True, text=True,
                                 timeout=args.timeout, check=False)
            log = run.stdout + run.stderr
            expected = ("Temporal properties were violated." if temporal else
                        f"Error: Invariant {invariant} is violated.")
            detected = run.returncode != 0 and expected in log
            if temporal and "Error: Invariant" in log:
                detected = False
            status = "detected" if detected else "unexpected_result"
        except subprocess.TimeoutExpired as exc:
            log = (exc.stdout or b"").decode() + (exc.stderr or b"").decode()
            status = "timeout"
        (folder / "tlc.log").write_text(log)
        results.append(dict(mutation=name, expected_invariant=invariant, status=status,
                            log=str(folder / "tlc.log")))
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))
    return 0 if all(r["status"] == "detected" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
