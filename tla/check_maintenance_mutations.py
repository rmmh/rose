#!/usr/bin/env python3
"""Check maintenance ownership and progress sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-maintenance-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    source = (root / "RoseMaintenance.tla").read_text()
    config = (root / "RoseMaintenance.cfg").read_text()
    live_config = (root / "RoseMaintenanceLive.cfg").read_text()
    cases = [
        ("retire_destination", [("    /\\ ~DestinationHeld(v)", "    /\\ TRUE")], "DestinationPreserved"),
        ("retire_reader", [("    /\\ ~ReaderHeld(v)", "    /\\ TRUE")], "ReaderReadable"),
        ("publish_without_durable_copy", [
            ("durable' = durable \\cup {dest[j]}", "durable' = durable"),
            ("/\\ dest[j] \\in durable /\\ dest[j] \\in mounted", "/\\ dest[j] \\in mounted")], "PublishedReadable"),
        ("repoint_stale_source", [
            ('ELSE /\\ UNCHANGED location', "ELSE /\\ location' = dest[j]")], "PublishedReadable"),
        ("unfair_finish", [("WF_vars(Finish(j))", "TRUE")], "JobsFinish"),
        ("unfair_reader_release", [("WF_vars(ReadEnd(h))", "TRUE")], "UnusedEventuallyRetires"),
        ("unfair_retirement", [("WF_vars(Retire(v))", "TRUE")], "UnusedEventuallyRetires"),
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
        (folder / "RoseMaintenance.tla").write_text(mutated)
        temporal = invariant in {"JobsFinish", "UnusedEventuallyRetires"}
        selected = live_config if temporal else config
        if temporal:
            selected = "\n".join(line for line in selected.splitlines() if not line.startswith("PROPERTY ") or line == "PROPERTY " + invariant) + "\n"
        (folder / "model.cfg").write_text(selected)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RoseMaintenance.tla"]
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
