#!/usr/bin/env python3
"""Check repair generations and cleanup sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-repair-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    source = (root / "RoseRepairEpoch.tla").read_text()
    config = (root / "RoseRepairEpoch.cfg").read_text()
    live_config = (root / "RoseRepairEpochLive.cfg").read_text()
    cases = [
        ("omit_source_compare", [("IF capturedSource = sourceEpoch", "IF TRUE")], "NoStalePublication"),
        ("omit_destination_compare", [("/\\ capturedDest = destEpoch\n", "/\\ TRUE\n")], "NoStalePublication"),
        ("omit_source_increment", [("sourceEpoch' = sourceEpoch + 1", "sourceEpoch' = sourceEpoch")], "NoStalePublication"),
        ("omit_destination_increment", [("destEpoch' = destEpoch + 1", "destEpoch' = destEpoch")], "NoStalePublication"),
        ("omit_availability_admission", [
            ('IF Live THEN "write" ELSE "cleanup"', '"write"'),
            ("          /\\ Live", "          /\\ TRUE")], "NoStalePublication"),
        ("omit_sync", [("durable' = TRUE", "durable' = FALSE")], "NoStalePublication"),
        ("delete_published_destination", [("exists' = IF mapped THEN exists ELSE FALSE", "exists' = FALSE")], "PublishedPreserved"),
        ("leak_rejected_destination", [("exists' = IF mapped THEN exists ELSE FALSE", "exists' = exists")], "NoIdleLeak"),
        ("unfair_cleanup", [("WF_vars(Cleanup)", "TRUE")], "AttemptTerminates"),
    ]
    for name, condition in [
        ("success", "mapped"),
        ("retry_success", "mapped /\\ attempt = 2"),
        ("source_return_rejected", 'phase = "cleanup" /\\ ~mapped /\\ sourceEpoch = 3 /\\ capturedSource = 1 /\\ sourceUp'),
        ("destination_return_rejected", 'phase = "cleanup" /\\ ~mapped /\\ destEpoch = 3 /\\ capturedDest = 1 /\\ Live'),
    ]:
        marker = "NoStalePublication == acceptedSafely"
        cases.append(("witness_" + name, [(marker, marker + "\nWitness == ~(" + condition + ")")], "Witness"))
    results = []
    for name, edits, invariant in cases:
        mutated = source
        for before, after in edits:
            if mutated.count(before) != 1:
                raise RuntimeError(f"{name}: mutation marker is absent or ambiguous")
            mutated = mutated.replace(before, after, 1)
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "RoseRepairEpoch.tla").write_text(mutated)
        temporal = invariant in {"AttemptTerminates"}
        selected = live_config if temporal else config
        if invariant == "Witness":
            selected += "\nINVARIANT Witness\n"
        if temporal:
            selected = "\n".join(line for line in selected.splitlines() if not line.startswith("PROPERTY ") or line == "PROPERTY " + invariant) + "\n"
        (folder / "model.cfg").write_text(selected)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RoseRepairEpoch.tla"]
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
