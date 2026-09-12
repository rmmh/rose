#!/usr/bin/env python3
"""Check repair recovery ownership and cleanup sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-repair-recovery-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    source = (root / "RoseRepairRecovery.tla").read_text()
    config = (root / "RoseRepairRecovery.cfg").read_text()
    live_config = (root / "RoseRepairRecoveryLive.cfg").read_text()
    cases = [
        ("omit_allocation_owner", [("owned' = owned \\cup {2}", "owned' = owned")], "NoAbandonedCatalog"),
        ("retire_raw", [("p \\in owned /\\ p # mapped", "p # mapped")], "RawPreserved"),
        ("retire_published", [("p \\in owned /\\ p # mapped", "p \\in owned")], "PublishedPreserved"),
        ("sweep_without_catalog_check", [("/\\ p \\notin catalog", "/\\ (p = 0 \\/ p \\notin catalog)")], "PublishedPreserved"),
        ("unfair_recovery", [("WF_vars(Recover)", "TRUE")], "EventuallyFinished"),
        ("unfair_sweep", [("WF_vars(Sweep(p))", "TRUE")], "EventuallyReclaimed"),
        ("unfair_return", [("WF_vars(Return)", "TRUE")], "EventuallyReclaimed"),
    ]
    for name, condition in [
        ("pre_copy_crash", 'phase = "recover" /\\ 2 \\in catalog /\\ 2 \\notin files'),
        ("published_crash", 'phase = "recover" /\\ mapped = 2'),
        ("retired_before_unlink_crash", 'phase = "recover" /\\ 2 \\notin catalog /\\ 2 \\in files'),
        ("recovered_unlink", 'phase = "done" /\\ crashes > 0 /\\ 2 \\notin catalog /\\ 2 \\notin files'),
    ]:
        marker = "RawPreserved == 1 \\in catalog /\\ 1 \\in files"
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
        (folder / "RoseRepairRecovery.tla").write_text(mutated)
        temporal = invariant in {"EventuallyFinished", "EventuallyReclaimed"}
        selected = live_config if temporal else config
        if invariant == "Witness":
            selected += "\nINVARIANT Witness\n"
        if temporal:
            selected = "\n".join(line for line in selected.splitlines() if not line.startswith("PROPERTY ") or line == "PROPERTY " + invariant) + "\n"
        (folder / "model.cfg").write_text(selected)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RoseRepairRecovery.tla"]
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
