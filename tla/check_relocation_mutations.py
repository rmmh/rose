#!/usr/bin/env python3
"""Check relocation outcome safety and recovery sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-relocation-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    source = (root / "RoseRelocationOutcome.tla").read_text()
    config = (root / "RoseRelocationOutcome.cfg").read_text()
    live_config = (root / "RoseRelocationOutcomeLive.cfg").read_text()
    cases = [
        ("omit_durable_copy", [("files' = files \\cup {1}", "files' = files")], "PublishedPreserved"),
        ("delete_destination_on_error", [("files' = files \\ {1 - catalog}", "files' = files \\ {1}")], "PublishedPreserved"),
        ("skip_remount", [("mounted' = IF success THEN catalog ELSE mounted", "mounted' = mounted")], "AccessCoherent"),
        ("skip_rollback_validation", [("phase' = IF succeeds /\\ ~uncertain THEN \"restore\" ELSE \"quarantine\"", "phase' = IF succeeds /\\ ~uncertain THEN \"cleanup\" ELSE \"quarantine\"")], "AccessibleClientsValidated"),
        ("ignore_source_epoch", [("sourceOK == catalog = 0 /\\ epoch = 0", "sourceOK == catalog = 0")], "ResolutionFresh"),
        ("ignore_destination_epoch", [("destOK == catalog = 1 /\\ epoch = after", "destOK == catalog = 1")], "ResolutionFresh"),
        ("omit_epoch_increment", [("epoch' = IF disk = catalog THEN epoch + 1 ELSE epoch", "epoch' = epoch")], "ResolutionFresh"),
        ("unfenced_resolution_failure", [("phase' = \"quarantine\" /\\ mounted' = 2", "phase' = \"quarantine\" /\\ mounted' = IF catalog = 0 THEN mounted ELSE 2")], "QuarantineFenced"),
        ("unfenced_rollback_failure", [("mounted' = IF succeeds /\\ ~uncertain THEN 0 ELSE 2", "mounted' = IF succeeds /\\ ~uncertain THEN 0 ELSE IF catalog' = 1 THEN mounted ELSE 2")], "AccessCoherent"),
        ("sweep_authoritative", [("/\\ disk \\in files /\\ disk # catalog", "/\\ disk \\in files")], "PublishedPreserved"),
        ("unfair_recovery", [("WF_vars(Recover)", "TRUE")], "EventuallyRecovered"),
        ("unfair_sweep", [("WF_vars(Sweep(disk))", "TRUE")], "EventuallyReclaimed"),
    ]
    for name, condition in [
        ("applied_uncertain", 'phase = "resolve" /\\ catalog = 1'),
        ("rejected_uncertain", 'phase = "resolve" /\\ catalog = 0 /\\ epoch = 0'),
        ("stale_destination_quarantine", 'phase = "quarantine" /\\ catalog = 1 /\\ ~destFresh'),
        ("rollback_token_changed", 'phase = "rollback" /\\ epoch = after /\\ ~rollbackFresh'),
        ("applied_uncertain_rollback", 'phase = "quarantine" /\\ catalog = 0 /\\ epoch = after + 1 /\\ changes = 0'),
        ("rollback_requires_validation", 'phase = "restore" /\\ catalog = 0 /\\ ~clientsReady'),
        ("recovered_stray_cleanup", 'phase = "done" /\\ crashes > 0 /\\ catalog = 1 /\\ files = {1}'),
    ]:
        marker = "ResolutionFresh == resolutionSafe"
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
        (folder / "RoseRelocationOutcome.tla").write_text(mutated)
        temporal = invariant in {"EventuallyRecovered", "EventuallyReclaimed"}
        selected = live_config if temporal else config
        if invariant == "Witness":
            selected += "\nINVARIANT Witness\n"
        if temporal:
            selected = "\n".join(line for line in selected.splitlines() if not line.startswith("PROPERTY ") or line == "PROPERTY " + invariant) + "\n"
        (folder / "model.cfg").write_text(selected)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RoseRelocationOutcome.tla"]
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
