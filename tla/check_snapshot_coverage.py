#!/usr/bin/env python3
"""Find witnesses for retention horizons; these are coverage, not safety proofs."""
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
    root = Path(__file__).resolve().parent
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-retention-coverage-"))
    source = (root / "RoseSnapshotGC.tla").read_text()
    config = (root / "RoseSnapshotGCRetention.cfg").read_text()
    results = []
    for predicate in ("NoDailyOnlySnapshot", "NoWeeklyOnlySnapshot", "NoPastWeeklyCandidate",
                      "NoExpiredSnapshot", "NoReusedSnapshotSlot"):
        folder = output / predicate
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "RoseSnapshotGC.tla").write_text(source)
        (folder / "coverage.cfg").write_text(config + f"\nINVARIANT {predicate}\n")
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "coverage.cfg", "RoseSnapshotGC.tla"]
        try:
            run = subprocess.run(command, cwd=folder, capture_output=True, text=True,
                                 timeout=args.timeout, check=False)
            log = run.stdout + run.stderr
            reached = run.returncode != 0 and f"Error: Invariant {predicate} is violated." in log
            status = "witness_found" if reached else "unexpected_result"
        except subprocess.TimeoutExpired as exc:
            log = (exc.stdout or b"").decode() + (exc.stderr or b"").decode()
            status = "timeout"
        (folder / "tlc.log").write_text(log)
        results.append(dict(predicate=predicate, status=status, log=str(folder / "tlc.log")))
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))
    return 0 if all(r["status"] == "witness_found" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
