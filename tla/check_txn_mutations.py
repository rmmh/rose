#!/usr/bin/env python3
"""Require known transaction-protocol regressions to violate named invariants.

Run the unmodified configurations separately (make test-txn). This command
checks mutation sensitivity, not correctness of the unmodified model.
"""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-txn-mutations-"))
    output.mkdir(parents=True, exist_ok=True)
    source = (root / "RoseTxnCommit.tla").read_text()
    config = (root / "RoseTxnCommitGlobal.cfg").read_text()
    publish = 'Publish(t) ==\n    /\\ txn_state[t] = "prepared"\n    /\\ WritesAllowed'
    repair = '    /\\ \\A s2 \\in Shards : <<t, s2>> \\notin durable_records[d] /\\ <<t, s2>> \\notin volatile_records[d]\n'
    cases = [
        ("publish_during_degradation", publish, publish.rsplit("\n", 1)[0], "NoDegradedPublication"),
        ("colocated_repair", repair, "", "NoColocatedShards"),
    ]
    results = []
    for name, before, after, invariant in cases:
        if source.count(before) != 1:
            raise RuntimeError(f"{name}: mutation marker is absent or ambiguous")
        folder = output / name
        folder.mkdir(exist_ok=True)
        (folder / "RoseTxnCommit.tla").write_text(source.replace(before, after, 1))
        (folder / "RoseTxnCommitGlobal.cfg").write_text(config)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config",
                   "RoseTxnCommitGlobal.cfg", "RoseTxnCommit.tla"]
        try:
            run = subprocess.run(command, cwd=folder, capture_output=True, text=True,
                                 timeout=args.timeout, check=False)
            log = run.stdout + run.stderr
            detected = run.returncode != 0 and f"Error: Invariant {invariant} is violated." in log
            status = "detected" if detected else "unexpected_result"
            exit_code = run.returncode
        except subprocess.TimeoutExpired as exc:
            log = (exc.stdout or b"").decode() + (exc.stderr or b"").decode()
            status, exit_code = "timeout", None
        (folder / "tlc.log").write_text(log)
        results.append(dict(mutation=name, expected_invariant=invariant,
                            status=status, exit_code=exit_code, log=str(folder / "tlc.log")))
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))
    return 0 if all(result["status"] == "detected" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
