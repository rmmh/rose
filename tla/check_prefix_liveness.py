#!/usr/bin/env python3
"""Check conditional prefix liveness sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-prefix-liveness-"))
    source = (root / "RosePrefixRecoveryLive.tla").read_text()
    config = (root / "RosePrefixRecoveryLive.cfg").read_text()
    cases = [('blocked_replay_retirement',
      '\\/ Publish \\/ Replay \\/ ReplaySync \\/ ReplayRemove \\/ ReplayRetire',
      '\\/ Publish \\/ Replay \\/ ReplaySync \\/ ReplayRemove',
      'RecoveryCompletes'),
     ('no_fair_completion',
      ' /\\ WF_liveVars(FairProgress)',
      '',
      'PrefixesEventuallyAcknowledged'),
     ('unbounded_faults',
      "faultsLeft' = faultsLeft - 1",
      "faultsLeft' = faultsLeft",
      'PrefixesEventuallyAcknowledged')]
    results = []
    for name, before, after, property_name in cases:
        if source.count(before) != 1:
            raise RuntimeError(f"{name}: mutation marker is absent or ambiguous")
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "RosePrefixRecoveryLive.tla").write_text(source.replace(before, after, 1))
        (folder / "RosePrefixRecovery.tla").write_text((root / "RosePrefixRecovery.tla").read_text())
        selected = "\n".join(line for line in config.splitlines()
                             if not line.startswith("PROPERTY ") or line == "PROPERTY " + property_name)
        (folder / "model.cfg").write_text(selected + "\n")
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RosePrefixRecoveryLive.tla"]
        try:
            run = subprocess.run(command, cwd=folder, capture_output=True, text=True,
                                 timeout=args.timeout, check=False)
            log = run.stdout + run.stderr
            detected = run.returncode != 0 and "Error: Temporal properties were violated." in log and "Error: Invariant" not in log
            status = "detected" if detected else "unexpected_result"
        except subprocess.TimeoutExpired as exc:
            log = (exc.stdout or b"").decode() + (exc.stderr or b"").decode()
            status = "timeout"
        (folder / "tlc.log").write_text(log)
        results.append(dict(mutation=name, expected_property=property_name, status=status,
                            log=str(folder / "tlc.log")))
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))
    return 0 if all(r["status"] == "detected" for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
