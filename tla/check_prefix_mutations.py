#!/usr/bin/env python3
"""Check prefix journal invariant sensitivity; run positive TLC separately."""
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
    output = args.output_dir or Path(tempfile.mkdtemp(prefix="rose-prefix-mutations-"))
    source = (root / "RosePrefixRecovery.tla").read_text()
    config = (root / "RosePrefixRecovery.cfg").read_text()
    cases = [('omit_journal_directory_sync',
      '    /\\ phase = "Install"\n    /\\ durableJournal\' = journal',
      '    /\\ phase = "Install"\n    /\\ durableJournal\' = durableJournal',
      'PublishedRecoverable'),
     ('omit_data_sync',
      '    /\\ phase = "SyncData"\n    /\\ durableFile\' = file',
      '    /\\ phase = "SyncData"\n    /\\ durableFile\' = durableFile',
      'PublishedRecoverable'),
     ('omit_retirement_directory_sync',
      '    /\\ phase = "Retire"\n    /\\ durableJournal\' = None',
      '    /\\ phase = "Retire"\n    /\\ durableJournal\' = durableJournal',
      'RollbackNotOlderThanPublished'),
     ('retry_skips_journal_sync',
      'ELSE /\\ backup\' = backup /\\ phase\' = "Install"',
      'ELSE /\\ backup\' = backup /\\ phase\' = "Write"',
      'PublishedRecoverable')]
    results = []
    for name, before, after, invariant in cases:
        if source.count(before) != 1:
            raise RuntimeError(f"{name}: mutation marker is absent or ambiguous")
        folder = output / name
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "RosePrefixRecovery.tla").write_text(source.replace(before, after, 1))
        (folder / "model.cfg").write_text(config)
        command = ["java", "-Xmx512m", "-cp", str(root.parent / "tla2tools.jar"),
                   "tlc2.TLC", "-workers", "2", "-deadlock", "-config", "model.cfg", "RosePrefixRecovery.tla"]
        try:
            run = subprocess.run(command, cwd=folder, capture_output=True, text=True,
                                 timeout=args.timeout, check=False)
            log = run.stdout + run.stderr
            detected = run.returncode != 0 and f"Error: Invariant {invariant} is violated." in log
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
