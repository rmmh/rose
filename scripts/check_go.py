#!/usr/bin/env python3
"""Run Go verification and retain explicit completion and skipped-test evidence."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def test_evidence(lines, returncode):
    packages = {}
    tests = {}
    malformed = False
    for line in lines:
        try:
            event = json.loads(line)
            package = event["Package"]
            action = event["Action"]
        except (ValueError, KeyError, TypeError):
            malformed = True
            continue
        if event.get("Test"):
            key = package + "/" + event["Test"]
            if action in ("run", "pass", "fail", "skip"):
                tests[key] = action
        elif action in ("start", "pass", "fail", "skip"):
            packages[package] = action
    skipped = sorted(key for key, action in tests.items() if action == "skip")
    passed = sorted(key for key, action in tests.items() if action == "pass")
    failed = sorted(key for key, action in tests.items() if action == "fail")
    complete = (not malformed and bool(packages) and bool(passed)
                and all(action in ("pass", "skip") for action in packages.values())
                and all(action in ("pass", "skip") for action in tests.values()))
    status = ("failed" if returncode != 0 or failed or "fail" in packages.values()
              else "passed" if complete else "incomplete")
    return dict(status=status, packages=packages, passed_tests=len(passed),
                skipped_tests=skipped, failed_tests=failed,
                coverage="partial" if skipped or not complete else "executed-tests-only",
                malformed_events=malformed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("test", "race", "vet"))
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty; preserve previous evidence")
    root = Path(__file__).resolve().parent.parent
    command = (["go", "vet", "./..."] if args.mode == "vet" else
               ["go", "test", "-json", "-count=1", "-timeout=180s"]
               + (["-race"] if args.mode == "race" else []) + ["./..."])
    report = dict(mode=args.mode, command=command, status="incomplete",
                  environment={"ROSE_NO_RAMDISK": "1", "ROSE_CHAOS": "0"})
    report_path = output / "results.json"

    def save():
        report_path.write_text(json.dumps(report, indent=2) + "\n")

    save()
    for key, cmd in (("go_version", ["go", "version"]),
                     ("revision", ["git", "rev-parse", "HEAD"]),
                     ("worktree", ["git", "status", "--porcelain"])):
        result = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=True)
        report[key] = result.stdout.strip()
    save()
    started = time.monotonic()
    with (output / "stdout.log").open("w") as stdout, (output / "stderr.log").open("w") as stderr:
        result = subprocess.run(command, cwd=root, stdout=stdout, stderr=stderr,
                                env=dict(os.environ, **report["environment"]))
    report.update(returncode=result.returncode,
                  duration_seconds=round(time.monotonic() - started, 3))
    if args.mode == "vet":
        report["status"] = "passed" if result.returncode == 0 else "failed"
    else:
        with (output / "stdout.log").open() as log:
            report.update(test_evidence(log, result.returncode))
    save()
    summary = [f"Go {args.mode}: {report['status']}"]
    if args.mode != "vet":
        summary += [f"Passed test events: {report['passed_tests']}",
                    f"Skipped tests: {len(report['skipped_tests'])}",
                    "A passing gate does not establish coverage for skipped tests."]
        summary += [f"- skipped: {name}" for name in report["skipped_tests"]]
        summary += [f"- failed: {name}" for name in report["failed_tests"]]
    text = "\n".join(summary) + "\n"
    (output / "summary.md").write_text(text)
    print(text, end="")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as target:
            target.write(text)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
