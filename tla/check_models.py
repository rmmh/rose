#!/usr/bin/env python3
"""Run complete TLC configurations and archive reproducible verification evidence."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time


FAST = [
    ("RosePrefixRecovery", "RosePrefixRecovery"),
    ("RosePrefixRecoveryLive", "RosePrefixRecoveryLive"),
    ("RoseTxnCommit", "RoseTxnCommit"),
    ("RoseTxnCommit", "RoseTxnCommitGlobal"),
    ("RoseSnapshotGC", "RoseSnapshotGC"),
    ("RoseRetryRetention", "RoseRetryRetention"),
    ("RoseRetryRetention", "RoseRetryRetentionLive"),
    ("RoseMaintenance", "RoseMaintenance"),
    ("RoseMaintenance", "RoseMaintenanceLive"),
    ("RoseRepairEpoch", "RoseRepairEpoch"),
    ("RoseRepairEpoch", "RoseRepairEpochLive"),
]
LARGE = [
    ("RoseMetadata", "RoseMetadata"),
    ("RoseStorage", "RoseStorageReplica"),
    ("RoseStorage", "RoseStorageDisk"),
    ("RoseStorage", "RoseStorageEC"),
    ("RoseStorage", "RoseStorageMultiNode"),
    ("RoseSnapshotGC", "RoseSnapshotGCRetention"),
]


def evidence(log, returncode, timed_out=False):
    """An exit code or partial progress counter alone is never passing evidence."""
    counts = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found, "
                        r"([\d,]+) states left on queue\.", log)
    count = tuple(int(value.replace(",", "")) for value in counts[-1]) if counts else None
    depth = re.search(r"depth of the complete state graph search is (\d+)", log)
    random = re.search(r"with fp (\d+) and seed (-?\d+)", log)
    complete = (returncode == 0 and count is not None and count[1] > 0 and count[2] == 0
                and "Model checking completed. No error has been found." in log
                and "Error:" not in log and depth is not None and random is not None)
    if timed_out:
        status = "timeout"
    elif complete:
        status = "passed"
    elif returncode != 0 or "Error:" in log:
        status = "failed"
    else:
        status = "incomplete"
    return dict(status=status, returncode=returncode,
                generated=count[0] if count else None,
                distinct=count[1] if count else None,
                remaining=count[2] if count else None,
                depth=int(depth.group(1)) if depth else None,
                fingerprint=int(random.group(1)) if random else None,
                seed=int(random.group(2)) if random else None)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier", choices=("fast", "large", "all"), default="fast")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--timeout", type=int, default=3600,
                        help="maximum seconds per configuration; timeout is not success")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.timeout <= 0 or args.workers <= 0:
        parser.error("timeout and workers must be positive")
    root = Path(__file__).resolve().parent
    jar = root.parent / "tla2tools.jar"
    output = (args.output_dir or Path(tempfile.mkdtemp(prefix="rose-model-checks-"))).resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory is not empty; preserve old evidence and use a new directory")
    configs = FAST if args.tier == "fast" else LARGE if args.tier == "large" else FAST + LARGE
    sources = {path.name: path.read_bytes() for pattern in ("*.tla", "*.cfg")
               for path in root.glob(pattern)}
    report = dict(tier=args.tier, tool=str(jar), tool_sha256=digest(jar),
                  sources={name: hashlib.sha256(data).hexdigest() for name, data in sources.items()},
                  status="incomplete", checks=[])
    version = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
    report["java_version"] = version.stdout + version.stderr
    (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    for module, config in configs:
        folder = output / config
        folder.mkdir(exist_ok=True)
        for name, data in sources.items():
            (folder / name).write_bytes(data)
        command = ["java", "-Xmx2g", "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
                   "-workers", str(args.workers), "-deadlock", "-config", config + ".cfg", module + ".tla"]
        started = time.monotonic()
        print(f"Checking {config}; log: {folder / 'tlc.log'}", flush=True)
        timed_out = False
        interrupted = False
        with (folder / "tlc.log").open("w") as log:
            process = subprocess.Popen(command, cwd=folder, stdout=log, stderr=subprocess.STDOUT)
            try:
                returncode = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.kill()
                returncode = process.wait()
            except KeyboardInterrupt:
                interrupted = True
                process.kill()
                returncode = process.wait()
        result = evidence((folder / "tlc.log").read_text(), returncode, timed_out)
        if interrupted:
            result["status"] = "interrupted"
        result.update(module=module, configuration=config, command=command,
                      duration_seconds=round(time.monotonic() - started, 3), log=str(folder / "tlc.log"))
        report["checks"].append(result)
        report["status"] = ("passed" if len(report["checks"]) == len(configs)
                            and all(check["status"] == "passed" for check in report["checks"])
                            else "incomplete")
        (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"{config}: {result['status']} ({result['distinct']} distinct states)", flush=True)
        if interrupted:
            break
    print(f"Evidence: {output / 'results.json'}", flush=True)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
