#!/usr/bin/env python3

import csv
import json
import re
import statistics
import sys
from pathlib import Path


def summarize_e2e(run_dir):
    rows = []
    for result in sorted(run_dir.glob("[0-9][0-9]-*.json")):
        run, variant = result.stem.split("-", 1)
        payload = json.loads(result.read_text())
        requests = payload["recorded_requests"]
        seconds = payload["recorded_seconds"]
        map_file = run_dir / f"{result.stem}-jemalloc-map.txt"
        rows.append(
            {
                "run": int(run),
                "variant": variant,
                "recorded_requests": requests,
                "recorded_seconds": f"{seconds:.15f}",
                "ms_per_request": f"{seconds * 1000 / requests:.9f}",
                "result_changes": payload["result_changes"],
                "jemalloc_map_lines": len(map_file.read_text().splitlines()),
            }
        )

    fields = list(rows[0])
    with (run_dir / "e2e-results.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fields)
        writer.writeheader()
        writer.writerows(rows)

    by_variant = {
        variant: [float(row["ms_per_request"]) for row in rows if row["variant"] == variant]
        for variant in ("baseline", "fixed")
    }
    baseline = statistics.median(by_variant["baseline"])
    fixed = statistics.median(by_variant["fixed"])
    print(f"baseline median: {baseline:.6f} ms/request")
    print(f"fixed median:    {fixed:.6f} ms/request")
    print(f"latency reduction: {(1 - fixed / baseline) * 100:.2f}%")
    print(f"throughput ratio:  {baseline / fixed:.3f}x")


def stat(text, name):
    match = re.search(rf"{re.escape(name)}\s*:\s*(\d+)", text)
    if not match:
        raise RuntimeError(f"missing perf c2c statistic: {name}")
    return int(match.group(1))


def summarize_c2c(run_dir):
    rows = []
    for variant in ("baseline", "fixed"):
        variant_dir = run_dir / variant
        payload = json.loads((variant_dir / "client.json").read_text())
        stats = (variant_dir / "c2c-stats.txt").read_text()
        hitm_lines = (variant_dir / "hitm.txt").read_text().splitlines()
        requests = payload["recorded_requests"]
        loads = stat(stats, "Load Operations")
        local = stat(stats, "Load Local HITM")
        remote = stat(stats, "Load Remote HITM")
        total = len(hitm_lines)
        target = sum("AsyncHorizonJoinRecordCursorFactory.processHorizonTimestamps" in line for line in hitm_lines)
        if total != local + remote:
            raise RuntimeError(
                f"HITM sample/report mismatch for {variant}: {total} != {local} + {remote}"
            )
        rows.append(
            {
                "variant": variant,
                "recorded_requests": requests,
                "recorded_seconds": f'{payload["recorded_seconds"]:.15f}',
                "total_records": stat(stats, "Total records"),
                "load_operations": loads,
                "local_hitm": local,
                "remote_hitm": remote,
                "total_hitm": total,
                "target_hitm": target,
                "shared_cache_lines": stat(stats, "Total Shared Cache Lines"),
                "hitm_per_query": f"{total / requests:.6f}",
                "target_hitm_per_query": f"{target / requests:.6f}",
                "hitm_per_1000_loads": f"{total * 1000 / loads:.6f}",
            }
        )

    fields = list(rows[0])
    with (run_dir / "c2c-results.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fields)
        writer.writeheader()
        writer.writerows(rows)

    baseline, fixed = rows
    raw_reduction = 1 - fixed["total_hitm"] / baseline["total_hitm"]
    normalized_reduction = 1 - (
        fixed["total_hitm"] / fixed["load_operations"]
    ) / (baseline["total_hitm"] / baseline["load_operations"])
    print(f"total HITM reduction: {raw_reduction * 100:.2f}%")
    print(f"HITM/load reduction:  {normalized_reduction * 100:.2f}%")
    print(f'target HITM: {baseline["target_hitm"]} -> {fixed["target_hitm"]}')


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("e2e", "c2c"):
        raise SystemExit(f"usage: {sys.argv[0]} e2e|c2c RUN_DIR")
    run_dir = Path(sys.argv[2])
    if sys.argv[1] == "e2e":
        summarize_e2e(run_dir)
    else:
        summarize_c2c(run_dir)


if __name__ == "__main__":
    main()
