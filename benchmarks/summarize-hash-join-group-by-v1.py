#!/usr/bin/env python3
#      ___                  _   ____  ____
#     / _ \ _   _  ___  ___| |_|  _ \| __ )
#    | | | | | | |/ _ \/ __| __| | | |  _ \
#    | |_| | |_| |  __/\__ \ |_| |_| | |_) |
#     \__\_\\__,_|\___||___/\__|____/|____/
#
#   Copyright (c) 2014-2019 Appsicle
#   Copyright (c) 2019-2026 QuestDB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

"""Validate retained RFC 130 samples and emit per-round latency, counters and memory CSV."""
import csv
from pathlib import Path
import re
import statistics
import sys


COUNTERS = ["build_rows", "build_keys", "build_bytes", "scanned_rows", "matched_pairs",
            "null_extended_rows", "surviving_rows", "merge_cardinality"]
PHASES = ["build_ns", "init_ns", "probe_ns", "merge_ns"]
MEMORY = ["sampled_query_peak_bytes", "sampled_batch_native_peak_delta_bytes",
          "sampled_native_peak_delta_bytes", "retained_query_bytes", "retained_native_delta_bytes"]


def summarize(root):
    summaries = []
    total = checks = 0
    for path in sorted(root.glob("*.txt")):
        if path.name in {"environment.txt", "environment-continuation.txt", "commands.txt"}:
            continue
        text = path.read_text()
        header = next((line for line in text.splitlines() if line.startswith("arm,repetition,")), None)
        if header is None:
            raise ValueError(f"missing benchmark samples: {path}")
        completed = re.findall(r"^# result_checks=(\d+)$", text, re.M)
        if len(completed) != 1:
            raise ValueError(f"incomplete benchmark: {path}")
        rows = list(csv.DictReader([header] + [line for line in text.splitlines()
                                            if line.startswith(("baseline,", "candidate,"))]))
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise ValueError(f"malformed samples: {path}")
        repetitions = sorted({int(row["repetition"]) for row in rows})
        owners = sorted({int(row.get("owner", 0)) for row in rows})
        runs = sorted({int(row["run"]) for row in rows})
        if repetitions != [0, 1] or runs != list(range(10)) or owners != list(range(len(owners))):
            raise ValueError(f"expected two rounds, ten runs and contiguous owners: {path}")
        identifiers = {(row["arm"], int(row["repetition"]), int(row["run"]), int(row.get("owner", 0))) for row in rows}
        if len(identifiers) != len(rows) or len(rows) != 40 * len(owners):
            raise ValueError(f"missing/duplicate samples: {path}")
        if int(completed[0]) != 52 * len(owners) - 1:
            raise ValueError(f"missing result checks: {path}")
        for field in COUNTERS + ["groups"]:
            values = {row[field] for row in rows if row["arm"] == "candidate"}
            if len(values) != 1:
                raise ValueError(f"unstable {field}: {path}: {values}")
        if "cold-helper=" in text:
            cache = re.findall(r"^# cold_cache files=\d+ pages=(\d+) resident_before=\d+ resident_after=(\d+)$", text, re.M)
            if len(cache) != 40 or any(int(pages) == 0 or int(after) / int(pages) > 0.01 for pages, after in cache):
                raise ValueError(f"unverified cold samples: {path}")
        for repetition in repetitions:
            baseline = [int(row["elapsed_ns"]) for row in rows
                        if row["arm"] == "baseline" and int(row["repetition"]) == repetition]
            for arm in ["baseline", "candidate"]:
                samples = [row for row in rows if row["arm"] == arm and int(row["repetition"]) == repetition]
                elapsed = [int(row["elapsed_ns"]) for row in samples]
                result = dict(case=path.stem, repetition=repetition, arm=arm, samples=len(samples),
                              median_ms=statistics.median(elapsed) / 1e6, min_ms=min(elapsed) / 1e6,
                              max_ms=max(elapsed) / 1e6, speedup=statistics.median(baseline) / statistics.median(elapsed),
                              groups=int(samples[0]["groups"]))
                for field in MEMORY + PHASES + COUNTERS:
                    values = [int(row[field]) for row in samples if row.get(field, "") != ""]
                    result[field + "_median"] = statistics.median(values) if values else ""
                    if field in MEMORY:
                        result[field + "_max"] = max(values) if values else ""
                batches = {int(row["run"]): int(row["batch_ns"]) for row in samples if "batch_ns" in row}
                result["batch_median_ms"] = statistics.median(batches.values()) / 1e6 if batches else ""
                result["queries_per_second"] = len(owners) * 1e9 / statistics.median(batches.values()) if batches else ""
                summaries.append(result)
        if path.stem == "primary-w4":
            if "# primary_gate=PASS" not in text or any(row["speedup"] < 2 for row in summaries
                                                        if row["case"] == path.stem and row["arm"] == "candidate"):
                raise ValueError("primary gate did not pass")
        total += len(rows)
        checks += int(completed[0])
    if not summaries:
        raise ValueError("no completed benchmarks")
    with (root / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(summaries[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(summaries)
    print(f"cases={len(summaries) // 4} measured_executions={total} result_checks={checks}")


if __name__ == "__main__":
    summarize(Path(sys.argv[1]))
