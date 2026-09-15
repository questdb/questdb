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

"""Validate RFC 130 task 9f against the pinned pre-breaker engine, case by case."""
import argparse
import csv
import importlib.util
from pathlib import Path
import re


def compare(reference, candidate, output):
    script = Path(__file__).with_name("summarize-hash-join-group-by-v1.py")
    spec = importlib.util.spec_from_file_location("v1_summary", script)
    summary = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(summary)
    expected = set(Path(__file__).with_name("parallel-hash-join-group-by-recovery-cases.txt").read_text().splitlines())
    for root, diagnostic in ((reference, True), (candidate, False)):
        actual = {p.stem for p in root.glob("*.txt") if p.name not in {"commands.txt", "environment.txt"}}
        if actual != expected:
            raise ValueError(f"incomplete recovery matrix: {root}: missing={expected - actual}, extra={actual - expected}")
        for case in expected:
            text = (root / (case + ".txt")).read_text()
            marker = "# breaker=noop diagnostic reference only" if diagnostic else "# breaker=active throttle=2000000 timeout=unlimited fd=-1"
            if marker not in text:
                raise ValueError(f"incorrect breaker configuration: {root}/{case}")
        summary.summarize(root, diagnostic_reference=diagnostic)

    def load(root):
        with (root / "summary.csv").open() as source:
            return {(r["case"], r["repetition"]): r for r in csv.DictReader(source) if r["arm"] == "candidate"}

    refs, candidates = load(reference), load(candidate)
    historical_root = Path(__file__).resolve().parents[1] / "docs/parallel-hash-join-group-by-v1"
    historical = load(historical_root)
    comparisons = []
    for key, current in sorted(candidates.items()):
        ref = refs[key]
        for counter in summary.COUNTERS:
            # Memory layout changed in tasks 9a-9c; compare logical work, not capacity.
            if counter != "build_bytes" and current[counter + "_median"] != ref[counter + "_median"]:
                raise ValueError(f"different workload counters: {key}: {counter}")
        a = (reference / (key[0] + ".txt")).read_text()
        b = (candidate / (key[0] + ".txt")).read_text()
        results_a = re.findall(r"^# (?:result=|reference_groups=)[^\n]*$", a, re.M)
        results_b = re.findall(r"^# (?:result=|reference_groups=)[^\n]*$", b, re.M)
        if not results_a or results_a != results_b:
            raise ValueError(f"different or missing ordered references: {key}")
        reference_ms, candidate_ms = float(ref["median_ms"]), float(current["median_ms"])
        historical_ms = float(historical[key]["median_ms"])
        # A slower reproduction must never relax the pinned task 10 performance target.
        limit_reference_ms = min(reference_ms, historical_ms)
        passed = candidate_ms * 10 <= limit_reference_ms * 11
        comparisons.append(dict(case=key[0], repetition=key[1], reference_ms=reference_ms,
                                historical_reference_ms=historical_ms, limit_reference_ms=limit_reference_ms,
                                candidate_ms=candidate_ms, ratio=candidate_ms / limit_reference_ms,
                                speedup=float(current["speedup"]), passed=passed))
    with output.open("w", newline="") as sink:
        writer = csv.DictWriter(sink, fieldnames=list(comparisons[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(comparisons)
    failures = [r for r in comparisons if not r["passed"]]
    for row in failures:
        print(f"FAIL {row['case']} round={row['repetition']} ratio={row['ratio']:.4f} limit=1.10")
    print(f"recovery comparisons={len(comparisons)} failures={len(failures)}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    compare(args.reference, args.candidate, args.output)
