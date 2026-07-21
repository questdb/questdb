#!/usr/bin/env python3

import argparse
import csv
import glob
import json
import math
import os
import statistics


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir")
    parser.add_argument("output_dir")
    return parser.parse_args()


def percentile(sorted_values, percentile_value):
    index = math.ceil(len(sorted_values) * percentile_value) - 1
    return sorted_values[max(0, index)]


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    summary_rows = []

    with open(os.path.join(args.output_dir, "jmh-raw.csv"), "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(("variant", "trial", "iteration", "score_ms"))
        for variant in ("baseline", "fixed"):
            paths = sorted(glob.glob(os.path.join(args.input_dir, f"jmh-{variant}-[1-6].json")))
            if len(paths) != 6:
                raise RuntimeError(f"expected 6 {variant} JMH files, found {len(paths)}")
            for trial, path in enumerate(paths, 1):
                with open(path, encoding="utf-8") as source:
                    result = json.load(source)[0]
                values = result["primaryMetric"]["rawData"][0]
                for iteration, score in enumerate(values, 1):
                    writer.writerow((variant, trial, iteration, score))
                summary_rows.append((
                    "jmh_process",
                    variant,
                    trial,
                    result["primaryMetric"]["score"],
                    statistics.median(values),
                    percentile(sorted(values), 0.95),
                    percentile(sorted(values), 0.99),
                    min(values),
                    max(values),
                ))

    with open(os.path.join(args.output_dir, "http-raw.csv"), "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(("variant", "trial", "request", "elapsed_ns", "status", "response_bytes", "connection_will_close"))
        for variant in ("baseline", "fixed"):
            paths = sorted(glob.glob(os.path.join(args.input_dir, f"http-{variant}-[1-6].csv")))
            if len(paths) != 6:
                raise RuntimeError(f"expected 6 {variant} HTTP files, found {len(paths)}")
            for trial, path in enumerate(paths, 1):
                with open(path, newline="", encoding="utf-8") as source:
                    rows = list(csv.DictReader(source))
                if len(rows) != 1000:
                    raise RuntimeError(f"expected 1000 requests in {path}, found {len(rows)}")
                if {row["status"] for row in rows} != {"200"}:
                    raise RuntimeError(f"non-200 response in {path}")
                if {row["connection_will_close"] for row in rows} != {"0"}:
                    raise RuntimeError(f"non-persistent response in {path}")
                elapsed_ms = sorted(int(row["elapsed_ns"]) / 1_000_000 for row in rows)
                for row in rows:
                    writer.writerow((
                        variant,
                        trial,
                        row["request"],
                        row["elapsed_ns"],
                        row["status"],
                        row["response_bytes"],
                        row["connection_will_close"],
                    ))
                summary_rows.append((
                    "http_process",
                    variant,
                    trial,
                    statistics.mean(elapsed_ms),
                    statistics.median(elapsed_ms),
                    percentile(elapsed_ms, 0.95),
                    percentile(elapsed_ms, 0.99),
                    min(elapsed_ms),
                    max(elapsed_ms),
                ))

    with open(os.path.join(args.output_dir, "summary.csv"), "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(("measurement", "variant", "trial", "mean_ms", "p50_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"))
        writer.writerows(summary_rows)


if __name__ == "__main__":
    main()
