#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from collections import defaultdict


SAMPLE_PREFIX = re.compile(
    r"^\s*\S+\s+(?P<tid>\d+)\s+\[(?P<cpu>\d+)\]\s+"
    r"(?P<address>[0-9a-fA-F]+)\s+"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Group perf-script HITM samples into 64-byte cache lines"
    )
    parser.add_argument("samples", help="hitm.txt produced by perf script")
    parser.add_argument(
        "--symbol",
        help="keep only samples whose resolved symbol contains this text",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    groups = defaultdict(lambda: {"count": 0, "tids": set(), "cpus": set()})

    with open(args.samples, encoding="utf-8") as samples:
        for line_number, line in enumerate(samples, 1):
            if args.symbol and args.symbol not in line:
                continue
            match = SAMPLE_PREFIX.match(line)
            if not match:
                raise RuntimeError(
                    f"cannot parse {args.samples}:{line_number}: {line.rstrip()}"
                )
            address = int(match.group("address"), 16)
            cache_line = address & ~63
            group = groups[cache_line]
            group["count"] += 1
            group["tids"].add(int(match.group("tid")))
            group["cpus"].add(int(match.group("cpu")))
            group.setdefault("address_lo", address)
            group["address_lo"] = min(group["address_lo"], address)
            group.setdefault("address_hi", address)
            group["address_hi"] = max(group["address_hi"], address)

    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(
        ["cache_line", "hitm_samples", "tids", "cpus", "address_range"]
    )
    for cache_line, group in sorted(
        groups.items(), key=lambda item: (-item[1]["count"], item[0])
    ):
        writer.writerow(
            [
                hex(cache_line),
                group["count"],
                "|".join(map(str, sorted(group["tids"]))),
                "|".join(map(str, sorted(group["cpus"]))),
                f'{hex(group["address_lo"])}-{hex(group["address_hi"])}',
            ]
        )


if __name__ == "__main__":
    main()
