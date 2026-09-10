#!/usr/bin/env python3
"""Turns two revisions' matrix output into the acceptance table.

    ./summarize-matrix.py <baseline-dir> <candidate-dir> [--md]

Each directory holds one run per file, named
``<shape>__fusion-<mode>__keys-<n>__run-<i>.tsv`` by run-matrix.sh. A run's own figure is
a median (or a p95) over its measured batches; a cell's figure is the median over its
runs' figures, which is what the matrix asks for - "medians across run medians and the
median of per-run p95 seal latencies".

WARMUP batches are dropped from every steady run. The first seals after the seed sit on a
cold page cache and on a state size that is still growing towards K, and the matrix
excludes them explicitly: complete captures after restore, rebinding or an incompatible
predecessor belong in their own named measurements and cannot be counted as steady
incremental samples.

A repair cell (a shape named ``repair-...``) measures its batches differently: the measured
rows are the ones whose refresh replayed base rows for a correction, which is every batch
from --o3-from-batch on, and the batch's refresh time is the repair's latency. Its
replayed rows, corrected output size and publication route are reported beside the gates,
and a run whose result oracle disagreed fails the cell whatever its timings say. The keyed
cell has no baseline of its own - the baseline's keyed route is not a valid reference - so
it is read against the baseline's whole-range control and marked as a route comparison.
"""
import os
import re
import statistics
import sys
from collections import Counter, defaultdict

WARMUP = 10

# metric -> (column, aggregate, direction). direction "lower" means the candidate must not
# exceed the limit, "higher" means it must not fall below it.
RUN_METRICS = {
    "seal_ms_median": ("checkpoint_ms", "median", "lower"),
    "seal_ms_p95": ("checkpoint_ms", "p95", "lower"),
    "refresh_ms_median": ("refresh_ms", "median", "lower"),
    "refresh_max_pass_ms_median": ("refresh_max_pass_ms", "median", "lower"),
    "rows_per_sec_median": ("rows_per_sec", "median", "higher"),
    "refresh_peak_mb_median": ("refresh_peak_mb", "median", "lower"),
    "alloc_mb_median": ("alloc_mb", "median", "lower"),
    "state_bytes_last": ("state_bytes", "last", "lower"),
    "meta_bytes_median": ("meta_bytes", "median", "lower"),
    "data_bytes_median": ("data_bytes", "median", "lower"),
    "meta_segs_total": ("meta_segs", "sum", "lower"),
    "data_segs_total": ("data_segs", "sum", "lower"),
    # Repair cells only: the batch's refresh is the correction, so these are the repair's
    # latency, the base rows its replay read and the live-view rows its publication wrote.
    "repair_ms_median": ("refresh_ms", "median", "lower"),
    "repair_ms_p95": ("refresh_ms", "p95", "lower"),
    "replayed_rows_median": ("o3_scan_rows", "median", "lower"),
    "corrected_rows_median": ("lv_phys_rows", "median", "lower"),
}
REPAIR_ONLY = {"repair_ms_median", "repair_ms_p95", "replayed_rows_median", "corrected_rows_median"}

# The limits the handoff proposes. Ratios are candidate / baseline.
LIMITS = {
    "seal_ms_median": 1.05,
    "seal_ms_p95": 1.10,
    "refresh_ms_median": 1.05,
    "rows_per_sec_median": 0.95,
    "refresh_peak_mb_median": 1.05,
    "alloc_mb_median": 1.05,
    "state_bytes_last": 1.00,
    "meta_bytes_median": 1.00,
    "data_bytes_median": 1.00,
    "meta_segs_total": 1.00,
    "data_segs_total": 1.00,
    "complete_seal_ms": 1.05,
    "restore_ms": 1.05,
    "first_reseal_ms": 1.05,
    "repair_ms_median": 1.05,
}
# Reported beside the gates, not gated: the route difference is what they describe.
DIAGNOSTICS = ["repair_ms_p95", "replayed_rows_median", "corrected_rows_median"]

# A candidate cell with no baseline counterpart is read against this baseline cell.
BASELINE_ALIAS = {"repair-closed-keyed": "repair-closed-whole"}

NAME = re.compile(r"^(?P<shape>.+?)__fusion-(?P<fusion>true|false)__keys-(?P<keys>\d+)__run-(?P<run>\d+)\.tsv$")


def is_repair_shape(shape):
    return shape.startswith("repair-")


def p95(values):
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    return ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]


def aggregate(values, how):
    if not values:
        return float("nan")
    if how == "median":
        return statistics.median(values)
    if how == "p95":
        return p95(values)
    if how == "sum":
        return float(sum(values))
    if how == "last":
        return values[-1]
    raise ValueError(how)


def parse_run(path, shape):
    """One run file -> {metric: value}, plus the run's window-state shape line."""
    header, rows, out = None, [], {}
    with open(path) as handle:
        for line in handle:
            line = line.rstrip("\n")
            if line.startswith("batch\t"):
                header = line.split("\t")
            elif header is not None and line and line[0].isdigit() and "\t" in line:
                rows.append(line.split("\t"))
            elif line.startswith("# seed_ms="):
                for key, value in re.findall(r"(\w+)=([-\d.]+)", line):
                    if key == "seed_checkpoint_ms":
                        out["complete_seal_ms"] = float(value)
            elif line.startswith("# restore "):
                fields = dict(re.findall(r"(\w+)=([-\d.]+)", line))
                if "read_back_ms" in fields:
                    out["restore_ms"] = float(fields["read_back_ms"])
                if "reseal_ms" in fields:
                    out["first_reseal_ms"] = float(fields["reseal_ms"])
            elif line.startswith("# window_state "):
                out["window_state"] = line[len("# window_state "):]
            elif line.startswith("# oracle "):
                match = re.search(r"verdict=(\w+)", line)
                out["oracle"] = match.group(1) if match else line
            elif line.startswith("# keyed "):
                # Closed segments the run corrected through the posting index rather than
                # by replaying them whole, and the rows it copied forward instead of reading.
                fields = dict(re.findall(r"(\w+)=([-\d.]+)", line))
                out["keyed_segments"] = float(fields.get("keyed_segments", "nan"))
                out["merged_rows"] = float(fields.get("merged_rows", "nan"))
    if header is None:
        return None
    index = {name: i for i, name in enumerate(header)}
    if is_repair_shape(shape):
        # Every batch whose refresh replayed base rows is a correction; the leading
        # strictly-forward batches only build the ladder the corrections resume from.
        measured = [row for row in rows if float(row[index["o3_scan_rows"]]) > 0]
        out["routes"] = Counter(row[index["repair"]] for row in measured)
    else:
        measured = rows[WARMUP:]
    if not measured:
        return None
    for metric, (column, how, _) in RUN_METRICS.items():
        if metric in REPAIR_ONLY and not is_repair_shape(shape):
            continue
        values = [float(row[index[column]]) for row in measured]
        out[metric] = aggregate(values, how)
    # Structural evidence, candidate-only: the baseline reports -1 for these.
    for column in ("win_caps", "win_inc", "win_visited", "win_imaged", "win_removed",
                   "fn_roots", "fn_inc", "fn_visited", "fn_imaged", "map_rows", "faults"):
        values = [float(row[index[column]]) for row in measured]
        out[column] = statistics.median(values)
    return out


def load(directory):
    cells = defaultdict(list)
    for name in sorted(os.listdir(directory)):
        match = NAME.match(name)
        if not match:
            continue
        run = parse_run(os.path.join(directory, name), match["shape"])
        if run is not None:
            cells[(match["shape"], match["fusion"], int(match["keys"]))].append(run)
    return cells


def cell_value(runs, metric):
    values = [run[metric] for run in runs if metric in run and run[metric] == run[metric]]
    return statistics.median(values) if values else None


def routes(runs):
    total = Counter()
    for run in runs:
        total.update(run.get("routes", {}))
    return " ".join(f"{route}x{count}" for route, count in sorted(total.items()))


def oracle_failures(runs):
    return sum(1 for run in runs if run.get("oracle", "match") != "match")


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    baseline, candidate = load(sys.argv[1]), load(sys.argv[2])
    is_markdown = "--md" in sys.argv

    if is_markdown:
        print("| Shape | Fusion | Keys | Metric | Baseline | Candidate | Ratio | Limit | Verdict |")
        print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    failures = 0
    diagnostics = []
    for key in sorted(set(baseline) | set(candidate)):
        shape, fusion, keys = key
        base_runs, cand_runs = baseline.get(key, []), candidate.get(key, [])
        label = shape
        if not base_runs and shape in BASELINE_ALIAS:
            base_runs = baseline.get((BASELINE_ALIAS[shape], fusion, keys), [])
            label = f"{shape} (vs {BASELINE_ALIAS[shape]})"
        if not base_runs or not cand_runs:
            print(f"| {label} | {fusion} | {keys} | - | "
                  f"{'MISSING' if not base_runs else len(base_runs)} runs | "
                  f"{'MISSING' if not cand_runs else len(cand_runs)} runs | - | - | incomplete |")
            failures += 1
            continue
        for metric in LIMITS:
            base = cell_value(base_runs, metric)
            cand = cell_value(cand_runs, metric)
            if base is None or cand is None:
                continue
            limit = LIMITS[metric]
            higher_is_better = RUN_METRICS.get(metric, (None, None, "lower"))[2] == "higher"
            if base == 0:
                ratio = float("inf") if cand > 0 else 1.0
            else:
                ratio = cand / base
            ok = ratio >= limit if higher_is_better else ratio <= limit
            if not ok:
                failures += 1
            print(f"| {label} | {fusion} | {keys} | {metric} | {base:.4g} | {cand:.4g} | "
                  f"{ratio:.3f} | {limit:.2f} | {'pass' if ok else 'FAIL'} |")
        for side, runs in (("baseline", base_runs), ("candidate", cand_runs)):
            bad = oracle_failures(runs)
            if bad:
                failures += 1
                print(f"| {label} | {fusion} | {keys} | oracle ({side}) | - | - | - | - | FAIL ({bad} run(s) mismatched) |")
        if is_repair_shape(shape):
            for metric in DIAGNOSTICS:
                base = cell_value(base_runs, metric)
                cand = cell_value(cand_runs, metric)
                if base is None or cand is None:
                    continue
                diagnostics.append(f"{label} fusion={fusion} keys={keys} {metric}: "
                                   f"baseline={base:.4g} candidate={cand:.4g} ratio={cand / base if base else float('inf'):.3f}")
            diagnostics.append(f"{label} fusion={fusion} keys={keys} route: "
                               f"baseline=[{routes(base_runs)}] candidate=[{routes(cand_runs)}]")
            diagnostics.append(f"{label} fusion={fusion} keys={keys} keyed_segments per run: "
                               f"baseline={cell_value(base_runs, 'keyed_segments')} "
                               f"candidate={cell_value(cand_runs, 'keyed_segments')}")
            diagnostics.append(f"{label} fusion={fusion} keys={keys} oracle: "
                               f"baseline={Counter(run.get('oracle', 'absent') for run in base_runs)} "
                               f"candidate={Counter(run.get('oracle', 'absent') for run in cand_runs)}")
    if diagnostics:
        print("\nRepair diagnostics (reported, not gated):")
        for line in diagnostics:
            print("  " + line)
    print(f"\n{failures} failed or incomplete gate(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
