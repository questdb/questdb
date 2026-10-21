#!/usr/bin/env python3
"""Report interleaved symbol-cache runs recorded by the local benchmark manifest.

Usage: python3 symbol_cache_results.py RESULTS_DIRECTORY
Each recorded round contains one independent JVM per variant. Confidence
intervals use paired JVM estimates, never the iterations within a fork.
"""

import json
import math
from pathlib import Path
import re
import statistics
import sys


def paired_ratio(baseline, candidate):
    rounds = sorted(baseline.keys() & candidate.keys())
    ratios = [math.log(candidate[r] / baseline[r]) for r in rounds]
    if not ratios:
        raise ValueError('no paired JVM rounds')
    mean = statistics.mean(ratios)
    estimate = math.exp(mean)
    critical = {3: 4.302652729911275, 6: 2.570581835636305}.get(len(ratios))
    if critical is None:
        return f'{estimate:.3f} (n={len(ratios)}, no CI)'
    # Two-sided Student t interval, 95%, df=n-1, on paired log ratios.
    margin = critical * statistics.stdev(ratios) / math.sqrt(len(ratios))
    return f'{estimate:.3f} [{math.exp(mean - margin):.3f}, {math.exp(mean + margin):.3f}]'


def quartile(values, fraction):
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lo = int(position)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (position - lo)


def main(directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    scores = {}
    query = {}
    query_parameters = None
    like = {}
    like_checksum = None
    like_parameters = None
    checksums = {}
    fills = {}
    overlaps = {}
    for entry in manifest:
        stage, variant, block = entry['stage'], entry['variant'], entry['round']
        log = directory / (entry['name'] + '.log')
        if stage == 'like':
            if variant not in {'baseline', 'candidate'}:
                raise ValueError(f'unknown LIKE variant: {variant}')
            parameters = entry['command'][-2:]
            if like_parameters is None:
                like_parameters = parameters
            elif like_parameters != parameters:
                raise ValueError('LIKE parameters changed between runs')
            samples = re.findall(r'SYMBOL_LIKE (\w+) samples_us=(\[[^\]]+\]) checksum=(-?\d+)', log.read_text())
            if len(samples) != 2 or {name for name, _, _ in samples} != {'first', 'repeated'}:
                raise ValueError(f'incomplete LIKE results: {log}')
            for name, values, checksum in samples:
                if like_checksum is None:
                    like_checksum = checksum
                elif like_checksum != checksum:
                    raise ValueError(f'LIKE result changed between runs or phases: {log}')
                target = like.setdefault(name, {}).setdefault(variant, {})
                if block in target:
                    raise ValueError(f'duplicate LIKE round: {name}, {variant}, {block}')
                target[block] = statistics.median(json.loads(values))
            continue
        if stage == 'query':
            # Cache mode and budget vary between controls; the workload must match.
            parameters = entry['command'][-6:-1]
            if query_parameters is None:
                query_parameters = parameters
            elif query_parameters != parameters:
                raise ValueError('query parameters changed between runs')
            samples = re.findall(r'SYMBOL_QUERY (\w+) samples_us=(\[[^\]]+\]) checksum=(-?\d+)', log.read_text())
            if len(samples) != 5 or {name for name, _, _ in samples} != {'json', 'like', 'join', 'parallel', 'moving'}:
                raise ValueError(f'incomplete query results: {log}')
            for name, values, checksum in samples:
                if checksums.setdefault(name, checksum) != checksum:
                    raise ValueError(f'query result changed between runs: {name}')
                target = query.setdefault(name, {}).setdefault(variant, {})
                if block in target:
                    raise ValueError(f'duplicate query round: {name}, {variant}, {block}')
                target[block] = statistics.median(json.loads(values))
            continue
        for row in json.loads((directory / (entry['name'] + '.json')).read_text()):
            if row['forks'] != 1:
                raise ValueError('each round must contain exactly one JVM fork')
            method = row['benchmark'].rsplit('.', 1)[-1]
            threads = row['threads']
            count = row.get('params', {}).get('symbolCount', '-')
            value = row['primaryMetric']['score']
            label = variant
            if stage == 'map':
                label = 'baseline' if method.startswith('heap') else 'candidate'
                method = method.removeprefix('heap').removeprefix('map').lower()
            elif stage == 'mixed':
                value = row['secondaryMetrics']['read']['score']
                label = 'baseline' if variant == 'separate' else 'candidate'
                overlaps.setdefault(threads, {}).setdefault(variant, []).extend(
                    float(v) for v in re.findall(r'min_read_overlap=([0-9.]+)', log.read_text())
                )
            elif stage == 'fill':
                counters = row['secondaryMetrics']
                fills.setdefault(threads, []).append((value, counters['admitted']['score'], counters['attempted']['score']))
                continue
            key = stage, method, threads, count
            target = scores.setdefault(key, {}).setdefault(label, {})
            if block in target:
                raise ValueError(f'duplicate round: {key}, {label}, {block}')
            target[block] = value

    for name, variants in like.items():
        if set(variants) != {'baseline', 'candidate'} or variants['baseline'].keys() != variants['candidate'].keys():
            raise ValueError(f'incomplete LIKE comparison: {name}')
    for name, variants in query.items():
        if set(variants) != {'baseline', 'candidate', 'nocache'} or not (
            variants['baseline'].keys() == variants['candidate'].keys() == variants['nocache'].keys()
        ):
            raise ValueError(f'incomplete query comparison: {name}')

    print('# Local symbol-cache measurements\n')
    if scores:
        print('Lookup timings include consuming two Latin-1 characters. Keys visits a precomputed permutation; chain obtains the next key from the preceding value. The cycle covers the full dictionary.\n')
    print('Ratios are geometric means of paired JVM ratios; intervals are approximate 95% Student t intervals on log ratios, using independent JVM pairs. With few pairs, distributional assumptions are weakly checked. Iterations within a JVM are not independent replicates.\n')
    for stage in ['reader', 'map', 'miss', 'mixed']:
        rows = [(key, value) for key, value in scores.items() if key[0] == stage]
        if not rows:
            continue
        print(f'## {stage}\n')
        comparison = 'CACHE / NOCACHE' if stage == 'miss' else 'shared / separate' if stage == 'mixed' else 'candidate / baseline'
        print(f'| Method | Threads | Symbols | Baseline ns/op | Candidate ns/op | Delta ns/op | {comparison}, 95% CI | NOCACHE ns/op |')
        print('|---|---:|---:|---:|---:|---:|---|---:|')
        for (_, method, threads, count), variants in sorted(rows):
            baseline = variants.get('nocache' if stage == 'miss' else 'baseline', {})
            candidate = variants.get('candidate', {})
            if not baseline or not candidate:
                continue
            rounds = sorted(baseline.keys() & candidate.keys())
            if not rounds:
                continue
            b = statistics.mean(baseline[r] for r in rounds)
            c = statistics.mean(candidate[r] for r in rounds)
            nocache = variants.get('nocache', {})
            nc = f'{statistics.mean(nocache.values()):.3f}' if nocache else '-'
            print(f'| {method} | {threads} | {count} | {b:.3f} | {c:.3f} | {c-b:+.3f} | {paired_ratio(baseline, candidate)} | {nc} |')
        print()
    if overlaps:
        print('Measured read/write overlap (minimum over read threads in each shot):\n')
        for threads, variants in sorted(overlaps.items()):
            for variant, values in sorted(variants.items()):
                print(f'- {threads} threads, {variant}: min={min(values):.1%}, median={statistics.median(values):.1%}, shots={len(values)}.')
        print()
    if fills:
        print('## Native-address cold fill\n')
        print('Timings are per attempt, including try-lock refusals; they are not latency per admitted symbol.\n')
        print('| Threads | ns/attempt | Admitted / attempted |')
        print('|---:|---:|---:|')
        for threads, values in sorted(fills.items()):
            print(f'| {threads} | {statistics.mean(v[0] for v in values):.3f} | {sum(v[1] for v in values) / sum(v[2] for v in values):.1%} |')
        print()
    if like:
        print(f'## LIKE dictionary scans: {like_parameters[0]} symbols, {like_parameters[1]} budget\n')
        print('Cells show the median and [Q1, Q3] of independent JVM medians, in microseconds, followed by the JVM count.\n')
        print('| Phase | Baseline | Candidate | Candidate / baseline, 95% CI |')
        print('|---|---:|---:|---|')
        for name, variants in sorted(like.items()):
            cells = []
            for variant in ['baseline', 'candidate']:
                values = list(variants[variant].values())
                cells.append(f'{statistics.median(values):.3f} [{quartile(values, .25):.3f}, {quartile(values, .75):.3f}] (n={len(values)})')
            print(f'| {name} | ' + ' | '.join(cells) + f" | {paired_ratio(variants['baseline'], variants['candidate'])} |")
        print('\nIndividual JVM medians:\n')
        for name, variants in sorted(like.items()):
            for variant, values in sorted(variants.items()):
                print(f'- {name}, {variant}: ' + ', '.join(f'{values[r]:.3f}' for r in sorted(values)))
        print()
    if query:
        print('## Queries\n')
        print('Cells show the median and [Q1, Q3] of independent JVM medians, in microseconds, followed by the JVM count.\n')
        print('| Query | Baseline | Candidate | NOCACHE | Candidate / baseline, 95% CI |')
        print('|---|---:|---:|---:|---|')
        for name, variants in sorted(query.items()):
            cells = []
            for variant in ['baseline', 'candidate', 'nocache']:
                values = list(variants[variant].values())
                cells.append(f'{statistics.median(values):.3f} [{quartile(values, .25):.3f}, {quartile(values, .75):.3f}] (n={len(values)})')
            print(f'| {name} | ' + ' | '.join(cells) + f" | {paired_ratio(variants['baseline'], variants['candidate'])} |")
        print('\nIndividual JVM medians:\n')
        for name, variants in sorted(query.items()):
            for variant, values in sorted(variants.items()):
                print(f'- {name}, {variant}: ' + ', '.join(f'{values[r]:.3f}' for r in sorted(values)))


if __name__ == '__main__':
    main(Path(sys.argv[1]))
