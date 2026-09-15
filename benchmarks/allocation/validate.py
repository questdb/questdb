#!/usr/bin/env python3
"""Reject incomplete allocation evidence, missing workers, and unexplained bytes."""
import csv
import gzip
import json
import pathlib
import sys
from collections import defaultdict

root = pathlib.Path(sys.argv[1])
cases = list(csv.DictReader((root / 'cases.csv').open()))
if len(cases) != 24 or len({case['case'] for case in cases}) != 24:
    raise SystemExit('expected 24 distinct cases')
summary = []
comparisons = 0
for case in cases:
    expected_threads = {'owner', *(f'alloc_worker_{i}' for i in range(4))}
    if case['owners'] == '2':
        expected_threads.add('alloc_peer')
    checksums = []
    for run_pass in ('bytes', 'sites'):
        path = root / f"{case['case']}-{run_pass}.log.gz"
        with gzip.open(path, 'rt') as stream:
            lines = stream.read().splitlines()
        if not any(line.startswith('PASS,') for line in lines):
            raise SystemExit(f'missing successful completion: {path}')
        if not any('PLAN,Async Hash Join Group By' in line for line in lines):
            raise SystemExit(f'missing fused plan: {path}')
        if case['join'] == 'right' and not any('inputSwapped: true' in line for line in lines):
            raise SystemExit(f'missing right normalization: {path}')
        by_thread = defaultdict(int)
        stages = set()
        attribution = {}
        results = {}
        workers = set()
        for line in lines:
            if line.startswith('BYTES,'):
                _, storage, mode, rows, phase, stage, thread, value = line.split(',')
                key = (phase, stage, thread)
                if key in stages or int(value) < 0:
                    raise SystemExit(f'invalid byte sample: {path}: {line}')
                stages.add(key)
                by_thread[phase, thread] += int(value)
            elif line.startswith('ATTRIBUTION,'):
                _, phase, thread, value = line.split(',')
                attribution[phase, thread] = int(value)
            elif line.startswith('RESULT,'):
                _, storage, mode, rows, phase, result = line.split(',', 5)
                if phase in results:
                    raise SystemExit(f'duplicate result: {path}: {phase}')
                results[phase] = json.loads(result)
            elif line.startswith('WORK,execution'):
                _, phase, thread_id, name, frames = line.split(',')
                if int(frames) > 0 and name.startswith('alloc_worker_'):
                    workers.add(name)
        if set(results) != {'setup1024', 'execution1', 'execution2', 'execution3'}:
            raise SystemExit(f'incomplete executions: {path}')
        if len(stages) != 4 * 4 * len(expected_threads):
            raise SystemExit(f'incomplete thread/stage counters: {path}')
        for phase in ('execution1', 'execution2', 'execution3'):
            if results[phase] != results['execution1']:
                raise SystemExit(f'reuse mismatch: {path}')
            if run_pass == 'bytes':
                for thread in sorted(expected_threads):
                    actual = by_thread[phase, thread]
                    shared = attribution[phase, thread]
                    if actual != shared:
                        raise SystemExit(f'unexplained bytes: {path}: {phase}/{thread}: {actual - shared}')
                    summary.append([case['case'], phase, thread, actual, shared, actual - shared])
        if run_pass == 'sites' and workers != {f'alloc_worker_{i}' for i in range(4)}:
            raise SystemExit(f'missing participating worker: {path}: {workers}')
        checksums.append(results['execution1'])
        comparisons += 3 * int(case['owners'])
    if checksums[0] != checksums[1]:
        raise SystemExit(f'byte/census pass result mismatch: {case["case"]}')
with (root / 'summary.csv').open('w') as output:
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(['case', 'execution', 'thread', 'heap_bytes', 'shared_framework_bytes', 'fused_bytes'])
    writer.writerows(summary)
print(f'Validated {len(cases)} cases, {comparisons} measured candidate executions, {len(summary)} thread windows, zero unexplained bytes, and all four workers in every census case.')
