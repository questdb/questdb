#!/usr/bin/env python3
"""Run bounded local symbol-cache measurements, one case at a time.

Uses classes/ and environment.json under benchmarks/target/symbol-cache-measure.
The baseline and candidate must be compiled before running this script.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


JVM_LIMITS = [
    '-Xms32m', '-Xmx256m', '-XX:MaxDirectMemorySize=64m',
    '-XX:ActiveProcessorCount=2', '-XX:CICompilerCount=2', '-XX:+UseSerialGC',
]


def native_hashes(core):
    libraries = {}
    for directory in ['bin', 'bin-local', 'rust']:
        for path in (core / 'io/questdb' / directory).rglob('*'):
            if path.is_file() and path.suffix in {'.dylib', '.so', '.dll'}:
                libraries[str(path.relative_to(core))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return libraries


def check_natives(environment):
    candidate = native_hashes(Path(environment['cores']['candidate']))
    baseline = native_hashes(Path(environment['cores']['baseline']))
    if not candidate or candidate != baseline:
        raise ValueError('baseline and candidate native libraries differ (including bin-local and rust overrides)')


def cases(stage, forks, threads, symbols):
    for iteration in range(forks):
        variants = ['baseline', 'candidate'] if stage == 'like' else ['baseline', 'candidate', 'nocache']
        offset = iteration % len(variants)
        variants = variants[offset:] + variants[:offset]
        if stage in {'query', 'like'}:
            for variant in variants:
                yield f'{stage}-{variant}-r{iteration}', variant, iteration, None, []
            continue
        for count in threads:
            prefix = f'{stage}-t{count}-r{iteration}'
            if stage == 'reader':
                for size in symbols:
                    for method in ['keys', 'chain']:
                        for variant in variants:
                            if method == 'chain' and variant == 'nocache':
                                continue
                            mode = 'NOCACHE' if variant == 'nocache' else 'CACHE'
                            yield f'{prefix}-s{size}-{method}-{variant}', variant, iteration, 'SymbolMapCacheReaderBenchmark.' + method, [
                                '-t', str(count), '-p', f'symbolCount={size}', '-p', 'cacheMode=' + mode,
                            ]
            elif stage == 'map':
                methods = ['heapKeys', 'mapKeys', 'heapChain', 'mapChain']
                if iteration % 2:
                    methods.reverse()
                for size in symbols:
                    for method in methods:
                        yield f'{prefix}-s{size}-{method}', 'candidate', iteration, 'SymbolMapCacheBenchmark.' + method, [
                            '-t', str(count), '-p', f'symbolCount={size}',
                        ]
            elif stage == 'miss':
                for variant in ['candidate', 'nocache'] if iteration % 2 == 0 else ['nocache', 'candidate']:
                    mode = 'NOCACHE' if variant == 'nocache' else 'CACHE'
                    yield f'{prefix}-{variant}', variant, iteration, 'SymbolMapCacheReaderBenchmark.keys', [
                        '-t', str(count), '-p', 'symbolCount=65536', '-p', 'scenario=stopped', '-p', 'cacheMode=' + mode,
                    ]
            elif stage == 'mixed':
                for target in ['shared', 'separate'] if iteration % 2 == 0 else ['separate', 'shared']:
                    yield f'{prefix}-{target}', target, iteration, 'SymbolMapCacheReadWriteBenchmark.mixed', [
                        '-t', str(count), '-tg', '1,1', '-p', 'target=' + target,
                    ]
            elif stage == 'fill':
                yield prefix, 'candidate', iteration, 'SymbolMapCacheBenchmark.coldFillNative', ['-t', str(count)]


def stop(process):
    # JMH forks must not outlive an interrupted or timed-out runner.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    finally:
        # The launcher can exit before a fork that ignores SIGTERM.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run(command, log):
    with log.open('w') as output:
        process = subprocess.Popen(
            command, stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
            env={**os.environ, 'QDB_LOG_W_STDOUT_LEVEL': 'ERROR'},
        )
        try:
            if process.wait(timeout=90) != 0:
                raise RuntimeError(f'measurement failed; see {log}')
        except BaseException:
            stop(process)
            raise


def main():
    root = Path(__file__).resolve().parents[4]
    work = root / 'benchmarks/target/symbol-cache-measure'
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['reader', 'map', 'miss', 'mixed', 'fill', 'query', 'like'])
    parser.add_argument('--forks', type=int, choices=[1, 3, 6], default=1)
    parser.add_argument('--threads', type=int, choices=[1, 2, 4, 8], nargs='+')
    parser.add_argument('--symbols', type=int, choices=[64, 65536], nargs='+', default=[64, 65536])
    parser.add_argument('--query-symbols', type=int, choices=[64, 65536, 1048576], default=64)
    parser.add_argument('--query-budget', choices=['ample', 'stopped'], default='stopped')
    parser.add_argument('--results', type=Path, default=work / 'bounded-results')
    args = parser.parse_args()
    threads = args.threads or ([2] if args.stage == 'mixed' else [1])
    if args.stage == 'mixed' and threads != [2]:
        parser.error('mixed uses exactly one reader and one writer (--threads 2)')
    if len(set(threads)) != len(threads) or len(set(args.symbols)) != len(args.symbols):
        parser.error('thread and symbol counts must be distinct')
    environment = json.loads((work / 'environment.json').read_text())
    if args.stage in {'reader', 'query', 'like'}:
        check_natives(environment)
    results = args.results.resolve()
    results.mkdir(parents=True, exist_ok=True)
    manifest_path = results / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else []
    if args.stage == 'like':
        parameters = [str(args.query_symbols), args.query_budget]
        for entry in manifest:
            if entry['stage'] == 'like' and entry['command'][-2:] != parameters:
                raise ValueError('LIKE parameters changed; use a new results directory')
    for name, variant, iteration, pattern, extra in cases(args.stage, args.forks, threads, args.symbols):
        core = 'baseline' if variant == 'baseline' else 'candidate'
        classpath = [work / 'classes', Path(environment['cores'][core]), *map(Path, environment['dependencies'])]
        for path in classpath:
            if not path.exists():
                raise FileNotFoundError(path)
        command = ['nice', '-n', '15', 'java', *environment['java_options'], *JVM_LIMITS, '-cp', os.pathsep.join(map(str, classpath))]
        if args.stage == 'like':
            command += ['org.questdb.SymbolMapCacheLikeBenchmark', str(args.query_symbols), args.query_budget]
        elif args.stage == 'query':
            warmups = '100'
            repetitions = '50' if args.query_symbols == 64 else '5'
            budget = 256 * 1024 * (1024 if args.query_budget == 'ample' else 1)
            command += ['org.questdb.SymbolMapCacheQueryBenchmark', 'NOCACHE' if variant == 'nocache' else 'CACHE', warmups, repetitions, '9', '1', str(args.query_symbols), str(budget)]
        else:
            command += ['org.openjdk.jmh.Main', pattern, '-f', '1', '-foe', 'true', '-rf', 'json', '-rff', str(results / (name + '.json')), *extra]
        if sys.platform == 'darwin':
            command = ['/usr/bin/time', '-l', *command]
        previous = next((entry for entry in manifest if entry['name'] == name), None)
        if previous is not None:
            if previous['command'] != command:
                raise ValueError('command changed; use a new results directory')
            print('REUSE', name, flush=True)
            continue
        print('START', name, flush=True)
        run(command, results / (name + '.log'))
        manifest.append({'name': name, 'stage': args.stage, 'variant': variant, 'round': iteration, 'command': command})
        pending = manifest_path.with_suffix('.tmp')
        pending.write_text(json.dumps(manifest, indent=2) + '\n')
        pending.replace(manifest_path)
        print('DONE', name, flush=True)
        time.sleep(5)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    main()
