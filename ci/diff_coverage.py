#!/usr/bin/env python3
"""
Local diff coverage for QuestDB: which changed lines, branches and conditions
do the tests you name leave unexecuted?

The intended loop, as with CI's coverage report or IntelliJ's coverage runner:
run the relevant tests once with coverage, read what never ran, write tests
that reach it, then measure only those new tests with --append. Their coverage
is added to the previous run's, so a large change never re-runs its whole test
set to check a few new tests.

Changed lines are the lines the working tree adds or modifies relative to the
base revision: committed, staged and unstaged changes, plus untracked files.
Only production sources count: Java under core/src/main/java and Rust under
core/rust/<crate>/src. Lines without coverage data (comments, declarations,
code the compiler drops) are not counted.

Java coverage comes from JaCoCo through `-P jacoco`, as in CI.

Rust coverage comes from LLVM source-based coverage, with branch coverage
(-Zcoverage-options=branch; the qdbr toolchain is nightly). The script builds
an instrumented libquestdbr, puts it in the dev-library slot that Os.java loads
in preference to the shipped binary (core/target/classes/io/questdb/rust/),
runs the Java tests so that every JNI call is counted, runs the unit tests of
each changed crate, and merges the profiles. The dev slot is restored
afterwards, and on the next run if this one was interrupted.

Branches include each condition of && and || (JaCoCo counts them per line;
for Rust, branch coverage records each operand's true and false outcome).
Branches on assert lines (Java assert, Rust assert!/debug_assert!) are not
counted: their failing side is unreachable by design.

Examples:
  python3 ci/diff_coverage.py --tests ShowTablesTest,TableVisibilityTest
  python3 ci/diff_coverage.py --tests 'LatestByTest#testMultiple*'
  python3 ci/diff_coverage.py --base HEAD~1
  python3 ci/diff_coverage.py --lang rust --rust-tests col_driver
  python3 ci/diff_coverage.py --append --tests NewGapTest   # add new tests' coverage
  python3 ci/diff_coverage.py --summary     # one line per file, for large changes
  python3 ci/diff_coverage.py --no-run      # re-analyse the last run

Without --tests, the script runs the test classes that the diff adds or
changes. Name the tests yourself whenever existing tests also cover the change.
A Rust-only change may use "--tests none" to measure Rust unit tests alone; add
Java tests whenever Java calls the changed Rust code through JNI. --paths limits
the measured files, e.g. to the files of one item when the working tree holds
unrelated edits.

With --append, coverage recorded for a production file that has changed since
the previous run may be lost (JaCoCo and LLVM drop data for a class or function
that no longer matches), and the script names those files: re-run their tests
without --append.

The script compiles through ci/incremental_compile.py, so a run compiles only
the changed sources and the sources that depend on what changed.

Prerequisites: the java-questdb-client version core/pom.xml expects, installed
in ~/.m2 (see CLAUDE.md), and for Rust the llvm-tools component of the qdbr
toolchain (the script prints the rustup command when it is missing).

Exit codes: 0 every changed line and branch is covered; 1 coverage gaps;
2 setup error; 3 build or test failure.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ElementTree
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import incremental_compile  # noqa: E402

JAVA_SRC = 'core/src/main/java/'
JAVA_TEST_SRC = 'core/src/test/java/'
RUST_CRATES = ('qdbr', 'qdb-core', 'qdb-parquet-meta', 'parquet2', 'parquet-format-safe')
RUST_SRC_RE = re.compile(r'^core/rust/([^/]+)/src/.+\.rs$')
HUNK_RE = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@')
ASSERT_RE = re.compile(r'^\s*(assert\b|(debug_)?assert(_eq|_ne)?!)')
DIFF_PATHS = ('core/src/main/java', 'core/src/test/java', 'core/rust')
LLVM_IGNORE_RE = r'(\.cargo/registry|\.cargo/git|/rustc/|\.rustup/)'
AUTO_SUMMARY_ROWS = 300


class SetupError(Exception):
    pass


class RunFailure(Exception):
    pass


def main():
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args()
    root = Path(git_out(None, 'rev-parse', '--show-toplevel').strip()).resolve()
    cov_dir = root / 'core/target/coverage'
    try:
        restore_dev_rust_lib(root, cov_dir)
        base, base_desc = resolve_base(root, args.base)
        changed = changed_lines(root, base)
        measured = {p: ls for p, ls in changed.items() if in_paths(p, args.paths)}
        java_files = {p: ls for p, ls in measured.items() if is_java_source(p)} if args.lang in ('all', 'java') else {}
        rust_files = {p: ls for p, ls in measured.items() if rust_crate(p)} if args.lang in ('all', 'rust') else {}
        print(f'Base: {base[:10]} ({base_desc}); changed sources: {len(java_files)} Java, {len(rust_files)} Rust')
        if not java_files and not rust_files:
            print('No changed Java or Rust source lines; nothing to measure.')
            return 0

        tests = '' if args.tests == 'none' else args.tests or default_tests(changed)
        if args.rust_tests is None:
            args.rust_tests = 'none' if args.append else 'all'
        if not args.no_run:
            cov_dir.mkdir(parents=True, exist_ok=True)
            run_coverage(root, cov_dir, tests, java_files, rust_files, args)
            save_measured_state(root, cov_dir, [*java_files, *rust_files], args.append)

        java_cov = load_jacoco(cov_dir, root, set(java_files)) if java_files else {}
        rust_cov = load_lcov(cov_dir / 'rust.lcov', root, set(rust_files)) if rust_files else ({}, {})
        report = build_report(root, java_files, rust_files, java_cov, rust_cov)
        report['base'] = base
        report['tests'] = tests or 'none'
        report_path = Path(args.json) if args.json else cov_dir / 'diff-coverage.json'
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2))
        print_report(report, args.max_lines, args.summary)
        print(f'JSON report: {report_path.relative_to(root) if report_path.is_relative_to(root) else report_path}')
        return 1 if report['gaps'] else 0
    except SetupError as e:
        print(f'SETUP ERROR: {e}', file=sys.stderr)
        return 2
    except RunFailure as e:
        print(f'RUN FAILED: {e}', file=sys.stderr)
        return 3


def parse_args():
    p = argparse.ArgumentParser(
        description='Report changed Java and Rust lines that the given tests do not execute.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__[__doc__.index('Examples:'):],
    )
    p.add_argument('--base', help='base revision; default: merge-base of HEAD and origin/master')
    p.add_argument('--tests', help='surefire -Dtest value, e.g. "FooTest,BarTest#testBaz*", or "none"; '
                                   'default: test classes the diff adds or changes')
    p.add_argument('--paths', nargs='+', metavar='PATH',
                   help='measure only changed files under these paths (files or directories)')
    p.add_argument('--rust-tests', metavar='all|none|FILTER',
                   help='unit tests to run in each changed Rust crate (default: all; none with --append)')
    p.add_argument('--append', action='store_true',
                   help='add the coverage of these tests to the previous run instead of starting over')
    p.add_argument('--summary', action='store_true',
                   help='print one line per file instead of the uncovered lines')
    p.add_argument('--lang', choices=('all', 'java', 'rust'), default='all', help='languages to measure')
    p.add_argument('--no-run', action='store_true', help='re-analyse the artifacts of the last run')
    p.add_argument('--ignore-test-failures', action='store_true',
                   help='report coverage even if some tests fail')
    p.add_argument('--maven-arg', action='append', default=[], help='extra Maven argument (repeatable)')
    p.add_argument('--max-lines', type=int, default=40, help='uncovered lines to print per file')
    p.add_argument('--json', help='JSON report path; default: core/target/coverage/diff-coverage.json')
    return p.parse_args()


# ---------------------------------------------------------------- git and diff

def git_out(root, *args):
    try:
        return subprocess.run(['git', *args], cwd=root, check=True, capture_output=True, text=True).stdout
    except subprocess.CalledProcessError as e:
        raise SetupError(f'git {" ".join(args)} failed: {e.stderr.strip()}')


def resolve_base(root, base):
    if base:
        return git_out(root, 'rev-parse', '--verify', base + '^{commit}').strip(), base
    for upstream in ('origin/master', 'master'):
        try:
            sha = git_out(root, 'merge-base', 'HEAD', upstream).strip()
            return sha, f'merge-base of HEAD and {upstream}'
        except SetupError:
            continue
    raise SetupError('cannot find origin/master or master; pass --base')


def changed_lines(root, base):
    """Maps each changed file to the set of its added or modified line numbers in the working tree."""
    diff = git_out(root, 'diff', '--no-color', '--no-ext-diff', '-U0', '--diff-filter=ACMR', base, '--', *DIFF_PATHS)
    files = {}
    current = None
    for line in diff.splitlines():
        if line.startswith('+++ '):
            path = line[4:].strip('"')
            current = None if path == '/dev/null' else path[2:] if path.startswith('b/') else path
            if current is not None:
                files.setdefault(current, set())
        elif line.startswith('@@') and current is not None:
            m = HUNK_RE.match(line)
            start, count = int(m.group(1)), int(m.group(2) or 1)
            files[current].update(range(start, start + count))
    for path in git_out(root, 'ls-files', '--others', '--exclude-standard', '--', *DIFF_PATHS).splitlines():
        try:
            with open(root / path, encoding='utf-8', errors='replace') as f:
                files[path] = set(range(1, sum(1 for _ in f) + 1))
        except OSError:
            pass
    return {p: ls for p, ls in files.items() if ls}


def in_paths(path, paths):
    return not paths or any(path == p.rstrip('/') or path.startswith(p.rstrip('/') + '/') for p in paths)


def is_java_source(path):
    return path.startswith(JAVA_SRC) and path.endswith('.java')


def rust_crate(path):
    m = RUST_SRC_RE.match(path)
    return m.group(1) if m and m.group(1) in RUST_CRATES else None


def default_tests(changed):
    names = sorted({Path(p).stem for p in changed
                    if p.startswith(JAVA_TEST_SRC) and p.endswith('Test.java')
                    and not Path(p).stem.startswith('Abstract')})
    return ','.join(names)


# ---------------------------------------------------------------- running

def run_coverage(root, cov_dir, tests, java_files, rust_files, args):
    if java_files and not tests:
        raise SetupError('the changed Java code needs Java tests: name them with --tests')
    if not tests and not (rust_files and args.rust_tests != 'none'):
        raise SetupError('nothing to run: name the tests with --tests')
    if tests:
        check_client_installed(root)
    elif rust_files:
        print('No Java tests: measuring Rust unit tests only (Rust code that Java calls through JNI '
              'needs Java tests in --tests)')
    rust = rust_toolchain(root) if rust_files else None
    if args.append:
        warn_changed_since_last_run(root, cov_dir, [*java_files, *rust_files], bool(java_files))
    else:
        remove(root / 'core/target/jacoco.exec')
        for stale in ('rust-unit', 'rust-objects.json', *[p.name for p in cov_dir.glob('jni*.profraw')]):
            remove(cov_dir / stale)
    for stale in ('rust.lcov', 'rust.profdata'):
        remove(cov_dir / stale)
    remove(root / 'core/target/site/jacoco')
    remove(root / 'core/questdbr-junit.profraw')

    objects = []
    jni_profile = None
    if tests:
        try:
            incremental_compile.compile_core(root, incremental_compile.compile_args(args.maven_arg),
                                             log=lambda message: print(message.strip()))
        except incremental_compile.CompileError as e:
            raise RunFailure(f'compilation failed\n{e}')
        if rust:
            objects.append(build_instrumented_rust_lib(root, cov_dir, rust))
            install_dev_rust_lib(root, cov_dir, objects[0])
        try:
            run_maven_tests(root, cov_dir, tests, args)
        finally:
            if rust:
                restore_dev_rust_lib(root, cov_dir)
                junit_profraw = root / 'core/questdbr-junit.profraw'
                if junit_profraw.exists():
                    jni_profile = cov_dir / f'jni-{len(list(cov_dir.glob("jni-*.profraw"))) + 1}.profraw'
                    shutil.move(junit_profraw, jni_profile)
    if rust:
        if args.rust_tests != 'none':
            for crate in sorted({rust_crate(p) for p in rust_files}):
                objects += run_rust_unit_tests(root, cov_dir, rust, crate, args.rust_tests)
        if tests and jni_profile is None:
            print('  WARNING: the Java tests left no Rust profile; they did not load the instrumented '
                  'library or never called into Rust.')
        export_rust_lcov(cov_dir, rust, remember_rust_objects(cov_dir, objects, args.append))


def file_digest(path):
    try:
        return hashlib.sha1(path.read_bytes()).hexdigest()
    except OSError:
        return None


def save_measured_state(root, cov_dir, paths, append):
    state = cov_dir / 'measured.json'
    recorded = json.loads(state.read_text()) if append and state.exists() else {}
    recorded.update({p: file_digest(root / p) for p in paths})
    state.write_text(json.dumps(recorded, indent=1))


def warn_changed_since_last_run(root, cov_dir, paths, has_java):
    state = cov_dir / 'measured.json'
    if not state.exists() or (has_java and not (root / 'core/target/jacoco.exec').exists()):
        raise SetupError('--append adds to a previous run, and there is none; run without --append first')
    recorded = json.loads(state.read_text())
    changed = sorted(p for p in paths if p in recorded and recorded[p] != file_digest(root / p))
    if changed:
        print('WARNING: these files changed since the previous run, so coverage recorded before the change '
              'may be lost; re-run their tests without --append:')
        for p in changed:
            print(f'  {p}')
    return changed


def remember_rust_objects(cov_dir, objects, append):
    """Keeps the instrumented binaries of earlier runs, so --append still maps their profiles."""
    state = cov_dir / 'rust-objects.json'
    known = json.loads(state.read_text()) if append and state.exists() else []
    merged = [o for o in dict.fromkeys([*known, *map(str, objects)]) if Path(o).exists()]
    state.write_text(json.dumps(merged, indent=1))
    return merged


def remove(path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def check_client_installed(root):
    pom = (root / 'core/pom.xml').read_text()
    m = re.search(r'<questdb\.client\.version>([^<]+)</questdb\.client\.version>', pom)
    if not m:
        return
    version = m.group(1)
    jar = Path.home() / '.m2/repository/org/questdb/questdb-client' / version / f'questdb-client-{version}.jar'
    if not jar.exists():
        raise SetupError(f'questdb-client {version} is not in ~/.m2. Build its native library and install it '
                         f'as CLAUDE.md describes ("cd java-questdb-client && mvn install -DskipTests", '
                         f'never with "clean").')


def run_maven_tests(root, cov_dir, tests, args):
    log = cov_dir / 'mvn-test.log'
    # incremental_compile has compiled everything; this keeps Maven from recompiling the module
    cmd = ['mvn', '-f', 'core/pom.xml', 'test', '-P', 'jacoco', f'-Dtest={tests}',
           '-Dsurefire.failIfNoSpecifiedTests=false', incremental_compile.QUICK]
    if args.ignore_test_failures:
        cmd.append('-Dmaven.test.failure.ignore=true')
    cmd += args.maven_arg
    print(f'Running Java tests with coverage: {tests}')
    print(f'  log: {log.relative_to(root)}')
    with open(log, 'w') as out:
        rc = subprocess.call(cmd, cwd=root, stdout=out, stderr=subprocess.STDOUT)
    text = log.read_text(errors='replace')
    totals = re.findall(r'Tests run: (\d+), Failures: (\d+), Errors: (\d+), Skipped: (\d+)\s*$', text, re.M)
    if totals:
        run, failures, errors, skipped = totals[-1]
        print(f'  tests run: {run}, failures: {failures}, errors: {errors}, skipped: {skipped}')
    if rc != 0:
        tail = [ln for ln in text.splitlines() if ln.startswith('[ERROR]')][:30]
        raise RunFailure('Maven failed; coverage was not computed.\n' + '\n'.join(tail) + f'\nFull log: {log}')
    if not totals or totals[-1][0] == '0':
        raise RunFailure(f'no test matched "{tests}"; check the class and method names')
    if not (root / 'core/target/site/jacoco/jacoco.xml').exists():
        raise RunFailure(f'JaCoCo produced no report; see {log}')


# ---------------------------------------------------------------- Rust

class RustToolchain:
    def __init__(self, name, host, profdata, llvm_cov):
        self.name = name
        self.host = host
        self.profdata = profdata
        self.llvm_cov = llvm_cov


def rust_toolchain(root):
    qdbr = root / 'core/rust/qdbr'

    def out(*cmd):
        try:
            return subprocess.run(cmd, cwd=qdbr, check=True, capture_output=True, text=True).stdout
        except (OSError, subprocess.CalledProcessError) as e:
            raise SetupError(f'{" ".join(cmd)} failed: {e}')

    name = out('rustup', 'show', 'active-toolchain').split()[0]
    host = re.search(r'^host: (\S+)', out('rustc', '-vV'), re.M).group(1)
    bin_dir = Path(out('rustc', '--print', 'sysroot').strip()) / 'lib/rustlib' / host / 'bin'
    profdata, llvm_cov = bin_dir / 'llvm-profdata', bin_dir / 'llvm-cov'
    if not (profdata.exists() and llvm_cov.exists()):
        raise SetupError(f'llvm-tools missing for the qdbr toolchain. Install them with:\n'
                         f'  rustup component add llvm-tools --toolchain {name}')
    return RustToolchain(name, host, profdata, llvm_cov)


def rust_env(root, profile_pattern):
    # RUSTFLAGS replaces the "-D warnings" from core/rust/qdbr/.cargo/config.toml, so repeat it.
    # The explicit --target keeps the flags off build scripts and proc macros.
    return dict(os.environ,
                RUSTFLAGS='-D warnings -C instrument-coverage -Zcoverage-options=branch',
                CARGO_TARGET_DIR=str(root / 'core/rust/qdbr/target/coverage'),
                LLVM_PROFILE_FILE=str(profile_pattern))


def rust_lib_name():
    system = platform.system()
    if system == 'Darwin':
        return 'libquestdbr.dylib'
    if system == 'Linux':
        return 'libquestdbr.so'
    raise SetupError(f'Rust coverage is not supported on {system}')


def build_instrumented_rust_lib(root, cov_dir, rust):
    log = cov_dir / 'cargo-build.log'
    print(f'Building instrumented libquestdbr ({rust.name}); log: {log.relative_to(root)}')
    cmd = ['cargo', f'+{rust.name}', 'build', '--lib', '--target', rust.host]
    with open(log, 'w') as out:
        rc = subprocess.call(cmd, cwd=root / 'core/rust/qdbr', stdout=out, stderr=subprocess.STDOUT,
                             env=rust_env(root, cov_dir / 'build-%p-%m.profraw'))
    if rc != 0:
        raise RunFailure(f'cargo build failed; see {log}')
    lib = root / 'core/rust/qdbr/target/coverage' / rust.host / 'debug' / rust_lib_name()
    if not lib.exists():
        raise RunFailure(f'cargo build produced no {lib}')
    return lib


def dev_slot(root):
    return root / 'core/target/classes/io/questdb/rust' / rust_lib_name()


def install_dev_rust_lib(root, cov_dir, lib):
    slot = dev_slot(root)
    existed = slot.exists()
    if existed:
        shutil.copy2(slot, cov_dir / ('saved-' + slot.name))
    (cov_dir / 'dev-slot.json').write_text(json.dumps({'existed': existed}))
    slot.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(lib, slot)


def restore_dev_rust_lib(root, cov_dir):
    state = cov_dir / 'dev-slot.json'
    if not state.exists():
        return
    slot = dev_slot(root)
    saved = cov_dir / ('saved-' + slot.name)
    if json.loads(state.read_text())['existed'] and saved.exists():
        shutil.move(saved, slot)
    elif slot.exists():
        slot.unlink()
    state.unlink()


def run_rust_unit_tests(root, cov_dir, rust, crate, selection):
    crate_dir = root / 'core/rust' / crate
    unit_dir = cov_dir / 'rust-unit'
    unit_dir.mkdir(parents=True, exist_ok=True)
    env = rust_env(root, unit_dir / f'{crate}-%p-%m.profraw')
    base_cmd = ['cargo', f'+{rust.name}', 'test', '--lib', '--target', rust.host]
    log = cov_dir / f'cargo-test-{crate}.log'
    print(f'Running Rust unit tests of {crate}' + ('' if selection == 'all' else f' matching "{selection}"'))
    print(f'  log: {log.relative_to(root)}')
    cmd = base_cmd + ([] if selection == 'all' else [selection])
    with open(log, 'w') as out:
        rc = subprocess.call(cmd, cwd=crate_dir, stdout=out, stderr=subprocess.STDOUT, env=env)
    summary = re.findall(r'^test result: .*$', log.read_text(errors='replace'), re.M)
    if summary:
        print('  ' + summary[-1])
        if selection != 'all' and ' 0 passed; 0 failed' in summary[-1]:
            print(f'  WARNING: no unit test of {crate} matched "{selection}"')
    if rc != 0:
        raise RunFailure(f'cargo test failed in {crate}; see {log}')
    listing = subprocess.run(base_cmd + ['--no-run', '--message-format=json'], cwd=crate_dir, env=env,
                             capture_output=True, text=True)
    binaries = []
    for line in listing.stdout.splitlines():
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        if msg.get('reason') == 'compiler-artifact' and msg.get('profile', {}).get('test') and msg.get('executable'):
            binaries.append(Path(msg['executable']))
    return binaries


def export_rust_lcov(cov_dir, rust, objects):
    profiles = sorted(cov_dir.glob('jni-*.profraw'))
    profiles += sorted((cov_dir / 'rust-unit').glob('*.profraw')) if (cov_dir / 'rust-unit').exists() else []
    if not profiles or not objects:
        raise RunFailure('no Rust coverage profile was produced')
    profdata = cov_dir / 'rust.profdata'
    subprocess.run([str(rust.profdata), 'merge', '-sparse', '-o', str(profdata), *map(str, profiles)],
                   check=True)
    cmd = [str(rust.llvm_cov), 'export', '--format=lcov', f'--instr-profile={profdata}',
           f'--ignore-filename-regex={LLVM_IGNORE_RE}']
    for obj in objects:
        cmd += ['--object', str(obj)]
    with open(cov_dir / 'rust.lcov', 'w') as out:
        subprocess.run(cmd, check=True, stdout=out)


# ---------------------------------------------------------------- coverage data

def load_jacoco(cov_dir, root, wanted):
    """Returns {path: {line: (missed instr, covered instr, missed branches, covered branches)}}."""
    xml = root / 'core/target/site/jacoco/jacoco.xml'
    if not xml.exists():
        raise SetupError(f'no JaCoCo report at {xml}; run without --no-run first')
    result = {}
    package = None
    for event, elem in ElementTree.iterparse(xml, events=('start', 'end')):
        if event == 'start':
            if elem.tag == 'package':
                package = elem.get('name')
            continue
        if elem.tag == 'sourcefile':
            path = f'{JAVA_SRC}{package}/{elem.get("name")}'
            if path in wanted:
                result[path] = {int(ln.get('nr')): (int(ln.get('mi')), int(ln.get('ci')),
                                                    int(ln.get('mb')), int(ln.get('cb')))
                                for ln in elem.iter('line')}
            elem.clear()
        elif elem.tag in ('class', 'package'):
            elem.clear()
    return result


def load_lcov(lcov, root, wanted):
    """Returns ({path: {line: count}}, {path: {line: {branch id: count}}}), merging records of a file."""
    if not lcov.exists():
        raise SetupError(f'no Rust coverage at {lcov}; run without --no-run first')
    lines, branches = {}, {}
    path = None
    real_root = os.path.realpath(root)
    with open(lcov) as f:
        for line in f:
            if line.startswith('SF:'):
                path = os.path.relpath(os.path.realpath(line[3:].strip()), real_root)
                if path not in wanted:
                    path = None
            elif path is None:
                continue
            elif line.startswith('DA:'):
                number, count = line[3:].split(',')[:2]
                file_lines = lines.setdefault(path, {})
                file_lines[int(number)] = max(file_lines.get(int(number), 0), int(count))
            elif line.startswith('BRDA:'):
                number, block, branch, taken = line[5:].strip().split(',')[:4]
                line_branches = branches.setdefault(path, {}).setdefault(int(number), {})
                key = (block, branch)
                line_branches[key] = max(line_branches.get(key, 0), 0 if taken == '-' else int(taken))
            elif line.startswith('end_of_record'):
                path = None
    return lines, branches


def build_report(root, java_files, rust_files, java_cov, rust_cov):
    files = []
    totals = {'java': [0, 0, 0, 0], 'rust': [0, 0, 0, 0]}  # lines covered, lines, branches covered, branches
    rust_lines, rust_branches = rust_cov
    for lang, changed in (('java', java_files), ('rust', rust_files)):
        for path in sorted(changed):
            data = java_cov.get(path, {}) if lang == 'java' else rust_lines.get(path, {})
            source = read_lines(root / path)
            entry = {'path': path, 'lang': lang, 'lines': [0, 0], 'branches': [0, 0],
                     'uncovered': [], 'partial': []}
            for n in sorted(changed[path]):
                if n not in data:
                    continue
                text = source[n - 1] if n <= len(source) else ''
                if lang == 'java':
                    _, ci, missed_branches, covered_branches = data[n]
                    covered = ci > 0
                else:
                    covered = data[n] > 0
                    outcomes = rust_branches.get(path, {}).get(n, {})
                    covered_branches = sum(1 for taken in outcomes.values() if taken > 0)
                    missed_branches = len(outcomes) - covered_branches
                if missed_branches + covered_branches and not ASSERT_RE.match(text):
                    total = missed_branches + covered_branches
                    entry['branches'][0] += covered_branches
                    entry['branches'][1] += total
                    if covered and missed_branches:
                        entry['partial'].append({'line': n, 'covered': covered_branches, 'total': total,
                                                 'text': text})
                entry['lines'][1] += 1
                if covered:
                    entry['lines'][0] += 1
                else:
                    entry['uncovered'].append({'line': n, 'text': text})
            if entry['lines'][1] == 0:
                continue
            t = totals[lang]
            t[0] += entry['lines'][0]
            t[1] += entry['lines'][1]
            t[2] += entry['branches'][0]
            t[3] += entry['branches'][1]
            files.append(entry)
    gaps = sum(len(e['uncovered']) + len(e['partial']) for e in files)
    return {'totals': totals, 'files': files, 'gaps': gaps}


def read_lines(path):
    try:
        return path.read_text(encoding='utf-8', errors='replace').splitlines()
    except OSError:
        return []


# ---------------------------------------------------------------- output

def plural(n, noun):
    return f'{n} {noun}' + ('' if n == 1 else 's')


def pct(covered, total):
    return f'{100.0 * covered / total:.1f}%' if total else 'n/a'


def print_report(report, max_lines, summary):
    print()
    gap_files = [e for e in report['files'] if e['uncovered'] or e['partial']]
    if not summary and report['gaps'] > AUTO_SUMMARY_ROWS:
        print(f'{report["gaps"]} uncovered lines and branches: one line per file follows; '
              f'use --paths for line detail.')
        summary = True
    if summary:
        gap_files.sort(key=lambda e: -(len(e['uncovered']) + len(e['partial'])))
        for e in gap_files[:80]:
            print(f"  {len(e['uncovered']):>5} not run  {len(e['partial']):>4} branch  "
                  f"lines {e['lines'][0]}/{e['lines'][1]}  {e['path']}")
        if len(gap_files) > 80:
            print(f'  ... {len(gap_files) - 80} more files in the JSON report')
        gap_files = []
    for e in gap_files:
        branches = f", branches {e['branches'][0]}/{e['branches'][1]}" if e['branches'][1] else ''
        print(f"{e['path']}  lines {e['lines'][0]}/{e['lines'][1]}{branches}")
        rows = [(u['line'], 'NOT RUN    ', u['text']) for u in e['uncovered']]
        rows += [(p['line'], f"BRANCH {p['covered']}/{p['total']}", p['text']) for p in e['partial']]
        rows.sort()
        for n, label, text in rows[:max_lines]:
            print(f'  {n:>6}  {label}  {text.strip()[:110]}')
        if len(rows) > max_lines:
            print(f'  ... {len(rows) - max_lines} more in the JSON report')
    print()
    for lang, (lc, lt, bc, bt) in report['totals'].items():
        if lt:
            branches = f', branches {bc}/{bt} ({pct(bc, bt)})' if bt else ''
            print(f'{lang.capitalize():<5} changed lines {lc}/{lt} ({pct(lc, lt)}){branches}')
    uncovered = sum(len(e['uncovered']) for e in report['files'])
    partial = sum(len(e['partial']) for e in report['files'])
    if report['gaps']:
        print(f'RESULT: GAPS - {plural(uncovered, "changed line")} never run, '
              f'{plural(partial, "line")} with an untaken branch')
    else:
        print('RESULT: OK - every changed line and branch is covered')


if __name__ == '__main__':
    sys.exit(main())
