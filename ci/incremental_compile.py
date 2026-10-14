#!/usr/bin/env python3
"""
Incremental compilation of the core module for local builds.

Maven's compiler plugin has two modes, and neither scales to a module with
thousands of sources:
- by default it recompiles every source of a module when any one changed; for
  core that is about 3,800 main and 2,300 test sources, some 40 seconds;
- with useIncrementalCompilation=false it compiles only the changed sources but
  not the classes that use them, which can leave those stale.

This script makes the second mode safe. It compiles only the changed sources,
then compares what other classes compile against - the non-private types and
members with their signatures, supertypes, annotations and constant values -
between the old and the new class files. When that is unchanged, no other class
can be stale; this covers the common edit to method bodies and private members.
When it changed, when sources were deleted, or when the build inputs
(core/pom.xml, the java-questdb-client jar, the profiles) changed, the script
falls back to Maven's full rebuild.

Usage:
  python3 ci/incremental_compile.py                          # compile core
  python3 ci/incremental_compile.py -- test -Dtest=FooTest   # then run Maven
  python3 ci/incremental_compile.py --full                   # full rebuild

Maven runs the arguments after "--" with
-Dmaven.compiler.useIncrementalCompilation=false, so it compiles nothing more;
the profiles (-P) and properties (-D) among them also apply to compilation.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

QUICK = '-Dmaven.compiler.useIncrementalCompilation=false'
FULL_REBUILD_THRESHOLD = 400
MODULES = {
    'main': ('src/main/java', 'target/classes', 'compile'),
    'test': ('src/test/java', 'target/test-classes', 'testCompile'),
}
ACC_PRIVATE = 0x0002
ACC_SUPER = 0x0020
ACC_SYNTHETIC = 0x1000
ANONYMOUS_OR_LOCAL_RE = re.compile(r'\$\d')


class CompileError(Exception):
    pass


class ClassFormatError(Exception):
    pass


def main(argv):
    sys.stdout.reconfigure(line_buffering=True)
    own, maven_args = (argv[:argv.index('--')], argv[argv.index('--') + 1:]) if '--' in argv else (argv, [])
    if [a for a in own if a not in ('--full',)]:
        print(__doc__)
        return 2
    root = Path(subprocess.run(['git', 'rev-parse', '--show-toplevel'], check=True, capture_output=True,
                               text=True).stdout.strip())
    try:
        compile_core(root, compile_args(maven_args), full='--full' in own)
    except CompileError as e:
        print(f'COMPILE FAILED: {e}', file=sys.stderr)
        return 1
    if maven_args:
        return subprocess.call(['mvn', '-f', 'core/pom.xml', *maven_args, QUICK], cwd=root)
    return 0


def compile_args(maven_args):
    """Keeps the profiles and properties of a Maven command line; they can change what compiles."""
    kept = []
    args = iter(maven_args)
    for a in args:
        if a in ('-P', '-D'):
            kept += [a, next(args, '')]
        elif a.startswith(('-P', '-D')) and not a.startswith(('-Dtest=', '-Dsurefire.')):
            kept.append(a)
    return kept


class Build:
    """One compilation: the build state on disk and the Maven runs it needs."""

    def __init__(self, root, maven_args, log):
        self.root = root
        self.core = root / 'core'
        self.maven_args = list(maven_args)
        self.log = log
        self.state_file = self.core / 'target/incremental-compile.json'
        self.inputs = build_inputs(self.core, self.maven_args)
        try:
            self.state = json.loads(self.state_file.read_text())
        except (OSError, ValueError):
            self.state = None

    def save(self, **extra):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps({'inputs': self.inputs, **extra}))

    def full_rebuild(self, modules, reason):
        self.log(f'Incremental compile: {reason}')
        self.save(pending_full=sorted(modules))
        for m in modules:
            remove(self.core / 'target/maven-status/maven-compiler-plugin' / MODULES[m][2])
        run_maven(self.root, self.maven_args, quick=False)
        self.save()


def compile_core(root, maven_args=(), full=False, log=print):
    """Compiles core main and test sources incrementally; raises CompileError when javac fails."""
    build = Build(root, maven_args, log)
    started = time.time()
    state = build.state
    if full or (state and (state.get('inputs') != build.inputs or state.get('pending_full'))):
        reason = 'requested' if full else 'build inputs changed' if state.get('inputs') != build.inputs \
            else 'an earlier rebuild did not finish'
        delete_orphans(build.core, {m: scan(build.core, m)[1] for m in MODULES})
        build.full_rebuild(('main', 'test'), f'full rebuild ({reason})')
    elif state is None or not (build.core / 'target/classes').is_dir():
        log('Incremental compile: no earlier state; Maven decides what to compile')
        run_maven(root, build.maven_args, quick=False)
        build.save()
    else:
        try:
            compiled = compile_changes(build)
        except ClassFormatError as e:
            build.full_rebuild(('main', 'test'), f'full rebuild (cannot read {e})')
            compiled = True
        if not compiled:
            return
    log(f'  compiled in {time.time() - started:.0f}s')


def compile_changes(build):
    """Compiles changed sources only, and falls back to a full rebuild when their API changed."""
    core = build.core
    scans = {m: scan(core, m) for m in MODULES}
    unverified = build.state.get('unverified', {})
    stale = {m: sorted(set(scans[m][0]) | {Path(s) for s in unverified.get(m, {})}) for m in MODULES}
    orphans = {m: scans[m][1] for m in MODULES}
    if not any(stale.values()) and not any(orphans.values()):
        build.log('Incremental compile: no source changed')
        return False
    if orphans['main'] or Path('module-info.java') in stale['main'] or len(stale['main']) > FULL_REBUILD_THRESHOLD:
        delete_orphans(core, orphans)
        build.full_rebuild(('main', 'test'),
                           'full rebuild (main sources deleted, module-info changed, or many changes)')
        return True

    # The API recorded before an earlier failed compile wins over class files that compile may have written.
    before = {m: {**api_of(core, m, stale[m], scans[m][2]), **{Path(s): apis for s, apis in unverified.get(m, {}).items()}}
              for m in MODULES}
    stamps = class_stamps(core, stale, scans)
    try:
        run_maven(build.root, build.maven_args, quick=True)
    except CompileError:
        build.save(unverified={m: {str(s): apis for s, apis in before[m].items()} for m in MODULES})
        raise
    delete_classes_not_rewritten(core, stale, scans, stamps)
    after = {m: api_of(core, m, stale[m], scans[m][2]) for m in MODULES}

    changed_main = api_changes(before['main'], after['main'])
    changed_test = api_changes(before['test'], after['test'])
    counts = f"{len(stale['main'])} main and {len(stale['test'])} test sources"
    if changed_main:
        build.full_rebuild(('main', 'test'), f'compiled {counts}; the API of {changed_main} changed, so full rebuild')
    elif orphans['test'] or changed_test or len(stale['test']) > FULL_REBUILD_THRESHOLD:
        delete_orphans(core, {'test': orphans['test']})
        what = f'the API of {changed_test} changed' if changed_test else 'test sources were deleted'
        build.full_rebuild(('test',), f'compiled {counts}; {what}, so full test rebuild')
    else:
        build.log(f'Incremental compile: compiled {counts}; their API is unchanged')
        build.save()
    return True


def build_inputs(core, maven_args):
    pom = core / 'pom.xml'
    inputs = {'pom': hashlib.sha1(pom.read_bytes()).hexdigest(), 'args': sorted(maven_args)}
    m = re.search(r'<questdb\.client\.version>([^<]+)</questdb\.client\.version>', pom.read_text())
    if m:
        jar = Path.home() / '.m2/repository/org/questdb/questdb-client' / m.group(1) / f'questdb-client-{m.group(1)}.jar'
        inputs['client'] = [jar.stat().st_mtime_ns, jar.stat().st_size] if jar.exists() else None
    return inputs


def run_maven(root, maven_args, quick):
    log = root / 'core/target/incremental-compile.log'
    log.parent.mkdir(parents=True, exist_ok=True)
    cmd = ['mvn', '-f', 'core/pom.xml', 'test-compile', *maven_args] + ([QUICK] if quick else [])
    with open(log, 'w') as out:
        rc = subprocess.call(cmd, cwd=root, stdout=out, stderr=subprocess.STDOUT)
    if rc != 0:
        lines = log.read_text(errors='replace').splitlines()
        errors = list(dict.fromkeys(ln for ln in lines if ln.startswith('[ERROR]') and '.java:[' in ln))
        errors = errors or [ln for ln in lines if ln.startswith('[ERROR]')]
        raise CompileError('\n'.join(errors[:30]) + f'\nFull log: {log}')


def remove(path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


# ---------------------------------------------------------------- sources and classes

def scan(core, module):
    """Returns (stale sources, orphan top-level classes, secondary classes by (package, source))."""
    src_root, out_root = core / MODULES[module][0], core / MODULES[module][1]
    stale = []
    for dir_path, _, files in os.walk(src_root):
        for name in files:
            if not name.endswith('.java'):
                continue
            source = Path(dir_path, name)
            rel = source.relative_to(src_root)
            try:
                if source.stat().st_mtime > (out_root / rel.with_suffix('.class')).stat().st_mtime:
                    stale.append(rel)
            except FileNotFoundError:
                stale.append(rel)
    orphans, secondary = [], {}
    for dir_path, _, files in os.walk(out_root):
        for name in files:
            if not name.endswith('.class') or '$' in name:
                continue
            cls = Path(dir_path, name)
            rel = cls.relative_to(out_root)
            if (src_root / rel.with_suffix('.java')).exists():
                continue
            source = read_class(cls).source_file
            if source and (src_root / rel.parent / source).exists():
                secondary.setdefault((rel.parent, source), []).append(cls.stem)
            else:
                orphans.append(cls)
    return stale, orphans, secondary


def delete_orphans(core, orphans):
    for classes in orphans.values():
        for cls in classes:
            for nested in [cls, *cls.parent.glob(cls.stem + '$*.class')]:
                nested.unlink(missing_ok=True)


def classes_of(out_root, source, secondary):
    """Class files compiled from a source: its top-level types and their nested classes."""
    package = out_root / source.parent
    found = []
    for top in [source.stem, *secondary.get((source.parent, source.name), [])]:
        found += [package / f'{top}.class'] if (package / f'{top}.class').exists() else []
        found += sorted(package.glob(f'{top}$*.class'))
    return found


def api_of(core, module, sources, secondary):
    """Maps each source to {class name: API}; a new source maps to no classes."""
    out_root = core / MODULES[module][1]
    result = {}
    for source in sources:
        apis = {}
        for cls in classes_of(out_root, source, secondary):
            if not ANONYMOUS_OR_LOCAL_RE.search(cls.stem):
                apis[cls.stem] = json.dumps(read_class(cls).api())
        result[source] = apis
    return result


def class_stamps(core, stale, scans):
    """Records when each class file of the stale sources was last written."""
    return {cls: cls.stat().st_mtime_ns
            for module, sources in stale.items()
            for source in sources
            for cls in classes_of(core / MODULES[module][1], source, scans[module][2])}


def delete_classes_not_rewritten(core, stale, scans, stamps):
    """Removes class files the compiled sources no longer produce, such as deleted nested classes."""
    for module, sources in stale.items():
        out_root = core / MODULES[module][1]
        for source in sources:
            for cls in classes_of(out_root, source, scans[module][2]):
                if stamps.get(cls) == cls.stat().st_mtime_ns:
                    cls.unlink()


def api_changes(before, after):
    changed = [str(s) for s, apis in before.items() if apis and after.get(s) != apis]
    return ', '.join(changed[:3]) + (f' and {len(changed) - 3} more' if len(changed) > 3 else '')


# ---------------------------------------------------------------- class files

def read_class(path):
    try:
        cls = ClassFile(path)
        cls.api()
        return cls
    except (IndexError, KeyError, TypeError, ValueError) as e:
        raise ClassFormatError(f'{path}: {e}') from e


class Reader:
    __slots__ = ('data', 'pos')

    def __init__(self, data, pos=0):
        self.data = data
        self.pos = pos

    def u1(self):
        self.pos += 1
        return self.data[self.pos - 1]

    def u2(self):
        self.pos += 2
        return int.from_bytes(self.data[self.pos - 2:self.pos], 'big')

    def u4(self):
        self.pos += 4
        return int.from_bytes(self.data[self.pos - 4:self.pos], 'big')

    def take(self, n):
        self.pos += n
        return self.data[self.pos - n:self.pos]


class ClassFile:
    """The parts of a class file that other classes compile against."""

    def __init__(self, path):
        self.path = path
        self.data = path.read_bytes()
        r = Reader(self.data, 8)
        count = r.u2()
        self.cp = cp = [None] * count
        i = 1
        while i < count:
            tag = r.u1()
            if tag == 1:
                cp[i] = r.take(r.u2()).decode('utf-8', 'replace')
            elif tag in (3, 4):
                cp[i] = (tag, r.take(4))
            elif tag in (5, 6):
                cp[i] = (tag, r.take(8))
                i += 1
            elif tag in (7, 8, 16, 19, 20):
                cp[i] = (tag, r.u2())
            elif tag in (9, 10, 11, 12, 17, 18):
                cp[i] = (tag, r.u2(), r.u2())
            elif tag == 15:
                cp[i] = (tag, r.u1(), r.u2())
            else:
                raise ValueError(f'{path}: unknown constant pool tag {tag}')
            i += 1
        self.access = r.u2()
        self.name = self.class_name(r.u2())
        super_index = r.u2()
        self.super = self.class_name(super_index) if super_index else ''
        self.interfaces = tuple(self.class_name(r.u2()) for _ in range(r.u2()))
        self.fields = [self.member(r) for _ in range(r.u2())]
        self.methods = [self.member(r) for _ in range(r.u2())]
        self.attributes = self.attribute_readers(r)
        source = self.attributes.get('SourceFile')
        self.source_file = cp[self.at(source).u2()] if source is not None else None

    def class_name(self, index):
        return self.cp[self.cp[index][1]]

    def constant(self, index):
        entry = self.cp[index]
        return f's:{self.cp[entry[1]]}' if entry[0] == 8 else f'{entry[0]}:{entry[1].hex()}'

    def attribute_readers(self, r):
        """Maps attribute names to their offsets; read one with self.at(attrs[name])."""
        attrs = {}
        for _ in range(r.u2()):
            name, length = self.cp[r.u2()], r.u4()
            attrs[name] = r.pos
            r.pos += length
        return attrs

    def at(self, offset):
        return Reader(self.data, offset)

    def member(self, r):
        access, name, descriptor = r.u2(), self.cp[r.u2()], self.cp[r.u2()]
        return access, name, descriptor, self.attribute_readers(r)

    def element_value(self, r):
        tag = chr(r.u1())
        if tag in 'BCDFIJSZ':
            return tag + self.constant(r.u2())
        if tag == 's':
            return 's' + self.cp[r.u2()]
        if tag == 'e':
            return f'e{self.cp[r.u2()]}.{self.cp[r.u2()]}'
        if tag == 'c':
            return 'c' + self.cp[r.u2()]
        if tag == '@':
            return '@' + self.annotation(r)
        if tag == '[':
            return '[' + ','.join(self.element_value(r) for _ in range(r.u2())) + ']'
        raise ValueError(f'{self.path}: unknown annotation element tag {tag}')

    def annotation(self, r):
        type_name = self.cp[r.u2()]
        pairs = [f'{self.cp[r.u2()]}={self.element_value(r)}' for _ in range(r.u2())]
        return f'{type_name}({",".join(pairs)})'

    def annotations(self, attrs):
        found = []
        for key in ('RuntimeVisibleAnnotations', 'RuntimeInvisibleAnnotations'):
            if key in attrs:
                r = self.at(attrs[key])
                found += [self.annotation(r) for _ in range(r.u2())]
        for key in ('RuntimeVisibleParameterAnnotations', 'RuntimeInvisibleParameterAnnotations'):
            if key in attrs:
                r = self.at(attrs[key])
                for parameter in range(r.u1()):
                    found += [f'{parameter}:{self.annotation(r)}' for _ in range(r.u2())]
        return tuple(sorted(found))

    def string_attr(self, attrs, key):
        return self.cp[self.at(attrs[key]).u2()] if key in attrs else ''

    def api(self):
        attrs = self.attributes
        fields, methods = [], []
        for access, name, descriptor, member_attrs in self.fields:
            if not access & (ACC_PRIVATE | ACC_SYNTHETIC):
                constant = self.constant(self.at(member_attrs['ConstantValue']).u2()) \
                    if 'ConstantValue' in member_attrs else ''
                fields.append((name, descriptor, access, self.string_attr(member_attrs, 'Signature'), constant,
                               self.annotations(member_attrs)))
        for access, name, descriptor, member_attrs in self.methods:
            if not access & (ACC_PRIVATE | ACC_SYNTHETIC):
                exceptions = ''
                if 'Exceptions' in member_attrs:
                    r = self.at(member_attrs['Exceptions'])
                    exceptions = ','.join(self.class_name(r.u2()) for _ in range(r.u2()))
                default = self.element_value(self.at(member_attrs['AnnotationDefault'])) \
                    if 'AnnotationDefault' in member_attrs else ''
                methods.append((name, descriptor, access, self.string_attr(member_attrs, 'Signature'), exceptions,
                                self.annotations(member_attrs), default))
        members = []
        if 'InnerClasses' in attrs:
            r = self.at(attrs['InnerClasses'])
            for _ in range(r.u2()):
                inner, outer, inner_name, flags = r.u2(), r.u2(), r.u2(), r.u2()
                if outer and inner_name and self.class_name(outer) == self.name:
                    members.append((self.class_name(inner), flags))
        permitted = ()
        if 'PermittedSubclasses' in attrs:
            r = self.at(attrs['PermittedSubclasses'])
            permitted = tuple(sorted(self.class_name(r.u2()) for _ in range(r.u2())))
        components = []
        if 'Record' in attrs:
            r = self.at(attrs['Record'])
            for _ in range(r.u2()):
                name, descriptor = self.cp[r.u2()], self.cp[r.u2()]
                component_attrs = self.attribute_readers(r)
                components.append((name, descriptor, self.string_attr(component_attrs, 'Signature'),
                                   self.annotations(component_attrs)))
        return (self.access & ~ACC_SUPER, self.name, self.super, self.interfaces,
                self.string_attr(attrs, 'Signature'), self.annotations(attrs), tuple(sorted(members)), permitted,
                tuple(components), tuple(sorted(fields)), tuple(sorted(methods)))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
