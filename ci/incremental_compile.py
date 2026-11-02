#!/usr/bin/env python3
"""
Incremental compilation of the core module for local builds.

Maven's compiler plugin has two modes, and neither scales to a module with
thousands of sources:
- by default it recompiles every source of a module when any one changed; for
  core that is about 3,800 main and 2,300 test sources, some 40 seconds;
- with useIncrementalCompilation=false it compiles only the changed sources but
  not the classes that use them, which can leave those stale.

This script compiles the changed sources, then the sources that depend on what
changed, and nothing else:

1. It compiles the changed sources with Maven's second mode.
2. It compares what other classes compile against - non-private types and
   members with their signatures, supertypes, annotations and constant values -
   between the old and the new class files. If nothing changed, it is done; this
   covers edits to method bodies and private members.
3. Otherwise it recompiles the dependents: sources whose class files name a
   changed class or one of its subclasses (javac records every type a class
   uses), and sources that mention a changed or removed constant, a removed type
   or a removed static member (javac copies constants into the classes that use
   them, and imports leave no trace in class files). It repeats until no API
   changes.

Deleted sources make their dependents recompile the same way. A change to
module-info.java, to the build inputs (core/pom.xml, the java-questdb-client
jar, the profiles), a class file it cannot read, or dependents that make up
more than 45% of the source code fall back to Maven's full rebuild, which is
faster at that size.

Usage:
  python3 ci/incremental_compile.py                          # compile core
  python3 ci/incremental_compile.py -- test -Dtest=FooTest   # then run Maven
  python3 ci/incremental_compile.py --full                   # full rebuild
  python3 ci/incremental_compile.py --verbose                # show what compiles and why

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
# Compiling part of the module costs about twice as much per source byte as compiling all of it.
FULL_REBUILD_SHARE = 0.45
MAX_ROUNDS = 8
MODULES = {
    'main': ('src/main/java', 'target/classes', 'compile'),
    'test': ('src/test/java', 'target/test-classes', 'testCompile'),
}
ACC_PRIVATE = 0x0002
ACC_STATIC = 0x0008
ACC_SUPER = 0x0020
ACC_SYNTHETIC = 0x1000
ANONYMOUS_OR_LOCAL_RE = re.compile(r'\$\d')
TYPE_END = rb'(?![A-Za-z0-9_$])'


class CompileError(Exception):
    pass


class ClassFormatError(Exception):
    pass


class FullRebuild(Exception):
    pass


def main(argv):
    sys.stdout.reconfigure(line_buffering=True)
    own, maven_args = (argv[:argv.index('--')], argv[argv.index('--') + 1:]) if '--' in argv else (argv, [])
    if [a for a in own if a not in ('--full', '--verbose')]:
        print(__doc__)
        return 2
    root = Path(subprocess.run(['git', 'rev-parse', '--show-toplevel'], check=True, capture_output=True,
                               text=True).stdout.strip())
    try:
        compile_core(root, compile_args(maven_args), full='--full' in own, verbose='--verbose' in own)
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

    def __init__(self, root, maven_args, log, verbose=False):
        self.verbose = verbose
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

    def full_rebuild(self, reason):
        self.log(f'Incremental compile: full rebuild ({reason})')
        self.save(pending_full=True)
        delete_orphans(self.core, {m: scan(self.core, m)[1] for m in MODULES})
        for m in MODULES:
            remove(self.core / 'target/maven-status/maven-compiler-plugin' / MODULES[m][2])
        run_maven(self.root, self.maven_args, quick=False)
        self.save()


def compile_core(root, maven_args=(), full=False, log=print, verbose=False):
    """Compiles core main and test sources incrementally; raises CompileError when javac fails."""
    build = Build(root, maven_args, log, verbose)
    started = time.time()
    state = build.state
    if full:
        build.full_rebuild('requested')
    elif state and state.get('inputs') != build.inputs:
        build.full_rebuild('build inputs changed')
    elif state and state.get('pending_full'):
        build.full_rebuild('an earlier rebuild did not finish')
    elif state is None or not (build.core / 'target/classes').is_dir():
        log('Incremental compile: no earlier state; Maven decides what to compile')
        run_maven(root, build.maven_args, quick=False)
        build.save()
    else:
        try:
            compiled = compile_changes(build)
        except (ClassFormatError, FullRebuild) as e:
            build.full_rebuild(str(e))
            compiled = True
        if not compiled:
            return
    log(f'  compiled in {time.time() - started:.0f}s')


def compile_changes(build):
    """Compiles the changed sources and their dependents; returns False when nothing changed."""
    core = build.core
    scans = {m: scan(core, m) for m in MODULES}
    unverified = build.state.get('unverified', {})
    todo = {m: set(scans[m][0]) | {Path(s) for s in unverified.get(m, {})} for m in MODULES}
    orphans = {m: scans[m][1] for m in MODULES}
    if not any(todo.values()) and not any(orphans.values()):
        build.log('Incremental compile: no source changed')
        return False
    if Path('module-info.java') in todo['main']:
        raise FullRebuild('module-info.java changed')
    project = Project(core, scans)

    changed_sources = sum(len(s) for s in todo.values())
    sizes = {m: scans[m][3] for m in MODULES}
    total_bytes = sum(sum(v.values()) for v in sizes.values())
    compiled_bytes = 0
    deleted = [project.internal_name(m, cls) for m in MODULES for cls in orphan_class_files(orphans[m])]
    if deleted:
        delete_orphans(core, orphans)
        project.forget_class_files()
        for m, sources in project.dependents([{'class': n, 'removed': True} for n in deleted]).items():
            todo[m] |= sources

    compiled, rounds, api_changed = 0, 0, 0
    while any(todo.values()):
        rounds += 1
        size = sum(len(s) for s in todo.values())
        compiled_bytes += sum(sizes[m].get(s, 0) for m in MODULES for s in todo[m])
        if rounds > MAX_ROUNDS:
            raise FullRebuild(f'API changes still spreading after {MAX_ROUNDS} rounds')
        if compiled_bytes > FULL_REBUILD_SHARE * total_bytes:
            raise FullRebuild(f'{compiled + size} sources to compile, '
                              f'{compiled_bytes * 100 // total_bytes}% of the code')
        before = {m: project.api_of(m, todo[m]) for m in MODULES}
        if rounds == 1:
            for m in MODULES:
                before[m].update({Path(s): apis for s, apis in unverified.get(m, {}).items()})
        project.mark_stale(todo)
        stamps = project.class_stamps(todo)
        if build.verbose:
            names = sorted(f'{m}:{s}' for m in MODULES for s in todo[m])
            build.log(f'  round {rounds}: {len(todo["main"])} main and {len(todo["test"])} test sources: '
                      + ', '.join(names[:12]) + (f' and {len(names) - 12} more' if len(names) > 12 else ''))
        round_started = time.time()
        try:
            run_maven(build.root, build.maven_args, quick=True)
        except CompileError:
            build.save(unverified={m: {str(s): apis for s, apis in before[m].items()} for m in MODULES})
            raise
        project.delete_classes_not_rewritten(todo, stamps)
        project.forget_class_files()
        after = {m: project.api_of(m, todo[m]) for m in MODULES}
        compiled += size
        changes = [c for m in MODULES for c in class_changes(before[m], after[m])]
        api_changed += len(changes)
        compiled_now = todo
        search_started = time.time()
        todo = project.dependents(changes) if changes else {}
        todo = {m: todo.get(m, set()) - compiled_now[m] for m in MODULES}
        if build.verbose:
            build.log(f'  round {rounds}: javac {search_started - round_started:.1f}s; '
                      f'{len(changes)} API changes, dependents found in {time.time() - search_started:.1f}s')

    build.save()
    dependents = compiled - changed_sources
    what = f'compiled {plural(changed_sources, "changed source")}'
    if dependents:
        what += f' and {plural(dependents, "dependent source")} in {plural(rounds, "round")}'
    if deleted:
        what += f'; {plural(len(deleted), "class")} deleted'
    what += f'; API changed in {plural(api_changed, "class")}' if api_changed else '; API unchanged'
    build.log(f'Incremental compile: {what}')
    return True


def plural(n, noun):
    return f'{n} {noun}' + ('' if n == 1 else 'es' if noun.endswith('s') else 's')


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
    """Returns (stale sources, orphan top-level classes, secondary classes by (package, source), source sizes)."""
    src_root, out_root = core / MODULES[module][0], core / MODULES[module][1]
    stale, sizes = [], {}
    for dir_path, _, files in os.walk(src_root):
        for name in files:
            if not name.endswith('.java'):
                continue
            source = Path(dir_path, name)
            rel = source.relative_to(src_root)
            source_stat = source.stat()
            sizes[rel] = source_stat.st_size
            try:
                if source_stat.st_mtime > (out_root / rel.with_suffix('.class')).stat().st_mtime:
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
    return stale, orphans, secondary, sizes


def orphan_class_files(top_level_classes):
    return [nested for cls in top_level_classes for nested in [cls, *cls.parent.glob(cls.stem + '$*.class')]]


def delete_orphans(core, orphans):
    for classes in orphans.values():
        for cls in orphan_class_files(classes):
            cls.unlink(missing_ok=True)


class Project:
    """Sources and class files of core, and the dependencies between them."""

    def __init__(self, core, scans):
        self.core = core
        self.secondary = {m: scans[m][2] for m in MODULES}
        self.source_of_secondary = {m: {(package, stem): source for (package, source), stems in scans[m][2].items()
                                        for stem in stems} for m in MODULES}
        self.class_files = None

    def out_root(self, module):
        return self.core / MODULES[module][1]

    def src_root(self, module):
        return self.core / MODULES[module][0]

    def internal_name(self, module, cls):
        return cls.relative_to(self.out_root(module)).with_suffix('').as_posix()

    def classes_of(self, module, source):
        """Class files compiled from a source: its top-level types and their nested classes."""
        package = self.out_root(module) / source.parent
        found = []
        for top in [source.stem, *self.secondary[module].get((source.parent, source.name), [])]:
            found += [package / f'{top}.class'] if (package / f'{top}.class').exists() else []
            found += sorted(package.glob(f'{top}$*.class'))
        return found

    def source_of(self, module, cls):
        rel = cls.relative_to(self.out_root(module))
        top = rel.stem.split('$')[0]
        if (self.src_root(module) / rel.parent / f'{top}.java').exists():
            return rel.parent / f'{top}.java'
        source = self.source_of_secondary[module].get((rel.parent, top))
        return rel.parent / source if source else None

    def api_of(self, module, sources):
        """Maps each source to {internal class name: API}; a new source maps to no classes."""
        result = {}
        for source in sources:
            result[source] = {self.internal_name(module, cls): read_class(cls).api()
                              for cls in self.classes_of(module, source)
                              if not ANONYMOUS_OR_LOCAL_RE.search(cls.stem)}
        return result

    def mark_stale(self, sources):
        """Makes Maven recompile sources whose class files are otherwise up to date."""
        for module, rels in sources.items():
            for source in rels:
                cls = self.out_root(module) / source.with_suffix('.class')
                if cls.exists():
                    os.utime(cls, (0, 0))

    def class_stamps(self, sources):
        return {cls: cls.stat().st_mtime_ns for module, rels in sources.items() for source in rels
                for cls in self.classes_of(module, source)}

    def delete_classes_not_rewritten(self, sources, stamps):
        """Removes class files the compiled sources no longer produce, such as deleted nested classes."""
        for module, rels in sources.items():
            for source in rels:
                for cls in self.classes_of(module, source):
                    if stamps.get(cls) == cls.stat().st_mtime_ns:
                        cls.unlink()

    def forget_class_files(self):
        self.class_files = None

    def all_class_files(self):
        if self.class_files is None:
            self.class_files = [(m, Path(d, f)) for m in MODULES for d, _, files in os.walk(self.out_root(m))
                                for f in files if f.endswith('.class')]
        return self.class_files

    def dependents(self, changes):
        """Sources that may compile differently because of the changes, by module."""
        found = {m: set() for m in MODULES}
        affected = {c['class'] for c in changes}
        frontier = set(affected)
        contents = {}
        while frontier:
            pattern = re.compile(b'(?:' + b'|'.join(re.escape(t.encode()) for t in sorted(frontier)) + b')' + TYPE_END)
            frontier = set()
            for module, cls in self.all_class_files():
                data = contents.get(cls)
                if data is None:
                    data = contents[cls] = cls.read_bytes()
                if not pattern.search(data):
                    continue
                source = self.source_of(module, cls)
                if source:
                    found[module].add(source)
                header = read_class(cls, data, header_only=True)
                if header.name not in affected and (header.super in affected or affected & set(header.interfaces)):
                    affected.add(header.name)
                    frontier.add(header.name)
        # javac leaves no trace of these in class files: copied constants, imports, and qualified uses
        # of a constant through any subtype. Search the sources for them instead.
        def simple(name):
            return name.rsplit('/', 1)[-1].rsplit('$', 1)[-1]

        types = sorted({simple(t) for t in affected})
        removed_types = {simple(c['class']) for c in changes if c.get('removed') or c.get('type_changed')}
        members = {m for c in changes for m in (*c.get('constants', ()), *c.get('static_members', ()))}
        alternatives = [r'\b' + re.escape(t) + r'\b' for t in sorted(removed_types)]
        if members:
            owners = '(?:' + '|'.join(map(re.escape, types)) + ')'
            alternatives.append(r'\b' + owners + r'\.(?:' + '|'.join(map(re.escape, sorted(members))) + r')\b')
            alternatives.append(r'\b' + owners + r'\.\*')
        if alternatives:
            pattern = re.compile('|'.join(alternatives))
            for module in MODULES:
                src_root = self.src_root(module)
                for d, _, files in os.walk(src_root):
                    for f in files:
                        path = Path(d, f)
                        if f.endswith('.java') and pattern.search(path.read_text(encoding='utf-8', errors='replace')):
                            found[module].add(path.relative_to(src_root))
        return found


def class_changes(before, after):
    """Describes how the API of each class compiled from these sources changed."""
    changes = []
    for source, old_apis in before.items():
        if not old_apis:
            continue
        new_apis = after.get(source, {})
        for name, old in old_apis.items():
            new = new_apis.get(name)
            if new == old:
                continue
            if new is None:
                changes.append({'class': name, 'removed': True})
                continue
            constants = [f for f, (_, _, _, value, _) in old['fields'].items()
                         if value and (f not in new['fields'] or new['fields'][f][3] != value)]
            static_members = [f for f, field in old['fields'].items()
                              if field[1] & ACC_STATIC and new['fields'].get(f) != field]
            static_members += [m.split('(')[0] for m, method in old['methods'].items()
                               if method[0] & ACC_STATIC and new['methods'].get(m) != method]
            changes.append({'class': name, 'type_changed': old['type'] != new['type'], 'constants': constants,
                            'static_members': static_members})
    return changes


# ---------------------------------------------------------------- class files

def read_class(path, data=None, header_only=False):
    try:
        cls = ClassFile(path, data, header_only)
        if not header_only:
            cls.api()
        return cls
    except (IndexError, KeyError, TypeError, ValueError) as e:
        raise ClassFormatError(f'cannot read {path}: {e}') from e


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

    def __init__(self, path, data=None, header_only=False):
        self.path = path
        self.data = data if data is not None else path.read_bytes()
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
                raise ValueError(f'unknown constant pool tag {tag}')
            i += 1
        self.access = r.u2()
        self.name = self.class_name(r.u2())
        super_index = r.u2()
        self.super = self.class_name(super_index) if super_index else ''
        self.interfaces = [self.class_name(r.u2()) for _ in range(r.u2())]
        if header_only:
            return
        self.fields = [self.member(r) for _ in range(r.u2())]
        self.methods = [self.member(r) for _ in range(r.u2())]
        self.attributes = self.attribute_offsets(r)
        source = self.attributes.get('SourceFile')
        self.source_file = cp[self.at(source).u2()] if source is not None else None

    def class_name(self, index):
        return self.cp[self.cp[index][1]]

    def constant(self, index):
        entry = self.cp[index]
        return f's:{self.cp[entry[1]]}' if entry[0] == 8 else f'{entry[0]}:{entry[1].hex()}'

    def attribute_offsets(self, r):
        """Maps attribute names to their offsets; read one with self.at(offset)."""
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
        return access, name, descriptor, self.attribute_offsets(r)

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
        raise ValueError(f'unknown annotation element tag {tag}')

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
        return sorted(found)

    def string_attr(self, attrs, key):
        return self.cp[self.at(attrs[key]).u2()] if key in attrs else ''

    def api(self):
        """The class as other classes see it: {'type': [...], 'fields': {...}, 'methods': {...}}."""
        attrs = self.attributes
        fields, methods = {}, {}
        for access, name, descriptor, member_attrs in self.fields:
            if not access & (ACC_PRIVATE | ACC_SYNTHETIC):
                constant = self.constant(self.at(member_attrs['ConstantValue']).u2()) \
                    if 'ConstantValue' in member_attrs else ''
                fields[name] = [descriptor, access, self.string_attr(member_attrs, 'Signature'), constant,
                                self.annotations(member_attrs)]
        for access, name, descriptor, member_attrs in self.methods:
            if not access & (ACC_PRIVATE | ACC_SYNTHETIC):
                exceptions = ''
                if 'Exceptions' in member_attrs:
                    r = self.at(member_attrs['Exceptions'])
                    exceptions = ','.join(self.class_name(r.u2()) for _ in range(r.u2()))
                default = self.element_value(self.at(member_attrs['AnnotationDefault'])) \
                    if 'AnnotationDefault' in member_attrs else ''
                methods[name + descriptor] = [access, self.string_attr(member_attrs, 'Signature'), exceptions,
                                              self.annotations(member_attrs), default]
        members = []
        if 'InnerClasses' in attrs:
            r = self.at(attrs['InnerClasses'])
            for _ in range(r.u2()):
                inner, outer, inner_name, flags = r.u2(), r.u2(), r.u2(), r.u2()
                if outer and inner_name and self.class_name(outer) == self.name:
                    members.append([self.class_name(inner), flags])
        permitted = []
        if 'PermittedSubclasses' in attrs:
            r = self.at(attrs['PermittedSubclasses'])
            permitted = sorted(self.class_name(r.u2()) for _ in range(r.u2()))
        components = []
        if 'Record' in attrs:
            r = self.at(attrs['Record'])
            for _ in range(r.u2()):
                name, descriptor = self.cp[r.u2()], self.cp[r.u2()]
                component_attrs = self.attribute_offsets(r)
                components.append([name, descriptor, self.string_attr(component_attrs, 'Signature'),
                                   self.annotations(component_attrs)])
        header = [self.access & ~ACC_SUPER, self.super, self.interfaces, self.string_attr(attrs, 'Signature'),
                  self.annotations(attrs), sorted(members), permitted, components]
        return {'type': header, 'fields': fields, 'methods': methods}


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
