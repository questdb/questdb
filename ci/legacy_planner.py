#!/usr/bin/env python3
"""
Generates the legacy query planner: master's SQL front end, kept behind a
switch while the logical-plan planner rolls out.

Every Java file under <src>/<root>/legacy/ is the file at the same path
without the "legacy" segment, taken from the base commit, with two mechanical
changes: its package, and each reference to another legacy class, move under
"legacy"; and each class it used from its own package without an import, and
which stays shared, gets an import. Everything else in the file is master's
code, plus the adaptations a developer makes by hand to fit it to the shared
runtime.

The base commit is recorded in <src>/<root>/legacy/package-info.java.

Both commands render every legacy file from the base commit and merge the
difference between the old and the new rendering into the file with
`git merge-file`, so the hand-made adaptations survive:

  add PATH...      fork more classes; PATH is relative to <src>, such as
                   io/questdb/griffin/SqlOptimiser.java
  remove PATH...   stop forking classes, so that legacy code uses the shared
                   ones
  rebase COMMIT    move the base to COMMIT, bringing in master's changes to the
                   forked classes
  diff             print the hand-made adaptations: each legacy file against its
                   rendering from the base commit

A conflict leaves the usual markers in the file and a non-zero exit code.

Enterprise forks its own classes on top of open source's:

  python3 questdb/ci/legacy_planner.py --src questdb-ent/src/main/java \\
      --root com.questdb --also questdb/core/src/main/java:io.questdb add ...
"""

import argparse
import difflib
import os
import re
import subprocess
import sys
import tempfile

LEGACY = 'legacy'
BASE_PATTERN = re.compile(r'\{@code ([0-9a-f]{40})}')
HEADER_PATTERN = re.compile(r'/\*.*?\*/', re.DOTALL)
CONFLICT_PATTERN = re.compile(
    r'^<<<<<<< legacy\n((?:.*\n)*?)\|\|\|\|\|\|\| old master\n((?:.*\n)*?)=======\n((?:.*\n)*?)>>>>>>> new master\n',
    re.MULTILINE
)
FQN_PATTERN = re.compile(r'\b((?:io|com)\.questdb(?:\.[a-z_][a-z0-9_]*)*)\.([A-Z]\w*)')
PACKAGE_PATTERN = re.compile(r'^package ([\w.]+);', re.MULTILINE)
IMPORT_PATTERN = re.compile(r'^import (static )?([\w.*]+);$')
DOC_REFERENCE_PATTERN = re.compile(r'(?:\{@link(?:plain)?|@see|@throws)\s+([A-Z]\w*)')
WORD_PATTERN = re.compile(r'\b[A-Z]\w*\b')

PACKAGE_INFO = """{header}/**
 * The query planner as it was at commit {{@code {base}}}, kept as a fallback
 * while the logical-plan planner rolls out. The {{@code cairo.sql.legacy.planner.enabled}} property, the
 * {{@code Legacy-Planner}} HTTP header and the {{@code legacy_planner}} PostgreSQL startup option select it.
 * <p>
 * {{@code ci/legacy_planner.py}} generates these classes from that commit. The legacy planner takes no fixes, and
 * goes once the new planner has shipped.
 */
package {package};
"""


def git(repo, *args, check=True):
    result = subprocess.run(['git', '-C', repo, *args], capture_output=True, text=True)
    if check and result.returncode != 0:
        sys.exit(f'git {" ".join(args)} failed: {result.stderr.strip()}')
    return result


class Tree:
    """A source root and the legacy classes under it."""

    def __init__(self, src, root):
        self.src = src
        self.root = root
        self.repo = git(src, 'rev-parse', '--show-toplevel').stdout.strip()
        self.root_dir = root.replace('.', '/') + '/'
        self.legacy_dir = os.path.join(src, self.root_dir + LEGACY)

    def legacy_path(self, path):
        assert path.startswith(self.root_dir), f'{path} is not under {self.root}'
        return os.path.join(self.src, self.root_dir + LEGACY + '/' + path[len(self.root_dir):])

    def repo_path(self, path):
        return os.path.relpath(os.path.abspath(os.path.join(self.src, path)), self.repo)

    def forked_paths(self, base):
        """The paths of the legacy classes copied from the base commit; the others are written by hand."""
        if base is None:
            return []
        listing = git(self.repo, 'ls-tree', '-r', '--name-only', base, '--', self.repo_path(self.root_dir)).stdout
        at_base = set(listing.split())
        paths = []
        for directory, _, files in os.walk(self.legacy_dir):
            for name in files:
                if name.endswith('.java') and name != 'package-info.java':
                    relative = os.path.relpath(os.path.join(directory, name), self.legacy_dir)
                    path = self.root_dir + relative
                    if self.repo_path(path) in at_base:
                        paths.append(path)
        return sorted(paths)

    def legacy_fqn(self, fqn):
        return self.root + '.' + LEGACY + fqn[len(self.root):]


def fqn_of(path):
    return path[:-len('.java')].replace('/', '.')


def strip_comments_and_strings(text):
    out = []
    i, n = 0, len(text)
    while i < n:
        if text.startswith('//', i):
            j = text.find('\n', i)
            i = n if j < 0 else j
        elif text.startswith('/*', i):
            j = text.find('*/', i + 2)
            i = n if j < 0 else j + 2
        elif text.startswith('"""', i):
            j = text.find('"""', i + 3)
            i = n if j < 0 else j + 3
        elif text[i] in '"\'':
            quote = text[i]
            i += 1
            while i < n and text[i] != quote:
                i += 2 if text[i] == '\\' else 1
            i += 1
        else:
            out.append(text[i])
            i += 1
    return ''.join(out)


def referenced_simple_names(text):
    """The class names the code after the imports uses, in code or in javadoc references."""
    body = text[PACKAGE_PATTERN.search(text).end():]
    body = '\n'.join(line for line in body.split('\n') if not IMPORT_PATTERN.match(line))
    names = set(WORD_PATTERN.findall(strip_comments_and_strings(body)))
    names.update(DOC_REFERENCE_PATTERN.findall(body))
    return names


def import_runs(lines, start):
    """The [start, end) ranges of consecutive import lines between the package line and the first type."""
    runs = []
    i = start
    while i < len(lines) and (not lines[i].strip() or lines[i].startswith('import ')):
        if IMPORT_PATTERN.match(lines[i]):
            run_start = i
            while i < len(lines) and IMPORT_PATTERN.match(lines[i]):
                i += 1
            runs.append((run_start, i))
        else:
            i += 1
    return runs


def rewrite_imports(text, extra_imports):
    """Adds the imports to the group of non-static, non-JDK imports and sorts every group by name, as master does."""
    lines = text.split('\n')
    after_package = next(i for i, line in enumerate(lines) if line.startswith('package ')) + 1
    if extra_imports:
        new_lines = [f'import {fqn};' for fqn in extra_imports]
        group = next(
            (run for run in import_runs(lines, after_package)
             if not lines[run[0]].startswith(('import static ', 'import java.', 'import javax.'))),
            None
        )
        if group is None:
            lines[after_package:after_package] = [''] + new_lines
        else:
            lines[group[1]:group[1]] = new_lines
    for start, end in reversed(import_runs(lines, after_package)):
        lines[start:end] = sorted(set(lines[start:end]), key=lambda line: IMPORT_PATTERN.match(line).group(2))
    return '\n'.join(lines)


class Renderer:
    def __init__(self, tree, forked_fqns, base):
        self.repo = tree.repo
        self.tree = tree
        self.forked_fqns = forked_fqns
        self.base = base
        self.package_classes = {}

    def classes_in_package(self, directory):
        if directory not in self.package_classes:
            listing = git(self.repo, 'ls-tree', '--name-only', self.base, directory + '/').stdout.split()
            self.package_classes[directory] = {
                os.path.basename(name)[:-len('.java')] for name in listing if name.endswith('.java')
            }
        return self.package_classes[directory]

    def legacy_fqn(self, fqn):
        for tree, fqns in self.forked_fqns:
            if fqn in fqns:
                return tree.legacy_fqn(fqn)
        return None

    def render(self, path):
        result = git(self.repo, 'show', f'{self.base}:{self.tree.repo_path(path)}', check=False)
        if result.returncode != 0:
            return None
        text = result.stdout
        package = PACKAGE_PATTERN.search(text).group(1)

        def replace(match):
            legacy = self.legacy_fqn(match.group(1) + '.' + match.group(2))
            return legacy if legacy is not None else match.group(0)

        text = FQN_PATTERN.sub(replace, text)
        own = os.path.basename(path)[:-len('.java')]
        shared = {
            name for name in self.classes_in_package(os.path.dirname(self.tree.repo_path(path)))
            if name != own and self.legacy_fqn(package + '.' + name) is None
        }
        imported = {match.group(2).rsplit('.', 1)[-1] for match in map(IMPORT_PATTERN.match, text.split('\n')) if match}
        extra = sorted(
            package + '.' + name for name in referenced_simple_names(text) & shared if name not in imported
        )
        text = PACKAGE_PATTERN.sub(f'package {self.tree.legacy_fqn(package)};', text, count=1)
        return rewrite_imports(text, extra)


def read_base(tree):
    path = os.path.join(tree.legacy_dir, 'package-info.java')
    if not os.path.exists(path):
        return None
    with open(path) as f:
        match = BASE_PATTERN.search(f.read())
    return match.group(1) if match else None


def write_package_info(tree, base, header_source):
    """Writes the legacy package's javadoc, under the license header of the sources it forks, if they carry one."""
    header = HEADER_PATTERN.match(git(tree.repo, 'show', f'{base}:{header_source}').stdout)
    os.makedirs(tree.legacy_dir, exist_ok=True)
    with open(os.path.join(tree.legacy_dir, 'package-info.java'), 'w') as f:
        f.write(PACKAGE_INFO.format(
            header=header.group(0) + '\n\n' if header else '', base=base, package=tree.root + '.' + LEGACY
        ).lstrip('\n'))


def merge(current, old, new):
    """Merges the change from the old rendering to the new one into the current file; returns the text and whether a
    conflict remains. A conflict between import lines alone resolves to the new imports plus those the current file
    added by hand."""
    with tempfile.TemporaryDirectory() as directory:
        files = []
        for name, text in (('current', current), ('old', old), ('new', new)):
            file = os.path.join(directory, name)
            with open(file, 'w') as f:
                f.write(text)
            files.append(file)
        result = subprocess.run(
            ['git', 'merge-file', '-p', '--diff3', '-L', 'legacy', '-L', 'old master', '-L', 'new master', *files],
            capture_output=True, text=True
        )
    text = CONFLICT_PATTERN.sub(resolve_import_conflict, result.stdout)
    if text != result.stdout:
        text = rewrite_imports(text, [])
    return text, '<<<<<<< legacy' in text


def resolve_import_conflict(match):
    ours, base, theirs = (part.split('\n')[:-1] for part in match.groups())
    if not all(IMPORT_PATTERN.match(line) or not line.strip() for line in ours + base + theirs):
        return match.group(0)
    added_by_hand = [line for line in ours if line.strip() and line not in base]
    return ''.join(line + '\n' for line in theirs + [line for line in added_by_hand if line not in theirs])


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--src', default='core/src/main/java', help='source root (default: %(default)s)')
    parser.add_argument('--root', default='io.questdb', help='root package (default: %(default)s)')
    parser.add_argument('--also', action='append', default=[], metavar='SRC:ROOT',
                        help='another tree whose legacy classes this one references')
    commands = parser.add_subparsers(dest='command', required=True)
    add = commands.add_parser('add', help='fork more classes')
    add.add_argument('--base', help='base commit, required for the first fork')
    add.add_argument('paths', nargs='+')
    remove = commands.add_parser('remove', help='stop forking classes')
    remove.add_argument('paths', nargs='+')
    rebase = commands.add_parser('rebase', help='move the base commit')
    rebase.add_argument('commit')
    commands.add_parser('diff', help='print the hand-made adaptations')
    args = parser.parse_args()

    tree = Tree(args.src, args.root)
    others = []
    for spec in args.also:
        src, root = spec.rsplit(':', 1)
        other = Tree(src, root)
        others.append((other, {fqn_of(path) for path in other.forked_paths(read_base(other))}))

    old_base = read_base(tree)
    old_paths = tree.forked_paths(old_base)
    if args.command == 'diff':
        if old_base is None:
            sys.exit('nothing is forked yet')
        renderer = Renderer(tree, [(tree, {fqn_of(path) for path in old_paths}), *others], old_base)
        for path in old_paths:
            target = tree.legacy_path(path)
            with open(target) as f:
                current = f.read()
            rendered = renderer.render(path)
            if current != rendered:
                sys.stdout.writelines(difflib.unified_diff(
                    rendered.splitlines(keepends=True), current.splitlines(keepends=True),
                    fromfile=f'{path} at {old_base[:10]}', tofile=os.path.relpath(target)
                ))
        return
    if args.command == 'add':
        base = old_base or args.base
        if base is None:
            sys.exit('the first fork needs --base')
        if old_base is not None and args.base is not None and args.base != old_base:
            sys.exit(f'the base is {old_base}; use rebase to move it')
        new_paths = sorted(set(old_paths) | set(args.paths))
    elif args.command == 'remove':
        unknown = set(args.paths) - set(old_paths)
        if unknown:
            sys.exit('not forked: ' + ', '.join(sorted(unknown)))
        base = old_base
        new_paths = sorted(set(old_paths) - set(args.paths))
    else:
        if old_base is None:
            sys.exit('nothing is forked yet')
        base = args.commit
        new_paths = old_paths
    base = git(tree.repo, 'rev-parse', '--verify', base + '^{commit}').stdout.strip()

    new_renderer = Renderer(tree, [(tree, {fqn_of(path) for path in new_paths}), *others], base)
    old_renderer = None if old_base is None else \
        Renderer(tree, [(tree, {fqn_of(path) for path in old_paths}), *others], old_base)

    conflicts = []
    for path in new_paths:
        new = new_renderer.render(path)
        if new is None:
            sys.exit(f'{path} does not exist at {base}')
        target = tree.legacy_path(path)
        if path in old_paths:
            old = old_renderer.render(path)
            with open(target) as f:
                current = f.read()
            new, conflicted = merge(current, old, new)
            if conflicted:
                conflicts.append(target)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, 'w') as f:
            f.write(new)
    for path in set(old_paths) - set(new_paths):
        os.remove(tree.legacy_path(path))
    write_package_info(tree, base, tree.repo_path(new_paths[0]))

    print(f'{len(new_paths)} legacy classes at {base[:10]}, {len(new_paths) - len(old_paths):+d}')
    if conflicts:
        print('conflicts:\n  ' + '\n  '.join(conflicts))
        sys.exit(1)


if __name__ == '__main__':
    main()
