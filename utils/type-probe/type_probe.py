#!/usr/bin/env python3
"""Lists the work of adding a column type to QuestDB.

    type_probe.py init <NAME> --like <EXISTING> --tag <n>
    type_probe.py run <facts.toml> [--out DIR] [--skip-native] [--skip-kit]
    type_probe.py audit <summary | like TYPE | places | check> [options]

`init` prints a facts file for a new type, copied from an existing type's type driver. `run`
registers the type the facts file declares, writes its type driver, builds Java, Rust and C,
runs the conformance kit and the coverage tests with the type declared, reads the code for the
places a type like the new one's namesake must decide (audit.py), and writes every place that
needs a decision into worklist.md. `audit` runs audit.py. README.md next to this file is the
manual.

Exit codes: 0 the worklist is empty; 1 it has items, or a step was skipped; 2 the facts file,
an anchor or the command line is wrong; 3 a step failed in a way the tool cannot parse.

Python 3.11 or newer, standard library only.
"""

import argparse
import datetime
import hashlib
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
import xml.etree.ElementTree as ElementTree
from dataclasses import dataclass
from pathlib import Path

import audit

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

LATER_TYPES_FILE = 'core/src/test/resources/io/questdb/test/cairo/types/later-types.txt'
CAIRO_DIR = 'core/src/main/java/io/questdb/cairo'
SUREFIRE_DIR = 'core/target/surefire-reports'

GROUPS = ('build-java', 'build-rust', 'build-c', 'refusal', 'kit', 'coverage', 'namesake')
DECISIONS = ('name-yourself', 'implement-pair', 'add-writer-arm', 'fill-driver-answer', 'declare-or-admit', 'decide-place')
COVERAGE_TESTS = (
    'RelationCoverageTest', 'ProtocolOpcodeCoverageTest', 'GeneratedAccessorCoverageTest', 'FunctionReachTest',
    'RelationRulesTest', 'TypeDriverTest', 'OverloadSoundnessTest', 'ColumnConversionSoundnessTest',
)
# the kit paths that hold an invariant for a type with no recording: every path of the kit
KIT_PATHS = ('storage.*', 'sql.*', 'ingest.*', 'http.*', 'pg.*', 'lv.*')
JAVA = 'core/src/main/java/io/questdb/'
# Where a kit or coverage failure that names no place of its own is located: the code the path or
# the test checks, (path or test prefix, file, method, decision), first match wins. These are
# hints for reading a failure, not places: the audit lists the places.
LAYERS = (
    ('http.csv', f'{JAVA}cutlass/http/processors/ExportQueryProcessor.java', 'csvOpcode', 'add-writer-arm'),
    ('http.json', f'{JAVA}cutlass/http/processors/JsonQueryProcessorState.java', 'jsonOpcode', 'add-writer-arm'),
    ('pg.', f'{JAVA}cutlass/pgwire/PGPipelineEntry.java', 'outColumnOpcode', 'add-writer-arm'),
    ('ingest.csv', f'{JAVA}cutlass/text/types/TypeManager.java', 'getTypeAdapter', 'add-writer-arm'),
    ('ingest.qwp-egress', f'{JAVA}cutlass/qwp/codec/QwpResultBatchBuffer.java', 'appendOpcode', 'add-writer-arm'),
    ('sql.cast', f'{JAVA}cairo/TypeDrivers.java', 'find', 'implement-pair'),
    ('sql.insert_convert', f'{JAVA}griffin/RecordToRowCopierUtils.java', 'copyOpcode', 'implement-pair'),
    # both orders go to the comparator, whose compare arm decides where NULL sorts
    ('sql.order_', f'{JAVA}griffin/engine/orderby/RecordComparatorCompiler.java', 'comparatorOpcode', 'add-writer-arm'),
    # a coverage test that checks one relation against its implementation, by test method
    ('RelationCoverageTest#testCaseEscalation', f'{JAVA}griffin/engine/functions/conditional/CaseCommon.java', 'castRow', 'implement-pair'),
    ('RelationCoverageTest#testCopier', f'{JAVA}griffin/RecordToRowCopierUtils.java', 'copyOpcode', 'implement-pair'),
    ('RelationCoverageTest#testUnion', f'{JAVA}griffin/SqlCodeGenerator.java', 'generateCastFunction', 'implement-pair'),
    ('RelationCoverageTest#testExplicitCast', f'{JAVA}cairo/TypeDrivers.java', 'find', 'implement-pair'),
    ('FunctionReachTest#testLaterTypes', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('RelationRulesTest', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('TypeDriverTest', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    # the soundness tests pin an answer per tag in their own switches, which the build lists first
    ('OverloadSoundnessTest#testOwnSignature', 'core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java', 'ownSignatureTag', 'name-yourself'),
    ('OverloadSoundnessTest', 'core/src/test/java/io/questdb/test/griffin/OverloadSoundnessTest.java', 'isExplicitCast', 'name-yourself'),
    ('ColumnConversionSoundnessTest', f'{JAVA}cairo/ColumnTypeConverter.java', 'convertFromFixedSize', 'implement-pair'),
    ('GeneratedAccessorCoverageTest', f'{JAVA}griffin/engine/orderby/SortKeyEncoder.java', 'keyKind', 'implement-pair'),
    # the type's own answers: its NULL, its column function, its relations
    ('storage.', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('sql.', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('ingest.', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('http.', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
    ('lv.', f'{JAVA}cairo/TypeDrivers.java', 'find', 'fill-driver-answer'),
)
# the guarded site a kit path reaches with a value of the type, whose refusal its failure may not
# name (ILP over TCP drops the row): (path prefix, guard label of places.tsv)
KIT_GUARDS = (
    ('ingest.ilp-', 'ILP column kind'),
    ('ingest.qwp', 'QWP WAL append'),
    ('sql.between_timestamp', 'between'),
    ('sql.copy_bind', 'COPY bind snapshot'),
    ('sql.eq_null_double', '= NULL'),
    ('sql.fill_linear', 'SAMPLE BY FILL(LINEAR)'),
    ('sql.fill_prev', 'SAMPLE BY FILL(PREV)'),
    ('sql.fill_value', 'SAMPLE BY FILL(value)'),
    ('sql.memoized', 'memoized virtual column'),
    ('storage.parquet_convert', 'Parquet conversion'),
)
# error texts an existing site raises, which locate a failure that holds one: (text, file, method, decision)
KEPT_TEXTS = (
    ('unknown QuestDB column tag code', 'core/rust/qdb-core/src/col_type.rs', 'try_from', 'name-yourself'),
    ('unsupported conversion', f'{JAVA}cairo/ColumnTypeConverter.java', 'convertFromFixedSize', 'implement-pair'),
    ('unsupported CASE value type', f'{JAVA}griffin/engine/functions/conditional/CaseCommon.java', 'getCaseFunctionConstructor', 'implement-pair'),
    ('inconvertible types', f'{JAVA}griffin/RecordToRowCopierUtils.java', 'copyOpcode', 'implement-pair'),
)
# the place each refusal lead other than the family-arm guard's names: (lead, file, method, decision)
LEADS = (
    ('no UNION cast', f'{JAVA}griffin/SqlCodeGenerator.java', 'generateCastFunction', 'implement-pair'),
    ('no compare arm', f'{JAVA}griffin/engine/orderby/RecordComparatorCompiler.java', 'comparatorOpcode', 'add-writer-arm'),
)
MESSAGE_LIMIT = 200
COVERAGE_MESSAGE_LIMIT = 2000

REFUSAL = re.compile(r'\b(no (?:family arm|compare arm|UNION cast)) for (.+?) at (.+?): (.+)$')
TEMP_DIR = re.compile(r'(?<![\w<])/\S*?/junit\d+/')
STACK_FRAME = re.compile(r'\s+at [\w.$]+\([\w.$]+(?::\d+)?\).*$')
# the kit names the type, the value row, the path and the mode of every failure
KIT_CONTEXT = re.compile(r'type=(\S+) row=(\S+) path=(\S+) mode=(\S+?):? ')
FAMILY_ARM_REFUSAL = 'no family arm for <type> at '
ANCHOR_PREFIX = 'type-registration: '
ANCHOR_SUFFIX = ' (see utils/type-probe/README.md)'


class UsageError(Exception):
    """Exit 2: the facts file, an anchor or the command line is wrong; one problem per line."""

    def __init__(self, problems):
        super().__init__('\n'.join(problems))
        self.problems = problems


class ToolError(Exception):
    """Exit 3: a step failed in a way the tool cannot parse."""


# ------------------------------------------------------------------ the tree's facts

def strip_comments(text):
    """Java, Rust or C text with comments and the insides of string and char literals blanked;
    every offset and every newline is kept, so line numbers stay true."""
    out = list(text)
    n = len(text)

    def blank(a, b):
        for k in range(a, min(b, n)):
            if out[k] != '\n':
                out[k] = ' '

    i = 0
    while i < n:
        c = text[i]
        if text.startswith('//', i):
            j = text.find('\n', i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif text.startswith('/*', i):
            j = text.find('*/', i + 2)
            j = n if j < 0 else j + 2
            blank(i, j)
            i = j
        elif text.startswith('"""', i):
            j = text.find('"""', i + 3)
            j = n if j < 0 else j + 3
            blank(i + 3, j - 3)
            i = j
        elif c in 'rb' and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == '_')) and re.match(r'b?r#*"', text[i:i + 40]):
            # a Rust raw string, r"..." or r#"..."#, which has no escapes
            m = re.match(r'b?r(#*)"', text[i:i + 40])
            end = text.find('"' + m.group(1), i + len(m.group(0)))
            end = n if end < 0 else end
            blank(i + len(m.group(0)), end)
            i = end + 1 + len(m.group(1))
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == '\\' else 1
            blank(i + 1, j)
            i = j + 1
        elif c == "'" and i + 2 < n and (text[i + 1] == '\\' or text[i + 2] == "'"):
            # a char literal; a lifetime ('a, '_) has no closing quote right after its first char
            j = text.find("'", i + 3) if text[i + 1] == '\\' else i + 2
            j = n if j < 0 else j
            blank(i + 1, j)
            i = j + 1
        else:
            i += 1
    return ''.join(out)


def enum_constants(text, name):
    """The constant names of `enum <name> {` in Java text, in declaration order."""
    stripped = strip_comments(text)
    m = re.search(r'\benum\s+' + re.escape(name) + r'\b[^{]*\{', stripped)
    if not m:
        return []
    i, depth, start, names = m.end(), 0, m.end(), []
    while i < len(stripped):
        c = stripped[i]
        if c in '({[':
            depth += 1
        elif c in ')}]':
            if depth == 0:
                break
            depth -= 1
        elif depth == 0 and c in ',;':
            item = stripped[start:i].strip()
            if item:
                names.append(re.match(r'[A-Za-z_][A-Za-z0-9_]*', item).group(0))
            start = i + 1
            if c == ';':
                return names
        i += 1
    item = stripped[start:i].strip()
    if item:
        names.append(re.match(r'[A-Za-z_][A-Za-z0-9_]*', item).group(0))
    return names


def split_args(text):
    """Top-level comma-separated arguments of an argument list, trimmed, comments removed. The
    commas are found in the text with comments and literals blanked, so neither splits."""
    stripped = strip_comments(text)
    args, depth, start = [], 0, 0
    for i, c in enumerate(stripped):
        if c in '({[':
            depth += 1
        elif c in ')}]':
            depth -= 1
        elif c == ',' and depth == 0:
            args.append(text[start:i])
            start = i + 1
    if stripped[start:].strip():
        args.append(text[start:])
    return [re.sub(r'//[^\n]*|/\*.*?\*/', ' ', a, flags=re.S).strip() for a in args]


def call_span(text, start):
    """(first, end) of the argument list of the call whose `(` is at or after `start`."""
    i = text.index('(', start)
    stripped = strip_comments(text)
    depth = 0
    for j in range(i, len(text)):
        if stripped[j] == '(':
            depth += 1
        elif stripped[j] == ')':
            depth -= 1
            if depth == 0:
                return i + 1, j
    raise ToolError(f'unbalanced call at offset {start}')


def call_args(text, start):
    """The argument list of the call whose `(` is at or after `start`."""
    first, end = call_span(text, start)
    return text[first:end]


def camel(name):
    """NN_INT -> NnInt, UINT32 -> Uint32: the type driver class and Rust variant stem."""
    return ''.join(p[:1].upper() + p[1:].lower() for p in name.split('_') if p)


class Tree:
    """What the checkout declares: tags, the enums a facts file names, taken signature chars."""

    def __init__(self, root=REPO):
        self.root = Path(root)

    def path(self, rel):
        return self.root / rel

    def read(self, rel):
        return self.path(rel).read_text(encoding='utf-8')

    def tags(self):
        """ColumnTypeTag constant name -> code."""
        text = self.read(f'{CAIRO_DIR}/ColumnTypeTag.java')
        return {m.group(1): int(m.group(2)) for m in re.finditer(r'^\s+([A-Za-z][A-Za-z0-9_]*)\((-?\d+)\)[,;]', text, re.M)}

    def null_code(self):
        return self.tags()['NULL']

    def enum(self, kind):
        files = {
            'Movement': 'PhysicalDescriptor.java',
            'Arithmetic': 'PhysicalDescriptor.java',
            'Accessor': 'PhysicalDescriptor.java',
            'NullPolicy': 'NullPolicy.java',
            'RelationKind': 'RelationKind.java',
            'WireKind': 'WireKind.java',
            'CastTarget': 'CastTarget.java',
        }
        return enum_constants(self.read(f'{CAIRO_DIR}/{files[kind]}'), kind)

    def pg_oids(self):
        return re.findall(r'public static final int (PG_[A-Z0-9_]+)\s*=', self.read(f'{CAIRO_DIR}/PgTypeOids.java'))

    def type_facts_calls(self):
        """(file, argument list) of every `new TypeFacts(` in the type drivers."""
        out = []
        for p in sorted(self.path(CAIRO_DIR).glob('*.java')):
            text = p.read_text(encoding='utf-8')
            for m in re.finditer(r'new TypeFacts\(', text):
                out.append((p, split_args(call_args(text, m.start()))))
        return out

    def signature_chars(self, exclude=None):
        """The signature chars every type driver answers, except the driver of `exclude`, and the pseudo types'."""
        chars = set()
        for p, args in self.type_facts_calls():
            if exclude and p.name == f'{camel(exclude)}TypeDriver.java':
                continue
            if len(args) > 10 and re.fullmatch(r"'.'", args[10]):
                chars.add(args[10][1])
        for p in sorted(self.path(CAIRO_DIR).glob('*TypeDriver.java')):
            text = p.read_text(encoding='utf-8')
            m = re.search(r"char getSignatureChar\(\)\s*\{\s*return '(.)';", text)
            if m:
                chars.add(m.group(1))
        # the pseudo types the function signatures name take theirs in FunctionFactoryDescriptor
        descriptor = self.path('core/src/main/java/io/questdb/griffin/FunctionFactoryDescriptor.java')
        if descriptor.exists():
            chars |= set(re.findall(r"pseudoSignature\(ColumnType\.\w+, '(.)'", descriptor.read_text(encoding='utf-8')))
        return chars

    def driver_facts(self, name):
        """The TypeFacts arguments of the type driver named `name`, or None."""
        p = self.path(f'{CAIRO_DIR}/{camel(name)}TypeDriver.java')
        if not p.exists():
            # INT's driver is IntTypeDriver, IPv4's IPv4TypeDriver, LONG256's Long256TypeDriver
            for q in self.path(CAIRO_DIR).glob('*TypeDriver.java'):
                if q.stem.lower() == f'{name}typedriver'.lower().replace('_', ''):
                    p = q
                    break
        if not p.exists():
            return None
        for path, args in self.type_facts_calls():
            if path == p and len(args) == 15:
                return args
        return None


# ------------------------------------------------------------------ the places

@dataclass
class Site:
    """What an item names: a label, the decision it asks for, its file and method, and for a guarded
    site the refusal it raises (<type> standing for the type's name)."""
    site: str
    decision: str
    file: str = ''
    method: str = ''
    message: str = ''


def label_of(place):
    return f'{Path(place.file).stem}.{place.method} {place.form}'


def build_decision(place):
    """The decision a compiler error at a place asks for: an arm in a switch over a value whose
    arm writes per row (the NULL policy, the wire kind), else the type named in its switch or
    table, else a type driver answer."""
    if place.form == 'value-switch' and any(v.startswith(('NullPolicy.', 'WireKind.')) for v in place.values):
        return 'add-writer-arm'
    if place.form in ('tag-switch', 'tag-enum-switch', 'tag-table', 'value-switch', 'rust-match', 'c-switch'):
        return 'name-yourself'
    return 'fill-driver-answer'


def guard_labels(decisions):
    """The labels of the guarded sites places.tsv decides refused: the sites a type may declare."""
    return {d.reason.split(': ', 1)[0] for d in decisions if d.decision == 'refused'}


class Sites:
    """The places audit.py finds in the tree and the decisions of places.tsv, read as the worklist
    needs them: the guarded sites a type may declare refused and the refusal each raises, the
    place a compiler error sits in, the places a test or kit path is decided to name."""

    def __init__(self, vocabulary, places, decisions, root=REPO):
        self.root = Path(root)
        self.vocabulary = vocabulary
        self.places = places
        self.decisions = decisions
        by_key = {p.key(): p for p in places}
        self.guards = {}
        self.claims = []
        for d in decisions:
            p = by_key.get(d.key())
            if p is None:
                continue
            if d.decision == 'refused':
                label, _, kept = d.reason.partition(': ')
                self.guards.setdefault(label, Site(label, 'declare-or-admit', p.file, p.method, kept or FAMILY_ARM_REFUSAL + label))
            elif d.decision == 'test':
                self.claims.append((d.reason, Site(label_of(p), 'implement-pair', p.file, p.method)))

    @staticmethod
    def load(tree):
        v, places = audit.scan(tree.root)
        return Sites(v, places, audit.load_decisions(tree.path(audit.PLACES_FILE)), tree.root)

    def declarable(self):
        return dict(self.guards)

    def at(self, file, line):
        """The innermost place around a line of a file, or None."""
        hits = [p for p in self.places if p.file == file and p.line <= line <= max(p.last_line, p.line)]
        if not hits:
            return None
        p = min(hits, key=lambda q: max(q.last_line, q.line) - q.line)
        return Site(label_of(p), build_decision(p), p.file, p.method)

    def by_refusal(self, lead, site):
        """The site a refusal names: the guard of its label, or the place its lead belongs to."""
        if lead == 'no family arm':
            return self.guards.get(site)
        return next((Site(f'{Path(f).stem}.{m}', d, f, m) for lead_, f, m, d in LEADS if lead_ == lead), None)

    def by_kept_text(self, text):
        """The site whose earlier error text a failure holds: a guard that keeps one, else KEPT_TEXTS."""
        for guard in self.guards.values():
            if not guard.message.startswith(FAMILY_ARM_REFUSAL) and guard.message in text:
                return guard
        return next((Site(f'{Path(f).stem}.{m}', d, f, m) for kept, f, m, d in KEPT_TEXTS if kept in text), None)

    def by_layer(self, path):
        """(site, decision) of the code a kit path or test checks (LAYERS), or (None, None)."""
        for prefix, file, method, decision in LAYERS:
            if path.startswith(prefix):
                return Site(f'{Path(file).stem}.{method}', decision, file, method), decision
        return None, None

    def by_instrument(self, ref, value_row=''):
        """The places places.tsv decides a test or kit path names; of several, the one whose method
        the kit's value row names (setBoolean: setBoolean0) first."""
        sites = [site for r, site in self.claims if r in (ref, f'kit:{ref}')]
        return sorted(sites, key=lambda site: not (value_row and site.method.startswith(value_row)))

    def by_kit_guard(self, path):
        """The guarded site a kit path reaches (KIT_GUARDS), or None."""
        return next((self.guards.get(label) for prefix, label in KIT_GUARDS if path.startswith(prefix)), None)

    def by_method(self, method):
        """The first place of a method of this name, the one a coverage message names."""
        p = next((q for q in self.places if q.method == method), None)
        return Site(label_of(p), 'implement-pair', p.file, p.method) if p else None


# ------------------------------------------------------------------ the facts file

FACT_FIELDS = {
    'type': ('name', 'tag', 'sql_names', 'storage'),
    'physical': ('movement', 'arithmetic', 'accessor', 'null_policy', 'null_word', 'wire_kind'),
    'relations': ('relation_kind', 'relation_bits', 'implicit_casts', 'cast_target'),
    'protocols': ('pg_oid', 'pg_array_oid'),
    'functions': ('signature_char',),
    'kit': ('paths', 'refused_sites'),
}


def load_facts(path):
    with open(path, 'rb') as f:
        try:
            return tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise UsageError([f'facts: {path}: not TOML: {e}'])


def registered(tree, facts):
    """Whether the tree holds the registration of the facts' type: its name, under its tag."""
    t = facts.get('type', {})
    return isinstance(t.get('name'), str) and tree.tags().get(t['name']) == t.get('tag')


def validate_facts(facts, tree, guards):
    """Every problem of a facts file, as `facts: <field>: <problem>` lines; empty when valid.
    `guards` holds the labels of the guarded sites a type may declare refused."""
    problems = []
    for section, names in FACT_FIELDS.items():
        if section not in facts or not isinstance(facts[section], dict):
            problems.append(f'facts: {section}: missing')
            continue
        for key in names:
            if key not in facts[section]:
                problems.append(f'facts: {section}.{key}: missing')
        for key in facts[section]:
            if key not in names:
                problems.append(f'facts: {section}.{key}: unknown field')
    for section in facts:
        if section not in FACT_FIELDS:
            problems.append(f'facts: {section}: unknown section')
    if problems:
        return problems

    t, ph, rel, pr, fn, kit = (facts[s] for s in FACT_FIELDS)
    tags = tree.tags()
    name = t['name']
    is_registered = registered(tree, facts)
    if not isinstance(name, str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*', name):
        problems.append(f'facts: type.name: {name!r} is not an upper-case Java identifier')
        name = None
    tag = t['tag']
    if not isinstance(tag, int) or isinstance(tag, bool):
        problems.append(f'facts: type.tag: {tag!r} is not a number')
    elif not is_registered:
        taken = {code: n for n, code in tags.items() if n != 'NULL'}
        if tag in taken:
            problems.append(f'facts: type.tag: {tag} is taken by {taken[tag]}')
        elif tag != tags['NULL']:
            problems.append(f'facts: type.tag: {tag} is not the next free tag, {tags["NULL"]} (NULL\'s, which moves up by one)')
        if tag >= 127:
            problems.append(f'facts: type.tag: {tag} does not fit the tag field (at most 126, NULL takes the next)')
    if name and not is_registered and name in tags:
        problems.append(f'facts: type.name: {name} is taken by a tag')
    if name and not is_registered and tree.path(f'{CAIRO_DIR}/{camel(name)}TypeDriver.java').exists():
        problems.append(f'facts: type.name: {camel(name)}TypeDriver.java already exists')
    sql_names = t['sql_names']
    if not isinstance(sql_names, list) or not sql_names or not all(isinstance(s, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', s) for s in sql_names):
        problems.append(f'facts: type.sql_names: {sql_names!r} is not a list of SQL identifiers')
    if t['storage'] not in ('fixed', 'var'):
        problems.append(f'facts: type.storage: {t["storage"]!r} is neither "fixed" nor "var"')

    def check_enum(field_name, value, kind, extra=()):
        values = tree.enum(kind)
        if value not in values and value not in extra:
            problems.append(f'facts: {field_name}: {value!r} is no {kind} ({", ".join(values + list(extra))})')

    check_enum('physical.movement', ph['movement'], 'Movement')
    check_enum('physical.arithmetic', ph['arithmetic'], 'Arithmetic')
    check_enum('physical.accessor', ph['accessor'], 'Accessor')
    check_enum('physical.null_policy', ph['null_policy'], 'NullPolicy')
    if ph['wire_kind'] == 'new':
        if name and not is_registered and name in tree.enum('WireKind'):
            problems.append(f'facts: physical.wire_kind: "new" names a constant {name}, which WireKind already has')
    else:
        check_enum('physical.wire_kind', ph['wire_kind'], 'WireKind', ('new',))
    if t['storage'] in ('fixed', 'var') and ph['movement'] in tree.enum('Movement'):
        if (ph['movement'] == 'VAR') != (t['storage'] == 'var'):
            problems.append(f'facts: physical.movement: {ph["movement"]} disagrees with type.storage "{t["storage"]}" (VAR only for var)')
    if not isinstance(ph['null_word'], str) or not ph['null_word'].strip():
        problems.append('facts: physical.null_word: not a Java long expression')
    check_enum('relations.relation_kind', rel['relation_kind'], 'RelationKind')
    check_enum('relations.cast_target', rel['cast_target'], 'CastTarget')
    if not isinstance(rel['relation_bits'], int) or isinstance(rel['relation_bits'], bool) or rel['relation_bits'] < 0:
        problems.append(f'facts: relations.relation_bits: {rel["relation_bits"]!r} is not a non-negative number')
    casts = rel['implicit_casts']
    if not isinstance(casts, list):
        problems.append('facts: relations.implicit_casts: not a list')
    else:
        for c in casts:
            if c != name and c not in tags:
                problems.append(f'facts: relations.implicit_casts: {c!r} names no tag')
    oids = tree.pg_oids()
    for key in ('pg_oid', 'pg_array_oid'):
        if pr[key] != '0' and pr[key] not in oids:
            problems.append(f'facts: protocols.{key}: {pr[key]!r} is neither "0" nor a PgTypeOids constant')
    sig = fn['signature_char']
    if not isinstance(sig, str) or (sig != '-' and len(sig) != 1):
        problems.append(f'facts: functions.signature_char: {sig!r} is neither one character nor "-" (none)')
    elif sig != '-':
        taken = {c.lower() for c in tree.signature_chars(exclude=name if is_registered else None)}
        if sig.lower() in taken:
            problems.append(f'facts: functions.signature_char: {sig!r} is taken by a type driver')
    paths = kit['paths']
    if not isinstance(paths, list) or not all(isinstance(p, str) and p and ' ' not in p and '|' not in p for p in paths):
        problems.append('facts: kit.paths: not a list of kit path patterns ("-" for none)')
    refused = kit['refused_sites']
    if not isinstance(refused, list):
        problems.append('facts: kit.refused_sites: not a list')
    else:
        for s in refused:
            if s not in guards:
                problems.append(f'facts: kit.refused_sites: {s!r} names no guarded site of {audit.PLACES_FILE}')
    # a field init left undecided is reported once, as undecided
    pending = [f'{sec}.{key}' for sec in FACT_FIELDS for key, value in facts[sec].items()
               if isinstance(value, str) and 'CHANGE-ME' in value]
    problems = [p for p in problems if not any(p.startswith(f'facts: {f}:') for f in pending)]
    problems += [f'facts: {f}: still CHANGE-ME, which init leaves for the author to decide' for f in pending]
    return problems


def init_facts(tree, name, like, tag):
    """The text of a facts file for a new type, every field copied from the `like` type's driver."""
    args = tree.driver_facts(like)
    if args is None:
        raise UsageError([f'init: --like {like}: no fixed-size type driver with a TypeFacts instance (a plain leaf such as INT)'])
    tags = tree.tags()
    if name in tags:
        raise UsageError([f'init: {name} is taken by a tag'])

    def last(expr):
        return expr.rsplit('.', 1)[-1]

    casts = [last(c.strip()) for c in re.sub(r'^new short\[]\{|}$', '', args[8]).split(',') if c.strip()]
    pg = last(args[9]) if args[9] != '0' else '0'
    pg_arr = last(args[11]) if args[11] != '0' else '0'
    movement = last(args[1])
    lines = [
        f'# facts for {name}, started from {like}; see utils/type-probe/README.md, "The facts file"',
        '[type]',
        f'name = "{name}"',
        f'tag = {tag}',
        f'sql_names = ["{name.lower()}"]',
        f'storage = "{"var" if movement == "VAR" else "fixed"}"',
        '',
        '[physical]',
        f'movement = "{movement}"',
        f'arithmetic = "{last(args[2])}"',
        f'accessor = "{last(args[3])}"',
        f'null_policy = "{last(args[4])}"',
        f'null_word = "{args[12]}"',
        'wire_kind = "CHANGE-ME"',
        '',
        '[relations]',
        f'relation_kind = "{last(args[6])}"',
        f'relation_bits = {args[7]}',
        'implicit_casts = [' + ', '.join(f'"{c}"' for c in [name] + casts) + ']',
        f'cast_target = "{last(args[13])}"',
        '',
        '[protocols]',
        f'pg_oid = "{pg}"',
        f'pg_array_oid = "{pg_arr}"',
        '',
        '[functions]',
        'signature_char = "CHANGE-ME"',
        '',
        '[kit]',
        'paths = [' + ', '.join(f'"{p}"' for p in KIT_PATHS) + ']',
        'refused_sites = []',
    ]
    return '\n'.join(lines) + '\n'


# ------------------------------------------------------------------ registration

def anchor_comment(name):
    return ANCHOR_PREFIX + name + ANCHOR_SUFFIX


ANCHORS = (
    ('tag list', f'{CAIRO_DIR}/ColumnTypeTag.java'),
    ('column type constant', f'{CAIRO_DIR}/ColumnType.java'),
    ('column type name', f'{CAIRO_DIR}/ColumnType.java'),
    ('type driver lookup', f'{CAIRO_DIR}/TypeDrivers.java'),
    ('rust tag', 'core/rust/qdb-core/src/col_type.rs'),
    ('native tag', 'core/src/main/c/share/column_type.h'),
    ('wire kind', f'{CAIRO_DIR}/WireKind.java'),
)


def find_anchor(lines, name, rel):
    marker = anchor_comment(name)
    hits = [i for i, line in enumerate(lines) if line.strip() in ('// ' + marker, '//' + marker)]
    if not hits:
        raise UsageError([f'anchor: "{name}" is missing from {rel}'])
    if len(hits) > 1:
        raise UsageError([f'anchor: "{name}" appears {len(hits)} times in {rel}'])
    return hits[0]


def register(tree, facts, log):
    """Inserts the registration lines above the anchors; a line already present is left alone."""
    t = facts['type']
    name, tag = t['name'], t['tag']
    is_new_kind = facts['physical']['wire_kind'] == 'new'
    changed = []
    files = {}
    for anchor, rel in ANCHORS:
        if anchor == 'wire kind' and not is_new_kind:
            continue
        lines = files.setdefault(rel, tree.read(rel).split('\n'))
        i = find_anchor(lines, anchor, rel)
        indent = re.match(r'\s*', lines[i]).group(0)
        if anchor == 'tag list':
            if not any(re.match(r'\s+' + name + r'\(', line) for line in lines):
                m = re.match(r'(\s*NULL\()(\d+)(\),\s*)$', lines[i + 1])
                if not m:
                    raise UsageError([f'anchor: "{anchor}" in {rel} is not directly above NULL\'s line'])
                lines[i + 1] = f'{m.group(1)}{tag + 1}{m.group(3)}'
                lines.insert(i, f'{indent}{name}({tag}),')
                changed.append(rel)
        elif anchor == 'column type constant':
            if not any(re.match(r'\s*public static final short ' + name + r' =', line) for line in lines):
                m = re.match(r'(\s*public static final short NULL = )(\w+)( \+ 1;\s*// = )(\d+)(;.*)$', lines[i + 1])
                if not m:
                    raise UsageError([f'anchor: "{anchor}" in {rel} is not directly above NULL\'s constant'])
                lines.insert(i, f'{indent}public static final short {name} = {m.group(2)} + 1; // = {tag};')
                lines[i + 2] = f'{m.group(1)}{name}{m.group(3)}{tag + 1}{m.group(5)}'
                changed.append(rel)
        elif anchor == 'column type name':
            for sql_name in t['sql_names']:
                line = f'{indent}nameTypeMap.put("{sql_name}", {name});'
                if line not in lines:
                    i = find_anchor(lines, anchor, rel)
                    lines.insert(i, line)
                    changed.append(rel)
        elif anchor == 'type driver lookup':
            line = f'{indent}case {name} -> {camel(name)}TypeDriver.INSTANCE;'
            if line not in lines:
                lines.insert(i, line)
                changed.append(rel)
        elif anchor == 'rust tag':
            if not any(re.match(r'\s*' + camel(name) + r' = ', line) for line in lines):
                lines.insert(i, f'{indent}{camel(name)} = {tag},')
                changed.append(rel)
        elif anchor == 'native tag':
            if not any(re.match(r'\s*' + name + r' = ', line) for line in lines):
                m = re.match(r'(\s*NULL_ = )(\d+)(,\s*)$', lines[i + 1])
                if not m:
                    raise UsageError([f'anchor: "{anchor}" in {rel} is not directly above NULL_\'s line'])
                lines[i + 1] = f'{m.group(1)}{tag + 1}{m.group(3)}'
                lines.insert(i, f'{indent}{name} = {tag},')
                changed.append(rel)
        elif anchor == 'wire kind':
            if not any(line.strip() in (name + ',', name + ';') for line in lines):
                lines.insert(i, f'{indent}{name},')
                changed.append(rel)
    for rel in sorted(set(changed)):
        tree.path(rel).write_text('\n'.join(files[rel]), encoding='utf-8')
        log.append(f'registered {name} in {rel}')
    return sorted(set(changed))


def driver_source(facts, template_dir=HERE / 'templates', tree=None):
    """The type driver class text: a facts instance for a fixed-size type, else a var-size class."""
    t, ph, rel, pr, fn = facts['type'], facts['physical'], facts['relations'], facts['protocols'], facts['functions']
    name = t['name']
    casts = ', '.join(f'ColumnType.{c}' for c in rel['implicit_casts'])
    values = {
        'NAME': name,
        'CLASS': f'{camel(name)}TypeDriver',
        'TAG': f'ColumnTypeTag.{name}',
        'MOVEMENT': f'PhysicalDescriptor.Movement.{ph["movement"]}',
        'ARITHMETIC': f'PhysicalDescriptor.Arithmetic.{ph["arithmetic"]}',
        'ACCESSOR': f'PhysicalDescriptor.Accessor.{ph["accessor"]}',
        'NULL_POLICY': f'NullPolicy.{ph["null_policy"]}',
        'WIRE_KIND': f'WireKind.{name if ph["wire_kind"] == "new" else ph["wire_kind"]}',
        'RELATION_KIND': f'RelationKind.{rel["relation_kind"]}',
        'RELATION_BITS': str(rel['relation_bits']),
        'IMPLICIT_CASTS': f'new short[]{{{casts}}}',
        'PG_OID': '0' if pr['pg_oid'] == '0' else f'PgTypeOids.{pr["pg_oid"]}',
        'SIGNATURE_CHAR': 'FunctionFactoryDescriptor.NO_SIGNATURE_CHAR' if fn['signature_char'] == '-' else repr_char(fn['signature_char']),
        'PG_ARRAY_OID': '0' if pr['pg_array_oid'] == '0' else f'PgTypeOids.{pr["pg_array_oid"]}',
        'NULL_WORD': ph['null_word'],
        'CAST_TARGET': f'CastTarget.{rel["cast_target"]}',
        'IMPORTS': 'import io.questdb.griffin.FunctionFactoryDescriptor;\n' if fn['signature_char'] == '-' else '',
    }
    if 'Numbers.' in ph['null_word']:
        values['IMPORTS'] += 'import io.questdb.std.Numbers;\n'
    if t['storage'] == 'var':
        imports, answers = var_answers(tree or Tree())
        values['IMPORTS'] = ''.join(sorted(set(values['IMPORTS'].splitlines(keepends=True)) | imports))
        values['ANSWERS'] = answers
    if values['IMPORTS']:
        values['IMPORTS'] += '\n'
    template = 'FixedSizeTypeDriver.java.tmpl' if t['storage'] == 'fixed' else 'VarSizeTypeDriver.java.tmpl'
    text = (Path(template_dir) / template).read_text(encoding='utf-8')
    for key, value in values.items():
        text = text.replace('${' + key + '}', value)
    leftover = re.findall(r'\$\{[A-Z_]+}', text)
    if leftover:
        raise ToolError(f'template {template}: unfilled {leftover}')
    return text


# the answers the var-size template fills from the facts; every other abstract answer gets a stub
VAR_TEMPLATE_ANSWERS = {
    'getAccessor', 'getArithmetic', 'getImplicitCasts', 'getMovement', 'getName', 'getNullAsLong', 'getNullLong',
    'getNullPolicy', 'getPgArrayOid', 'getPgOid', 'getRelationBits', 'getRelationKind', 'getSignatureChar', 'getTag',
    'getWireKind', 'isCastTarget',
}


def abstract_methods(text):
    """(return type, name, parameters, throws) of every abstract method of a Java interface."""
    stripped = strip_comments(text)
    body = stripped[stripped.index('{', stripped.index('interface ')) + 1:]
    out, depth, start = [], 0, 0
    for i, c in enumerate(body):
        if c == '{':
            if depth == 0:
                start = None
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                start = i + 1
        elif c == ';' and depth == 0:
            decl = ' '.join(body[start:i].split()) if start is not None else ''
            start = i + 1
            m = re.match(r'^(?:@\w+\s+)*(?!default\b|static\b)((?:[\w.]+)(?:<[^()]*>)?(?:\[\])*)\s+(\w+)\((.*)\)(?:\s+throws\s+([\w., ]+))?$', decl)
            if m and '=' not in decl:
                out.append((m.group(1), m.group(2), m.group(3), m.group(4)))
    return out


def var_answers(tree):
    """The imports and the method stubs of a var-size type driver's answers that are code."""
    imports, stubs, seen = set(), [], set()
    for rel in (f'{CAIRO_DIR}/TypeDriver.java', f'{CAIRO_DIR}/ColumnTypeDriver.java'):
        text = tree.read(rel)
        imports |= {line + '\n' for line in text.splitlines() if line.startswith('import ')}
        for ret, name, params, throws in abstract_methods(text):
            if name in VAR_TEMPLATE_ANSWERS or (name, params) in seen:
                continue
            seen.add((name, params))
            call = f'{name}Answer' + ('();' if ret == 'void' else ';')
            stubs.append('\n    @Override\n'
                         f'    public {ret} {name}({params})' + (f' throws {throws}' if throws else '') + ' {\n'
                         f'        // write this answer: javac reports the name below until it is replaced\n'
                         f'        {"" if ret == "void" else "return "}{call}\n'
                         '    }\n')
    return imports, ''.join(stubs)


def repr_char(c):
    if c in ("'", '\\'):
        return "'\\" + c + "'"
    if not c.isascii() or not c.isprintable():
        return f"'\\u{ord(c):04x}'"
    return f"'{c}'"


def later_types_line(facts):
    t, ph, kit = facts['type'], facts['physical'], facts['kit']
    paths = ' '.join(kit['paths'])
    refused = ', '.join(kit['refused_sites'])
    return f'{t["name"]} | {t["sql_names"][0]} | {ph["null_policy"]} | {paths} | {ph["arithmetic"]} |' + (f' {refused}' if refused else '')


def later_driver_files(tree):
    """The type driver files of the types later-types.txt declares, relative to the tree."""
    path = tree.path(LATER_TYPES_FILE)
    if not path.exists():
        return set()
    names = (line.split('|')[0].strip() for line in path.read_text(encoding='utf-8').splitlines() if '|' in line)
    return {f'{CAIRO_DIR}/{camel(name)}TypeDriver.java' for name in names if name}


def write_generated(tree, facts, log):
    """Writes the type driver (once: the author edits it) and the type's later-types.txt line."""
    name = facts['type']['name']
    driver = tree.path(f'{CAIRO_DIR}/{camel(name)}TypeDriver.java')
    fresh = driver_source(facts, tree=tree)
    if not driver.exists():
        driver.write_text(fresh, encoding='utf-8')
        log.append(f'wrote {driver.relative_to(tree.root)}')
    elif facts['type']['storage'] == 'fixed':
        # the facts instance follows the facts file; the code answers are the author's and stay
        text = driver.read_text(encoding='utf-8')
        marker = 'new TypeFacts('
        if marker in text:
            first, end = call_span(text, text.index(marker))
            new_first, new_end = call_span(fresh, fresh.index(marker))
            updated = text[:first] + fresh[new_first:new_end] + text[end:]
            missing = [l for l in fresh.splitlines() if l.startswith('import ') and l not in text.splitlines()]
            if missing:
                package = updated.index('\n', updated.index('package ')) + 1
                updated = updated[:package] + '\n' + '\n'.join(missing) + updated[package:]
            if updated != text:
                driver.write_text(updated, encoding='utf-8')
                log.append(f'rewrote the facts of {driver.relative_to(tree.root)}')
    path = tree.path(LATER_TYPES_FILE)
    lines = path.read_text(encoding='utf-8').split('\n') if path.exists() else []
    line = later_types_line(facts)
    kept = [l for l in lines if not l.split('|')[0].strip() == name]
    if line not in lines or len(kept) != len(lines) - 1:
        while kept and kept[-1] == '':
            kept.pop()
        kept.append(line)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('\n'.join(kept) + '\n', encoding='utf-8')
        log.append(f'wrote the {name} line of {LATER_TYPES_FILE}')


# ------------------------------------------------------------------ build and test steps

def run_logged(cmd, log_path, cwd, env=None, timeout=None):
    """Runs a command with stdout and stderr into one log; returns (exit code, log text)."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, 'w', encoding='utf-8') as out:
        out.write('$ ' + ' '.join(str(c) for c in cmd) + '\n')
        out.flush()
        try:
            proc = subprocess.run([str(c) for c in cmd], cwd=cwd, stdout=out, stderr=subprocess.STDOUT, env=env, timeout=timeout)
            code = proc.returncode
        except FileNotFoundError as e:
            raise ToolError(f'{cmd[0]}: not found ({e})')
        except subprocess.TimeoutExpired:
            raise ToolError(f'{cmd[0]}: no answer within {timeout} s; see {log_path}')
    return code, log_path.read_text(encoding='utf-8', errors='replace')


def classpath(tree, out):
    """The core module's dependency path, resolved by Maven once per run (with the local client)."""
    cp_file = out / 'classpath.txt'
    code, text = run_logged(
        ['mvn', '-o', '-q', '-B', '-pl', 'core', '-P', 'local-client', 'dependency:build-classpath',
         f'-Dmdep.outputFile={cp_file}'],
        out / 'logs' / 'classpath.log', tree.root)
    if code != 0 or not cp_file.exists():
        raise ToolError(f'maven could not resolve the core classpath; see {out / "logs" / "classpath.log"}')
    return cp_file.read_text(encoding='utf-8').strip()


# javac keeps checking after the first error, so every exhaustive switch over a closed enum reports
JAVAC_FLAGS = ['-proc:none', '-Xmaxerrs', '100000', '-Xmaxwarns', '0', '-nowarn', '-Xlint:none', '-XDshould-stop.ifError=FLOW']


def javac(tree, out, cp):
    """javac over core main and core test, the two modules in one call, so the test sources'
    switches report with the main sources' though main does not compile; returns the log."""
    classes = out / 'classes'
    shutil.rmtree(classes, ignore_errors=True)
    classes.mkdir(parents=True)
    main, test = tree.path('core/src/main/java'), tree.path('core/src/test/java')
    sources = out / 'sources.txt'
    files = sorted(main.rglob('*.java')) + sorted(test.rglob('*.java'))
    sources.write_text('\n'.join(str(p) for p in files) + '\n', encoding='utf-8')
    cmd = ['javac', *JAVAC_FLAGS, '--add-exports=java.base/jdk.internal.vm=io.questdb,io.questdb.test',
           '--module-source-path', f'io.questdb={main}', '--module-source-path', f'io.questdb.test={test}',
           '--module-path', cp, '-d', classes, f'@{sources}']
    log = out / 'logs' / 'javac.log'
    code, text = run_logged(cmd, log, tree.root)
    if code != 0 and not parse_javac(text, tree.root):
        raise ToolError(f'javac failed without a diagnostic it can parse; see {log}')
    return text


def cargo(tree, out):
    """cargo check and clippy, JSON messages, in both Rust crates; returns the JSON texts."""
    texts = []
    for crate in ('core/rust/qdbr', 'core/rust/qdb-core'):
        for tool in ('check', 'clippy'):
            log = out / 'logs' / f'cargo-{tool}-{Path(crate).name}.json'
            code, text = run_logged(['cargo', tool, '--all-targets', '--message-format=json'], log, tree.path(crate))
            if code != 0 and not parse_cargo(text):
                raise ToolError(f'cargo {tool} in {crate} failed without a diagnostic it can parse; see {log}')
            texts.append(text)
    return texts


def cmake(tree, out):
    """The native build of core into <out>/cmake-build; returns its log."""
    build = out / 'cmake-build'
    log = out / 'logs' / 'cmake.log'
    code, text = run_logged(['cmake', '-S', 'core', '-B', build, '-DCMAKE_BUILD_TYPE=Release'], out / 'logs' / 'cmake-configure.log', tree.root)
    if code != 0:
        raise ToolError(f'cmake could not configure the native build; see {out / "logs" / "cmake-configure.log"}')
    # keep going past a failing file, so every switch the compiler lists reports in one run
    code, text = run_logged(['cmake', '--build', build, '--config', 'Release', '-j', str(os.cpu_count() or 4), '--', '-k'], log, tree.root)
    if code != 0 and not parse_cmake(text, tree.root):
        raise ToolError(f'the native build failed without a diagnostic it can parse; see {log}')
    return text


KIT_TYPES_PROPERTY = 'questdb.test.kit.types'


def kit(tree, out, is_rust_from_tree=True, only=None):
    """The conformance kit and the coverage tests, with the local client; returns the reports and
    whether the whole kit ran. With `only`, a type's name, the kit runs that type alone first,
    about a minute; the whole kit, every type, runs only when that pass leaves no failure, so the
    existing types are checked once the new type is green. The kit loads the tree's native code:
    the C++ library the CMake step builds, and a debug Rust library Maven's build-rust-library
    profile builds, unless the run skips native builds."""
    if only:
        reports = kit_pass(tree, out, is_rust_from_tree, only, 'kit-type.log', 'surefire-type')
        if any(parse_surefire(r) for r in reports):
            return reports, False
    return kit_pass(tree, out, is_rust_from_tree, None, 'kit.log', 'surefire'), True


def kit_pass(tree, out, is_rust_from_tree, only, log_name, copy_name):
    names = '|'.join(['TypeConformance.*Test'] + list(COVERAGE_TESTS))
    reports = tree.path(SUREFIRE_DIR)
    shutil.rmtree(reports, ignore_errors=True)
    log = out / 'logs' / log_name
    profiles = 'local-client,build-rust-library' if is_rust_from_tree else 'local-client'
    cmd = ['mvn', '-o', '-B', '-Dtest.exclude=None', '-DfailIfNoTests=false', '-Dsurefire.failIfNoSpecifiedTests=false',
           '-pl', 'core', 'test', '-P', profiles, f'-Dtest.include=%regex[.*({names})\\.class]']
    if only:
        cmd.append(f'-D{KIT_TYPES_PROPERTY}={only}')
    code, text = run_logged(cmd, log, tree.root)
    copied = out / 'logs' / copy_name
    shutil.rmtree(copied, ignore_errors=True)
    copied.mkdir(parents=True)
    xmls = sorted(reports.glob('TEST-*.xml')) if reports.exists() else []
    for p in xmls:
        shutil.copy(p, copied / p.name)
    if not xmls:
        raise ToolError(f'the kit run left no test report (exit {code}); see {log}')
    return [p.read_text(encoding='utf-8', errors='replace') for p in sorted(copied.glob('TEST-*.xml'))]


# ------------------------------------------------------------------ parsers

@dataclass
class Diag:
    file: str
    line: int
    col: int
    message: str


def rel_path(path, root):
    p = Path(path)
    if p.is_absolute():
        try:
            return str(p.resolve().relative_to(Path(root).resolve()))
        except ValueError:
            return str(p)
    return str(p)


def parse_javac(text, root=REPO):
    """javac errors: `path:line: error: message`, the `symbol:` detail of a missing symbol kept."""
    out = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r'^(.+?\.java):(\d+): error: (.*)$', line)
        if not m:
            continue
        message = m.group(3)
        for detail in lines[i + 1:i + 6]:
            d = re.match(r'^\s+symbol:\s+(.*)$', detail)
            if d:
                message += ': ' + d.group(1).strip()
                break
            if re.match(r'^.+?\.java:\d+: ', detail):
                break
        out.append(Diag(rel_path(m.group(1), root), int(m.group(2)), 0, message))
    return out


RUST_LINTS = ('wildcard_enum_match_arm', 'match_wildcard_for_single_variants')


def parse_cargo(text):
    """rustc and clippy diagnostics from cargo's JSON messages: errors, and the enum-match lints."""
    import json
    out = []
    seen = set()
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get('reason') != 'compiler-message':
            continue
        m = msg.get('message') or {}
        code = (m.get('code') or {}).get('code') or ''
        if m.get('level') not in ('error', 'warning') or (m.get('level') == 'warning' and not any(code.endswith(l) for l in RUST_LINTS)):
            continue
        if not m.get('spans'):
            continue
        span = next((s for s in m['spans'] if s.get('is_primary')), m['spans'][0])
        base = Path(msg.get('manifest_path', '')).parent
        file = span['file_name']
        if not Path(file).is_absolute():
            file = str(base / file)
        # the library and its test build report a match once each, naming the enum two ways
        key = (file, span['line_start'])
        if key in seen:
            continue
        seen.add(key)
        out.append(Diag(file, span['line_start'], span.get('column_start', 0), m.get('message', '')))
    return out


def parse_cmake(text, root=REPO):
    """C and C++ compiler errors in a native build log: `path:line:col: error: message`."""
    out = []
    seen = set()
    for line in text.splitlines():
        m = re.match(r'^(.+?\.(?:c|cc|cpp|cxx|h|hpp)):(\d+):(\d+): (?:fatal )?error: (.*)$', line.strip())
        if m:
            key = (m.group(1), m.group(2), m.group(4))
            if key not in seen:
                seen.add(key)
                out.append(Diag(rel_path(m.group(1), root), int(m.group(2)), int(m.group(3)), m.group(4)))
    return out


@dataclass
class Failure:
    classname: str
    name: str
    message: str
    text: str


def parse_surefire(xml_text):
    """The failed and errored test cases of one surefire XML report."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as e:
        raise ToolError(f'a surefire report does not parse: {e}')
    out = []
    for case in root.iter('testcase'):
        for child in case:
            if child.tag in ('failure', 'error'):
                out.append(Failure(case.get('classname', ''), case.get('name', ''), child.get('message') or '', child.text or ''))
    return out


def find_refusals(text):
    """Every refusal of the three leads in a text: (lead, type, site, decision text). A test that
    prints the exception with its first frame on the same line leaves the frame out, and a closing
    bracket a server's message wraps the refusal in is not part of the decision text."""
    out = []
    for line in text.splitlines():
        m = REFUSAL.search(line)
        if m:
            lead, _type, site, decision = (g.strip() for g in m.groups())
            decision = STACK_FRAME.sub('', decision)
            while decision and decision[-1] in ')]' and decision.count(decision[-1]) > decision.count('(' if decision[-1] == ')' else '['):
                decision = decision[:-1].rstrip()
            out.append((lead, _type, site, decision))
    return out


# ------------------------------------------------------------------ the worklist

@dataclass(frozen=True)
class Item:
    group: str
    decision: str
    location: str
    message: str
    site: str = ''

    def line(self):
        # a place keeps its whole description; a coverage failure lists a line per case
        if self.group == 'namesake':
            message = ascii_message(self.message, None)
        elif self.group == 'coverage':
            message = ascii_message(self.message, COVERAGE_MESSAGE_LIMIT, is_every_line=True)
        else:
            message = ascii_message(self.message)
        tail = f' | site: {self.site}' if self.site else ''
        return f'- [ ] {self.decision} | {self.location} | {message}{tail}'


def ascii_message(text, limit=MESSAGE_LIMIT, is_every_line=False):
    """The first line of a message, or every line joined by ` / `, ASCII only, `|` as `/`, at
    most `limit` characters (no limit for None); a test's temporary directory, which differs on
    every run, reads as `<tmp>`."""
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()] or ['']
    first = ' / '.join(lines) if is_every_line else lines[0]
    first = TEMP_DIR.sub('<tmp>/', first)
    first = first.encode('ascii', 'replace').decode('ascii').replace('|', '/').strip()
    return first if limit is None or len(first) <= limit else first[:limit - 3] + '...'


def enclosing_method(path, line):
    """The name of the innermost method, function or constructor around a line of a source file."""
    best, best_size = '', None
    for name, first, last in method_spans(path):
        if first <= line <= last and (best_size is None or last - first < best_size):
            best, best_size = name, last - first
    return best


def method_spans(path):
    """(name, first line, last line) of every method, function or constructor of a source file."""
    try:
        text = Path(path).read_text(encoding='utf-8')
    except OSError:
        return []
    stripped = strip_comments(text)
    if str(path).endswith('.rs'):
        pattern = re.compile(r'\bfn\s+([A-Za-z_][A-Za-z0-9_]*)\b[^;{]*\{')
    else:
        pattern = re.compile(r'(?<![\w.])([A-Za-z_][A-Za-z0-9_]*)\s*\([^;{}()]*(?:\([^()]*\)[^;{}()]*)*\)\s*(?:throws [\w., ]+)?\{')
    spans = []
    for m in pattern.finditer(stripped):
        if m.group(1) in ('if', 'for', 'while', 'switch', 'catch', 'synchronized', 'return', 'new', 'try', 'else'):
            continue
        depth, j = 0, m.end() - 1
        while j < len(stripped):
            if stripped[j] == '{':
                depth += 1
            elif stripped[j] == '}':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        spans.append((m.group(1), stripped.count('\n', 0, m.start()) + 1, stripped.count('\n', 0, j) + 1))
    return spans


def site_location(tree, row):
    """`file:line` of a site row's method at the tree, or None without a tree or a row."""
    if tree is None or row is None or not row.file:
        return None
    for name, first, _last in method_spans(tree.path(row.file)):
        if name == row.method:
            return f'`{row.file}:{first}`'
    return f'`{row.file}`'


def build_item(group, diag, sites, tree, driver_file):
    """A compiler diagnostic as a worklist item: the decision the place it sits in asks for, or a
    driver answer in the type driver of the run's type or of another type later-types.txt declares
    (two types added together build each other's drivers). A test's own exhaustive switch over the
    tag, which pins an answer per tag, takes the type's answer."""
    location = f'`{diag.file}:{diag.line}' + (f':{diag.col}' if diag.col else '') + '`'
    if diag.file == driver_file or diag.file in later_driver_files(tree):
        return Item(group, 'fill-driver-answer', location, diag.message)
    if diag.file.startswith('core/src/test/'):
        method = enclosing_method(tree.path(diag.file), diag.line)
        return Item(group, 'name-yourself', location, diag.message, f'{Path(diag.file).stem}.{method} test switch')
    site = sites.at(diag.file, diag.line)
    if site is None:
        return Item(group, 'fill-driver-answer', location, diag.message, 'unmapped')
    return Item(group, site.decision, location, diag.message, site.site)


def kit_segments(failure):
    """A kit failure that lists several paths (the SQL kit runs every path, then reports each
    failing one on a line opening with its context) as one failure per path; any other failure
    as itself."""
    cls = failure.classname.rsplit('.', 1)[-1]
    message = failure.message or failure.text
    starts = [m.start() for m in KIT_CONTEXT.finditer(message)]
    if not cls.startswith('TypeConformance') or cls == 'TypeConformanceTypesTest' or len(starts) < 2:
        return [failure]
    ends = starts[1:] + [len(message)]
    parts = [message[s:e].strip() for s, e in zip(starts, ends)]
    return [Failure(failure.classname, failure.name, part, part) for part in parts]


def failure_items(failure, sites, facts, tree=None):
    """The worklist items one failed test gives: its refusals, else its kit or coverage failure,
    one per path for a kit failure that lists several. A refusal, and a coverage failure mapped
    to a site, is located at the site's method."""
    segments = kit_segments(failure)
    if len(segments) > 1:
        return [i for s in segments for i in failure_items(s, sites, facts, tree)]
    items = []
    text = failure.text if failure.message and failure.message in failure.text else failure.message + '\n' + failure.text
    declared = set(facts['kit']['refused_sites'])
    cls = failure.classname.rsplit('.', 1)[-1]
    test = re.sub(r'\[.*$', '', failure.name)
    for lead, _type, label, _decision in find_refusals(text):
        if lead == 'no family arm' and label in declared:
            continue
        site = sites.by_refusal(lead, label)
        decision = 'declare-or-admit' if lead == 'no family arm' else (site.decision if site else 'add-writer-arm')
        items.append(Item('refusal', decision, site_location(tree, site) or f'`{cls}#{test}`',
                          f'{lead} for {_type} at {label}: {_decision}', site.site if site else 'unmapped'))
    if items:
        return items
    contexts = KIT_CONTEXT.findall(text)
    kept = sites.by_kept_text(text)
    message = failure.message or failure.text
    if cls.startswith('TypeConformance') and cls != 'TypeConformanceTypesTest':
        if contexts:
            _t, value_row, path, mode = contexts[0]
            location = f'`kit:{path}@{mode}#{value_row}`'
        else:
            value_row, path = '', ''
            location = f'`kit:{cls}#{test}`'
        named = sites.by_instrument(path, value_row) if path else []
        site = kept or (named[0] if named else None) or sites.by_kit_guard(path)
        if site is None:
            site, _decision = sites.by_layer(path or cls)
        return [Item('kit', site.decision if site else 'implement-pair', location, message, site.site if site else 'unmapped')]
    location = f'`{cls}#{test}`'
    # a precise match (a kept text, or each function a coverage test names) is located at its site;
    # a test that only names the code it checks stays located at itself, so each failing test is an item
    found = [kept] if kept is not None else []
    if not found:
        for method in dict.fromkeys(re.findall(r'\b([a-z][A-Za-z0-9]*): \S+ is not handled', text)):
            site = sites.guards.get('ILP column kind') if method == 'columnKind' else sites.by_method(method)
            if site is not None:
                found.append(site)
    if found:
        return [Item('coverage', 'implement-pair', site_location(tree, site) or location, message, site.site) for site in found]
    site, decision = sites.by_layer(f'{cls}#{test}')
    if site is None:
        named = sites.by_instrument(cls)
        site = named[0] if named else None
        decision = 'implement-pair'
    return [Item('coverage', decision, location, message, site.site if site else 'unmapped')]


def namesake_items(sites, facts):
    """The places of the view by namesake (audit.py `like`) still open for the type: those that
    name its namesake, the existing type its accessor family is named after, those that switch on
    a value its facts share, and the tables by tag. A place closes when its code names the type,
    or by a decision in places.tsv for the type or for every type. The problems of places.tsv
    come first, located at the file."""
    t, ph, rel = facts['type'], facts['physical'], facts['relations']
    name, namesake = t['name'], ph['accessor']
    wire = name if ph['wire_kind'] == 'new' else ph['wire_kind']
    values = {f'Accessor.{ph["accessor"]}', f'Arithmetic.{ph["arithmetic"]}', f'Movement.{ph["movement"]}',
              f'NullPolicy.{ph["null_policy"]}', f'WireKind.{wire}', f'RelationKind.{rel["relation_kind"]}',
              f'CastTarget.{rel["cast_target"]}'}
    view = audit.like(sites.vocabulary, sites.places, sites.decisions, namesake, values, sites.root, own=name, type_name=name)
    items = [Item('namesake', 'decide-place', f'`{audit.PLACES_FILE}`', problem)
             for problem in audit.check(sites.root, sites.places, sites.decisions)]
    for i in view.open():
        items.append(Item('namesake', 'decide-place', f'`{i.place.where()}`',
                          f'{audit.GROUP_COUNTS[i.group].format(n=namesake)}: {audit.describe(i)}'))
    return items


def sort_items(items, label=''):
    """One item per location and decision, by group, then by location. Of the failures at one
    location, the one of the type the run adds (its kit label, `type=<label>`) is kept, then one
    that names its site, then the first by message, so a rerun keeps the same one whatever order
    the tests ran in."""
    own = f'type={label.lower()} ' if label else None
    unique = {}
    for item in items:
        k = (item.group, item.location, item.decision)
        rank = (own is not None and own not in item.message, item.site in ('', 'unmapped'), ascii_message(item.message))
        if k not in unique or rank < unique[k][0]:
            unique[k] = (rank, item)
    unique = {k: item for k, (_rank, item) in unique.items()}

    def key(item):
        loc = item.location.strip('`')
        m = re.match(r'^(.*?):(\d+)(?::(\d+))?$', loc)
        if m:
            return GROUPS.index(item.group), m.group(1), int(m.group(2)), int(m.group(3) or 0), item.decision
        return GROUPS.index(item.group), loc, 0, 0, item.decision

    return sorted(unique.values(), key=key)


def render_worklist(name, facts_path, facts_sha, tree_desc, started, elapsed, items, notes):
    counts = {g: sum(1 for i in items if i.group == g) for g in GROUPS}
    lines = [
        f'# Worklist: {name}',
        '',
        f'- Facts: `{facts_path}` (sha256 {facts_sha})',
        f'- Tree: {tree_desc}',
        f'- Run: {started:%Y-%m-%d %H:%M}, {format_elapsed(elapsed)}',
        f'- Items: {len(items)} (' + ', '.join(f'{g} {counts[g]}' for g in GROUPS) + ')',
    ]
    lines += [f'- Note: {n}' for n in notes]
    for g in GROUPS:
        lines += ['', f'## {g} ({counts[g]})', '']
        lines += [i.line() for i in items if i.group == g]
    return '\n'.join(lines).replace('\n\n\n', '\n\n') + '\n'


def format_elapsed(seconds):
    minutes, secs = divmod(int(round(seconds)), 60)
    return f'{minutes} min {secs} s' if minutes else f'{secs} s'


def exit_code(items, skipped):
    return 1 if items or skipped else 0


# ------------------------------------------------------------------ the commands

def tree_description(tree):
    def git(*args):
        try:
            return subprocess.run(['git', *args], cwd=tree.root, capture_output=True, text=True).stdout.strip()
        except OSError:
            return ''

    branch = git('rev-parse', '--abbrev-ref', 'HEAD') or 'detached'
    commit = git('rev-parse', '--short=10', 'HEAD') or 'unknown'
    return f'`{branch}` at `{commit}` plus the generated changes'


def cmd_init(args, tree):
    sys.stdout.write(init_facts(tree, args.name, args.like, args.tag))
    return 0


def cmd_run(args, tree):
    started = datetime.datetime.now()
    t0 = time.monotonic()
    facts_path = Path(args.facts).resolve()
    facts = load_facts(facts_path)
    try:
        decisions = audit.load_decisions(tree.path(audit.PLACES_FILE))
    except audit.UsageError as e:
        raise ToolError(str(e))
    problems = validate_facts(facts, tree, guard_labels(decisions))
    if problems:
        raise UsageError(problems)
    name = facts['type']['name']
    out = Path(args.out).resolve() if args.out else tree.path(f'utils/target/type-probe/{name}')
    (out / 'logs').mkdir(parents=True, exist_ok=True)
    log = []
    register(tree, facts, log)
    write_generated(tree, facts, log)
    (out / 'logs' / 'register.log').write_text('\n'.join(log) + '\n', encoding='utf-8')
    driver_file = f'{CAIRO_DIR}/{camel(name)}TypeDriver.java'
    # the places as the code stands with the type registered, so a place that names it is closed
    v, places = audit.scan(tree.root)
    sites = Sites(v, places, decisions, tree.root)

    items, notes, skipped = [], [], []
    cp = classpath(tree, out)
    items += [build_item('build-java', d, sites, tree, driver_file) for d in parse_javac(javac(tree, out, cp), tree.root)]
    if args.skip_native:
        skipped += ['build-rust', 'build-c']
        notes.append('build-rust and build-c did not run (--skip-native)')
    else:
        for text in cargo(tree, out):
            for d in parse_cargo(text):
                d.file = rel_path(d.file, tree.root)
                items.append(build_item('build-rust', d, sites, tree, driver_file))
        items += [build_item('build-c', d, sites, tree, driver_file) for d in parse_cmake(cmake(tree, out), tree.root)]
    if args.skip_kit:
        skipped += ['refusal', 'kit', 'coverage']
        notes.append('the kit and the coverage tests did not run (--skip-kit)')
    elif items:
        notes.append('the kit and the coverage tests did not run, because the build lists items')
    else:
        reports, is_whole_kit = kit(tree, out, not args.skip_native, facts['type']['sql_names'][0])
        if not is_whole_kit:
            notes.append(f'the kit ran for {name} alone; the whole kit runs once that pass is green')
        for report in reports:
            for failure in parse_surefire(report):
                items += failure_items(failure, sites, facts, tree)
    items += namesake_items(sites, facts)
    items = sort_items(items, facts['type']['sql_names'][0])
    sha = hashlib.sha256(facts_path.read_bytes()).hexdigest()
    text = render_worklist(name, facts_path, sha, tree_description(tree), started, time.monotonic() - t0, items, notes)
    (out / 'worklist.md').write_text(text, encoding='utf-8')
    print(f'{out / "worklist.md"}: {len(items)} items')
    return exit_code(items, skipped)


def main(argv=None):
    ap = argparse.ArgumentParser(prog='type_probe.py', description='Lists the work of adding a column type.')
    sub = ap.add_subparsers(dest='command', required=True)
    p_init = sub.add_parser('init', help='print a facts file for a new type, started from an existing one')
    p_init.add_argument('name')
    p_init.add_argument('--like', required=True)
    p_init.add_argument('--tag', required=True, type=int)
    p_run = sub.add_parser('run', help='register the type, build, test and write worklist.md')
    p_run.add_argument('facts')
    p_run.add_argument('--out')
    p_run.add_argument('--skip-native', action='store_true')
    p_run.add_argument('--skip-kit', action='store_true')
    p_audit = sub.add_parser('audit', help='run audit.py: the places that decide by a type, and places.tsv')
    p_audit.add_argument('rest', nargs=argparse.REMAINDER)
    try:
        args = ap.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    tree = Tree(os.environ.get('TYPE_PROBE_TREE', REPO))
    if args.command == 'audit':
        return audit.main(['--repo', str(tree.root)] + args.rest)
    try:
        return cmd_init(args, tree) if args.command == 'init' else cmd_run(args, tree)
    except UsageError as e:
        for p in e.problems:
            print(p, file=sys.stderr)
        return 2
    except ToolError as e:
        print(f'type_probe: {e}', file=sys.stderr)
        return 3


if __name__ == '__main__':
    sys.exit(main())
