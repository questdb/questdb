#!/usr/bin/env python3
"""Lists every place in QuestDB's main code that decides by a column type.

    audit.py [--repo PATH] summary
    audit.py [--repo PATH] like <TYPE> [--type NAME] [--all] [--rows] [--out FILE]
    audit.py [--repo PATH] places [--out FILE]
    audit.py [--repo PATH] check

A place is code that treats a column type, or a value a type driver supplies, in its own way, so
that a type added later may have to change it: a switch over a tag, a comparison with a tag, a
call of a predicate that tests tags, a table indexed by a tag, a switch or test over a type driver
value (accessor family, NULL policy, wire kind, relation kind, arithmetic tier, movement), a Rust
match or test over the tag enum, a C or C++ switch or test over the native tag enum.

The scan reads the code on every run and works out each place's form: whether the compiler names
it for a new tag or value (an exhaustive switch expression over an enum, a Rust match with no
catch-all arm, a C++ enum switch where -Wswitch is an error), whether the family-arm guard refuses
a type unlike its family first, and which tags or values the place names. Nothing about a place is
stored but a decision and its reason (places.tsv next to this file), keyed by the file, the method,
the form and the place's own code text, never by line.

summary  counts per form, and the problems `check` reports
like     the places a type declared like <TYPE> must look at: those that name <TYPE> (its tag
         takes a path a new tag does not), those that switch on a value the two share (the new
         type takes <TYPE>'s arm), and the tables indexed by tag (every new tag needs an entry);
         --type closes the places the type's own tag or its decisions close, --all lists closed
         places too, --rows prints places.tsv rows for the open places instead
places   every place, one line each, with its decisions
check    the stored decisions against the code: a decision whose place is gone, a test that does
         not exist, a guard label that is not at its place, two decisions for one place and one
         type; exit 1 when there is one

README.md next to this file is the manual (section 5). Exit codes: 0 done, nothing to report;
1 `check` found a problem; 2 the command line or places.tsv is wrong. Python 3.11 or newer,
standard library only.
"""

import argparse
import bisect
import hashlib
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

PLACES_FILE = 'utils/type-probe/places.tsv'
JAVA_MAIN = 'core/src/main/java'
JAVA_TEST = 'core/src/test/java'
CAIRO = 'core/src/main/java/io/questdb/cairo'
RUST_CRATES = ('core/rust/qdb-core/src', 'core/rust/qdbr/src', 'core/rust/qdb-parquet-meta/src')
NATIVE_DIR = 'core/src/main/c'

# the enums whose constants a type driver supplies, by the Java class that declares each
VALUE_ENUMS = {
    'Accessor': 'PhysicalDescriptor.java',
    'Arithmetic': 'PhysicalDescriptor.java',
    'Movement': 'PhysicalDescriptor.java',
    'NullPolicy': 'NullPolicy.java',
    'WireKind': 'WireKind.java',
    'RelationKind': 'RelationKind.java',
    'CastTarget': 'CastTarget.java',
}
# the TypeFacts components that hold those values, by position
FACT_VALUES = {1: 'Movement', 2: 'Arithmetic', 3: 'Accessor', 4: 'NullPolicy', 5: 'WireKind', 6: 'RelationKind',
               13: 'CastTarget'}
# the Rust enums of type driver values, and the Java enum each mirrors
RUST_VALUE_ENUMS = {'ColumnMovement': 'Movement', 'ColumnNullPolicy': 'NullPolicy', 'ColumnArithmetic': 'Arithmetic'}
# calls that answer a type's accessor family; the family-arm guard's are the last two
FAMILY_CALLS = ('accessorOf', 'accessorOpcodeOf', 'familyArmOf', 'familyArmOpcodeOf', 'columnKind')
GUARD_CALLS = ('familyArmOf', 'familyArmOpcodeOf')
# ColumnType predicates that test a flag rather than a tag, and the tags the flag marks
FLAG_PREDICATES = {
    'isGeoHash': ('GEOBYTE', 'GEOSHORT', 'GEOINT', 'GEOLONG', 'GEOHASH'),
    'isArrayWithWeakDims': ('ARRAY',),
}
FORMS = ('tag-switch', 'tag-enum-switch', 'tag-test', 'tag-table', 'value-switch', 'value-test',
         'rust-match', 'rust-test', 'c-switch', 'c-test')
DECISIONS = ('not-reached', 'no-change', 'refused', 'test')
COLUMNS = ('file', 'method', 'form', 'anchor', 'n', 'decision', 'reason', 'type')
ANCHOR_LIMIT = 100
KEYWORDS = {'if', 'for', 'while', 'switch', 'catch', 'synchronized', 'return', 'new', 'else', 'try', 'do', 'super',
            'this', 'throw', 'yield', 'assert'}


class UsageError(Exception):
    """Exit 2."""


# ------------------------------------------------------------------ source text

_JAVA_NOISE = re.compile(r'//[^\n]*|/\*.*?\*/|"""(?:.|\n)*?"""|"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])+\'', re.S)
_RUST_NOISE = re.compile(
    r'//[^\n]*|/\*.*?\*/|b?r(#*)"(?:.|\n)*?"\1|b?"(?:\\.|[^"\\])*"|b?\'(?:\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.)|[^\'\\\n])\'',
    re.S)


def _blank(m):
    text = m.group(0)
    if text.startswith(('//', '/*')):
        return re.sub(r'[^\n]', ' ', text)
    # a literal keeps its delimiters and loses its inside
    d = re.match(r'b?r(#*)"|b?"""|b?"|b?\'', text)
    opening = d.group(0)
    if opening.endswith('"""'):
        closing = '"""'
    elif 'r' in opening:
        closing = '"' + d.group(1)
    else:
        closing = opening[-1]
    inner = text[len(opening):len(text) - len(closing)]
    return opening + re.sub(r'[^\n]', ' ', inner) + closing


def strip_java(text):
    """Java or C text with comments and the insides of string and char literals blanked; every
    offset and every newline is kept, so line numbers stay true."""
    return _JAVA_NOISE.sub(_blank, text)


def strip_rust(text):
    """Rust text, as strip_java; a lifetime ('a) is no char literal."""
    return _RUST_NOISE.sub(_blank, text)


class Source:
    """One file: its text, the text with comments and literals blanked, and line lookup."""

    def __init__(self, rel, raw, stripped):
        self.rel = rel
        self.raw = raw
        self.s = stripped
        self.newlines = [i for i, c in enumerate(stripped) if c == '\n']

    def line(self, pos):
        return bisect.bisect_left(self.newlines, pos) + 1


def match_close(s, i, open_c, close_c):
    """The offset of the bracket that closes the one at i, or -1."""
    depth = 0
    for j in range(i, len(s)):
        c = s[j]
        if c == open_c:
            depth += 1
        elif c == close_c:
            depth -= 1
            if depth == 0:
                return j
    return -1


def match_open(s, i, open_c, close_c):
    depth = 0
    for j in range(i, -1, -1):
        c = s[j]
        if c == close_c:
            depth += 1
        elif c == open_c:
            depth -= 1
            if depth == 0:
                return j
    return -1


def java_methods(s):
    """(body start, body end, name) of every method or constructor: a `{` after `)` and an
    optional throws clause, whose `(` follows a name that is no keyword and no call."""
    spans = []
    for m in re.finditer(r'\)\s*(?:throws\s+[\w.,\s]+?)?\s*\{', s):
        open_ = match_open(s, m.start(), '(', ')')
        if open_ < 0:
            continue
        head = s[max(0, open_ - 160):open_]
        nm = re.search(r'(\w+)\s*$', head)
        if not nm or nm.group(1) in KEYWORDS:
            continue
        before = head[:nm.start()].rstrip()
        if before.endswith(('.', 'new', '->', '=', '(', ',', 'return', '?', ':')):
            continue
        b = m.end() - 1
        e = match_close(s, b, '{', '}')
        if e > 0:
            spans.append((b, e, nm.group(1)))
    return spans


def rust_functions(s):
    spans = []
    for m in re.finditer(r'\bfn\s+(\w+)[^{;]*\{', s):
        b = m.end() - 1
        e = match_close(s, b, '{', '}')
        if e > 0:
            spans.append((b, e, m.group(1)))
    return spans


def c_functions(s):
    spans = []
    for m in re.finditer(r'\b(\w+)\s*\([^;{)]*\)\s*(?:const\s*)?(?:noexcept\s*)?\{', s):
        if m.group(1) in KEYWORDS:
            continue
        b = m.end() - 1
        e = match_close(s, b, '{', '}')
        if e > 0:
            spans.append((b, e, m.group(1)))
    return spans


def enclosing(spans, pos):
    """The name of the innermost span around pos, or <init> for code outside any method."""
    best = None
    for b, e, name in spans:
        if b <= pos <= e and (best is None or b > best[0]):
            best = (b, e, name)
    return best[2] if best else '<init>'


def enclosing_start(spans, pos):
    """The offset where the innermost method around pos starts, or 0."""
    best = 0
    for b, e, _name in spans:
        if b <= pos <= e and b > best:
            best = b
    return best


def statement_span(s, pos):
    """(start, end) of the statement around pos: from the `;`, `{` or `}` before it to the one
    after it, skipping bracketed groups, so a condition and a whole boolean expression stay one."""
    depth, i = 0, pos
    while i > 0:
        i -= 1
        c = s[i]
        if c in ')]':
            depth += 1
        elif c in '([':
            depth -= 1
        elif c in ';{}' and depth <= 0:
            break
    start = i + 1 if i > 0 else 0
    # an arrow arm's label, or an `else` after a block, is no part of the statement
    head = re.match(r'\s*(?:(?:case\b(?:[^;{}]*?)|default\s*)->|else\b)\s*', s[start:pos])
    if head:
        start += head.end()
    depth, j = 0, pos
    while j < len(s):
        c = s[j]
        if c in '([':
            depth += 1
        elif c in ')]':
            depth -= 1
        elif c in ';{}' and depth <= 0:
            break
        j += 1
    return start, j


def squash(text):
    return re.sub(r'\s+', '', text)


def anchor_of(text):
    """A place's own code text as a key: whitespace removed; a long text keeps its head and a short
    hash of the whole, so it stays one readable cell."""
    t = squash(text).replace('\t', ' ')
    if len(t) <= ANCHOR_LIMIT:
        return t
    return t[:ANCHOR_LIMIT - 9] + '~' + hashlib.sha1(t.encode()).hexdigest()[:8]


def top_level_labels(body):
    """The case labels at a switch's own level, and whether it has a default arm there."""
    depth, has_default, labels = 0, False, []
    for m in re.finditer(r'[{}()]|\bdefault\b\s*(?:->|:)|\bcase\b((?:[^:>]|::)*?)(?:->|:(?!:))', body):
        t = m.group(0)
        if t in '{(':
            depth += 1
        elif t in '})':
            depth -= 1
        elif depth == 0:
            if t.startswith('default'):
                has_default = True
            else:
                label = m.group(1).strip()
                if re.search(r'\bdefault\b', label):
                    has_default = True
                labels.append(label)
    return labels, has_default


def is_switch_expression(s, pos):
    """Whether the switch at pos is an expression, which javac checks for exhaustiveness over an
    enum, rather than a statement, which it does not."""
    before = s[max(0, pos - 40):pos].rstrip()
    return before.endswith(('=', '(', ',', '->', '?', 'return', 'yield', '&&', '||', '!', '+')) and not before.endswith(('==', '!=', '<=', '>='))


# ------------------------------------------------------------------ the tree's vocabulary

@dataclass
class Vocabulary:
    """What the checkout declares: tags and their codes, the ColumnType constants that stand for a
    tag, the predicates and what each tests, the value enums, and the Rust and native tag names."""
    tags: dict = field(default_factory=dict)            # Java tag name -> code
    aliases: dict = field(default_factory=dict)         # ColumnType constant -> tag name
    tag_predicates: dict = field(default_factory=dict)  # ColumnType predicate -> tag names it tests
    value_predicates: dict = field(default_factory=dict)  # predicate -> values it tests
    values: dict = field(default_factory=dict)          # value enum -> its constants
    rust_tags: dict = field(default_factory=dict)       # Rust variant -> Java tag name
    native_tags: dict = field(default_factory=dict)     # native enum or qdb_col name -> Java tag name

    def by_code(self, code):
        return next((n for n, c in self.tags.items() if c == code), None)


def read(root, rel):
    return (Path(root) / rel).read_text(encoding='utf-8', errors='replace')


def vocabulary(root):
    v = Vocabulary()
    tag_text = read(root, f'{CAIRO}/ColumnTypeTag.java')
    for m in re.finditer(r'^\s+([A-Za-z][A-Za-z0-9_]*)\((-?\d+)\)[,;]', tag_text, re.M):
        if int(m.group(2)) >= 0:
            v.tags[m.group(1)] = int(m.group(2))
    ct = read(root, f'{CAIRO}/ColumnType.java')
    cts = strip_java(ct)
    for name in v.tags:
        v.aliases[name] = name
    # an int constant defined from one tag stands for that tag (TIMESTAMP_NANO, INTERVAL_RAW); the
    # bound MAX_TAG does not
    for m in re.finditer(r'public static final (?:int|short) ([A-Z][A-Z0-9_]*)\s*=\s*([^;]+);', cts):
        name, expr = m.group(1), m.group(2)
        if name in v.tags or name == 'MAX_TAG':
            continue
        named = [t for t in re.findall(r'\b([A-Z][A-Za-z0-9_]*)\b', expr) if t in v.tags]
        if len(named) == 1 and not re.search(r'\(', expr):
            v.aliases[name] = named[0]
    for enum, file in VALUE_ENUMS.items():
        v.values[enum] = enum_constants(read(root, f'{CAIRO}/{file}'), enum)
    classify_predicates(v, cts)
    rust = strip_rust(read(root, 'core/rust/qdb-core/src/col_type.rs'))
    body = rust[rust.index('pub enum ColumnTypeTag'):]
    body = body[:body.index('}')]
    for m in re.finditer(r'^\s+([A-Z][A-Za-z0-9]*)\s*=\s*(\d+)', body, re.M):
        name = v.by_code(int(m.group(2)))
        if name:
            v.rust_tags[m.group(1)] = name
    native = strip_java(read(root, f'{NATIVE_DIR}/share/column_type.h'))
    body = native[native.index('enum class ColumnType'):]
    body = body[:body.index('}')]
    for m in re.finditer(r'^\s+([A-Z][A-Z0-9_]*)\s*=\s*([^,\n]+)', body, re.M):
        code = re.fullmatch(r'\d+', m.group(2).strip())
        name = v.by_code(int(code.group(0))) if code else None
        if name is None:
            ref = [t for t in re.findall(r'\b([A-Z][A-Z0-9_]*)\b', m.group(2)) if t in v.native_tags]
            name = v.native_tags[ref[0]] if ref else None
        if name:
            v.native_tags[m.group(1)] = name
    return v


def enum_constants(text, name):
    """The constant names of `enum <name> {` in Java text, in declaration order."""
    s = strip_java(text)
    m = re.search(r'\benum\s+' + re.escape(name) + r'\b[^{]*\{', s)
    if not m:
        return []
    out, depth, start = [], 0, m.end()
    for i in range(m.end(), len(s)):
        c = s[i]
        if c in '({[':
            depth += 1
        elif c in ')}]':
            if depth == 0:
                item = s[start:i].strip()
                if item:
                    out.append(re.match(r'\w+', item).group(0))
                return out
            depth -= 1
        elif depth == 0 and c in ',;':
            item = s[start:i].strip()
            if item:
                out.append(re.match(r'\w+', item).group(0))
            start = i + 1
            if c == ';':
                return out
    return out


def classify_predicates(v, cts):
    """Sorts ColumnType's boolean predicates: one that compares a tag tests those tags; one that
    reads a relation kind or a type driver's table tests a value; one over two types is a relation,
    which the relation rules derive and which names no place."""
    bodies = {}
    for m in re.finditer(r'public static boolean (is[A-Z]\w*)\(([^)]*)\)\s*\{', cts):
        b = m.end() - 1
        e = match_close(cts, b, '{', '}')
        if m.group(2).count(',') >= 1:
            continue
        bodies.setdefault(m.group(1), []).append(cts[b + 1:e])
    resolved = {}

    def tags_of(name, seen=()):
        if name in resolved:
            return resolved[name]
        if name in FLAG_PREDICATES:
            resolved[name] = set(FLAG_PREDICATES[name])
            return resolved[name]
        out = set()
        for body in bodies.get(name, ()):
            out |= {v.aliases[t] for t in re.findall(r'\b([A-Z][A-Z0-9_]*)\b', body) if t in v.aliases}
            for lo, hi in re.findall(r'>=\s*([A-Z][A-Z0-9_]*)\s*&&\s*\w+\s*<=\s*([A-Z][A-Z0-9_]*)', body):
                if lo in v.aliases and hi in v.aliases:
                    a, z = v.tags[v.aliases[lo]], v.tags[v.aliases[hi]]
                    out |= {t for t, c in v.tags.items() if a <= c <= z}
            for call in re.findall(r'\b(is[A-Z]\w*)\(', body):
                if call != name and call in bodies and call not in seen:
                    out |= tags_of(call, seen + (name,))
        out.discard('MAX_TAG')
        resolved[name] = out
        return out

    for name, bs in bodies.items():
        body = ' '.join(bs)
        kinds = re.findall(r'RelationKind\.([A-Z]+)', body)
        if kinds:
            v.value_predicates[name] = {f'RelationKind.{k}' for k in kinds}
        elif re.search(r'Widths\.|nonPersistedTypes|arrayTypeSet|getTypeDriver|TypeDrivers', body):
            v.value_predicates[name] = {'facts'}
        else:
            tags = tags_of(name)
            if tags:
                v.tag_predicates[name] = tags


# ------------------------------------------------------------------ places

@dataclass
class Place:
    file: str
    line: int
    method: str
    form: str
    anchor: str
    tags: frozenset = frozenset()     # tags the place names
    values: frozenset = frozenset()   # driver values the place names, as Enum.CONSTANT
    ranges: tuple = ()                # (operator, tag code) of the range comparisons it makes
    is_checked: bool = False          # the compiler names it for a new tag or value
    is_guarded: bool = False          # the family-arm guard refuses an unlike type first
    fallback: str = ''                # a switch's fallback: 'default', 'none'
    label: str = ''                   # the guard label at the place
    condition: str = ''               # a range test's text, which `diverges` evaluates
    last_line: int = 0
    n: int = 1

    def key(self):
        return self.file, self.method, self.form, self.anchor, self.n

    def where(self):
        return f'{self.file}:{self.line}'


def scan(root=REPO):
    """Every place of the checkout, in file order, with its ordinal among equal keys."""
    root = Path(root)
    v = vocabulary(root)
    places = []
    for path in sorted((root / JAVA_MAIN).rglob('*.java')):
        raw = path.read_text(encoding='utf-8', errors='replace')
        if not re.search(r'ColumnType|ColumnTypeTag|Accessor|NullPolicy|WireKind|RelationKind|Arithmetic|Movement|'
                         r'CastTarget|' + '|'.join(FAMILY_CALLS), raw):
            continue
        src = Source(str(path.relative_to(root)), raw, strip_java(raw))
        places += scan_java(src, v)
    for crate in RUST_CRATES:
        if not (root / crate).is_dir():
            continue
        for path in sorted((root / crate).rglob('*.rs')):
            if '/tests/' in str(path) or path.name in ('tests.rs',) or path.name.endswith('_tests.rs'):
                continue
            raw = path.read_text(encoding='utf-8', errors='replace')
            if not re.search(r'ColumnTypeTag|ColumnArithmetic|ColumnNullPolicy|ColumnMovement', raw):
                continue
            places += scan_rust(Source(str(path.relative_to(root)), raw, strip_rust(raw)), v)
    for path in sorted((root / NATIVE_DIR).rglob('*')):
        if path.suffix not in ('.c', '.cc', '.cpp', '.h', '.hpp'):
            continue
        raw = path.read_text(encoding='utf-8', errors='replace')
        if not re.search(r'ColumnType::|qdb_col::', raw):
            continue
        places += scan_native(Source(str(path.relative_to(root)), raw, strip_java(raw)), v)
    counts = {}
    for p in places:
        k = (p.file, p.method, p.form, p.anchor)
        counts[k] = counts.get(k, 0) + 1
        p.n = counts[k]
    return v, places


def tag_names_in(text, v, is_bare):
    """The tags a text names: ColumnType.X or ColumnTypeTag.X, and bare names where the file
    imports ColumnType's members statically."""
    out = {v.aliases[m] for m in re.findall(r'\bColumnType(?:Tag)?\.([A-Za-z][A-Za-z0-9_]*)\b(?!\s*\()', text) if m in v.aliases}
    if is_bare:
        out |= {v.aliases[m] for m in re.findall(r'(?<![.\w])([A-Z][A-Z0-9_]*|IPv4)\b(?!\s*[.(])', text) if m in v.aliases}
    return out


SELECTOR_ENUMS = (
    (r'\bColumnTypeTag\.of\(|\.getTag\(\)', 'ColumnTypeTag'),
    (r'getNullPolicy\(|getColumnNullPolicy\(|NullPolicy\.of\(', 'NullPolicy'),
    (r'WireKind\.of\(|getWireKind\(', 'WireKind'),
    (r'\bfamilyArmOf\(|\baccessorOf\(|getAccessor\(\)', 'Accessor'),
    (r'\bfamilyArmOpcodeOf\(|\baccessorOpcodeOf\(|\bcolumnKind\(', 'family-opcode'),
    (r'getRelationKind\(|RelationRules\.kind\(|^kind\(', 'RelationKind'),
    (r'getArithmetic\(', 'Arithmetic'),
    (r'getMovement\(', 'Movement'),
    (r'\btagOf\(', 'tag'),
)


def selector_kind(sel, s, pos):
    """What a switch selects on, as far as the text tells: an enum, a tag, a family opcode, another
    opcode, or something else."""
    t = sel.strip()
    for pattern, kind in SELECTOR_ENUMS:
        if re.search(pattern, t):
            return kind
    if re.fullmatch(r'[A-Za-z_]\w*', t):
        decl = None
        for m in re.finditer(r'([\w.<>\[\]]+)\s+' + re.escape(t) + r'\s*[=;,)]', s[max(0, pos - 30_000):pos]):
            decl = m
        if decl:
            declared = decl.group(1).split('.')[-1]
            if declared in ('ColumnTypeTag', 'NullPolicy', 'WireKind', 'Accessor', 'RelationKind', 'Arithmetic',
                            'Movement', 'CastTarget'):
                return declared
            tail = s[max(0, pos - 30_000) + decl.end() - 1:max(0, pos - 30_000) + decl.end() + 200]
            init = re.match(r'\s*=\s*([^;]*)', tail)
            if init:
                for pattern, kind in SELECTOR_ENUMS:
                    if re.search(pattern, init.group(1)):
                        return kind
                if re.search(r'(?i)opcode', init.group(1)):
                    return 'opcode'
    if re.search(r'(?i)opcode', t) or re.fullmatch(r'ops?(?:\[[^\]]*\])?|\w*Op', t):
        return 'opcode'
    return 'other'


COMPARE = re.compile(r'(==|!=)\s*ColumnType(?:Tag)?\.([A-Za-z][A-Za-z0-9_]*)\b(?!\s*\()'
                     r'|\bColumnType(?:Tag)?\.([A-Za-z][A-Za-z0-9_]*)\b\s*(==|!=)')
COMPARE_BARE = re.compile(r'(==|!=)\s*([A-Z][A-Z0-9_]*|IPv4)\b(?!\s*[.(])|(?<![.\w])([A-Z][A-Z0-9_]*|IPv4)\b\s*(==|!=)')
RANGE = re.compile(r'(<=|>=|<(?![<=])|(?<![-=>])>(?![>=]))\s*ColumnType\.([A-Z][A-Z0-9_]*)\b(?!\s*\()'
                   r'|\bColumnType\.([A-Z][A-Z0-9_]*)\b\s*(<=|>=|<(?![<=])|>(?![>=]))')
PREDICATE_CALL = re.compile(r'(?<![\w])(?:ColumnType\.)?(is[A-Z]\w*)\(')
VALUE_COMPARE = re.compile(r'\b(Accessor|NullPolicy|WireKind|RelationKind|Arithmetic|Movement|CastTarget)\.([A-Z][A-Za-z0-9_]*)\b')
FAMILY_CALL = re.compile(r'\b(' + '|'.join(FAMILY_CALLS + ('noFamilyArm',)) + r')\(')
TABLE = re.compile(r'\b(\w+)\s*(?P<br>\[|\.getQuick\(|\.getQuiet\(|\.get\()\s*(?:ColumnType\.)?tagOf\(')
GUARD_LITERAL = re.compile(r'\b(?:familyArmOf|noFamilyArm)\(\s*[^;]*?,\s*"([^"]+)"\s*\)', re.S)


def scan_java(src, v):
    s, raw, rel = src.s, src.raw, src.rel
    in_column_type = rel.endswith('cairo/ColumnType.java')
    # ColumnType's own code, and a file that imports its members statically, name tags bare
    is_bare = in_column_type or re.search(r'import static io\.questdb\.cairo\.ColumnType\.\*;', raw) is not None
    spans = java_methods(s)
    places = []
    selectors = []
    # switches
    for m in re.finditer(r'\bswitch\s*\(', s):
        o = m.end() - 1
        c = match_close(s, o, '(', ')')
        b = s.find('{', c)
        e = match_close(s, b, '{', '}')
        if c < 0 or b < 0 or e < 0:
            continue
        selectors.append((o, c))
        sel = s[o + 1:c]
        body = s[b + 1:e]
        labels, has_default = top_level_labels(body)
        kind = selector_kind(sel, s, m.start())
        label_text = ' '.join(labels)
        tags = tag_names_in(label_text, v, is_bare)
        if kind in ('other', 'opcode') and not tags:
            continue
        method = enclosing(spans, m.start())
        common = dict(file=rel, line=src.line(m.start()), last_line=src.line(e), method=method,
                      anchor=anchor_of(f'switch({sel})'), fallback='default' if has_default else 'none')
        if kind == 'ColumnTypeTag':
            names = {v.aliases[n] for n in re.findall(r'\b([A-Za-z][A-Za-z0-9_]*)\b', label_text) if n in v.tags}
            places.append(Place(form='tag-enum-switch', tags=frozenset(names),
                                is_checked=not has_default and is_switch_expression(s, m.start()), **common))
        elif kind in v.values or kind == 'family-opcode':
            enum = 'Accessor' if kind == 'family-opcode' else kind
            names = {f'{enum}.{n}' for n in re.findall(r'\b([A-Z][A-Za-z0-9_]*)\b', label_text) if n in v.values[enum]}
            if kind == 'family-opcode':
                names = {f'Accessor.{n}' for n in tag_names_in(label_text, v, True) if n in v.values['Accessor']}
            guarded = (re.search(r'\b(' + '|'.join(GUARD_CALLS) + r')\(', sel) is not None
                       or 'isLikeFamilyNamesake(' in s[enclosing_start(spans, m.start()):m.start()])
            lit = GUARD_LITERAL.search(raw[o:c + 1])
            places.append(Place(form='value-switch', values=frozenset(names), is_guarded=guarded,
                                is_checked=kind != 'family-opcode' and not has_default and is_switch_expression(s, m.start()),
                                label=lit.group(1) if lit else '', **common))
        elif kind == 'opcode':
            # a per-row writer's switch over an opcode a setup switch chose: not a place
            continue
        elif tags or kind == 'tag':
            places.append(Place(form='tag-switch', tags=frozenset(tags), **common))
    # tests: comparisons with a tag or a value, and predicate calls, one place per statement
    statements = {}

    def note(pos, what, item):
        if any(o <= pos <= c for o, c in selectors):
            return
        st = statement_span(s, pos)
        statements.setdefault(st, []).append((what, item))

    for m in COMPARE.finditer(s):
        name = m.group(2) or m.group(3)
        if name in v.aliases:
            note(m.start(), 'tag', v.aliases[name])
    if is_bare or in_column_type:
        for m in COMPARE_BARE.finditer(s):
            name = m.group(2) or m.group(3)
            if name in v.aliases:
                note(m.start(), 'tag', v.aliases[name])
    for m in RANGE.finditer(s):
        name = m.group(2) or m.group(3)
        op = m.group(1) or m.group(4)
        if name in v.aliases:
            if m.group(3):
                op = {'<': '>', '>': '<', '<=': '>=', '>=': '<='}[op]
            note(m.start(), 'range', (op, v.tags[v.aliases[name]]))
    for m in PREDICATE_CALL.finditer(s):
        name = m.group(1)
        qualified = s[m.start():m.end()].startswith('ColumnType.')
        if not qualified and not (is_bare or in_column_type):
            continue
        if not qualified and m.start() > 0 and s[m.start() - 1] == '.':
            continue
        if re.search(r'(?:boolean|static)\s+$', s[max(0, m.start() - 30):m.start()]):
            continue
        if name in v.tag_predicates:
            for t in v.tag_predicates[name]:
                note(m.start(), 'tag', t)
        elif name in v.value_predicates:
            for x in v.value_predicates[name]:
                note(m.start(), 'value', x)
    for m in VALUE_COMPARE.finditer(s):
        enum, const = m.group(1), m.group(2)
        if const in v.values.get(enum, ()):
            # a comparison: `x == Accessor.INT`, `x != PhysicalDescriptor.Accessor.INT`, `Accessor.INT == x`
            if re.search(r'[!=]=\s*(?:\w+\.)*$', s[max(0, m.start() - 80):m.start()]) or re.match(r'\s*[!=]=', s[m.end():m.end() + 8]):
                note(m.start(), 'value', f'{enum}.{const}')
    for m in FAMILY_CALL.finditer(s):
        if rel.endswith(('cairo/PhysicalDescriptor.java', 'cutlass/line/LineUtils.java')):
            continue
        if re.search(r'(?:static|public|private|protected)\s+[\w.<>\[\]]+\s+$', s[max(0, m.start() - 60):m.start()]):
            continue
        note(m.start(), 'family', m.group(1))
    for (a, b), items in statements.items():
        text = s[a:b]
        method = enclosing(spans, a + len(text) - len(text.lstrip()))
        tags = frozenset(i for w, i in items if w == 'tag')
        ranges = tuple(sorted({i for w, i in items if w == 'range'}))
        values = {i for w, i in items if w == 'value'}
        families = {i for w, i in items if w == 'family'}
        # an opcode compared with a tag constant is a family value, not a tag
        if families and re.search(r'(?i)opcode|columnKind', text):
            values |= {f'Accessor.{t}' for t in tags if t in v.values['Accessor']}
            tags = frozenset()
        common = dict(file=rel, line=src.line(a + len(text) - len(text.lstrip())), last_line=src.line(b),
                      method=method, anchor=anchor_of(text))
        if tags or ranges:
            places.append(Place(form='tag-test', tags=tags, ranges=ranges, condition=text if ranges else '', **common))
        if values or families:
            lit = GUARD_LITERAL.search(raw[a:b + 1])
            places.append(Place(form='value-test', values=frozenset(values),
                                is_guarded=any(f in GUARD_CALLS + ('noFamilyArm',) for f in families),
                                label=lit.group(1) if lit else '', **common))
    # tables indexed by a tag, directly or through a variable that holds one
    tag_vars = set(re.findall(r'\b(?:short|int)\s+(\w+)\s*=\s*(?:ColumnType\.)?tagOf\(', s))
    patterns = [TABLE]
    if tag_vars:
        names = '|'.join(re.escape(x) for x in sorted(tag_vars))
        patterns.append(re.compile(r'\b(\w+)\s*(?P<br>\[|\.getQuick\(|\.getQuiet\(|\.get\()\s*(?:' + names + r')\s*[\])]'))
    seen = set()
    for pattern in patterns:
        for m in pattern.finditer(s):
            if m.group(1) in KEYWORDS or m.group(1) == 'ColumnType' or m.start() in seen:
                continue
            seen.add(m.start())
            opening = m.end('br') - 1
            closing = match_close(s, opening, s[opening], ']' if s[opening] == '[' else ')')
            places.append(Place(file=rel, line=src.line(m.start()), last_line=src.line(m.start()), method=enclosing(spans, m.start()),
                                form='tag-table', anchor=anchor_of(s[m.start():closing + 1])))
    # a local that holds a family answer for the switch below it is that switch's, not a place
    switched = {(p.method, p.anchor[len('switch('):-1]) for p in places if p.form == 'value-switch'}

    def is_switched_local(p):
        local = re.match(r'(?:final)?[\w.<>]+?(?:Accessor|int|short)(\w+)=', p.anchor)
        return local is not None and (p.method, local.group(1)) in switched

    places = [p for p in places if not (p.form == 'value-test' and not p.values and not p.is_guarded and is_switched_local(p))]
    places.sort(key=lambda p: (p.line, FORMS.index(p.form)))
    return places


def scan_rust(src, v):
    s, rel = src.s, src.rel
    tests = []
    for m in re.finditer(r'#\[cfg\(test\)\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{', s):
        tests.append((m.end() - 1, match_close(s, m.end() - 1, '{', '}')))
    spans = rust_functions(s)
    glob = re.search(r'\buse\s+[\w:]*ColumnTypeTag::\*', s) is not None
    places = []

    def in_tests(pos):
        return any(b <= pos <= e for b, e in tests)

    def names(text):
        tags = {v.rust_tags[n] for n in re.findall(r'\bColumnTypeTag::([A-Z][A-Za-z0-9]*)', text) if n in v.rust_tags}
        if glob:
            tags |= {v.rust_tags[n] for n in re.findall(r'(?<![:\w])([A-Z][A-Za-z0-9]*)\b(?!\s*[:(])', text) if n in v.rust_tags}
        values = set()
        for enum, java in RUST_VALUE_ENUMS.items():
            values |= {f'{java}.{n.upper()}' for n in re.findall(r'\b' + enum + r'::([A-Z][A-Za-z0-9]*)', text)}
        return tags, values

    matched = []
    for m in re.finditer(r'\bmatch\b([^{;]*)\{', s):
        if in_tests(m.start()):
            continue
        b = m.end() - 1
        e = match_close(s, b, '{', '}')
        if e < 0:
            continue
        patterns = rust_arm_patterns(s[b + 1:e])
        tags, values = names(' '.join(patterns))
        if not tags and not values:
            # a match from tag codes to tags is the decode table every new tag needs an arm in;
            # one from something else to tags (a precision to a decimal width) names those tags
            results, _ = names(s[b + 1:e])
            if len(results) >= 3 and all(re.fullmatch(r'\d+|_', p) for p in patterns):
                matched.append((m.start(), e))
                places.append(Place(file=rel, line=src.line(m.start()), last_line=src.line(e),
                                    method=enclosing(spans, m.start()), form='tag-table', anchor=anchor_of('match' + m.group(1)),
                                    tags=frozenset(results)))
                continue
            tags = results
            if not tags:
                continue
        catch_all = any(is_catch_all(p) for p in patterns)
        matched.append((m.start(), e))
        places.append(Place(file=rel, line=src.line(m.start()), last_line=src.line(e), method=enclosing(spans, m.start()),
                            form='rust-match', anchor=anchor_of('match' + m.group(1)), tags=frozenset(tags),
                            values=frozenset(values), is_checked=not catch_all, fallback='default' if catch_all else 'none'))
    statements = {}
    path = r'(?:Some\(\s*)?(?:\w+::)*'
    for m in re.finditer(r'(==|!=)\s*' + path + r'ColumnTypeTag::\w+|ColumnTypeTag::\w+\)?\s*(==|!=)|\bmatches!\s*\('
                         r'|\bif\s+let\s+' + path + r'ColumnTypeTag::'
                         r'|(==|!=)\s*' + path + r'(?:ColumnArithmetic|ColumnNullPolicy|ColumnMovement)::\w+', s):
        if in_tests(m.start()) or any(a <= m.start() <= b for a, b in matched):
            continue
        st = statement_span(s, m.start())
        if m.group(0).startswith('matches!'):
            close = match_close(s, s.index('(', m.start()), '(', ')')
            tags, values = names(s[m.start():close + 1])
            if not tags and not values:
                continue
        statements.setdefault(st, True)
    for a, b in statements:
        text = s[a:b]
        tags, values = names(text)
        if not tags and not values:
            continue
        start = a + len(text) - len(text.lstrip())
        places.append(Place(file=rel, line=src.line(start), last_line=src.line(b), method=enclosing(spans, start),
                            form='rust-test', anchor=anchor_of(text), tags=frozenset(tags), values=frozenset(values)))
    places.sort(key=lambda p: p.line)
    return places


def rust_arm_patterns(body):
    """The pattern of every arm of a match body: the text before each `=>` at the body's own
    level, back to the previous arm's end (a `,` outside brackets, or the end of a block)."""
    patterns, braces, parens, boundary, i = [], 0, 0, 0, 0
    while i < len(body):
        c = body[i]
        if c == '{':
            braces += 1
        elif c == '}':
            braces -= 1
            if braces == 0:
                boundary = i + 1
        elif braces == 0:
            if c in '([':
                parens += 1
            elif c in ')]':
                parens -= 1
            elif c == ',' and parens == 0:
                boundary = i + 1
            elif body.startswith('=>', i) and parens == 0:
                patterns.append(body[boundary:i].strip())
                boundary = i + 2
                i += 2
                continue
        i += 1
    return patterns


def is_catch_all(pattern):
    """Whether an arm's pattern takes every value: `_`, a binding, or a tuple of those, with no guard."""
    if re.search(r'\bif\b', pattern):
        return False
    one = r'(?:_|\.\.|[a-z_][a-z0-9_]*|ref\s+[a-z_]\w*|mut\s+[a-z_]\w*)'
    return re.fullmatch(r'\s*(?:' + one + r'|\(\s*' + one + r'(?:\s*,\s*' + one + r')*\s*,?\s*\))\s*', pattern) is not None


def native_switch_errors(s):
    """(start, end) offsets where `#pragma GCC diagnostic error "-Wswitch"` holds."""
    regions, start, depth = [], None, 0
    for m in re.finditer(r'#\s*pragma\s+GCC\s+diagnostic\s+(push|pop|error\s+"-Wswitch")', s):
        what = m.group(1)
        if what == 'push':
            depth += 1
        elif what == 'pop':
            if start is not None:
                regions.append((start, m.start()))
                start = None
            depth -= 1
        elif start is None:
            start = m.end()
    if start is not None:
        regions.append((start, len(s)))
    return regions


def scan_native(src, v):
    s, rel = src.s, src.rel
    spans = c_functions(s)
    # the pragma's warning name is a string, which the stripped text blanks
    errors = native_switch_errors(src.raw)
    qdb_col = {}
    ns = re.search(r'namespace\s+qdb_col\s*\{', s)
    if ns:
        body = s[ns.end():match_close(s, ns.end() - 1, '{', '}')]
        qdb_col = {n: v.by_code(int(c)) for n, c in re.findall(r'\b([A-Z][A-Z0-9_]*)\s*=\s*(\d+)\s*;', body)}

    def names(text):
        tags = {v.native_tags[n] for n in re.findall(r'\bColumnType::([A-Z][A-Z0-9_]*)', text) if n in v.native_tags}
        tags |= {qdb_col[n] for n in re.findall(r'\bqdb_col::([A-Z][A-Z0-9_]*)', text) if qdb_col.get(n)}
        return tags

    places = []
    switches = []
    for m in re.finditer(r'\bswitch\s*\(', s):
        o = m.end() - 1
        c = match_close(s, o, '(', ')')
        b = s.find('{', c)
        e = match_close(s, b, '{', '}')
        if c < 0 or b < 0 or e < 0:
            continue
        labels, has_default = top_level_labels(s[b + 1:e])
        tags = names(' '.join(labels))
        if not tags:
            continue
        switches.append((m.start(), e))
        is_enum = any('ColumnType::' in l for l in labels)
        checked = is_enum and not has_default and any(a <= m.start() <= z for a, z in errors)
        places.append(Place(file=rel, line=src.line(m.start()), last_line=src.line(e), method=enclosing(spans, m.start()),
                            form='c-switch', anchor=anchor_of(f'switch({s[o + 1:c]})'), tags=frozenset(tags),
                            is_checked=checked, fallback='default' if has_default else 'none'))
    statements = set()
    for m in re.finditer(r'(==|!=)\s*(?:ColumnType|qdb_col)::\w+|\b(?:ColumnType|qdb_col)::\w+\s*(==|!=)', s):
        statements.add(statement_span(s, m.start()))
    for a, b in sorted(statements):
        text = s[a:b]
        tags = names(text)
        if tags:
            start = a + len(text) - len(text.lstrip())
            places.append(Place(file=rel, line=src.line(start), last_line=src.line(b), method=enclosing(spans, start),
                                form='c-test', anchor=anchor_of(text), tags=frozenset(tags)))
    places.sort(key=lambda p: p.line)
    return places


# ------------------------------------------------------------------ decisions

@dataclass
class Decision:
    file: str
    method: str
    form: str
    anchor: str
    n: int
    decision: str
    reason: str
    type: str = ''

    def key(self):
        return self.file, self.method, self.form, self.anchor, self.n

    def line(self):
        return '\t'.join([self.file, self.method, self.form, self.anchor, str(self.n), self.decision, self.reason, self.type])


def load_decisions(path):
    """The rows of places.tsv; a missing file holds none."""
    path = Path(path)
    if not path.exists():
        return []
    lines = path.read_text(encoding='utf-8').splitlines()
    if not lines:
        return []
    header = lines[0].split('\t')
    if tuple(header) != COLUMNS:
        raise UsageError(f'{path}: the header must be {" ".join(COLUMNS)}')
    out = []
    for i, line in enumerate(lines[1:], 2):
        if not line.strip():
            continue
        cells = line.split('\t')
        if len(cells) != len(COLUMNS):
            raise UsageError(f'{path}:{i}: {len(cells)} cells, the header has {len(COLUMNS)}')
        rec = dict(zip(COLUMNS, cells))
        if rec['decision'] not in DECISIONS:
            raise UsageError(f'{path}:{i}: decision "{rec["decision"]}" is none of {", ".join(DECISIONS)}')
        if not rec['reason'].strip():
            raise UsageError(f'{path}:{i}: a decision needs its reason')
        out.append(Decision(rec['file'], rec['method'], rec['form'], rec['anchor'], int(rec['n']), rec['decision'],
                            rec['reason'], rec['type']))
    return out


def refusal_label(reason):
    """The guard label of a `refused` decision's reason, `<label>` or `<label>: <the text it raises>`."""
    return reason.split(': ', 1)[0]


def test_exists(root, ref):
    """Whether a test a decision names exists: `Class#method` under core/src/test, or a kit path
    `kit:<path>` the kit's sources name."""
    root = Path(root)
    if ref.startswith('kit:'):
        # the SQL kit names its paths without the area prefix: sql.fill_null is "fill_null" there
        path = ref[4:]
        names = (f'"{path}"', f'"{path.split(".", 1)[-1]}"')
        kit = root / JAVA_TEST / 'io/questdb/test/cairo/types'
        return any(n in p.read_text(encoding='utf-8', errors='replace') for p in kit.glob('TypeConformance*.java') for n in names)
    cls, _, method = ref.partition('#')
    files = list((root / JAVA_TEST).rglob(f'{cls}.java'))
    if not files:
        return False
    return not method or any(re.search(r'\bvoid\s+' + re.escape(method) + r'\s*\(', f.read_text(encoding='utf-8')) for f in files)


def check(root, places, decisions):
    """The problems of the stored decisions against the scan, one line each: a decision whose place
    is gone, a test that does not exist, a guard label that is not at its place, two decisions for
    one place and one type."""
    by_key = {p.key(): p for p in places}
    problems, seen = [], set()
    for d in decisions:
        p = by_key.get(d.key())
        where = f'{d.file} {d.method} {d.form} {d.anchor} #{d.n}'
        if (d.key(), d.type) in seen:
            problems.append(f'twice: {where}: a second decision for {d.type or "every type"}')
        seen.add((d.key(), d.type))
        if p is None:
            problems.append(f'gone: {where}: the place of this decision ({d.decision}: {d.reason}) is no longer in the code')
            continue
        if d.decision == 'test' and not test_exists(root, d.reason):
            problems.append(f'lost its test: {p.where()}: {d.reason} does not exist')
        if d.decision == 'refused':
            label = refusal_label(d.reason)
            # a guard that keeps an earlier error has no label in the code; its place must still be guarded
            if p.label != label and not (p.is_guarded and not p.label):
                problems.append(f'wrong label: {p.where()}: decided refused at "{label}", the place names "{p.label}"')
    return problems


# ------------------------------------------------------------------ views

def type_values(root, name):
    """The driver values of the type named `name`, from its type driver's facts or its answers."""
    root = Path(root)
    path = driver_file(root, name)
    if path is None:
        return set()
    s = strip_java((root / path).read_text(encoding='utf-8'))
    out = set()
    m = re.search(r'new TypeFacts\(', s)
    if m:
        args, depth, start = [], 0, m.end()
        for i in range(m.end(), len(s)):
            c = s[i]
            if c in '({[':
                depth += 1
            elif c in ')}]':
                if depth == 0:
                    args.append(s[start:i])
                    break
                depth -= 1
            elif c == ',' and depth == 0:
                args.append(s[start:i])
                start = i + 1
        for pos, enum in FACT_VALUES.items():
            if pos < len(args):
                out.add(f'{enum}.{args[pos].strip().rsplit(".", 1)[-1]}')
    else:
        for enum in VALUE_ENUMS:
            for c in re.findall(r'\b' + enum + r'\.([A-Z][A-Za-z0-9_]*)\b', s):
                out.add(f'{enum}.{c}')
    return out


def driver_file(root, name):
    """The repository path of the type driver of the tag `name` (INT: IntTypeDriver.java), or None."""
    want = name.replace('_', '').lower() + 'typedriver'
    return next((str(p.relative_to(root)) for p in (Path(root) / CAIRO).glob('*TypeDriver.java') if p.stem.lower() == want), None)


def diverges(place, v, code, new_code):
    """Whether a test with range comparisons treats the namesake's tag and a new tag, whose code is
    above every tag's, differently. A condition made of tag comparisons only is evaluated whole;
    any other is judged by its range comparisons one by one, which may list a place that agrees."""
    whole = evaluate(place.condition, v, code), evaluate(place.condition, v, new_code)
    if None not in whole:
        return whole[0] != whole[1]
    ops = {'<': lambda a, b: a < b, '<=': lambda a, b: a <= b, '>': lambda a, b: a > b, '>=': lambda a, b: a >= b}
    return any(ops[op](code, bound) != ops[op](new_code, bound) for op, bound in place.ranges)


def evaluate(condition, v, code):
    """The value of a condition built only of comparisons of one tag with tag constants, for the
    tag `code`; None when it holds anything else."""
    operand = r'(?:[\w.]+(?:\([^()]*\))?)'

    def compare(op, name):
        if name not in v.aliases:
            raise ValueError(name)
        return f' ({code} {op} {v.tags[v.aliases[name]]}) '

    try:
        expr = re.sub(r'^\s*(?:if|return|while)\b|^[^=]*\b(?:boolean|final)\s+\w+\s*=', '', condition)
        expr = re.sub(operand + r'\s*(==|!=|<=|>=|<|>)\s*ColumnType\.(\w+)\b', lambda m: compare(m.group(1), m.group(2)), expr)
        expr = re.sub(r'ColumnType\.(\w+)\s*(==|!=|<=|>=|<|>)\s*' + operand,
                      lambda m: compare({'<': '>', '>': '<', '<=': '>=', '>=': '<='}.get(m.group(2), m.group(2)), m.group(1)), expr)
    except ValueError:
        return None
    expr = expr.replace('&&', ' and ').replace('||', ' or ')
    expr = re.sub(r'!(?!=)', ' not ', expr)
    if not re.fullmatch(r'[\s()0-9<>=!]*(?:(?:and|or|not)[\s()0-9<>=!]*)*', expr):
        return None
    try:
        return bool(eval(expr, {'__builtins__': {}}))
    except Exception:
        return None


GROUPS = ('names', 'shares', 'table')


@dataclass
class Item:
    """A place of the view by namesake: its group, whether it is closed and why, and the decisions
    other types recorded at it."""
    place: Place
    group: str
    closed: str = ''
    precedents: list = field(default_factory=list)


@dataclass
class View:
    name: str
    values: set
    items: list
    checked: list

    def open(self, group=None):
        return [i for i in self.items if not i.closed and (group is None or i.group == group)]

    def closed(self, group=None):
        return [i for i in self.items if i.closed and (group is None or i.group == group)]


def like(v, places, decisions, name, values, root=REPO, own='', type_name=''):
    """The places a type declared like the tag `name` must look at: those that name `name` (its
    tag takes a path a new tag does not), those that switch on or test a value in `values` (the
    new type takes `name`'s arm), and the tables indexed by tag. A place is closed for the type
    `type_name` when it names `own`, the type's own tag, or when places.tsv decides it for every
    type or for `type_name`; another type's decision at it is a precedent."""
    if name not in v.tags:
        raise UsageError(f'{name} is no tag')
    code = v.tags[name]
    new_code = max(v.tags.values()) + 1
    by_key = {}
    for d in decisions:
        by_key.setdefault(d.key(), []).append(d)
    own_driver = driver_file(root, name)
    items, checked = [], []
    for p in places:
        if p.file == own_driver or (p.file.endswith('cairo/ColumnType.java') and
                                    (p.method in v.tag_predicates or p.method in v.value_predicates)):
            # the namesake's own type driver, which a new type replaces with its own, and the
            # definitions of the predicates, whose callers are the places
            continue
        if p.form == 'tag-table':
            group = 'table'
        elif name in p.tags or (p.ranges and diverges(p, v, code, new_code)):
            if p.is_checked:
                checked.append(p)
                continue
            group = 'names'
        elif p.values & values or (p.form == 'value-test' and not p.values and not p.is_guarded):
            # an exhaustive switch over a value names a new value, not a type that shares one
            group = 'shares'
        else:
            continue
        item = Item(p, group)
        for d in by_key.get(p.key(), ()):
            if d.type in ('', type_name):
                item.closed = f'{d.decision}: {d.reason}' + (f' ({d.type})' if d.type else '')
            else:
                item.precedents.append(d)
        if not item.closed and own and own in p.tags:
            item.closed = f'names {own}'
        items.append(item)
    # first the places nothing else checks: no guard, no compiler check for a new value
    items.sort(key=lambda i: (GROUPS.index(i.group), i.place.is_guarded, i.place.is_checked))
    return View(name, values, items, checked)


GROUP_COUNTS = {'names': 'names {n}', 'shares': 'shares a value with {n}', 'table': 'tables'}
GROUP_TEXT = {
    'names': ('Places that name {n}', '{n}\'s tag takes a path here that a new tag does not take: decide whether the new type takes it.'),
    'shares': ('Places that switch on a value shared with {n}', 'The new type takes {n}\'s arm here: decide whether that arm is right for it '
                                                              '(its NULL, order and range). A guarded place refuses a type unlike its family first.'),
    'table': ('Tables indexed by a tag', 'Every new tag needs its entry, or a reason it needs none.'),
}


def describe(item):
    """One line of text about an item's place: method, form, code, and what else checks it."""
    p = item.place
    extra = []
    if p.is_guarded:
        extra.append(f'guarded{" at " + p.label if p.label else ""}')
    if p.is_checked and p.values:
        extra.append('exhaustive: the compiler names a new value, not a shared one')
    if p.fallback:
        extra.append('default arm' if p.fallback == 'default' else 'no default arm')
    if item.closed:
        extra.append(f'closed: {item.closed}')
    for d in item.precedents:
        extra.append(f'precedent: {d.type} {d.decision}: {d.reason}')
    return f'{p.method}, {p.form}: `{p.anchor}`' + (f' ({"; ".join(extra)})' if extra else '')


def render_like(view, decisions_path, is_all=False):
    n = view.name
    counts = '; '.join(f'{GROUP_COUNTS[g].format(n=n)}: {len(view.open(g))} open, {len(view.closed(g))} closed' for g in GROUPS)
    out = [f'# Places a type declared like {n} must look at', '',
           f'- Values it shares with {n}: {", ".join(sorted(view.values)) or "none known"}',
           f'- Decisions read from `{decisions_path}`',
           f'- {counts}; listed by the compiler, not repeated here: {len(view.checked)}']
    for g in GROUPS:
        items = view.open(g) + (view.closed(g) if is_all else [])
        title, text = GROUP_TEXT[g]
        out += ['', f'## {title.format(n=n)} ({len(items)})', '', text.format(n=n), '']
        out += [f'- [{"x" if i.closed else " "}] `{i.place.where()}` {describe(i)}' for i in items]
    return '\n'.join(out) + '\n'


def render_rows(view, type_name):
    """places.tsv rows for the view's open places, the decision and reason left for the author."""
    out = ['\t'.join(COLUMNS)]
    for i in view.open():
        p = i.place
        out.append('\t'.join([p.file, p.method, p.form, p.anchor, str(p.n), '', '', type_name]))
    return '\n'.join(out) + '\n'


def render_places(places, decisions):
    by_key = {}
    for d in decisions:
        by_key.setdefault(d.key(), []).append(f'{d.decision}{" (" + d.type + ")" if d.type else ""}: {d.reason}')
    out = ['file\tline\tmethod\tform\tchecked\tguarded\tfallback\ttags\tvalues\tanchor\tn\tdecisions']
    for p in places:
        out.append('\t'.join([p.file, str(p.line), p.method, p.form, 'yes' if p.is_checked else '', 'yes' if p.is_guarded else '',
                              p.fallback, ' '.join(sorted(p.tags)), ' '.join(sorted(p.values)), p.anchor, str(p.n),
                              ' / '.join(by_key.get(p.key(), ()))]))
    return '\n'.join(out) + '\n'


def render_summary(places, decisions, problems):
    out = ['| form | places | the compiler names it | guarded |', '|---|---|---|---|']
    for form in FORMS:
        ps = [p for p in places if p.form == form]
        out.append(f'| {form} | {len(ps)} | {sum(p.is_checked for p in ps)} | {sum(p.is_guarded for p in ps)} |')
    out.append(f'| all | {len(places)} | {sum(p.is_checked for p in places)} | {sum(p.is_guarded for p in places)} |')
    out += ['', f'Decisions: {len(decisions)}; problems: {len(problems)}'] + [f'- {p}' for p in problems]
    return '\n'.join(out) + '\n'


# ------------------------------------------------------------------ command line

def main(argv=None):
    ap = argparse.ArgumentParser(prog='audit.py', description='Lists every place that decides by a column type.')
    ap.add_argument('--repo', default=str(REPO))
    sub = ap.add_subparsers(dest='command', required=True)
    sub.add_parser('summary')
    p_like = sub.add_parser('like')
    p_like.add_argument('name')
    p_like.add_argument('--type', default='', help='the type being added: its decisions and its own tag close places')
    p_like.add_argument('--all', action='store_true', help='list closed places too')
    p_like.add_argument('--rows', action='store_true', help='print places.tsv rows for the open places instead')
    p_like.add_argument('--out')
    p_places = sub.add_parser('places')
    p_places.add_argument('--out')
    sub.add_parser('check')
    try:
        args = ap.parse_args(argv)
    except SystemExit as e:
        return 2 if e.code else 0
    root = Path(args.repo)
    try:
        v, places = scan(root)
        decisions = load_decisions(root / PLACES_FILE)
        problems = check(root, places, decisions)
        if args.command == 'summary':
            sys.stdout.write(render_summary(places, decisions, problems))
            return 0
        if args.command == 'check':
            for p in problems:
                print(p)
            return 1 if problems else 0
        if args.command == 'like':
            own = args.type if args.type in v.tags else ''
            view = like(v, places, decisions, args.name, type_values(root, args.name), root, own, args.type)
            text = render_rows(view, args.type) if args.rows else render_like(view, PLACES_FILE, args.all)
        else:
            text = render_places(places, decisions)
        if args.out:
            Path(args.out).write_text(text, encoding='utf-8')
            print(f'{args.out}: written')
        else:
            sys.stdout.write(text)
        return 0
    except UsageError as e:
        print(f'audit: {e}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
