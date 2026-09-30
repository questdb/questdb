#!/usr/bin/env bash
#
# Fails when a diff adds per-type dispatch that a new column type would pass through silently.
#
#   ci/check-type-dispatch.sh <base> [<head>]      (run from the repository root; <head> defaults to HEAD)
#
# Only the lines the diff adds are checked, in core/src/main/java, outside the type definitions
# (the *TypeDriver.java classes, which compare with their own tag by design).
#
# Rule D: an added `default` arm whose innermost enclosing block is `switch (ColumnTypeTag...)`.
#         A catch-all arm takes every tag added later without a decision. List the tags
#         instead: javac then reports the switch when a tag is added.
# Rule E: an added `== ColumnType.X` outside comments and literals, where X is not a pseudo tag.
#         Such a comparison decides for one type in code that every type passes through. Ask
#         the type's definition instead (ColumnType.getTypeDriver(...)). Comparisons with the
#         pseudo tags, which no value is stored as, are allowed.
#
# A line that must keep such a comparison or arm carries the marker `ratchet-ok: <reason>`,
# on the line itself or in a comment on the line above. The check reports it as silenced.
#
# Exit: 0 when no hit is left unsilenced, 1 otherwise, 2 on a usage error.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: $0 <base> [<head>]" >&2
    exit 2
fi
base=$1
head=${2:-HEAD}

read -r -d '' checker <<'PY' || true
import re
import subprocess
import sys

head = sys.argv[1]
MARKER = 'ratchet-ok:'
PSEUDO_TAGS = {
    'UNDEFINED', 'CURSOR', 'VAR_ARG', 'RECORD', 'GEOHASH', 'DECIMAL', 'REGCLASS', 'REGPROCEDURE',
    'ARRAY_STRING', 'PARAMETER', 'NULL',
}
RULE_E = re.compile(r'==\s*ColumnType\.([A-Z][A-Za-z0-9_]*)')
DEFAULT = re.compile(r'\bdefault\s*(->|:)')
SWITCH_TAG = re.compile(r'switch\s*\(\s*ColumnTypeTag\b')


def strip_code(text):
    """Blanks out comments and string, text-block and char literals, keeping offsets and newlines."""
    out = list(text)
    i, n = 0, len(text)

    def blank(a, b):
        for k in range(a, b):
            if out[k] != '\n':
                out[k] = ' '

    while i < n:
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
            blank(i, j)
            i = j
        elif text[i] in '"\'':
            q, j = text[i], i + 1
            while j < n and text[j] != q and text[j] != '\n':
                j += 2 if text[j] == '\\' else 1
            blank(i, min(j + 1, n))
            i = j + 1
        else:
            i += 1
    return ''.join(out)


cache = {}


def source(path):
    if path not in cache:
        raw = subprocess.run(['git', 'show', f'{head}:{path}'], capture_output=True, text=True, check=True).stdout
        code = strip_code(raw)
        starts = [0]
        for m in re.finditer('\n', code):
            starts.append(m.end())
        cache[path] = (raw.split('\n'), code, starts)
    return cache[path]


def enclosing_header(code, pos):
    """The header before the `{` of the innermost block around pos."""
    depth = 0
    i = pos - 1
    while i >= 0:
        c = code[i]
        if c == '}':
            depth += 1
        elif c == '{':
            if depth == 0:
                j = i - 1
                while j >= 0 and code[j] not in ';{}':
                    j -= 1
                return code[j + 1:i]
            depth -= 1
        i -= 1
    return ''


def is_silenced(raw_lines, idx):
    if MARKER in raw_lines[idx]:
        return True
    above = raw_lines[idx - 1].strip() if idx > 0 else ''
    return above.startswith('//') and MARKER in above


hits = []
path = None
line_no = 0
for line in sys.stdin:
    line = line.rstrip('\n')
    if line.startswith('+++ '):
        path = None if line == '+++ /dev/null' else line[6:]
        if path is not None and path.endswith('TypeDriver.java'):
            path = None
        continue
    m = re.match(r'@@ -\S+ \+(\d+)(?:,(\d+))? @@', line)
    if m:
        line_no = int(m.group(1))
        continue
    if path is None or not line.startswith('+') or line.startswith('+++'):
        continue
    raw_lines, code, starts = source(path)
    idx = line_no - 1
    line_no += 1
    if idx >= len(starts):
        continue
    code_line = code[starts[idx]:starts[idx + 1] if idx + 1 < len(starts) else len(code)]
    text = raw_lines[idx].strip() if idx < len(raw_lines) else ''
    silenced = is_silenced(raw_lines, idx)
    for dm in DEFAULT.finditer(code_line):
        if SWITCH_TAG.search(enclosing_header(code, starts[idx] + dm.start())):
            hits.append(('D', path, idx + 1, silenced, text))
    if any(em.group(1) not in PSEUDO_TAGS for em in RULE_E.finditer(code_line)):
        hits.append(('E', path, idx + 1, silenced, text))

failed = 0
for rule, p, n, silenced, text in hits:
    print(f"{'silenced' if silenced else 'HIT'} rule {rule}: {p}:{n}: {text}")
    failed += 0 if silenced else 1
print(f'check-type-dispatch: {failed} hit(s), {len(hits) - failed} silenced')
if failed:
    print('Rule D: list the tags instead of a default arm. Rule E: ask the type definition instead of comparing with a tag.'
          ' Where the line must stay, mark it with "ratchet-ok: <reason>" (see ci/check-type-dispatch.sh).')
sys.exit(1 if failed else 0)
PY

git diff --no-color --no-ext-diff -U0 "$base" "$head" -- 'core/src/main/java/*.java' | python3 -c "$checker" "$head"
