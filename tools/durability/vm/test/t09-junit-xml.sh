#!/usr/bin/env bash
# t09 — the JUnit report is well-formed, counts honestly, and survives the engine's own output.
#
# NO VM: drives lib/junit.sh directly with synthetic verdicts. Seconds.
#
# The interesting cases are not "does it write a file". They are:
#   * NO_COMMIT must be <skipped>, never a pass -- otherwise a run where most boundaries
#     measured nothing reports as a wall of green, which is the vacuity this harness rejects.
#   * the failure body carries raw engine output, which contains <, &, quotes and CONTROL
#     CHARACTERS. Control characters are illegal in XML 1.0 at any escaping, so a naive
#     escaper produces a file that a dashboard rejects -- or worse, silently truncates.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/verdict.sh
source "$HERE/../lib/verdict.sh"
# shellcheck source=../lib/junit.sh
source "$HERE/../lib/junit.sh"
fails=0

ok()    { printf '  ok   %s\n' "$1"; }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
XML="$TMP/junit.xml"

echo "t09 — junit xml"

junit_begin "$XML" "durability.product.adaptive.W50000.bitmap"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-406"  RPO_OK    2 "DETAIL fine
RPO_OK F=345 >= Wm=177"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-1321" DURABLE   1 "DURABLE count=1"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-2236" NO_COMMIT 1 "NO_COMMIT cut landed early"
# A failure body shaped like the real thing: engine log lines, XML metacharacters, and a
# control character of the sort QuestDB's own logger emits.
junit_case "durability.product.adaptive.W50000.bitmap" "flush-3151" SILENT_CORRUPTION 3 "DETAIL i.q.c.TableWriter o3 <commit> \"quoted\" & 'single'
DETAIL control-char:$(printf '\001')here
SILENT_CORRUPTION row=43264 expected_v=1 actual_v=2 a<b && c>d"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-4066" LOUD_FAILURE 1 "LOUD_FAILURE: verifier produced no verdict (exit=134 killed-by-signal-6)"
junit_finish

# ---- 1. well-formed XML, by a real parser, not a regex ----------------------------------
if python3 -c "import xml.etree.ElementTree as E; E.parse('$XML')" 2>/dev/null; then
    ok "parses as XML (python xml.etree)"
else
    bad "XML IS MALFORMED — a dashboard would reject the whole report"
    python3 -c "import xml.etree.ElementTree as E; E.parse('$XML')" 2>&1 | head -3 | sed 's/^/       /'
fi

# ---- 2. counts are honest ---------------------------------------------------------------
read -r tests failures skipped < <(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
s = E.parse(sys.argv[1]).getroot().find('testsuite')
print(s.get('tests'), s.get('failures'), s.get('skipped'))
PY
)
check "tests counted"    "$tests"    "5"
check "failures counted" "$failures" "2"
check "skipped counted"  "$skipped"  "1"

# ---- 3. NO_COMMIT is skipped, NOT a pass and NOT a failure -------------------------------
kind=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'flush-2236':
        print('skipped' if c.find('skipped') is not None else
              'failure' if c.find('failure') is not None else 'pass')
PY
)
check "NO_COMMIT -> skipped" "$kind" "skipped"

# ---- 4. a passing case has no failure element -------------------------------------------
kind=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'flush-406':
        print('failure' if c.find('failure') is not None else 'pass')
PY
)
check "RPO_OK -> pass" "$kind" "pass"

# ---- 5. the failure MESSAGE is the verdict line, and the BODY keeps the evidence ---------
# The dashboard shows the message; the evidence is what makes it diagnosable. Losing either
# one turns the report back into "something went red".
read -r msg_ok body_ok < <(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'flush-3151':
        f = c.find('failure')
        msg = f.get('message') or ''
        body = f.text or ''
        print('yes' if msg.startswith('SILENT_CORRUPTION row=43264') else 'no:' + msg[:40],
              'yes' if 'i.q.c.TableWriter' in body and 'a<b && c>d' in body else 'no')
PY
)
check "failure message is the verdict line" "$msg_ok" "yes"
check "failure body keeps the DETAIL evidence, unescaped after parsing" "$body_ok" "yes"

# ---- 6. the failure type names the verdict token -----------------------------------------
t=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'flush-4066':
        print(c.find('failure').get('type'))
PY
)
check "failure type is the verdict token" "$t" "LOUD_FAILURE"

# ---- 7. no temp files left, and the report is renamed into place ------------------------
# issues/19's rule: a consumer must never see a half-written file.
leftovers=$(find "$TMP" -name 'junit.xml.tmp.*' -o -name 'junit.xml.part.*' | wc -l)
check "no temp files left behind" "$leftovers" "0"

echo
if [ "$fails" -eq 0 ]; then
    echo "t09 PASSED"
    exit 0
fi
echo "t09 FAILED: $fails assertion(s)"
exit 1
