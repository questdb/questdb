#!/usr/bin/env bash
# t09 — the JUnit report is well-formed, counts honestly, and survives the engine's own output.
# Drives lib/junit.sh directly with synthetic verdicts; needs no VM.
#
# What the assertions protect:
#   * NO_COMMIT renders as <skipped>, never a pass, so a run that measured nothing cannot
#     report as a wall of green.
#   * failure bodies carry raw engine output: <, &, quotes, and control characters that XML 1.0
#     forbids at any escaping, so a naive escaper yields a file the dashboard rejects.
#   * an instrument fault renders as <error> and a product fault as <failure>; the two are
#     different alarms and decide who gets paged.
#   * the report names the build it belongs to, and says when a product run is degraded.
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
junit_property arm product
junit_property mode adaptive
junit_property product_dist "questdb-10.0.2-SNAPSHOT-no-jre-bin.tar.gz"
junit_property harness_commit "d2c1bb9a"
junit_property degraded false
junit_case "durability.product.adaptive.W50000.bitmap" "flush-406"  RPO_OK    2 "DETAIL fine
RPO_OK F=345 >= Wm=177"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-1321" DURABLE   1 "DURABLE count=1"
junit_case "durability.product.adaptive.W50000.bitmap" "flush-2236" NO_COMMIT 1 "NO_COMMIT cut landed early"
# A failure body shaped like the real thing: engine log lines, XML metacharacters, and a
# control character of the sort QuestDB's own logger emits.
junit_case "durability.product.adaptive.W50000.bitmap" "flush-3151" SILENT_CORRUPTION 3 "DETAIL i.q.c.TableWriter o3 <commit> \"quoted\" & 'single'
DETAIL control-char:$(printf '\001')here
SILENT_CORRUPTION row=43264 expected_v=1 actual_v=2 a<b && c>d"
# Fixtures are copied from the producer. Invented wording would let the test drift into
# asserting against a line nothing emits.
junit_case "durability.product.adaptive.W50000.bitmap" "flush-4066" LOUD_FAILURE 1 "LOUD_FAILURE: the shipped server did not start on the crashed database"
junit_finish

# ---- 1. well-formed XML, by a real parser rather than a regex ---------------------------
if python3 -c "import xml.etree.ElementTree as E; E.parse('$XML')" 2>/dev/null; then
    ok "parses as XML (python xml.etree)"
else
    bad "XML IS MALFORMED — a dashboard would reject the whole report"
    python3 -c "import xml.etree.ElementTree as E; E.parse('$XML')" 2>&1 | head -3 | sed 's/^/       /'
fi

# ---- 2. counts are honest ---------------------------------------------------------------
read -r tests failures skipped < <(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
# The root is <testsuite>; accept a <testsuites> wrapper too so this helper survives a change
# of shape.
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(s.get('tests'), s.get('failures'), s.get('skipped'))
PY
)
check "tests counted"    "$tests"    "5"
check "failures counted" "$failures" "2"
check "skipped counted"  "$skipped"  "1"

# ---- 2b. the attributes the XSD marks required -------------------------------------------
# PublishTestResults@2 names the windyroad JUnit.xsd as its format. It requires timestamp= and
# hostname= on a suite, and package= and id= on every child of a <testsuites> wrapper, which is
# why the root here is a bare <testsuite>. A report the publisher rejects goes green and shows
# nothing rather than failing the job.
read -r root ts host < <(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(r.tag, s.get('timestamp'), s.get('hostname'))
PY
)
check "root element is <testsuite>, so package=/id= are not required" "$root" "testsuite"
if printf '%s' "$ts" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$'; then
    ok "timestamp= matches the XSD pattern (ISO8601, NO timezone)"
else
    bad "timestamp='$ts' does not match the XSD pattern; a trailing Z is the usual cause"
fi
if [ -n "$host" ] && [ "$host" != None ]; then
    ok "hostname= present (names the agent that produced the run)"
else
    bad "hostname= is REQUIRED by the schema and is missing"
fi

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

# ---- 5. the failure message is the verdict line, and the body keeps the evidence ---------
# The dashboard shows the message; the body is what makes it diagnosable.
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
# A consumer must never see a half-written report.
leftovers=$(find "$TMP" -name 'junit.xml.tmp.*' -o -name 'junit.xml.part.*' -o -name 'junit.xml.props.*' | wc -l)
check "no temp files left behind" "$leftovers" "0"

# ---- 8. the run's identity is in the report ----------------------------------------------
# A dashboard can trend a number, but without these properties it cannot say which build
# produced it. The identity has to reach a green run too, not only a failure body.
props=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
s = r if r.tag == 'testsuite' else r.find('testsuite')
p = s.find('properties')
print('MISSING' if p is None else ','.join(
    '%s=%s' % (q.get('name'), q.get('value')) for q in p.iter('property')))
PY
)
case "$props" in
    *arm=product*) ok "properties carry the arm" ;;
    *) bad "properties do not carry the arm (got '$props')" ;;
esac
case "$props" in
    *product_dist=questdb-10.0.2-SNAPSHOT-no-jre-bin.tar.gz*)
        ok "properties name the ARTIFACT under test" ;;
    *) bad "properties do not name the artifact (got '$props')" ;;
esac
case "$props" in
    *harness_commit=d2c1bb9a*) ok "properties name the harness commit" ;;
    *) bad "properties do not name the harness commit (got '$props')" ;;
esac

# ---- 8b. the child sequence the XSD declares ---------------------------------------------
# The windyroad JUnit.xsd models testsuite as the sequence properties, testcase*, system-out,
# system-err, and none of the four carries minOccurs="0". All four must be present and in that
# order. Checking only the first child would leave a missing or misplaced tail invisible.
shape_of() {  # XML -> collapsed child-tag sequence, repeats marked with *
    python3 - "$1" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
s = r if r.tag == 'testsuite' else r.find('testsuite')
out = []
for c in s:
    if out and out[-1].rstrip('*') == c.tag:
        out[-1] = c.tag + '*'
    else:
        out.append(c.tag)
print(','.join(out))
PY
}
check "child sequence is the XSD's (properties, testcase*, system-out, system-err)" \
    "$(shape_of "$XML")" "properties,testcase*,system-out,system-err"

# A suite with no properties must still emit all four elements: emitting them only when there
# is content to put in them yields a report the publisher rejects while the job goes green.
XML_NP="$TMP/junit-noprops.xml"
junit_begin "$XML_NP" "durability.noprops"
junit_case c "flush-1" DURABLE 1 "DURABLE count=1"
junit_finish
check "no properties set: all four elements are still emitted" \
    "$(shape_of "$XML_NP")" "properties,testcase,system-out,system-err"

# ---- 8c. a real validator, when the host has one -----------------------------------------
# Runs the schema itself when xmllint is on PATH and a schema is available, from $QDB_JUNIT_XSD
# or junit.xsd beside this test. The schema is not vendored: it is third-party licensed and this
# is a public repo. A skipped check announces itself rather than passing quietly.
XSD="${QDB_JUNIT_XSD:-$HERE/junit.xsd}"
if ! command -v xmllint >/dev/null 2>&1; then
    echo "  SKIP schema validation: xmllint not on PATH (the structural check above still ran)"
elif [ ! -f "$XSD" ]; then
    echo "  SKIP schema validation: no schema at $XSD (set QDB_JUNIT_XSD=/path/to/JUnit.xsd)"
else
    for f in "$XML" "$XML_NP"; do
        if xmllint --noout --schema "$XSD" "$f" >/dev/null 2>"$TMP/xmllint.err"; then
            ok "xmllint --schema validates $(basename "$f")"
        else
            bad "xmllint --schema REJECTS $(basename "$f") — the publisher would too"
            head -5 "$TMP/xmllint.err" | sed 's/^/       /'
        fi
    done
fi

# ---- 9. instrument faults are <error>, product faults are <failure> ----------------------
# This routing decides whether the product owner or the rig owner gets paged. The token list
# lives in verdict_is_instrument_fault; this asserts junit.sh honours it.
XML2="$TMP/junit2.xml"
junit_begin "$XML2" "durability.reference.SYNC.W0.bitmap"
junit_property arm product
junit_property degraded true
junit_property degraded_reason "no LOCAL durable-ack tier at mode=SYNC; the arm runs the plain-qwp contract"
junit_case c "flush-10" DURABILITY_FAILURE 1 "DURABILITY_FAILURE acked txn 44 lost"
junit_case c "flush-11" SILENT_CORRUPTION  1 "SILENT_CORRUPTION row=9 expected_v=1 actual_v=2"
# MOUNT_FAILED is a product finding: an ext4 that will not mount after a power cut is the
# damage this instrument exists to catch.
junit_case c "flush-12" MOUNT_FAILED       1 "MOUNT_FAILED"
# LOUD_FAILURE is a product finding too: it means the product refused, loudly. The cases where
# nothing was measured carry NOT_EVALUATED instead.
junit_case c "flush-13" LOUD_FAILURE       1 "LOUD_FAILURE: shipped artifacts were not durable"
junit_case c "flush-14" UNPARSEABLE        1 "DETAIL something
what even is this"
junit_case c "flush-17" DURABLE            1 "DURABLE count=7"
# The oracle never reached a verdict, so nothing was measured and the rig owner is the one to
# page. Rendering this as <failure> would raise a data-loss alarm for a JVM the agent killed.
junit_case c "flush-18" NOT_EVALUATED     1 "NOT_EVALUATED: verifier produced no verdict (exit=134 killed-by-signal-6)"
junit_finish

read -r t2 f2 e2 s2 < <(python3 - "$XML2" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(s.get('tests'), s.get('failures'), s.get('errors'), s.get('skipped'))
PY
)
check "mixed suite: tests counted"                    "$t2" "7"
check "mixed suite: product faults -> failures=4"     "$f2" "4"
check "mixed suite: instrument faults -> errors=2"    "$e2" "2"
check "mixed suite: a pass and no skips are not counted as either" "$s2" "0"

element_of() {  # XML NAME -> error|failure|skipped|pass
    python3 - "$1" "$2" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == sys.argv[2]:
        print('error'   if c.find('error')   is not None else
              'failure' if c.find('failure') is not None else
              'skipped' if c.find('skipped') is not None else 'pass')
PY
}
check "DURABILITY_FAILURE -> <failure> (product)"  "$(element_of "$XML2" flush-10)" "failure"
check "SILENT_CORRUPTION  -> <failure> (product)"  "$(element_of "$XML2" flush-11)" "failure"
check "MOUNT_FAILED       -> <failure> (product)"  "$(element_of "$XML2" flush-12)" "failure"
check "LOUD_FAILURE       -> <failure> (louder alarm)" "$(element_of "$XML2" flush-13)" "failure"
check "UNPARSEABLE        -> <error> (instrument)" "$(element_of "$XML2" flush-14)" "error"
check "DURABLE            -> pass"                 "$(element_of "$XML2" flush-17)" "pass"
check "NOT_EVALUATED      -> <error> (instrument)" "$(element_of "$XML2" flush-18)" "error"
# Two boundaries, both red, different alarms. If these ever collapse to the same element the
# routing has been undone.
check "NOT_EVALUATED and LOUD_FAILURE are different alarms" \
    "$(element_of "$XML2" flush-18)/$(element_of "$XML2" flush-13)" "error/failure"

# the error element still names the token and carries the evidence
read -r etype emsg < <(python3 - "$XML2" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'flush-14':
        e = c.find('error')
        print(e.get('type'), 'yes' if (e.get('message') or '') == 'what even is this' else 'no')
PY
)
check "error type names the verdict token" "$etype" "UNPARSEABLE"
check "error message is the verdict line, DETAIL filtered" "$emsg" "yes"

# the degrade is visible in the machine-readable report
degraded=$(python3 - "$XML2" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
s = r if r.tag == 'testsuite' else r.find('testsuite')
p = s.find('properties')
print('MISSING' if p is None else next(
    (q.get('value') for q in p.iter('property') if q.get('name') == 'degraded'), 'MISSING'))
PY
)
check "a DEGRADED run says so in the XML" "$degraded" "true"

# ---- 10. verdict_line reuse: an all-DETAIL body must not yield message="" ----------------
# junit.sh routes the message through verdict_line rather than a local copy. A body that is
# entirely DETAIL lines, or one ending in a blank line, must still name a reason, or the
# dashboard shows a red boundary with nothing on it.
XML3="$TMP/junit3.xml"
junit_begin "$XML3" "s"
junit_case c "all-detail"  UNPARSEABLE 1 "DETAIL only evidence here
DETAIL and more"
# The fixture carries a real blank line after the verdict, not just a trailing newline:
# verify.sh's output reaches the caller through ssh and command substitution, and only an empty
# last line exercises the blank-line filter. Without it the assertion passes either way.
junit_case c "trailing-nl" SILENT_CORRUPTION 1 "DETAIL noise
SILENT_CORRUPTION row=1

"
junit_finish
msg_all=$(python3 - "$XML3" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'all-detail':
        print(repr(c.find('error').get('message')))
PY
)
if [ "$msg_all" = "''" ]; then
    bad "all-DETAIL body yields message=\"\" — a red boundary with no reason on it"
else
    ok "all-DETAIL body still carries a message ($msg_all)"
fi
msg_tr=$(python3 - "$XML3" <<'PY'
import sys, xml.etree.ElementTree as E
for c in E.parse(sys.argv[1]).getroot().iter('testcase'):
    if c.get('name') == 'trailing-nl':
        print(c.find('failure').get('message'))
PY
)
check "a trailing blank line does not blank the message" "$msg_tr" "SILENT_CORRUPTION row=1"

# ---- 11. time= is escaped like every other attribute ------------------------------------
# Today's callers pass arithmetic, so this is unreachable from them. An attribute exempt from
# escaping only because of what its callers happen to pass is one call site away from a report
# the dashboard rejects whole.
XML4="$TMP/junit4.xml"
junit_begin "$XML4" "s"
junit_case c "odd-time" DURABILITY_FAILURE 'x"y<z' "DURABILITY_FAILURE nope"
junit_finish
if python3 -c "import xml.etree.ElementTree as E; E.parse('$XML4')" 2>/dev/null; then
    ok "a non-numeric time= still produces well-formed XML"
else
    bad "a non-numeric time= produced MALFORMED XML — the whole report would be rejected"
fi

echo
if [ "$fails" -eq 0 ]; then
    echo "t09 PASSED"
    exit 0
fi
echo "t09 FAILED: $fails assertion(s)"
exit 1
