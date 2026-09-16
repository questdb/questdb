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
#   * an INSTRUMENT fault must be <error> and a PRODUCT fault <failure>. For a durability gate
#     that is the difference between "the rig broke" and "an acked transaction was lost", and
#     it decides who gets paged. errors= was hardcoded to 0, so the two were one number.
#   * the run's IDENTITY must be in the report. A trend that cannot name the build it belongs
#     to cannot answer issues/06's question, and a DEGRADED product run must not look like a
#     full-claim one.
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
# The wording here USED to be "verifier produced no verdict", which since issues/21 is a
# NOT_EVALUATED line, not a LOUD_FAILURE one. A fixture that cites a producer must move when
# the producer moves, or the test drifts into asserting against wording nothing emits.
junit_case "durability.product.adaptive.W50000.bitmap" "flush-4066" LOUD_FAILURE 1 "LOUD_FAILURE: the shipped server did not start on the crashed database"
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
r = E.parse(sys.argv[1]).getroot()
# ROOT IS <testsuite> since the XSD check (issues/21 follow-up): the <testsuites>
# aggregate requires package= and id= on every child, which we never emitted. Accept
# either shape here so this helper does not have to change again if that is revisited.
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(s.get('tests'), s.get('failures'), s.get('skipped'))
PY
)
check "tests counted"    "$tests"    "5"
check "failures counted" "$failures" "2"
check "skipped counted"  "$skipped"  "1"

# ---- 2b. the attributes the XSD calls REQUIRED --------------------------------------------
# PublishTestResults@2 documents the windyroad JUnit.xsd as its supported format. Four
# violations shipped undetected until that schema was read against a real sweep's output:
# timestamp and hostname were absent, and the <testsuites> wrapper we used requires package=
# and id= on every child suite, neither of which we emitted. A rejected report does not fail
# the job -- it goes green and shows nothing, which is the worst way for a durability gate to
# break. These assertions exist so it cannot regress silently.
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
leftovers=$(find "$TMP" -name 'junit.xml.tmp.*' -o -name 'junit.xml.part.*' -o -name 'junit.xml.props.*' | wc -l)
check "no temp files left behind" "$leftovers" "0"

# ---- 8. the run's identity is IN the report ---------------------------------------------
# Without this a dashboard can trend a number but cannot say WHICH BUILD produced it, which is
# the question issues/06 exists to answer. On a green product run the artifact name reached the
# XML nowhere at all, because the only place it ever appeared was inside a failure body.
props=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
# ROOT IS <testsuite> since the XSD check (issues/21 follow-up): the <testsuites>
# aggregate requires package= and id= on every child, which we never emitted. Accept
# either shape here so this helper does not have to change again if that is revisited.
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

# <properties> MUST BE THE FIRST CHILD of <testsuite>. The JUnit XSD models testsuite as a
# SEQUENCE (properties, testcase*, system-out?, system-err?), so a properties block written
# after the testcases is schema-invalid. A report the dashboard rejects is worse than no
# report, because the job still goes green.
first=$(python3 - "$XML" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
# ROOT IS <testsuite> since the XSD check (issues/21 follow-up): the <testsuites>
# aggregate requires package= and id= on every child, which we never emitted. Accept
# either shape here so this helper does not have to change again if that is revisited.
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(list(s)[0].tag if len(s) else 'EMPTY')
PY
)
check "properties come FIRST inside testsuite (XSD sequence order)" "$first" "properties"

# ---- 9. instrument faults are <error>, product faults are <failure> ----------------------
# The split that decides whether the product owner or the rig owner gets paged. The token list
# lives in lib/verdict.sh's verdict_is_instrument_fault; this asserts junit.sh honours it.
XML2="$TMP/junit2.xml"
junit_begin "$XML2" "durability.reference.SYNC.W0.bitmap"
junit_property arm product
junit_property degraded true
junit_property degraded_reason "no LOCAL durable-ack tier at mode=SYNC; the arm runs the plain-qwp contract"
junit_case c "flush-10" DURABILITY_FAILURE 1 "DURABILITY_FAILURE acked txn 44 lost"
junit_case c "flush-11" SILENT_CORRUPTION  1 "SILENT_CORRUPTION row=9 expected_v=1 actual_v=2"
# MOUNT_FAILED is a PRODUCT finding: an ext4 that will not mount after a power cut is exactly
# the damage this instrument exists to catch. run-flush-sweep.sh has said so in a comment since
# the sweep was written; the token now says it where a machine can read it.
junit_case c "flush-12" MOUNT_FAILED       1 "MOUNT_FAILED"
# LOUD_FAILURE is now a PRODUCT finding outright: issues/21 moved the not-evaluated cases to
# their own token, so this one means only "the product refused, loudly".
junit_case c "flush-13" LOUD_FAILURE       1 "LOUD_FAILURE: shipped artifacts were not durable"
junit_case c "flush-14" UNPARSEABLE        1 "DETAIL something
what even is this"
junit_case c "flush-15" RPO_UNVERIFIED     1 "RPO_UNVERIFIED gap=12 rows; client Wm unavailable"
junit_case c "flush-16" PREFLIGHT_FAILED   1 "PREFLIGHT_FAILED the cut is not cutting"
junit_case c "flush-17" DURABLE            1 "DURABLE count=7"
# THE POINT OF issues/21: the oracle never reached a verdict, so nothing was measured and the
# RIG owner is the one to page. Before the split this line was a LOUD_FAILURE and rendered as
# <failure> -- a data-loss alarm for a JVM the agent killed.
junit_case c "flush-18" NOT_EVALUATED     1 "NOT_EVALUATED: verifier produced no verdict (exit=134 killed-by-signal-6)"
junit_finish

read -r t2 f2 e2 s2 < <(python3 - "$XML2" <<'PY'
import sys, xml.etree.ElementTree as E
r = E.parse(sys.argv[1]).getroot()
# ROOT IS <testsuite> since the XSD check (issues/21 follow-up): the <testsuites>
# aggregate requires package= and id= on every child, which we never emitted. Accept
# either shape here so this helper does not have to change again if that is revisited.
s = r if r.tag == 'testsuite' else r.find('testsuite')
print(s.get('tests'), s.get('failures'), s.get('errors'), s.get('skipped'))
PY
)
check "mixed suite: tests counted"                    "$t2" "9"
check "mixed suite: product faults -> failures=4"     "$f2" "4"
check "mixed suite: instrument faults -> errors=4"    "$e2" "4"
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
check "RPO_UNVERIFIED     -> <error> (instrument)" "$(element_of "$XML2" flush-15)" "error"
check "PREFLIGHT_FAILED   -> <error> (instrument)" "$(element_of "$XML2" flush-16)" "error"
check "DURABLE            -> pass"                 "$(element_of "$XML2" flush-17)" "pass"
check "NOT_EVALUATED      -> <error> (instrument)" "$(element_of "$XML2" flush-18)" "error"
# The pair that is the whole point of issues/21: two boundaries, both red, DIFFERENT alarms.
# If these ever collapse to the same element the split has been undone.
check "NOT_EVALUATED and LOUD_FAILURE are different alarms" \
    "$(element_of "$XML2" flush-18)/$(element_of "$XML2" flush-13)" "error/failure"

# RPO_UNVERIFIED IS INCONCLUSIVE, NOT EXCULPATORY. It is an instrument fault AND a non-pass:
# the product was neither convicted nor cleared, so it must stay red. This is the one token a
# future refactor could plausibly "simplify" into a pass, which would turn an unenforceable
# RPO bar into a green boundary.
if verdict_is_pass RPO_UNVERIFIED; then
    bad "RPO_UNVERIFIED counts as a PASS — an unenforceable RPO bar would report green"
else
    ok "RPO_UNVERIFIED is not a pass"
fi
if verdict_is_instrument_fault RPO_UNVERIFIED; then
    ok "RPO_UNVERIFIED is an instrument fault"
else
    bad "RPO_UNVERIFIED is not routed to <error>"
fi

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
# ROOT IS <testsuite> since the XSD check (issues/21 follow-up): the <testsuites>
# aggregate requires package= and id= on every child, which we never emitted. Accept
# either shape here so this helper does not have to change again if that is revisited.
s = r if r.tag == 'testsuite' else r.find('testsuite')
p = s.find('properties')
print('MISSING' if p is None else next(
    (q.get('value') for q in p.iter('property') if q.get('name') == 'degraded'), 'MISSING'))
PY
)
check "a DEGRADED run says so in the XML" "$degraded" "true"

# ---- 10. verdict_line reuse: an all-DETAIL body must not yield message="" ----------------
# junit.sh used to re-implement verdict_line WITHOUT its blank-line filter -- the third copy of
# it. A body that is entirely DETAIL lines, or one ending in a blank line, produced an empty
# message, so the dashboard showed a red boundary with no reason on it.
XML3="$TMP/junit3.xml"
junit_begin "$XML3" "s"
junit_case c "all-detail"  UNPARSEABLE 1 "DETAIL only evidence here
DETAIL and more"
# A REAL blank line after the verdict, not just a trailing newline: verify.sh's output reaches
# the caller through ssh and command substitution, and a body whose last line is empty is what
# the missing blank-line filter actually mishandles. The fixture needs the empty line to exist,
# or the assertion passes against the broken implementation too -- which it did, first try.
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
# It was the one attribute interpolated raw. Unreachable from today's callers, which pass
# arithmetic -- but an attribute exempt from escaping only because of what its callers happen
# to do is one call site away from a report the dashboard rejects WHOLE.
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
