#!/usr/bin/env bash
# t12 — the verdict vocabulary itself: one line of oracle output -> one token.
#
# NO VM, NO ROOT, NO FIXTURES: pure string in, token out. About a second.
#
# WHY THIS EXISTS. verdict_classify decides what every boundary in this harness MEANS. Every
# pass/fail, every kept disk, every JUnit case and the sweep's exit status derive from its
# answer. Its three callers -- t05, t07, t10 -- all need a VM, so until this file nothing CI
# could run in seconds covered the function at all.
#
# The two failure modes it has, and why both are silent:
#
#   TOO SHY. The last case is a catch-all: anything unrecognised becomes UNPARSEABLE. So if
#   guest/verify.sh or CrashVerifier ever reword a verdict, every boundary classifies as
#   UNPARSEABLE and the sweep reports a wall of failures that never happened. That is loud,
#   but it points at the product instead of at the wording.
#
#   TOO EAGER, which is worse. Every case is PREFIX-ANCHORED. Relax one to a substring match
#   and a failure line gets absorbed by an earlier, passing case -- a lost transaction read as
#   DURABLE. Two such near-misses already exist in the real vocabulary and are pinned below:
#   DURABLE vs DURABILITY_FAILURE, and power-cut-vm.sh's RPO_UNVERIFIED downgrade, which
#   deliberately prepends its token to a line that still literally contains "DURABLE".
#
# Every fixture line below is a REAL line, copied from the code that emits it, with the
# producer cited. Invented wording would test this file against itself: the whole risk is that
# the vocabulary and its producers drift apart, so the fixtures have to come from the producers.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/verdict.sh
source "$HERE/../lib/verdict.sh"
fails=0
oks=0

ok()    { printf '  ok   %s\n' "$1"; oks=$((oks + 1)); }
bad()   { printf '  FAIL %s\n' "$1"; fails=$((fails + 1)); }
check() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }
row()   { check "$3" "$(verdict_classify "$2")" "$1"; }

echo "t12 — verdict vocabulary"

# ---- 1. every token the function can emit, from a real producer -------------------------
echo "  the table: one row per token, fixture lines taken from the emitter"

row PREFLIGHT_OK \
    "PREFLIGHT_OK kept=1 lost=MISSING ranged=NEW" \
    "PREFLIGHT_OK          <- guest/preflight.sh"
row PREFLIGHT_FAILED \
    "PREFLIGHT_FAILED fsynced-file-lost kept=MISSING" \
    "PREFLIGHT_FAILED      <- guest/preflight.sh"
row DURABLE \
    "DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)" \
    "DURABLE               <- CrashVerifier"
row RPO_OK \
    "RPO_OK F=345 >= Wm=177 (every acked txn survived); at-risk txns lost=3 in (Wm=177, C=348] (RPO<=W=50000)" \
    "RPO_OK                <- CrashVerifier"
row DURABILITY_FAILURE \
    "DURABILITY_FAILURE table left suspended after recovery (F=12 C=14 Wm=12)" \
    "DURABILITY_FAILURE    <- CrashVerifier"
row SILENT_CORRUPTION \
    "SILENT_CORRUPTION count=43264 is not a multiple of K=1000" \
    "SILENT_CORRUPTION     <- CrashVerifier"
row LOUD_FAILURE \
    "LOUD_FAILURE: verifier produced no verdict (jvm-crash-log-present)" \
    "LOUD_FAILURE          <- guest/verify.sh:318"
row NO_COMMIT \
    "NO_COMMIT cut landed before any commit (watermark=absent)" \
    "NO_COMMIT             <- guest/verify.sh:406"
row MOUNT_FAILED \
    "MOUNT_FAILED" \
    "MOUNT_FAILED          <- run-flush-sweep.sh:401 (the whole line, no detail)"
row UNPARSEABLE \
    "what even is this" \
    "UNPARSEABLE           <- the catch-all"

# CONSISTENT is an ALIAS, not a token of its own: CrashVerifier:329 uses this wording on the
# non-WAL path and it means what DURABLE means. If the alias is dropped, a perfectly good
# SYNC/NOSYNC run starts reporting UNPARSEABLE.
row DURABLE \
    "CONSISTENT count=18000 watermark=18" \
    "CONSISTENT -> DURABLE <- CrashVerifier:329, aliased on purpose"

# ---- 2. the near-misses: prefix anchoring is load-bearing -------------------------------
# These are the rows that catch a case relaxed into a substring match. Each one is a line that
# CONTAINS a passing token's text while MEANING a failure.
echo "  prefix anchoring: lines that contain a passing token but are not one"

# DURABLE* is checked BEFORE DURABILITY_FAILURE*, and the two share five characters. Shorten
# the first pattern to DURAB* -- a plausible "simplification" -- and a lost acked transaction
# classifies as DURABLE, which verdict_is_pass calls a pass. The run goes green.
row DURABILITY_FAILURE \
    "DURABILITY_FAILURE qwp-sf: the LOCAL durable-ack tier was never requested (localAcks=-1)" \
    "DURABILITY_FAILURE is not DURABLE (5 shared characters)"

# power-cut-vm.sh:290 downgrades an unenforceable RPO bar by PREPENDING its token to the
# verdict it just received, so the line literally contains "DURABLE ... zero loss". Only the
# prefix anchor keeps it out of the DURABLE case -- and DURABLE is a pass while RPO_UNVERIFIED
# is deliberately not one. This single row is the difference between "the RPO bar could not be
# enforced" and "the RPO bar held".
#
# THE BRANCH THAT EMITS THIS IS CURRENTLY DORMANT: RPO_ENFORCEABLE is set to 1 at
# power-cut-vm.sh:72 and to nothing else anywhere, so :290 cannot fire today and the token has
# no live producer. The row stays because the vocabulary still carries the token, junit.sh
# still routes it and verdict_is_pass still excludes it -- and because the shape of the line is
# the thing under test. If the downgrade is ever revived, see the note in t12's report: as
# written it matches the RAW BLOB, so it would silently fail to fire for the same reason the
# defect at the top of lib/verdict.sh did.
row RPO_UNVERIFIED \
    "RPO_UNVERIFIED DURABLE count=7 F=7 C=7 (adaptive W=0 == SYNC, zero loss) (gap measured; RPO bar NOT enforced — client Wm unavailable)" \
    "RPO_UNVERIFIED wins over the DURABLE it wraps"

# run-st8-probe.sh:416 prints this when the replay device will not mount, and its own parser at
# :418 reads a= and b= out of it. It must NOT reach the sweep's MOUNT_FAILED case: the probe
# reports per-candidate readbacks, not a boundary verdict, and verdict.sh:52 says so.
row UNPARSEABLE \
    "ST8_READBACK a=MOUNT_FAILED b=MOUNT_FAILED" \
    "ST8_READBACK stays UNPARSEABLE, not MOUNT_FAILED"

# PREFLIGHT_OK* is checked before PREFLIGHT_FAILED* and they share ten characters.
row PREFLIGHT_FAILED \
    "PREFLIGHT_FAILED unsynced-file-survived lost=NEW" \
    "PREFLIGHT_FAILED is not PREFLIGHT_OK (10 shared characters)"

# power-cut-vm.sh:293 appends run identity AFTER the verdict. The token must survive a suffix.
row DURABLE \
    "DURABLE count=7 F=7 C=7 (adaptive W=0 == SYNC, zero loss) [seed=42 cutAfterMs=1209]" \
    "a trailing [seed=... cutAfterMs=...] does not change the token"

# ---- 3. the degenerate inputs --------------------------------------------------------------
echo "  degenerate input"

row UNPARSEABLE "" "an empty line is UNPARSEABLE, not a pass"
# CURRENT BEHAVIOUR, and it fails CLOSED: an indented verdict is unrecognised rather than
# silently passing. Pinned so that a future trim() is a deliberate decision with a test to
# change, not an accident.
row UNPARSEABLE "  DURABLE count=1" "a leading-space verdict is UNPARSEABLE (fails closed)"
row UNPARSEABLE "durable count=1" "classification is case-sensitive"
row UNPARSEABLE "DETAIL i.q.c.TableWriter o3 commit" "a DETAIL evidence line is not a verdict"

# ---- 4. the boundary with verdict_line -----------------------------------------------------
# The division of labour: verdict_line picks the ONE verdict line out of the oracle's blob,
# verdict_classify reads that line. Classifying the blob directly is the defect documented at
# the top of lib/verdict.sh -- it cost 27 kept run directories and 28 GB in one session,
# because every live cut classified its own green run as UNPARSEABLE.
echo "  verdict_line: the blob -> line half of the contract"

blob_evidence_first="DETAIL loading functions from /opt/vmcrash/benchmarks.jar
DETAIL loaded 1118 functions
DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)"
check "classifying the RAW BLOB is UNPARSEABLE — the documented 28 GB defect" \
    "$(verdict_classify "$blob_evidence_first")" "UNPARSEABLE"
check "classifying verdict_line's output recovers the real verdict" \
    "$(verdict_classify "$(verdict_line "$blob_evidence_first")")" "DURABLE"

# DETAIL-ERR lines can legitimately follow the verdict, which is why verdict_line filters
# rather than taking tail -1. A tail -1 implementation returns the DETAIL-ERR line here and
# the boundary classifies as UNPARSEABLE.
blob_err_after="DETAIL replaying to flush 451
DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)
DETAIL-ERR jvm wrote to stderr during shutdown"
check "a DETAIL-ERR line AFTER the verdict does not win (filter, not tail -1)" \
    "$(verdict_line "$blob_err_after")" \
    "DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)"

# The blob reaches callers through ssh and command substitution, so a trailing blank line is
# routine. Without the blank-line filter the verdict is an empty string -> UNPARSEABLE.
blob_trailing="DETAIL noise
SILENT_CORRUPTION count=43264 is not a multiple of K=1000

"
check "a trailing blank line does not blank the verdict" \
    "$(verdict_classify "$(verdict_line "$blob_trailing")")" "SILENT_CORRUPTION"

# An all-DETAIL body means the oracle produced evidence but no verdict. That must arrive as
# UNPARSEABLE -- an instrument fault -- and never as a pass.
blob_all_detail="DETAIL only evidence
DETAIL and more evidence"
check "an all-DETAIL body yields no verdict line" "$(verdict_line "$blob_all_detail")" ""
check "an all-DETAIL body classifies as UNPARSEABLE" \
    "$(verdict_classify "$(verdict_line "$blob_all_detail")")" "UNPARSEABLE"

# The asymmetry that makes verdict_line mandatory rather than advisory: a glob's * spans
# newlines, so a blob whose FIRST line is the verdict classifies correctly by luck, while the
# same blob with evidence first does not. Callers must not rely on the lucky shape.
blob_verdict_first="DURABLE count=1 F=1 C=1
DETAIL trailing evidence"
check "a blob that HAPPENS to start with the verdict classifies by luck (* spans newlines)" \
    "$(verdict_classify "$blob_verdict_first")" "DURABLE"

# ---- 5. the predicates, for every token ----------------------------------------------------
# verdict_is_pass decides whether the disks are kept and whether the sweep exits 0.
# verdict_is_instrument_fault decides whether the rig owner or the product owner gets paged.
echo "  predicates: pass, and instrument-fault, for every token"

pass_is() {  # TOKEN yes|no
    if verdict_is_pass "$1"; then got=yes; else got=no; fi
    check "$(printf 'verdict_is_pass %-18s = %s' "$1" "$2")" "$got" "$2"
}
fault_is() {  # TOKEN yes|no
    if verdict_is_instrument_fault "$1"; then got=yes; else got=no; fi
    check "$(printf 'instrument_fault %-18s = %s' "$1" "$2")" "$got" "$2"
}

pass_is DURABLE            yes
pass_is RPO_OK             yes
pass_is PREFLIGHT_OK       yes
# NO_COMMIT is a pass ON PURPOSE: the cut landed before anything was committed, which is a
# legitimate sample that measured nothing. It must not fail the run -- and lib/junit.sh must
# still report it as <skipped>, so it cannot inflate the durable count either.
pass_is NO_COMMIT          yes
pass_is DURABILITY_FAILURE no
pass_is SILENT_CORRUPTION  no
pass_is LOUD_FAILURE       no
pass_is MOUNT_FAILED       no
pass_is PREFLIGHT_FAILED   no
pass_is UNPARSEABLE        no
# INCONCLUSIVE IS NOT EXCULPATORY. The gap was measured but the RPO bar could not be enforced,
# so the product was neither convicted nor cleared. This is the one token a refactor could
# plausibly "simplify" into a pass, which would turn an unenforceable bar into a green run.
pass_is RPO_UNVERIFIED     no

fault_is UNPARSEABLE       yes
fault_is PREFLIGHT_FAILED  yes
fault_is RPO_UNVERIFIED    yes
fault_is DURABLE           no
fault_is RPO_OK            no
fault_is PREFLIGHT_OK      no
fault_is NO_COMMIT         no
fault_is DURABILITY_FAILURE no
fault_is SILENT_CORRUPTION no
# LOUD_FAILURE spans both meanings and cannot be split by token, so it takes the LOUDER alarm:
# a rig fault shown as a product failure gets investigated, the reverse gets ignored. See
# verdict_is_instrument_fault's note and issues/21.
fault_is LOUD_FAILURE      no
# MOUNT_FAILED is a PRODUCT finding: an ext4 that will not mount after a power cut is exactly
# the damage this instrument exists to catch.
fault_is MOUNT_FAILED      no

# ---- 6. the table above covers every token the function can emit ---------------------------
# Without this, adding a token to lib/verdict.sh and forgetting to test it is silent -- and the
# new token would inherit whatever the predicates do by default, which is "not a pass, not an
# instrument fault". That default is right for a product failure and wrong for a rig fault, so
# the omission would misroute an alarm rather than break anything visibly.
emitted=$(sed -n '/^verdict_classify()/,/^}/p' "$HERE/../lib/verdict.sh" \
    | grep -vE '^[[:space:]]*#' \
    | grep -oE '\)[[:space:]]+echo[[:space:]]+[A-Z_]+' \
    | awk '{print $NF}' | sort -u | tr '\n' ' ')
covered="DURABILITY_FAILURE DURABLE LOUD_FAILURE MOUNT_FAILED NO_COMMIT PREFLIGHT_FAILED PREFLIGHT_OK RPO_OK RPO_UNVERIFIED SILENT_CORRUPTION UNPARSEABLE "
check "every token verdict_classify can emit has a row above" "$emitted" "$covered"

echo
if [ "$fails" -eq 0 ]; then
    echo "t12 PASSED ($oks assertions)"
    exit 0
fi
echo "t12 FAILED: $fails assertion(s) of $((oks + fails))"
exit 1
