#!/usr/bin/env bash
# t12 — the verdict vocabulary itself: one line of oracle output -> one token.
# Pure string in, token out; needs no VM and no root.
#
# verdict_classify decides what every boundary means: every pass/fail, every kept disk, every
# JUnit case and the sweep's exit status derive from its answer. Both its failure modes are
# silent. Too shy, and a reworded producer line turns every boundary into UNPARSEABLE, blaming
# the product for a wording change. Too eager, and a failure line gets absorbed by an earlier
# passing case: relax one prefix anchor to a substring match and a lost transaction reads as
# DURABLE.
#
# Every fixture below is copied from the code that emits it. Invented wording would test this
# file against itself, when the risk being guarded is drift between the two.
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

row DURABLE \
    "DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)" \
    "DURABLE               <- CrashVerifier"
row RPO_OK \
    "RPO_OK F=345 >= Wm=177 (every acked txn survived); at-risk txns lost=3 in (Wm=177, C=348] (RPO<=W=50000)" \
    "RPO_OK                <- CrashVerifier"
row PRECONDITION_NOT_MET \
    "PRECONDITION_NOT_MET mat-view needs recovered F < C, got F=348 C=348 with Wm=347; the base did not roll back" \
    "PRECONDITION_NOT_MET  <- CrashVerifier"
row DURABILITY_FAILURE \
    "DURABILITY_FAILURE table left suspended after recovery (F=12 C=14 Wm=12)" \
    "DURABILITY_FAILURE    <- CrashVerifier"
row SILENT_CORRUPTION \
    "SILENT_CORRUPTION count=43264 is not a multiple of K=1000" \
    "SILENT_CORRUPTION     <- CrashVerifier"
row LOUD_FAILURE \
    "LOUD_FAILURE: the shipped server did not start on the crashed database" \
    "LOUD_FAILURE          <- guest/verify.sh (product side)"
# The oracle never reached a verdict, so nothing was measured and this is the rig's finding
# rather than a statement about the product. Both wordings verify.sh emits are pinned: the
# second has no colon after the token, and a tightened anchor would drop it.
row NOT_EVALUATED \
    "NOT_EVALUATED: verifier produced no verdict (jvm-crash-log-present)" \
    "NOT_EVALUATED         <- guest/verify.sh"
row NOT_EVALUATED \
    "NOT_EVALUATED qwp-sf: impossible negative replay delta (-1230974) — the oracle's own numbers did not parse, so this boundary was not evaluated" \
    "NOT_EVALUATED qwp-sf  <- guest/verify.sh (no colon after the token)"
row NO_COMMIT \
    "NO_COMMIT cut landed before any commit (watermark=absent)" \
    "NO_COMMIT             <- guest/verify.sh"
row MOUNT_FAILED \
    "MOUNT_FAILED" \
    "MOUNT_FAILED          <- run-flush-sweep.sh (the whole line, no detail)"
row UNPARSEABLE \
    "what even is this" \
    "UNPARSEABLE           <- the catch-all"

# CONSISTENT is an alias rather than a token of its own: CrashVerifier uses this wording on the
# non-WAL path, meaning what DURABLE means. Drop it and a good SYNC or NOSYNC run reports
# UNPARSEABLE.
row DURABLE \
    "CONSISTENT count=18000 watermark=18" \
    "CONSISTENT -> DURABLE <- CrashVerifier, aliased on purpose"

# ---- 2. the near-misses: prefix anchoring is load-bearing -------------------------------
# Each row here is a line that contains a passing token's text while meaning a failure.
echo "  prefix anchoring: lines that contain a passing token but are not one"

# DURABLE* is matched before DURABILITY_FAILURE*, and the two share five characters. Shorten the
# first pattern to DURAB* and a lost acked transaction classifies as DURABLE, which
# verdict_is_pass calls a pass, so the run goes green.
row DURABILITY_FAILURE \
    "DURABILITY_FAILURE qwp-sf: the LOCAL durable-ack tier was never requested (localAcks=-1)" \
    "DURABILITY_FAILURE is not DURABLE (5 shared characters)"

# A verdict line can carry a run-identity suffix. The token must survive it.
row DURABLE \
    "DURABLE count=7 F=7 C=7 (adaptive W=0 == SYNC, zero loss) [seed=42 cutAfterMs=1209]" \
    "a trailing [seed=... cutAfterMs=...] does not change the token"

# ---- 3. the degenerate inputs --------------------------------------------------------------
echo "  degenerate input"

row UNPARSEABLE "" "an empty line is UNPARSEABLE, not a pass"
# An indented verdict is unrecognised rather than silently passing, so this fails closed. Pinned
# so that adding a trim is a deliberate decision with a test to change rather than an accident.
row UNPARSEABLE "  DURABLE count=1" "a leading-space verdict is UNPARSEABLE (fails closed)"
row UNPARSEABLE "durable count=1" "classification is case-sensitive"
row UNPARSEABLE "DETAIL i.q.c.TableWriter o3 commit" "a DETAIL evidence line is not a verdict"

# ---- 4. the boundary with verdict_line -----------------------------------------------------
# verdict_line picks the one verdict line out of the oracle's blob, and verdict_classify reads
# that line. A caller that classifies the blob matches its first line, which is evidence, and so
# reads a green run as UNPARSEABLE.
echo "  verdict_line: the blob -> line half of the contract"

blob_evidence_first="DETAIL loading functions from /opt/vmcrash/benchmarks.jar
DETAIL loaded 1118 functions
DURABLE count=18000 F=18 C=18 (adaptive W=0 == SYNC, zero loss)"
check "classifying the RAW BLOB is UNPARSEABLE, not the verdict it ends with" \
    "$(verdict_classify "$blob_evidence_first")" "UNPARSEABLE"
check "classifying verdict_line's output recovers the real verdict" \
    "$(verdict_classify "$(verdict_line "$blob_evidence_first")")" "DURABLE"

# DETAIL-ERR lines can legitimately follow the verdict, which is why verdict_line filters rather
# than taking tail -1. A tail -1 returns the DETAIL-ERR line here, and the boundary classifies
# as UNPARSEABLE.
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

# A glob's * spans newlines, so a blob whose first line is the verdict classifies correctly by
# luck while the same blob with evidence first does not. That asymmetry is what makes
# verdict_line mandatory rather than advisory; callers must not rely on the lucky shape.
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
# A single boundary that does not complete the startup repair is a legitimate skip.
# run-flush-sweep.sh adds a failing NOT_EVALUATED suite case if every mat-view boundary returns it.
pass_is PRECONDITION_NOT_MET yes
# NO_COMMIT passes deliberately: the cut landed before anything was committed, a legitimate
# sample that measured nothing, so it must not fail the run. lib/junit.sh still renders it as
# <skipped> so it cannot inflate the durable count either.
pass_is NO_COMMIT          yes
pass_is DURABILITY_FAILURE no
pass_is SILENT_CORRUPTION  no
pass_is LOUD_FAILURE       no
# A boundary the oracle could not evaluate is a finding rather than something to skip past, and
# must not inherit NO_COMMIT's pass: that is a valid sample that measured nothing, this is an
# invalid one.
pass_is NOT_EVALUATED      no
pass_is MOUNT_FAILED       no
pass_is UNPARSEABLE        no

fault_is UNPARSEABLE       yes
fault_is DURABLE           no
fault_is RPO_OK            no
fault_is PRECONDITION_NOT_MET no
fault_is NO_COMMIT         no
fault_is DURABILITY_FAILURE no
fault_is SILENT_CORRUPTION no
# LOUD_FAILURE means the product refused, loudly; the cases where nothing was measured carry
# NOT_EVALUATED instead, which is what keeps this token unambiguous.
fault_is LOUD_FAILURE      no
# A rig fault must not raise the data-loss alarm. Do that weekly and the real one stops being
# believed.
fault_is NOT_EVALUATED     yes
# MOUNT_FAILED is a product finding: an ext4 that will not mount after a power cut is the damage
# this instrument exists to catch.
fault_is MOUNT_FAILED      no

# ---- 5b. the producer side: what guest/verify.sh actually emits ----------------------------
# Everything above tests verdict_classify against fixtures this file owns, so a producer that
# reworded or re-tokenised an exit would leave every assertion green. verify.sh runs inside the
# guest and cannot be executed here, but it can be read.
echo "  producer scan: guest/verify.sh's own verdict lines"

VERIFY_SH="$HERE/../guest/verify.sh"

# Every verdict guest/verify.sh emits, as the literal text after echo " or line=".
# Comments are stripped first: this file's own explanatory prose names these tokens.
emitted_lines=$(grep -vE '^[[:space:]]*#' "$VERIFY_SH" \
    | grep -oE '(echo "|line=")(NOT_EVALUATED|LOUD_FAILURE|DURABILITY_FAILURE|NO_COMMIT)[^"]*' \
    | sed -e 's/^echo "//' -e 's/^line="//')

unknown=0
while IFS= read -r l; do
    [ -z "$l" ] && continue
    [ "$(verdict_classify "$l")" = UNPARSEABLE ] && { unknown=$((unknown + 1)); echo "       UNCLASSIFIED: $l"; }
done <<< "$emitted_lines"
check "every verdict line verify.sh emits is classifiable" "$unknown" "0"

# The instrument-side exits, matched by message text rather than line number, because line
# numbers move. Each phrase must sit on a line whose token is an instrument fault.
producer_is_instrument() {  # PHRASE
    local phrase="$1" line tok
    line=$(grep -vE '^[[:space:]]*#' "$VERIFY_SH" \
        | grep -oE '(echo "|line=")[A-Z_]+[^"]*'"$(printf '%s' "$phrase" | sed 's/[].[^$*\/]/\\&/g')"'[^"]*' \
        | sed -e 's/^echo "//' -e 's/^line="//' | head -1)
    if [ -z "$line" ]; then
        bad "producer: no verdict line carries '$phrase' (wording changed?)"
        return
    fi
    tok=$(verdict_classify "$line")
    if verdict_is_instrument_fault "$tok"; then
        ok "producer: '$phrase' -> $tok (instrument)"
    else
        bad "producer: '$phrase' -> $tok, which pages the PRODUCT owner (split regression)"
    fi
}
producer_is_instrument "verifier produced no verdict"
producer_is_instrument "did not parse, so this boundary was not evaluated"
producer_is_instrument "product distribution unusable in the guest"
producer_is_instrument "verify.sh unknown argument"
producer_is_instrument "unknown arm"

# The converse, so the scan cannot be satisfied by moving everything to NOT_EVALUATED: the
# product-side exits must still raise the product alarm.
producer_is_product() {  # PHRASE
    local phrase="$1" line tok
    line=$(grep -vE '^[[:space:]]*#' "$VERIFY_SH" \
        | grep -oE '(echo "|line=")[A-Z_]+[^"]*'"$(printf '%s' "$phrase" | sed 's/[].[^$*\/]/\\&/g')"'[^"]*' \
        | sed -e 's/^echo "//' -e 's/^line="//' | head -1)
    if [ -z "$line" ]; then
        bad "producer: no verdict line carries '$phrase' (wording changed?)"
        return
    fi
    tok=$(verdict_classify "$line")
    if verdict_is_instrument_fault "$tok"; then
        bad "producer: '$phrase' -> $tok, but a product failure must NOT be a rig glitch"
    else
        ok "producer: '$phrase' -> $tok (product)"
    fi
}
producer_is_product "shipped artifacts were not durable"
producer_is_product "the shipped server did not start on the crashed database"

# ---- 6. the table above covers every token the function can emit ---------------------------
# A token added to lib/verdict.sh but not tested here inherits the predicates' default, "not a
# pass, not an instrument fault": right for a product failure, wrong for a rig fault, and so it
# misroutes an alarm rather than breaking anything visibly.
emitted=$(sed -n '/^verdict_classify()/,/^}/p' "$HERE/../lib/verdict.sh" \
    | grep -vE '^[[:space:]]*#' \
    | grep -oE '\)[[:space:]]+echo[[:space:]]+[A-Z_]+' \
    | awk '{print $NF}' | sort -u | tr '\n' ' ')
covered="DURABILITY_FAILURE DURABLE LOUD_FAILURE MOUNT_FAILED NO_COMMIT NOT_EVALUATED PRECONDITION_NOT_MET RPO_OK SILENT_CORRUPTION UNPARSEABLE "
check "every token verdict_classify can emit has a row above" "$emitted" "$covered"

echo
if [ "$fails" -eq 0 ]; then
    echo "t12 PASSED ($oks assertions)"
    exit 0
fi
echo "t12 FAILED: $fails assertion(s) of $((oks + fails))"
exit 1
