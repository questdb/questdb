#!/usr/bin/env bash
# lib/junit.sh — JUnit XML for CI, ALONGSIDE the text log. Source this file; do not execute it.
#
# The text log stays exactly as it is. It is the evidence trail, and every time a result
# needed explaining the explanation was in it. This is for the dashboard: without a
# machine-readable file, a CI job shows a pass/fail exit code and a log blob -- no
# per-boundary visibility, no trend, and no way to ask "boundary 13776 regressed between
# build N and N+1", which is the question a durability sweep exists to answer.
#
# Shape, one <testcase> per verified boundary:
#
#     name      = "flush-13776"
#     classname = "durability.adaptive.W50000.bitmap"
#     failure   = the verdict line plus the DETAIL lines already captured in $OUTDIR
#
# NO_COMMIT MAPS TO <skipped>, NOT TO A PASS. A cut that landed before anything was committed
# is a legitimate but UNINFORMATIVE sample. Counting it as a pass lets a run where most
# boundaries measured nothing report as a wall of green -- the exact vacuity this harness
# rejects everywhere else. run-fuzz.sh already tracks "N informative, M NO_COMMIT"; this
# preserves that distinction where CI can see it.
#
# INSTRUMENT FAULTS ARE <error>, PRODUCT FAULTS ARE <failure>. "MOUNT_FAILED, the rig broke"
# and "an acked transaction was lost" are different alarms and, for a durability gate, the
# distinction decides whether to page someone. The token list is NOT here: it is
# verdict_is_instrument_fault in lib/verdict.sh, next to verdict_is_pass, so this file cannot
# drift from the rest of the harness about what a token means.
#
# Written incrementally to a temp file and renamed at the end, for the reason issues/19
# established: a consumer must never see a half-written file, and a run killed mid-sweep
# leaves no misleading partial result.

# junit_begin FILE SUITE_NAME -> start a report.
junit_begin() {
    JUNIT_FILE="$1"
    JUNIT_SUITE="$2"
    JUNIT_TMP="${1}.tmp.$$"
    JUNIT_PROPS="${1}.props.$$"
    JUNIT_TESTS=0
    JUNIT_FAILURES=0
    JUNIT_ERRORS=0
    JUNIT_SKIPPED=0
    JUNIT_STARTED=$(date +%s)
    : > "$JUNIT_TMP"
    : > "$JUNIT_PROPS"
}

# junit_property NAME VALUE -> record one fact about the run's identity.
#
# WHAT THIS IS FOR. issues/06 asks for "a way to say 'boundary 13776 regressed between build N
# and N+1'", and that is unanswerable from a report that does not name the build. The text log
# names the artifact (run-flush-sweep.sh prints `product: dist=... recoveryPass=...`); the XML
# named none of it, so the machine-readable half could not attribute a trend to anything. On a
# GREEN product run the dist name appeared nowhere at all, because the only place it reached
# was a failure body.
#
# THE SWEEP SUPPLIES THE VALUES; this file does not guess at them. Every fact here is already
# held by the caller, and a second derivation is a second source of truth -- the drift pattern
# this harness keeps paying for. So: a setter the sweep calls, not an environment scrape.
junit_property() {  # NAME VALUE
    [ -n "${JUNIT_PROPS:-}" ] || return 0
    printf '      <property name="%s" value="%s"/>\n' \
        "$(junit_escape "$1")" "$(junit_escape "$2")" >> "$JUNIT_PROPS"
}

# junit_escape TEXT -> XML-safe text.
#
# & FIRST, or every entity emitted by the later rules gets its own ampersand escaped again
# and the file is corrupt in a way that still parses. Control characters are stripped
# because they are ILLEGAL IN XML 1.0 at any escaping -- and the engine's own log output,
# which lands in these failure bodies, carries them.
junit_escape() {
    printf '%s' "$1" \
        | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g" \
        | tr -d '\000-\010\013\014\016-\037'
}

# junit_case CLASSNAME NAME VERDICT SECONDS [BODY]
#
# VERDICT is a token from lib/verdict.sh; the pass/fail decision is verdict_is_pass and the
# alarm routing is verdict_is_instrument_fault, so this file cannot drift from the rest of the
# harness about what counts as a pass or about who a red boundary blames.
junit_case() {
    local classname="$1" name="$2" verdict="$3" secs="${4:-0}" body="${5:-}"
    JUNIT_TESTS=$((JUNIT_TESTS + 1))
    {
        # $secs IS ESCAPED LIKE EVERY OTHER ATTRIBUTE. It was the one interpolated raw, on the
        # reasoning that a caller always passes arithmetic -- but an attribute that is exempt
        # from escaping only because of what its callers currently do is one call site away
        # from producing a file the dashboard rejects WHOLE. A malformed time= does not lose
        # one boundary, it loses the report.
        printf '    <testcase classname="%s" name="%s" time="%s"' \
            "$(junit_escape "$classname")" "$(junit_escape "$name")" "$(junit_escape "$secs")"
        if [ "$verdict" = "NO_COMMIT" ]; then
            JUNIT_SKIPPED=$((JUNIT_SKIPPED + 1))
            printf '>\n      <skipped message="%s"/>\n    </testcase>\n' \
                "$(junit_escape "NO_COMMIT — the cut landed before anything was committed; this boundary measured nothing")"
        elif verdict_is_pass "$verdict"; then
            printf '/>\n'
        else
            # THE MESSAGE COMES FROM verdict_line, not from a local copy of it. This file used
            # to re-implement it as `grep -vE '^DETAIL' | tail -1`, which is the THIRD copy and
            # was missing the blank-line filter, so a body ending in a blank line lost its
            # message.
            #
            # verdict_line ALONE IS NOT ENOUGH, and t09 is what established that: a body made
            # ENTIRELY of DETAIL lines filters down to nothing under either implementation, so
            # the message is empty and the dashboard shows a red boundary with no reason on it.
            # That is reachable -- guest/verify.sh:116 documents the JVM dying with only DETAIL
            # emitted. So fall back to the token and say WHY there is no line, rather than
            # emitting an empty attribute.
            local message
            message="$(verdict_line "$body")"
            [ -n "$message" ] || message="$verdict (no verdict line in the output; the body is evidence only)"
            local element=failure
            if verdict_is_instrument_fault "$verdict"; then
                element=error
                JUNIT_ERRORS=$((JUNIT_ERRORS + 1))
            else
                JUNIT_FAILURES=$((JUNIT_FAILURES + 1))
            fi
            printf '>\n      <%s type="%s" message="%s">%s</%s>\n    </testcase>\n' \
                "$element" \
                "$(junit_escape "$verdict")" \
                "$(junit_escape "$message")" \
                "$(junit_escape "$body")" \
                "$element"
        fi
    } >> "$JUNIT_TMP"
}

# junit_finish -> write the suite element and rename into place.
junit_finish() {
    [ -n "${JUNIT_TMP:-}" ] || return 0
    local elapsed=$(( $(date +%s) - JUNIT_STARTED ))
    {
        printf '<?xml version="1.0" encoding="UTF-8"?>\n'
        printf '<testsuites>\n'
        # errors= IS NOW REAL. It was hardcoded to 0, so an instrument fault and a lost
        # transaction were the same number on the dashboard.
        printf '  <testsuite name="%s" tests="%d" failures="%d" errors="%d" skipped="%d" time="%d">\n' \
            "$(junit_escape "$JUNIT_SUITE")" "$JUNIT_TESTS" "$JUNIT_FAILURES" "$JUNIT_ERRORS" \
            "$JUNIT_SKIPPED" "$elapsed"
        # <properties> FIRST, before any <testcase>. The JUnit XSD models testsuite as a
        # SEQUENCE (properties, testcase*, system-out?, system-err?), so a properties block
        # after the cases is schema-invalid; PublishTestResults@2 validates against that shape,
        # and a report it rejects is worth less than no report.
        if [ -s "${JUNIT_PROPS:-/dev/null}" ]; then
            printf '    <properties>\n'
            cat "$JUNIT_PROPS"
            printf '    </properties>\n'
        fi
        cat "$JUNIT_TMP"
        printf '  </testsuite>\n</testsuites>\n'
    } > "${JUNIT_FILE}.part.$$"
    mv -f "${JUNIT_FILE}.part.$$" "$JUNIT_FILE"
    rm -f "$JUNIT_TMP" "${JUNIT_PROPS:-}"
    JUNIT_TMP=""
    JUNIT_PROPS=""
}
