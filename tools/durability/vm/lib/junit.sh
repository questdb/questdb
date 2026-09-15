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
# Written incrementally to a temp file and renamed at the end, for the reason issues/19
# established: a consumer must never see a half-written file, and a run killed mid-sweep
# leaves no misleading partial result.

# junit_begin FILE SUITE_NAME -> start a report.
junit_begin() {
    JUNIT_FILE="$1"
    JUNIT_SUITE="$2"
    JUNIT_TMP="${1}.tmp.$$"
    JUNIT_TESTS=0
    JUNIT_FAILURES=0
    JUNIT_SKIPPED=0
    JUNIT_STARTED=$(date +%s)
    : > "$JUNIT_TMP"
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
# VERDICT is a token from lib/verdict.sh; the pass/fail decision is verdict_is_pass, so this
# file cannot drift from the rest of the harness about what counts as a pass.
junit_case() {
    local classname="$1" name="$2" verdict="$3" secs="${4:-0}" body="${5:-}"
    JUNIT_TESTS=$((JUNIT_TESTS + 1))
    {
        printf '    <testcase classname="%s" name="%s" time="%s"' \
            "$(junit_escape "$classname")" "$(junit_escape "$name")" "$secs"
        if [ "$verdict" = "NO_COMMIT" ]; then
            JUNIT_SKIPPED=$((JUNIT_SKIPPED + 1))
            printf '>\n      <skipped message="%s"/>\n    </testcase>\n' \
                "$(junit_escape "NO_COMMIT — the cut landed before anything was committed; this boundary measured nothing")"
        elif verdict_is_pass "$verdict"; then
            printf '/>\n'
        else
            JUNIT_FAILURES=$((JUNIT_FAILURES + 1))
            printf '>\n      <failure type="%s" message="%s">%s</failure>\n    </testcase>\n' \
                "$(junit_escape "$verdict")" \
                "$(junit_escape "$(printf '%s' "$body" | grep -vE '^DETAIL' | tail -1)")" \
                "$(junit_escape "$body")"
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
        printf '  <testsuite name="%s" tests="%d" failures="%d" skipped="%d" errors="0" time="%d">\n' \
            "$(junit_escape "$JUNIT_SUITE")" "$JUNIT_TESTS" "$JUNIT_FAILURES" "$JUNIT_SKIPPED" "$elapsed"
        cat "$JUNIT_TMP"
        printf '  </testsuite>\n</testsuites>\n'
    } > "${JUNIT_FILE}.part.$$"
    mv -f "${JUNIT_FILE}.part.$$" "$JUNIT_FILE"
    rm -f "$JUNIT_TMP"
    JUNIT_TMP=""
}
