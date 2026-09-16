#!/usr/bin/env bash
# lib/junit.sh — JUnit XML for CI, alongside the text log. Source this file; do not execute it.
#
# One <testcase> per verified boundary:
#
#     name      = "flush-13776"
#     classname = "durability.adaptive.W50000.bitmap"
#     failure   = the verdict line plus the DETAIL lines already captured in $OUTDIR
#
# NO_COMMIT maps to <skipped>, not to a pass: the cut landed before anything was committed, so
# the boundary is a valid sample that measured nothing. Counting it as a pass would let a sweep
# that measured nothing report as a wall of green.
#
# Instrument faults render as <error> and product faults as <failure>, because for a durability
# gate that decides who gets paged. The token list lives in verdict_is_instrument_fault in
# lib/verdict.sh, so this file cannot disagree with the rest of the harness about a token.
#
# The report is written to a temp file and renamed into place, so a consumer never sees a
# partial file and a run killed mid-sweep leaves no misleading result.

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
# A trend cannot be attributed to a build the report does not name, so the identity has to reach
# the XML rather than the text log alone. The caller supplies the values; deriving them a second
# time here would be a second source of truth for facts the sweep already holds.
junit_property() {  # NAME VALUE
    [ -n "${JUNIT_PROPS:-}" ] || return 0
    printf '      <property name="%s" value="%s"/>\n' \
        "$(junit_escape "$1")" "$(junit_escape "$2")" >> "$JUNIT_PROPS"
}

# junit_escape TEXT -> XML-safe text.
#
# The ampersand rule must run first, or the entities emitted by the later rules have their own
# ampersands escaped again and the file corrupts in a way that still parses. Control characters
# are stripped because XML 1.0 forbids them at any escaping, and the engine's log output carries
# them into these failure bodies.
junit_escape() {
    printf '%s' "$1" \
        | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g' -e "s/'/\&apos;/g" \
        | tr -d '\000-\010\013\014\016-\037'
}

# junit_case CLASSNAME NAME VERDICT SECONDS [BODY]
#
# VERDICT is a token from lib/verdict.sh, which also owns the pass decision (verdict_is_pass)
# and the alarm routing (verdict_is_instrument_fault).
junit_case() {
    local classname="$1" name="$2" verdict="$3" secs="${4:-0}" body="${5:-}"
    JUNIT_TESTS=$((JUNIT_TESTS + 1))
    {
        # $secs is escaped like every other attribute, even though today's callers pass
        # arithmetic: a malformed attribute does not lose one boundary, it loses the report.
        printf '    <testcase classname="%s" name="%s" time="%s"' \
            "$(junit_escape "$classname")" "$(junit_escape "$name")" "$(junit_escape "$secs")"
        if [ "$verdict" = "NO_COMMIT" ]; then
            JUNIT_SKIPPED=$((JUNIT_SKIPPED + 1))
            printf '>\n      <skipped message="%s"/>\n    </testcase>\n' \
                "$(junit_escape "NO_COMMIT — the cut landed before anything was committed; this boundary measured nothing")"
        elif verdict_is_pass "$verdict"; then
            printf '/>\n'
        else
            # The message comes from verdict_line rather than a local reimplementation of it.
            # A body made entirely of DETAIL lines filters down to nothing, which is reachable
            # when the JVM dies after emitting only evidence, so fall back to the token and say
            # why there is no line instead of emitting an empty attribute.
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
    # The root is <testsuite>, not a <testsuites> aggregate. The JUnit XSD that
    # PublishTestResults@2 accepts (windyroad JUnit.xsd) declares testsuite as a root element in
    # its own right, and requires package= and id= on every child of an aggregate. One sweep
    # writes one suite, so the wrapper would only add attributes to get wrong.
    local host stamp
    host=$(hostname 2>/dev/null || echo localhost)
    # timestamp= is required, and its XSD pattern is ISO8601 without a timezone, so the trailing
    # Z that $STAMP carries elsewhere in the harness must not appear here.
    stamp=$(date -u +%Y-%m-%dT%H:%M:%S)
    {
        printf '<?xml version="1.0" encoding="UTF-8"?>\n'
        # timestamp= and hostname= are required by the schema; hostname also names the agent
        # that produced the run, which is the first question asked when a result looks odd.
        printf '<testsuite name="%s" timestamp="%s" hostname="%s" tests="%d" failures="%d" errors="%d" skipped="%d" time="%d">\n' \
            "$(junit_escape "$JUNIT_SUITE")" "$stamp" "$(junit_escape "$host")" \
            "$JUNIT_TESTS" "$JUNIT_FAILURES" "$JUNIT_ERRORS" \
            "$JUNIT_SKIPPED" "$elapsed"
        # The XSD models testsuite as the sequence (properties, testcase*, system-out,
        # system-err), and none of the four carries minOccurs="0". All four are therefore
        # emitted unconditionally, empty where there is nothing to say; gating one on content
        # produces a report the publisher rejects, and it rejects silently.
        printf '    <properties>\n'
        [ -s "${JUNIT_PROPS:-/dev/null}" ] && cat "$JUNIT_PROPS"
        printf '    </properties>\n'
        cat "$JUNIT_TMP"
        # The identity is repeated here because ADO's JUnit parser has limited support for
        # <properties> and may never surface it, while system-out is displayed per suite. The
        # properties block stays for consumers that do read it.
        printf '    <system-out>'
        if [ -s "${JUNIT_PROPS:-/dev/null}" ]; then
            sed -e 's/.*name="//' -e 's/" value="/=/' -e 's/"\/>[[:space:]]*$//' "$JUNIT_PROPS" \
                | sed 's/^[[:space:]]*//'
        fi
        printf '</system-out>\n'
        # system-err is required and stays empty: the harness routes diagnostics to the text log
        # and to per-case failure bodies.
        printf '    <system-err></system-err>\n'
        printf '</testsuite>\n'
    } > "${JUNIT_FILE}.part.$$"
    mv -f "${JUNIT_FILE}.part.$$" "$JUNIT_FILE"
    rm -f "$JUNIT_TMP" "${JUNIT_PROPS:-}"
    JUNIT_TMP=""
    JUNIT_PROPS=""
}
