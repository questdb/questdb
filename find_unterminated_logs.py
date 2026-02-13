#!/usr/bin/env python3
"""
Find QuestDB log statements not properly terminated with .$() or .I$().

QuestDB's async logger requires every log chain to be flushed by calling
.$() or .I$() at the end:

    LOG.info().$("message").$();                  // OK
    LOG.info().$("key=").$(value).I$();            // OK  (.I$() appends ']' then flushes)
    LOG.info().$("message");                       // BAD - never flushed!

Log statements may span multiple lines:

    LOG.info().$("retrying plan [q=`").$(queryModel)
            .$("`, fd=").$(executionContext.getRequestFd())
            .I$();                                 // OK

Usage:
    python3 find_unterminated_logs.py [directory] [--no-tests]

    directory   -- root to scan (default: current directory)
    --no-tests  -- skip test source trees
"""

import os
import re
import sys

# All log-level methods on the Log interface that return a LogRecord.
LOG_LEVELS = (
    "advisory", "advisoryW",
    "critical",
    "debug", "debugW",
    "error", "errorW",
    "info", "infoW",
    "xDebugW", "xInfoW",
    "xadvisory", "xcritical", "xdebug", "xerror", "xinfo",
)

_levels_alt = "|".join(LOG_LEVELS)

# Matches any call to a log-level method:  .info()  .errorW()  .advisory()  etc.
# This catches LOG.info(), log.info(), bootstrap.getLog().info(), and similar.
LOG_START_RE = re.compile(
    rf"\.(?:{_levels_alt})\(\)"
)

# Detects LogRecord variable captures (these terminate separately).
LOG_RECORD_CAPTURE_RE = re.compile(
    r"(?:LogRecord|final\s+LogRecord|var)\s+\w+\s*="
)


def _is_comment_line(stripped):
    """Return True if the stripped line is purely a comment."""
    return (
        stripped.startswith("//")
        or stripped.startswith("*")
        or stripped.startswith("/*")
    )


def _find_semicolon(lines, start_line, start_col):
    """
    Starting from (start_line, start_col), find the first `;` that is
    outside of string/char literals, comments, and nested brace blocks
    (lambdas, anonymous classes).

    Returns (line_index, col_index) or (None, None).
    """
    in_string = False
    in_char = False
    in_block_comment = False
    in_text_block = False
    escaped = False
    brace_depth = 0

    # Don't search forever — 60 lines should be more than enough for any
    # log statement.
    end = min(start_line + 60, len(lines))

    for i in range(start_line, end):
        line = lines[i]
        j = start_col if i == start_line else 0

        while j < len(line):
            ch = line[j]

            # ---- escaped character inside literal ----
            if escaped:
                escaped = False
                j += 1
                continue

            if (in_string or in_char) and ch == "\\":
                escaped = True
                j += 1
                continue

            # ---- block comment ----
            if in_block_comment:
                if ch == "*" and j + 1 < len(line) and line[j + 1] == "/":
                    in_block_comment = False
                    j += 2
                    continue
                j += 1
                continue

            # ---- text block (triple-quoted string) ----
            if in_text_block:
                if ch == '"' and j + 2 < len(line) and line[j + 1] == '"' and line[j + 2] == '"':
                    in_text_block = False
                    j += 3
                    continue
                j += 1
                continue

            # ---- inside ordinary string ----
            if in_string:
                if ch == '"':
                    in_string = False
                j += 1
                continue

            # ---- inside char literal ----
            if in_char:
                if ch == "'":
                    in_char = False
                j += 1
                continue

            # ---- detect starts of literals / comments ----
            if ch == "/" and j + 1 < len(line):
                nxt = line[j + 1]
                if nxt == "/":
                    break  # rest of line is a line-comment
                if nxt == "*":
                    in_block_comment = True
                    j += 2
                    continue

            if ch == '"':
                if j + 2 < len(line) and line[j + 1] == '"' and line[j + 2] == '"':
                    in_text_block = True
                    j += 3
                    continue
                in_string = True
                j += 1
                continue

            if ch == "'":
                in_char = True
                j += 1
                continue

            # ---- track braces (lambdas, anonymous classes) ----
            if ch == "{":
                brace_depth += 1
                j += 1
                continue
            if ch == "}":
                brace_depth -= 1
                j += 1
                continue

            # ---- the semicolon we are looking for ----
            if ch == ";" and brace_depth == 0:
                return i, j

            j += 1

    return None, None


def _extract(lines, sl, sc, el, ec):
    """Extract text from (sl, sc) to (el, ec) inclusive."""
    if sl == el:
        return lines[sl][sc : ec + 1]
    parts = [lines[sl][sc:]]
    for i in range(sl + 1, el):
        parts.append(lines[i])
    parts.append(lines[el][: ec + 1])
    return "".join(parts)


def _has_proper_terminator(statement):
    """
    Return True if *statement* (including the trailing `;`) is properly
    terminated with .$() or .I$().

    Handles the case where `.` and `$()` are separated by whitespace
    across lines, e.g.:
        .$(']').
                $();
    """
    s = statement.rstrip()
    if s.endswith(";"):
        s = s[:-1].rstrip()

    # Collapse whitespace so `.  \\n  $()` becomes `.$()`.
    collapsed = re.sub(r"\s+", "", s)
    return collapsed.endswith(".$()") or collapsed.endswith(".I$()")


def _prefix_is_in_comment_or_string(line, match_start):
    """
    Quick heuristic: return True if the LOG match is likely inside a
    string literal or trailing comment.
    """
    prefix = line[:match_start]
    # Inside a string? (odd number of unescaped quotes before the match)
    if prefix.count('"') % 2 == 1:
        return True
    # After a line comment?
    # Find the last // that is not inside a string.
    idx = 0
    in_str = False
    while idx < len(prefix) - 1:
        if prefix[idx] == '"':
            in_str = not in_str
        elif not in_str and prefix[idx] == "/" and prefix[idx + 1] == "/":
            return True
        idx += 1
    return False


def _is_logrecord_assignment(line, match_start):
    """
    Return True if the log-level call is being assigned to a variable,
    either in a declaration (`LogRecord rec = LOG.info()`) or a plain
    assignment (`rec = LOG.info()`).
    """
    prefix = line[:match_start].rstrip()
    # Look for `=` (but not ==, !=, <=, >=) anywhere in the prefix.
    if re.search(r"(?<![=!<>])=(?!=)", prefix):
        return True
    return False


def _is_return_statement(line, match_start):
    """Return True if the log-level call appears in a return statement."""
    prefix = line[:match_start].strip()
    return prefix.startswith("return ") or prefix == "return"


def _is_nested_in_call(stmt):
    """
    Return True if the extracted statement has more closing parens than
    opening parens, meaning the LOG.level() is nested inside a larger
    expression (e.g. a method argument).
    """
    depth = 0
    in_string = False
    in_char = False
    escaped = False
    in_text_block = False

    i = 0
    while i < len(stmt):
        ch = stmt[i]

        if escaped:
            escaped = False
            i += 1
            continue

        if (in_string or in_char) and ch == "\\":
            escaped = True
            i += 1
            continue

        if in_text_block:
            if ch == '"' and i + 2 < len(stmt) and stmt[i + 1] == '"' and stmt[i + 2] == '"':
                in_text_block = False
                i += 3
                continue
            i += 1
            continue

        if in_string:
            if ch == '"':
                in_string = False
            i += 1
            continue

        if in_char:
            if ch == "'":
                in_char = False
            i += 1
            continue

        if ch == '"':
            if i + 2 < len(stmt) and stmt[i + 1] == '"' and stmt[i + 2] == '"':
                in_text_block = True
                i += 3
                continue
            in_string = True
            i += 1
            continue

        if ch == "'":
            in_char = True
            i += 1
            continue

        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return True

        i += 1

    return False


def scan_file(filepath):
    """Return a list of issue dicts for unterminated log statements."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return []

    issues = []
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if _is_comment_line(stripped):
            i += 1
            continue

        m = LOG_START_RE.search(line)
        if not m:
            i += 1
            continue

        if _prefix_is_in_comment_or_string(line, m.start()):
            i += 1
            continue

        # Skip LogRecord variable declarations on this line.
        if LOG_RECORD_CAPTURE_RE.search(line):
            end_line, _ = _find_semicolon(lines, i, m.start())
            i = (end_line + 1) if end_line is not None else (i + 1)
            continue

        # Skip assignments to already-declared variables: `rec = LOG.info()`.
        if _is_logrecord_assignment(line, m.start()):
            end_line, _ = _find_semicolon(lines, i, m.start())
            i = (end_line + 1) if end_line is not None else (i + 1)
            continue

        # Skip return statements (methods that return a LogRecord).
        if _is_return_statement(line, m.start()):
            end_line, _ = _find_semicolon(lines, i, m.start())
            i = (end_line + 1) if end_line is not None else (i + 1)
            continue

        end_line, end_col = _find_semicolon(lines, i, m.start())

        if end_line is None:
            issues.append({
                "file": filepath,
                "line": i + 1,
                "end_line": i + 1,
                "statement": stripped,
            })
            i += 1
            continue

        stmt = _extract(lines, i, m.start(), end_line, end_col)

        # Skip isEnabled() guard checks — not actual log output.
        if ".isEnabled()" in stmt:
            i = end_line + 1
            continue

        # Skip if the LOG.level() is nested inside a method call
        # (e.g. passed as argument to another function).
        if _is_nested_in_call(stmt):
            i = end_line + 1
            continue

        if not _has_proper_terminator(stmt):
            issues.append({
                "file": filepath,
                "line": i + 1,
                "end_line": end_line + 1,
                "statement": stmt.strip(),
            })

        i = end_line + 1

    return issues


def main():
    search_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    exclude_tests = "--no-tests" in sys.argv

    # Collect --exclude=<pattern> arguments.
    excludes = []
    for arg in sys.argv:
        if arg.startswith("--exclude="):
            excludes.append(arg[len("--exclude="):])

    all_issues = []

    for root, dirs, files in os.walk(search_dir):
        if exclude_tests and ("/test/" in root or "/test-" in root):
            continue
        for fname in sorted(files):
            if not fname.endswith(".java"):
                continue
            filepath = os.path.join(root, fname)
            if any(ex in filepath for ex in excludes):
                continue
            issues = scan_file(filepath)
            all_issues.extend(issues)

    if not all_issues:
        print("No unterminated log statements found.")
        return

    print(f"Found {len(all_issues)} unterminated log statement(s):\n")

    for issue in all_issues:
        rel = os.path.relpath(issue["file"], search_dir)
        loc = f"{rel}:{issue['line']}"
        if issue["end_line"] != issue["line"]:
            loc += f"-{issue['end_line']}"
        print(loc)

        stmt = issue["statement"].replace("\n", "\n  ")
        if len(stmt) > 300:
            stmt = stmt[:300] + " ..."
        print(f"  {stmt}")
        print()


if __name__ == "__main__":
    main()
