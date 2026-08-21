/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

/**
 * Shared helpers + codec for the row-expiry feature (read filter in {@code SqlParser}, cleanup in
 * {@link RowExpiryCleanupJob}, validation/SHOW CREATE/catalogue in the compiler).
 * <p>
 * An {@code EXPIRE ROWS} policy is persisted as a single string in the table's {@code _meta}
 * {@code expiryPredicate} slot. Encoding the relative modes into that string (rather than adding a new
 * {@code _meta} field) keeps the storage, replication and metadata-interface plumbing unchanged. The
 * encodings, distinguished by a non-printable unit-separator (0x1F) sentinel + a mode char, are:
 * <ul>
 *     <li><b>scalar WHEN</b> — a plain (un-prefixed) boolean predicate. A row expires when it is TRUE.</li>
 *     <li><b>KEEP LATEST</b> ({@code 0x1F 'L'} + raw PARTITION BY column list) — keep only the latest row per
 *         key (a {@code LATEST ON} rewrite).</li>
 *     <li><b>KEEP [N] HIGHEST/LOWEST</b> ({@code 0x1F 'N'} + {@code n 0x1F dir 0x1F col 0x1F keys}) — keep the
 *         group max/min ({@code n==0}, all ties) or the top-N by a column ({@code n>0}); desugars to a window
 *         predicate at use (the designated timestamp is needed only for the top-N tiebreak, known then).</li>
 *     <li><b>window WHEN</b> ({@code 0x1F 'W'} + predicate) — an arbitrary boolean predicate that references
 *         window functions (e.g. {@code v < max(v) OVER (PARTITION BY k)}).</li>
 * </ul>
 * The multi-field encodings (KEEP LATEST, KEEP [N] HIGHEST/LOWEST) escape their fields: a column name or a
 * raw PARTITION BY list may hold the separator (0x1F) or the escape character (0x1E), since a quoted
 * identifier accepts any character a file name accepts, so {@code appendEscapedField} rewrites those two as
 * {@code 0x1E 'S'} and {@code 0x1E 'E'} and the accessors decode them back. The single-field encodings
 * (scalar WHEN, window WHEN) store the predicate text verbatim: nothing splits them, and the mode check
 * reads only the first two characters, which a predicate cannot start with - SQL has no expression that
 * begins with a bare control character. A legacy raw predicate therefore still decodes as a scalar WHEN
 * (backward compatible). EXPIRE ROWS is a materialized-view-only feature: a plain table uses TTL, and
 * EXPIRE ROWS on a plain CREATE TABLE / CTAS / LIKE is rejected for every mode (scalar WHEN included). The
 * read filter is likewise applied only to materialized views ({@code isMatView()}).
 */
public final class RowExpiryUtil {

    /**
     * Default {@code CLEANUP EVERY} cadence (1 hour) used when the clause is omitted. Shared so the
     * parser default and the SHOW CREATE "omit when default" check (and any divisor) cannot drift.
     */
    public static final long DEFAULT_CLEANUP_INTERVAL_MICROS = 3_600_000_000L;

    /**
     * {@code materialized_views().expire_enforcement} value for a policy the cleanup job reclaims disk for:
     * every read hides the expired rows AND the background job eventually deletes them from disk.
     */
    public static final String ENFORCEMENT_FILTER_AND_RECLAIM = "FILTER_AND_RECLAIM";

    /**
     * {@code materialized_views().expire_enforcement} value for a policy the cleanup job skips: every read
     * hides the expired rows, but they keep occupying disk until a full refresh rebuilds the view. See
     * {@link #isReclaimingPolicy}.
     */
    public static final String ENFORCEMENT_FILTER_ONLY = "FILTER_ONLY";

    /**
     * Synthetic boolean column name used by the projection-CASE read filter / cleanup for window and keep-by
     * policies: the inner projection computes {@code CASE WHEN (<pred>) THEN false ELSE true END} as this
     * column and the outer query filters on it. Unlikely to collide with a real user column.
     */
    public static final String KEEP_COLUMN = "__qdb_re_keep";

    private static final char DIR_HIGHEST = 'H';
    private static final char DIR_LOWEST = 'O';
    private static final char ESCAPED_ESCAPE = 'E';   // <escape>E decodes to the escape char itself
    private static final char ESCAPED_SENTINEL = 'S'; // <escape>S decodes to the field separator
    private static final char MODE_KEEP_BY = 'N';     // keep-max/min (n=0) or top-N (n>0), structural
    private static final char MODE_KEEP_LATEST = 'L'; // keep only the latest row per key
    private static final char MODE_WINDOW = 'W';      // an arbitrary window-function WHEN predicate
    private static final char POLICY_ESCAPE = (char) 0x1E;
    private static final char POLICY_SENTINEL = (char) 0x1F;
    private static final String KEEP_BY_PREFIX = "" + POLICY_SENTINEL + MODE_KEEP_BY;
    private static final String KEEP_LATEST_PREFIX = "" + POLICY_SENTINEL + MODE_KEEP_LATEST;
    private static final String WINDOW_PREFIX = "" + POLICY_SENTINEL + MODE_WINDOW;

    private RowExpiryUtil() {
    }

    /**
     * Renders {@code micros} as a {@code CLEANUP EVERY} stride (e.g. {@code 30m}, {@code 1h}, {@code 2d})
     * into {@code sink}, picking the largest whole unit that divides it evenly. Shared by SHOW CREATE TABLE
     * and the catalogue functions so the rendering cannot drift.
     */
    public static void appendCleanupEvery(CharSink<?> sink, long micros) {
        if (micros % 86_400_000_000L == 0) {
            sink.put(micros / 86_400_000_000L).put('d');
        } else if (micros % 3_600_000_000L == 0) {
            sink.put(micros / 3_600_000_000L).put('h');
        } else if (micros % 60_000_000L == 0) {
            sink.put(micros / 60_000_000L).put('m');
        } else {
            sink.put(micros / 1_000_000L).put('s');
        }
    }

    /**
     * Appends the human-readable clause body of a stored policy (everything after {@code EXPIRE ROWS}) to
     * {@code sink}: {@code WHEN <predicate>} for scalar/window, or the {@code KEEP ...} form for the relative
     * modes. Used by SHOW CREATE; the rendering round-trips through the grammar.
     */
    public static void appendExpireClause(CharSink<?> sink, CharSequence stored) {
        if (isKeepLatest(stored)) {
            appendKeepLatestClause(sink, stored);
        } else if (isKeepBy(stored)) {
            appendKeepByClause(sink, stored);
        } else if (isWindow(stored)) {
            sink.putAscii("WHEN ").put(windowBody(stored));
        } else {
            sink.putAscii("WHEN ").put(stored);
        }
    }

    /**
     * Builds the keep-rows filter (the rows that have NOT expired) for a scalar-WHEN policy:
     * {@code CASE WHEN (<predicate>) THEN false ELSE true END}, which keeps every row whose predicate is
     * not TRUE. The predicate is wrapped in parentheses so its internal operator precedence cannot
     * leak. Applies to any predicate shape, including compound ones and {@code IN}.
     * <p>
     * All three users of this filter call this method, so all three run the same text: the read filter
     * {@code SqlParser.keepFilterWhereText} builds (except in the flip case, which is a bare
     * {@code NOT (...)}), the cleanup sweep, and the DDL validation in
     * {@code SqlCompilerImpl.validateExpiryPredicateOnMetadata}. That is what makes the sweep delete only
     * rows a read already hides, and what lets validation refuse a predicate whose wrapped form would fail
     * to compile even though the bare predicate binds - a {@code SYMBOL} column compared for equality with
     * an integer, say, which the wrapped form turns into a {@code switch} that then rejects the types.
     * <p>
     * A NULL operand follows QuestDB's two-valued comparison semantics: {@code v < 2.0} is FALSE for a
     * NULL {@code v} and keeps the row, while {@code NOT (v >= 2.0)}, {@code v != 2.0} and
     * {@code v IS NULL} are TRUE for it, so this sweep deletes it. Spellings that read as the same rule
     * can thus differ on NULL rows; see {@code SqlParser.keepFilterWhereText} for the full account.
     */
    public static String buildRowExpiryKeepFilter(String predicate) {
        return "CASE WHEN (" + predicate.trim() + ") THEN false ELSE true END";
    }

    /**
     * Appends the human-readable rendering of a stored policy used by catalogue functions.
     */
    public static void appendDisplayPredicate(CharSink<?> sink, CharSequence stored) {
        if (isKeepLatest(stored) || isKeepBy(stored)) {
            appendExpireClause(sink, stored);
        } else if (isWindow(stored)) {
            sink.put(windowBody(stored));
        } else {
            sink.put(stored);
        }
    }

    /**
     * Human-readable rendering of a stored policy for catalogue functions ({@code tables()},
     * {@code materialized_views()}): the predicate for scalar/window, or the {@code KEEP ...} clause for the
     * relative modes. Returns null for no policy.
     */
    public static String displayPredicate(CharSequence stored) {
        if (stored == null) {
            return null;
        }
        final StringSink sink = new StringSink();
        appendDisplayPredicate(sink, stored);
        return sink.toString();
    }

    public static String encodeKeepBy(int n, boolean isHighest, CharSequence col, CharSequence keysCsv) {
        final StringSink sink = new StringSink();
        sink.put(KEEP_BY_PREFIX).put(n).put(POLICY_SENTINEL)
                .put(isHighest ? DIR_HIGHEST : DIR_LOWEST).put(POLICY_SENTINEL);
        appendEscapedField(sink, col);
        sink.put(POLICY_SENTINEL);
        appendEscapedField(sink, keysCsv);
        return sink.toString();
    }

    public static String encodeKeepLatest(CharSequence ts, CharSequence keysCsv) {
        // body = <ts-or-empty> SEP <keys>; an empty ts means "use the designated timestamp".
        final StringSink sink = new StringSink();
        sink.put(KEEP_LATEST_PREFIX);
        if (ts != null) {
            appendEscapedField(sink, ts);
        }
        sink.put(POLICY_SENTINEL);
        appendEscapedField(sink, keysCsv);
        return sink.toString();
    }

    public static String encodeWindow(CharSequence predicate) {
        return WINDOW_PREFIX + predicate;
    }

    /**
     * Renders the cleanup cadence as a stride string (e.g. {@code 30m}, {@code 1h}, {@code 2d}), or null
     * when there is no policy ({@code micros <= 0}). Shared by the {@code tables()} and
     * {@code materialized_views()} catalogue functions.
     */
    public static String formatCleanupEvery(long micros) {
        if (micros <= 0) {
            return null;
        }
        final StringSink sink = new StringSink();
        appendCleanupEvery(sink, micros);
        return sink.toString();
    }

    /**
     * True if {@code stored} is an encoded KEEP [N] HIGHEST/LOWEST policy.
     */
    public static boolean isKeepBy(CharSequence stored) {
        return hasMode(stored, MODE_KEEP_BY);
    }

    /**
     * True when the group extreme of {@code columnType} is well-defined, i.e. the bare
     * {@code KEEP HIGHEST|LOWEST <col>} form ({@link KeepBy#n} == 0) may desugar to
     * {@code <col> < max(<col>) OVER (...)} on it. The window {@code max}/{@code min} overloads take LONG,
     * DOUBLE, DATE, TIMESTAMP or DECIMAL, and only the types listed here reach one of them through a
     * widening cast that preserves order. A text-ish column reaches them through an implicit parsing cast
     * that throws per row at read time, and LONG256 binds to the LONG overload through a cast that keeps
     * the low 64 bits alone, which ranks by the wrong value. The top-N form orders the column instead, and
     * ORDER BY accepts every comparable type, so it uses {@link ColumnType#isComparable} rather than this.
     */
    public static boolean isKeepExtremeType(int columnType) {
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE, ColumnType.SHORT, ColumnType.INT, ColumnType.LONG, ColumnType.FLOAT,
                 ColumnType.DOUBLE, ColumnType.DATE, ColumnType.TIMESTAMP -> true;
            default -> ColumnType.isDecimal(columnType);
        };
    }

    /**
     * True if {@code stored} is an encoded KEEP LATEST policy.
     */
    public static boolean isKeepLatest(CharSequence stored) {
        return hasMode(stored, MODE_KEEP_LATEST);
    }

    /**
     * True when the background cleanup job frees disk space for {@code stored}, i.e. the policy is both
     * scalar (see {@link #isStructuralPolicy}) and monotonic ({@code isMonotonicPredicate}: a row expired
     * now stays expired). A policy that fails either half stays query-correct - the read filter hides its
     * expired rows on every read - but its rows keep occupying disk until a full refresh rebuilds the view.
     * This is the single rule behind the cleanup job's skip, the DDL advisory and the
     * {@code expire_enforcement} column of {@code materialized_views()}.
     */
    public static boolean isReclaimingPolicy(CharSequence stored, boolean isMonotonicPredicate) {
        return isMonotonicPredicate && !isStructuralPolicy(stored);
    }

    /**
     * True if {@code stored} is an encoded KEEP LATEST, KEEP [N] HIGHEST/LOWEST or window-function policy,
     * i.e. one whose keep-verdict depends on the other rows in the view rather than on the row alone. A
     * later refresh can remove or replace the current winner and make an older row visible again, so
     * physical cleanup skips these modes: once it has deleted that older row, an incremental refresh cannot
     * reconstruct it. The check reads the encoded text alone, so callers holding no metadata (and no
     * compiler) can ask it.
     */
    public static boolean isStructuralPolicy(CharSequence stored) {
        return isKeepLatest(stored) || isKeepBy(stored) || isWindow(stored);
    }

    /**
     * True if {@code stored} is an encoded window-function WHEN policy.
     */
    public static boolean isWindow(CharSequence stored) {
        return hasMode(stored, MODE_WINDOW);
    }

    /**
     * The raw PARTITION BY column-list text of an encoded KEEP LATEST policy (check {@link #isKeepLatest}).
     */
    public static CharSequence keepLatestKeys(CharSequence stored) {
        return unescapeField(stored, sentinelIndex(stored, 2) + 1, stored.length());
    }

    /**
     * The explicit {@code ON <ts>} column of a KEEP LATEST policy, or empty when none was specified.
     */
    public static CharSequence keepLatestTs(CharSequence stored) {
        return unescapeField(stored, 2, sentinelIndex(stored, 2));
    }

    /**
     * Quotes an identifier for executable SQL and doubles embedded quote characters.
     */
    public static String quoteIdentifier(CharSequence identifier) {
        final StringSink sink = new StringSink();
        sink.putAscii('"');
        for (int i = 0, n = identifier.length(); i < n; i++) {
            final char c = identifier.charAt(i);
            if (c == '"') {
                sink.putAscii('"');
            }
            sink.put(c);
        }
        return sink.putAscii('"').toString();
    }

    /**
     * The window-function WHEN predicate text of a window policy: the stored predicate for {@link #isWindow},
     * or the desugared keep-max/min/top-N predicate for {@link #isKeepBy} (the {@code designatedTs} is used
     * only for the top-N ordering tiebreak; pass null to omit it). Returns null when {@code stored} is not a
     * window/keep-by policy.
     */
    public static String windowPredicate(CharSequence stored, CharSequence designatedTs) {
        if (isWindow(stored)) {
            return windowBody(stored).toString();
        }
        if (isKeepBy(stored)) {
            return buildKeepByPredicate(stored, designatedTs);
        }
        return null;
    }

    /**
     * Appends one field of a multi-field policy encoding, escaping the two characters the encoding reserves:
     * the field separator (0x1F) and the escape character (0x1E) itself. A quoted identifier accepts either
     * character, so the encoding escapes them here rather than the identifier grammar rejecting them - a name
     * an earlier release accepted must keep working.
     */
    private static void appendEscapedField(CharSink<?> sink, CharSequence field) {
        for (int i = 0, n = field.length(); i < n; i++) {
            final char c = field.charAt(i);
            if (c == POLICY_SENTINEL) {
                sink.put(POLICY_ESCAPE).put(ESCAPED_SENTINEL);
            } else if (c == POLICY_ESCAPE) {
                sink.put(POLICY_ESCAPE).put(ESCAPED_ESCAPE);
            } else {
                sink.put(c);
            }
        }
    }

    private static void appendKeepByClause(CharSink<?> sink, CharSequence stored) {
        // The keep column is stored UNQUOTED (the parser unquote()s it), so re-quote it when its name needs
        // quoting (spaces / non-identifier chars / leading digit) so SHOW CREATE round-trips. The PARTITION
        // BY keys are captured raw (with any quotes the user wrote) and already round-trip, so emit verbatim.
        final KeepBy k = new KeepBy(stored);
        sink.putAscii("KEEP ");
        if (k.n > 0) {
            sink.put(k.n).putAscii(' ');
        }
        sink.putAscii(k.isHighest ? "HIGHEST " : "LOWEST ");
        appendMaybeQuotedName(sink, k.col);
        if (k.keys.length() > 0) {
            sink.putAscii(" PARTITION BY ").put(k.keys);
        }
    }

    private static void appendKeepLatestClause(CharSink<?> sink, CharSequence stored) {
        sink.putAscii("KEEP LATEST");
        final CharSequence ts = keepLatestTs(stored);
        if (ts.length() > 0) {
            // ON <ts> is stored unquoted -> re-quote when needed for round-trip (see appendKeepByClause).
            sink.putAscii(" ON ");
            appendMaybeQuotedName(sink, ts);
        }
        sink.putAscii(" PARTITION BY ").put(keepLatestKeys(stored));
    }

    /**
     * Appends an identifier, double-quoting (and escaping internal quotes) only when its name is not a bare
     * identifier — so common lowercase names render cleanly while spaces / special chars / a leading digit
     * still round-trip through the grammar.
     */
    private static void appendMaybeQuotedName(CharSink<?> sink, CharSequence name) {
        if (!identifierNeedsQuoting(name)) {
            sink.put(name);
            return;
        }
        sink.putAscii('"');
        for (int i = 0, n = name.length(); i < n; i++) {
            final char c = name.charAt(i);
            if (c == '"') {
                sink.putAscii('"'); // escape an embedded quote by doubling it
            }
            sink.put(c);
        }
        sink.putAscii('"');
    }

    private static boolean identifierNeedsQuoting(CharSequence name) {
        final int n = name.length();
        if (n == 0) {
            return true;
        }
        for (int i = 0; i < n; i++) {
            final char c = name.charAt(i);
            final boolean bare = Character.isLetter(c) || c == '_' || c == '$' || (Character.isDigit(c) && i > 0);
            if (!bare) {
                return true;
            }
        }
        return false;
    }

    private static String buildKeepByPredicate(CharSequence stored, CharSequence designatedTs) {
        final KeepBy k = new KeepBy(stored);
        final StringSink sink = new StringSink();
        if (k.n == 0) {
            // keep every row tied at the group max/min: a row expires when its value is strictly past it.
            sink.put(quoteIdentifier(k.col)).putAscii(k.isHighest ? " < max(" : " > min(")
                    .put(quoteIdentifier(k.col)).putAscii(") OVER (");
            if (k.keys.length() > 0) {
                sink.putAscii("PARTITION BY ").put(k.keys);
            }
            sink.put(')');
        } else {
            // keep the top-N per group by the column; the designated timestamp makes the order total so the
            // boundary is deterministic (and the policy monotonic). NULL handling note: QuestDB has no NULLS
            // LAST, and where a NULL sorts is TYPE-DEPENDENT -- under DESC a floating-point NULL (NaN) sorts
            // FIRST (kept while within N) but an integer/timestamp NULL (a MIN sentinel) sorts LAST (expired
            // first); "<col> IS NULL" is not a legal window sort key so the position cannot be forced. A NULL
            // row may thus be kept or expired under KEEP <N>; use KEEP HIGHEST/LOWEST (no N) to always keep NULLs.
            sink.putAscii("row_number() OVER (");
            if (k.keys.length() > 0) {
                sink.putAscii("PARTITION BY ").put(k.keys).putAscii(' ');
            }
            sink.putAscii("ORDER BY ").put(quoteIdentifier(k.col)).putAscii(k.isHighest ? " DESC" : " ASC");
            if (designatedTs != null) {
                sink.putAscii(", ").put(quoteIdentifier(designatedTs)).putAscii(" DESC");
            }
            sink.putAscii(") > ").put(k.n);
        }
        return sink.toString();
    }

    private static boolean hasMode(CharSequence s, char mode) {
        return s != null && s.length() >= 2 && s.charAt(0) == POLICY_SENTINEL && s.charAt(1) == mode;
    }

    private static int sentinelIndex(CharSequence s, int from) {
        for (int i = from, n = s.length(); i < n; i++) {
            if (s.charAt(i) == POLICY_SENTINEL) {
                return i;
            }
        }
        return s.length();
    }

    /**
     * Reverses {@link #appendEscapedField} over {@code [lo, hi)}. A field carrying no escape - every field
     * of every ordinary policy - is returned as a sub-sequence, so the read-filter path allocates nothing.
     */
    private static CharSequence unescapeField(CharSequence stored, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (stored.charAt(i) == POLICY_ESCAPE) {
                return unescapeField0(stored, i, lo, hi);
            }
        }
        return stored.subSequence(lo, hi);
    }

    private static String unescapeField0(CharSequence stored, int firstEscape, int lo, int hi) {
        final StringSink sink = new StringSink();
        sink.put(stored, lo, firstEscape);
        for (int i = firstEscape; i < hi; i++) {
            final char c = stored.charAt(i);
            if (c == POLICY_ESCAPE && i + 1 < hi) {
                final char code = stored.charAt(i + 1);
                if (code == ESCAPED_SENTINEL) {
                    sink.put(POLICY_SENTINEL);
                    i++;
                    continue;
                }
                if (code == ESCAPED_ESCAPE) {
                    sink.put(POLICY_ESCAPE);
                    i++;
                    continue;
                }
            }
            // A lone escape char, or one followed by an unknown code, is not something the encoder emits;
            // pass it through verbatim rather than guessing what a corrupt policy string meant.
            sink.put(c);
        }
        return sink.toString();
    }

    private static CharSequence windowBody(CharSequence stored) {
        return stored.subSequence(2, stored.length());
    }

    /**
     * Decoded view of a KEEP [N] HIGHEST/LOWEST policy ({@code 0x1F 'N' n 0x1F dir 0x1F col 0x1F keys}), used
     * by SHOW CREATE, the catalogue functions, the read-filter rewrite and DDL validation. Construct it for a
     * policy {@link #isKeepBy} accepts.
     * <p>
     * Every field tolerates a truncated or otherwise malformed policy string: a missing separator reads as an
     * empty field and an unparseable {@code n} reads as 0. Such a string means a damaged {@code _meta} -
     * {@link #encodeKeepBy} never produces one - and it decodes as the bare KEEP HIGHEST/LOWEST form, the
     * shape under the stricter validation rules, so SHOW CREATE, the catalogue functions and the read filter
     * all have an answer for it.
     */
    public static final class KeepBy {
        /**
         * The keep column, unquoted.
         */
        public final String col;
        public final boolean isHighest;
        /**
         * The raw PARTITION BY column-list text, empty when the policy has no keys.
         */
        public final String keys;
        /**
         * The row count {@code N}, or 0 for the bare KEEP HIGHEST/LOWEST form that keeps every row tied at
         * the group extreme.
         */
        public final int n;

        public KeepBy(CharSequence stored) {
            final int len = stored.length();
            final int s1 = sentinelIndex(stored, 2);
            final int s2 = sentinelIndex(stored, s1 + 1);
            final int s3 = sentinelIndex(stored, s2 + 1);
            int n;
            try {
                n = Numbers.parseInt(stored, 2, s1);
            } catch (NumericException e) {
                n = 0;
            }
            this.n = n;
            this.isHighest = s1 + 1 < len && stored.charAt(s1 + 1) == DIR_HIGHEST;
            this.col = unescapeField(stored, Math.min(s2 + 1, len), s3).toString();
            this.keys = unescapeField(stored, Math.min(s3 + 1, len), len).toString();
        }
    }
}
