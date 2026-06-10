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
 * The sentinel cannot occur in a real predicate, so a legacy raw predicate always decodes as a scalar WHEN
 * (backward compatible). Scalar WHEN is the only mode usable on a plain table — every other mode is
 * passthrough-materialized-view-only.
 */
public final class RowExpiryUtil {

    /**
     * Default {@code CLEANUP EVERY} cadence (1 hour) used when the clause is omitted. Shared so the
     * parser default and the SHOW CREATE "omit when default" check (and any divisor) cannot drift.
     */
    public static final long DEFAULT_CLEANUP_INTERVAL_MICROS = 3_600_000_000L;

    /**
     * Synthetic boolean column name used by the projection-CASE read filter / cleanup for window and keep-by
     * policies: the inner projection computes {@code CASE WHEN (<pred>) THEN false ELSE true END} as this
     * column and the outer query filters on it. Unlikely to collide with a real user column.
     */
    public static final String KEEP_COLUMN = "__qdb_re_keep";

    private static final char DIR_HIGHEST = 'H';
    private static final char DIR_LOWEST = 'O';
    private static final char MODE_KEEP_BY = 'N';     // keep-max/min (n=0) or top-N (n>0), structural
    private static final char MODE_KEEP_LATEST = 'L'; // keep only the latest row per key
    private static final char MODE_WINDOW = 'W';      // an arbitrary window-function WHEN predicate
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
     * Builds the keep-rows filter (the rows that have NOT expired) for the cleanup job's scalar-WHEN path:
     * {@code CASE WHEN (<predicate>) THEN false ELSE true END}, which keeps rows for which the predicate is
     * FALSE or NULL. The predicate is wrapped in parentheses so its internal operator precedence cannot
     * leak. Correct for any predicate shape, including compound ones, {@code IN}, and NULL operands.
     */
    public static String buildRowExpiryKeepFilter(String predicate) {
        return "CASE WHEN (" + predicate.trim() + ") THEN false ELSE true END";
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
        if (isKeepLatest(stored) || isKeepBy(stored)) {
            final StringSink sink = new StringSink();
            appendExpireClause(sink, stored);
            // appendExpireClause emits the full "KEEP ..." clause body for these modes (no leading "WHEN").
            return sink.toString();
        }
        if (isWindow(stored)) {
            return windowBody(stored).toString();
        }
        return stored.toString();
    }

    public static String encodeKeepBy(int n, boolean highest, CharSequence col, CharSequence keysCsv) {
        return KEEP_BY_PREFIX + n + POLICY_SENTINEL + (highest ? DIR_HIGHEST : DIR_LOWEST)
                + POLICY_SENTINEL + col + POLICY_SENTINEL + keysCsv;
    }

    public static String encodeKeepLatest(CharSequence ts, CharSequence keysCsv) {
        // body = <ts-or-empty> SEP <keys>; an empty ts means "use the designated timestamp".
        return KEEP_LATEST_PREFIX + (ts == null ? "" : ts) + POLICY_SENTINEL + keysCsv;
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

    /** True if {@code stored} is an encoded KEEP [N] HIGHEST/LOWEST policy. */
    public static boolean isKeepBy(CharSequence stored) {
        return hasMode(stored, MODE_KEEP_BY);
    }

    /** True if {@code stored} is an encoded KEEP LATEST policy. */
    public static boolean isKeepLatest(CharSequence stored) {
        return hasMode(stored, MODE_KEEP_LATEST);
    }

    /** True if {@code stored} is an encoded window-function WHEN policy. */
    public static boolean isWindow(CharSequence stored) {
        return hasMode(stored, MODE_WINDOW);
    }

    /** The raw PARTITION BY column-list text of an encoded KEEP LATEST policy (check {@link #isKeepLatest}). */
    public static CharSequence keepLatestKeys(CharSequence stored) {
        return stored.subSequence(sentinelIndex(stored, 2) + 1, stored.length());
    }

    /** The explicit {@code ON <ts>} column of a KEEP LATEST policy, or empty when none was specified. */
    public static CharSequence keepLatestTs(CharSequence stored) {
        return stored.subSequence(2, sentinelIndex(stored, 2));
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

    private static void appendKeepByClause(CharSink<?> sink, CharSequence stored) {
        final KeepBy k = new KeepBy(stored);
        sink.putAscii("KEEP ");
        if (k.n > 0) {
            sink.put(k.n).putAscii(' ');
        }
        sink.putAscii(k.highest ? "HIGHEST " : "LOWEST ").put(k.col);
        if (k.keys.length() > 0) {
            sink.putAscii(" PARTITION BY ").put(k.keys);
        }
    }

    private static void appendKeepLatestClause(CharSink<?> sink, CharSequence stored) {
        sink.putAscii("KEEP LATEST");
        final CharSequence ts = keepLatestTs(stored);
        if (ts.length() > 0) {
            sink.putAscii(" ON ").put(ts);
        }
        sink.putAscii(" PARTITION BY ").put(keepLatestKeys(stored));
    }

    private static String buildKeepByPredicate(CharSequence stored, CharSequence designatedTs) {
        final KeepBy k = new KeepBy(stored);
        final StringSink sink = new StringSink();
        if (k.n == 0) {
            // keep every row tied at the group max/min: a row expires when its value is strictly past it.
            sink.put('"').put(k.col).put('"').putAscii(k.highest ? " < max(\"" : " > min(\"")
                    .put(k.col).putAscii("\") OVER (");
            if (k.keys.length() > 0) {
                sink.putAscii("PARTITION BY ").put(k.keys);
            }
            sink.put(')');
        } else {
            // keep the top-N per group by the column; the designated timestamp makes the order total so the
            // boundary is deterministic (and the policy monotonic). NULL handling note: QuestDB's window
            // ORDER BY has no NULLS LAST and sorts NULLs FIRST under DESC, so NULL-valued rows occupy the
            // leading ranks and are kept while there is room within N (they cannot be pushed to the tail to
            // rank only real values -- "<col> IS NULL" is not a legal window sort key). Top-N therefore keeps
            // up to N NULLs ahead of real values; use KEEP HIGHEST/LOWEST (no N) when every NULL must be kept.
            sink.putAscii("row_number() OVER (");
            if (k.keys.length() > 0) {
                sink.putAscii("PARTITION BY ").put(k.keys).putAscii(' ');
            }
            sink.putAscii("ORDER BY \"").put(k.col).putAscii(k.highest ? "\" DESC" : "\" ASC");
            if (designatedTs != null) {
                sink.putAscii(", \"").put(designatedTs).putAscii("\" DESC");
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

    private static CharSequence windowBody(CharSequence stored) {
        return stored.subSequence(2, stored.length());
    }

    /** Decoded view of a KEEP [N] HIGHEST/LOWEST policy ({@code 0x1F 'N' n 0x1F dir 0x1F col 0x1F keys}). */
    private static final class KeepBy {
        final String col;
        final boolean highest;
        final String keys;
        final int n;

        KeepBy(CharSequence stored) {
            final String body = stored.subSequence(2, stored.length()).toString();
            final int s1 = body.indexOf(POLICY_SENTINEL);
            final int s2 = body.indexOf(POLICY_SENTINEL, s1 + 1);
            final int s3 = body.indexOf(POLICY_SENTINEL, s2 + 1);
            this.n = Integer.parseInt(body.substring(0, s1));
            this.highest = body.charAt(s1 + 1) == DIR_HIGHEST;
            this.col = body.substring(s2 + 1, s3);
            this.keys = body.substring(s3 + 1);
        }
    }
}
