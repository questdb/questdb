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
 * Shared helper for the row-expiry feature's background cleanup job ({@link RowExpiryCleanupJob}).
 * The stored expiry predicate describes WHEN a row expires; the cleanup job needs the complementary
 * "keep" filter (the rows that have NOT expired) to select the surviving rows.
 * <p>
 * A row expires only when the predicate is TRUE, so the keep-filter is
 * {@code CASE WHEN (predicate) THEN false ELSE true END}, which keeps rows for which the predicate is
 * FALSE or NULL. A plain {@code NOT(predicate)} would be wrong: QuestDB filtering is three-valued, so for
 * a NULL predicate operand the predicate is UNKNOWN and {@code NOT(UNKNOWN)} is UNKNOWN — the row would be
 * dropped (deleted) even though it never expired. ({@code (predicate) IS NOT TRUE} handles NULL but is
 * unreliable for composite booleans such as {@code IN}, so the CASE form is used.) This matches the
 * read-time filter (see {@code SqlParser.buildKeepFilter}); the read filter additionally flips a
 * designated-timestamp comparison to a bare comparison so the optimiser can prune partitions, which the
 * cleanup's already-single-partition survivor query does not need.
 */
public final class RowExpiryUtil {

    /**
     * Default {@code CLEANUP EVERY} cadence (1 hour) used when the clause is omitted. Shared so the
     * parser default and the SHOW CREATE "omit when default" check (and any divisor) cannot drift.
     */
    public static final long DEFAULT_CLEANUP_INTERVAL_MICROS = 3_600_000_000L;

    // The EXPIRE ROWS policy is persisted as a single string in the table's _meta {@code expiryPredicate}
    // slot. A plain (legacy) string is a boolean WHEN predicate. The relative "KEEP LATEST" mode (a
    // passthrough mat view keeping only the latest row per key) cannot be expressed as a per-row predicate,
    // so it is encoded with a non-printable sentinel prefix (unit-separator 0x1F, then 'L') followed by the
    // raw PARTITION BY column-list text. Encoding here (rather than adding a new _meta field) keeps the
    // storage, replication and metadata-interface plumbing unchanged. The sentinel cannot occur in a real
    // predicate, so a legacy raw predicate always decodes as a WHEN predicate (backward compatible).
    private static final char KEEP_LATEST_MODE = 'L';
    private static final char POLICY_SENTINEL = (char) 0x1F;
    private static final String KEEP_LATEST_PREFIX = String.valueOf(POLICY_SENTINEL) + KEEP_LATEST_MODE;

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
     * Builds the keep-rows filter (the rows that have NOT expired) for the cleanup job:
     * {@code CASE WHEN (<predicate>) THEN false ELSE true END}, which keeps rows for which the predicate is
     * FALSE or NULL. The predicate is wrapped in parentheses so its internal operator precedence cannot
     * leak. Correct for any predicate shape, including compound ones, {@code IN}, and NULL operands (see
     * the class note on why a plain {@code NOT(...)} would wrongly delete not-expired rows whose predicate
     * column is NULL).
     */
    public static String buildRowExpiryKeepFilter(String predicate) {
        return "CASE WHEN (" + predicate.trim() + ") THEN false ELSE true END";
    }

    /**
     * Human-readable rendering of a stored policy for catalogue functions ({@code tables()},
     * {@code materialized_views()}): the KEEP LATEST clause body for the relative mode, or the raw predicate
     * (unchanged) otherwise. Returns null for no policy.
     */
    public static String displayPredicate(CharSequence stored) {
        if (stored == null) {
            return null;
        }
        if (isKeepLatest(stored)) {
            return "KEEP LATEST PARTITION BY " + keepLatestKeys(stored);
        }
        return stored.toString();
    }

    /**
     * Encodes a {@code KEEP LATEST PARTITION BY <keysCsv>} policy for storage in the {@code expiryPredicate}
     * slot (see the {@link #KEEP_LATEST_PREFIX} note). {@code keysCsv} is the raw, comma-separated column
     * list as written by the user (quoting preserved).
     */
    public static String encodeKeepLatest(CharSequence keysCsv) {
        return KEEP_LATEST_PREFIX + keysCsv;
    }

    /**
     * Renders the cleanup cadence as a stride string (e.g. {@code 30m}, {@code 1h}, {@code 2d}), or null
     * when there is no policy ({@code micros <= 0}). Shared by the {@code tables()} and
     * {@code materialized_views()} catalogue functions. Allocates a String — acceptable on the cold
     * catalogue path; use {@link #appendCleanupEvery} to render into an existing sink.
     */
    public static String formatCleanupEvery(long micros) {
        if (micros <= 0) {
            return null;
        }
        final StringSink sink = new StringSink();
        appendCleanupEvery(sink, micros);
        return sink.toString();
    }

    /** True if {@code stored} is an encoded KEEP LATEST policy (vs a plain WHEN predicate or no policy). */
    public static boolean isKeepLatest(CharSequence stored) {
        return stored != null && stored.length() >= 2
                && stored.charAt(0) == POLICY_SENTINEL && stored.charAt(1) == KEEP_LATEST_MODE;
    }

    /** The raw PARTITION BY column-list text of an encoded KEEP LATEST policy (check {@link #isKeepLatest} first). */
    public static CharSequence keepLatestKeys(CharSequence stored) {
        return stored.subSequence(2, stored.length());
    }
}
