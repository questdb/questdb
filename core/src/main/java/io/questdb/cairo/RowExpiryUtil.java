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

    private RowExpiryUtil() {
    }

    /**
     * Renders {@code micros} as a {@code CLEANUP EVERY} stride (e.g. {@code 30m}, {@code 1h},
     * {@code 2d}) into {@code sink}, picking the largest whole unit that divides it evenly. Shared by
     * SHOW CREATE TABLE and the {@code tables()}/{@code materialized_views()} catalogue functions so
     * the rendering cannot drift.
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
}
