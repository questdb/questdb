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

/**
 * Shared helper for the row-expiry feature's background cleanup job ({@link RowExpiryCleanupJob}).
 * The stored expiry predicate describes WHEN a row expires; the cleanup job needs the logical
 * negation of it (the "keep" filter) to select the surviving rows.
 * <p>
 * The read-time filter does NOT use this helper: it negates the predicate on the parsed expression
 * tree (see {@code SqlParser.expandExpiringTable}) so the optimiser can prune partitions. The
 * cleanup job's survivor query is already restricted to a single partition, so it has no such need,
 * and a plain {@code NOT(...)} wrap — correct for every predicate shape — is sufficient here.
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
     * Builds the keep-rows filter (the rows that have NOT expired) for the cleanup job: the negation
     * of the stored expiry predicate. Always {@code NOT (<predicate>)} — correct for any predicate,
     * including compound ones. The predicate is wrapped in parentheses so its internal operator
     * precedence cannot leak past the NOT.
     */
    public static String buildRowExpiryKeepFilter(String predicate) {
        return "NOT (" + predicate.trim() + ")";
    }
}
