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
 * Shared helpers for the row-expiry feature. The stored expiry predicate describes WHEN a row
 * expires; both the read-time filter (in {@code SqlParser}) and the background cleanup job
 * ({@link RowExpiryCleanupJob}) need the logical negation of it (the "keep" filter), so the helper
 * lives here to keep the two in lockstep.
 */
public final class RowExpiryUtil {

    private RowExpiryUtil() {
    }

    /**
     * Builds the keep-rows filter for a row-expiry predicate. The stored predicate describes WHEN
     * a row expires, so the kept rows are the negation of it. For the common timestamp-vs-now()
     * shapes ({@code col < now()}, {@code col <= now()}, {@code now() > col}, {@code now() >= col})
     * we emit the equivalent {@code col >= now()} / {@code col > now()} form so the planner can use
     * an interval/timestamp scan; otherwise we fall back to a generic {@code NOT (...)} wrap.
     * <p>
     * Ported verbatim from the expiring-view draft (193dddfd0c) buildTimestampCompareFilter.
     */
    public static String buildRowExpiryKeepFilter(String predicate) {
        final String trimmed = predicate.trim();
        final String lower = trimmed.toLowerCase();

        if (lower.endsWith("< now()") || lower.endsWith("<now()")) {
            final String col = trimmed.substring(0, trimmed.lastIndexOf('<')).trim();
            return col + " >= now()";
        }
        if (lower.endsWith("<= now()") || lower.endsWith("<=now()")) {
            final String col = trimmed.substring(0, trimmed.lastIndexOf('<')).trim();
            return col + " > now()";
        }
        if (lower.startsWith("now() >")) {
            String rest = trimmed.substring(7).trim();
            if (rest.startsWith("=")) {
                // now() >= col means col <= now() is expired, so keep NOT(col <= now()) = col > now()
                rest = rest.substring(1).trim();
                return rest + " > now()";
            }
            // now() > col means col < now() is expired, so keep col >= now()
            return rest + " >= now()";
        }
        // Fallback to generic negation
        return "NOT (" + trimmed + ")";
    }
}
