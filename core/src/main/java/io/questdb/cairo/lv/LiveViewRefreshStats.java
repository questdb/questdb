/*+*****************************************************************************
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

package io.questdb.cairo.lv;

/**
 * Lifetime counters of one {@link LiveViewRefreshJob}, read by tests and by the steady-state
 * benchmark to tell which branch a refresh took. No refresh decision reads them. Each counter
 * is a slot in one flat array, so an update allocates nothing. Not thread-safe: only the
 * owning worker writes it.
 */
public final class LiveViewRefreshStats {
    // WAL-loss re-derives that completed only after recompiling against drifted base metadata.
    public static final int BASE_METADATA_DRIFT_RECOMPILE_COUNT = 0;
    // Replay turns run on the isolated repair runtime rather than through the primary one.
    public static final int ISOLATED_REPLAY_TURN_COUNT = 1;
    // Key-domain splices declined because the interval held a root a partial publication
    // cannot build on.
    public static final int KEY_DOMAIN_SPLICE_DECLINE_COUNT = 2;
    // Stored output rows keyed closed-segment repairs copied forward instead of recomputing.
    public static final int KEYED_REPLAY_MERGED_ROWS = 3;
    // Closed segments whose repair started following the correction's keys.
    public static final int KEYED_REPLAY_SEGMENT_COUNT = 4;
    // Priced closed segments whose keyed scan reads less than the whole segment.
    public static final int KEYED_SCAN_CHEAPER_COUNT = 5;
    // Posting rows a keyed scan of every priced closed segment would read; a floor where the
    // count stopped at the whole-range price.
    public static final int KEYED_SCAN_POSTING_ROWS = 6;
    public static final int KEYED_SCAN_PRICED_COUNT = 7;
    public static final int KEYED_SCAN_UNPRICED_COUNT = 8;
    // Base rows a whole-segment scan of every priced closed segment reads.
    public static final int KEYED_SCAN_WHOLE_RANGE_ROWS = 9;
    // Keyed resumes and cold keyed replays that derived checkpoint row positions from the
    // exact insert delta instead of scanning stored rows.
    public static final int OPEN_SEGMENT_ARITHMETIC_ROW_POSITION_COUNT = 10;
    // The OPEN_SEGMENT_COLD_KEYED_* slots price and count the open segment from a cold
    // anchor-segment origin, when no checkpoint root sits below the correction.
    public static final int OPEN_SEGMENT_COLD_KEYED_CHEAPER_COUNT = 11;
    public static final int OPEN_SEGMENT_COLD_KEYED_POSTING_ROWS = 12;
    public static final int OPEN_SEGMENT_COLD_KEYED_PRICED_COUNT = 13;
    // Cold keyed head-miss replays started.
    public static final int OPEN_SEGMENT_COLD_KEYED_REPLAY_COUNT = 14;
    public static final int OPEN_SEGMENT_COLD_KEYED_UNPRICED_COUNT = 15;
    public static final int OPEN_SEGMENT_COLD_KEYED_WHOLE_RANGE_ROWS = 16;
    // The OPEN_SEGMENT_KEYED_* slots price the open segment's resume from a checkpoint root.
    public static final int OPEN_SEGMENT_KEYED_CHEAPER_COUNT = 17;
    // Posting rows the priced resumes' keyed scans would read; a floor where the count
    // stopped at its budget.
    public static final int OPEN_SEGMENT_KEYED_POSTING_ROWS = 18;
    public static final int OPEN_SEGMENT_KEYED_PRICED_COUNT = 19;
    // Resumes started following the correction's keys rather than reading every row above
    // the anchor.
    public static final int OPEN_SEGMENT_KEYED_RESUME_COUNT = 20;
    public static final int OPEN_SEGMENT_KEYED_UNPRICED_COUNT = 21;
    public static final int OPEN_SEGMENT_KEYED_WHOLE_RANGE_ROWS = 22;
    // Priced resumes the keyed scan won only once the root restore beside it was counted.
    public static final int OPEN_SEGMENT_RESTORE_AWARE_CHEAPER_COUNT = 23;
    // Keyed resumes that published as an upsert on the view's own dedup keys.
    public static final int OPEN_SEGMENT_SPARSE_RESUME_COUNT = 24;
    // The OUTPUT_UNIQUENESS_* slots describe the (timestamp, projected key) check a segment
    // repair's output went through.
    public static final int OUTPUT_UNIQUENESS_CHECKED_REPAIRS = 25;
    public static final int OUTPUT_UNIQUENESS_CHECKED_ROWS = 26;
    public static final int OUTPUT_UNIQUENESS_DUPLICATE_ROWS = 27;
    // The widest equal-timestamp group any checked repair emitted; a maximum, not a sum.
    public static final int OUTPUT_UNIQUENESS_MAX_GROUP_ROWS = 28;
    // Repairs whose output carries no key the check can name.
    public static final int OUTPUT_UNIQUENESS_UNCHECKED_REPAIRS = 29;
    public static final int OUTPUT_UNIQUENESS_UNIQUE_REPAIRS = 30;
    // O3 resumes served from the live window state instead of restoring the same root.
    public static final int RUNTIME_ANCHOR_REUSE_COUNT = 31;
    // Base rows the change-set decomposition read one by one out of their WAL segments,
    // rather than folding their commit's span into the residual whole.
    public static final int SEGMENT_CLASSIFY_WALKED_ROWS = 32;
    // Closed anchor segments repaired over their own range whose replacement applied.
    public static final int SEGMENT_REPAIR_COUNT = 33;
    // Segment replays parked on the refresh turn's budget.
    public static final int SEGMENT_YIELD_COUNT = 34;
    // Segment repairs and keyed resumes published as an upsert on the view's dedup keys.
    public static final int SPARSE_PUBLICATION_COUNT = 35;
    // Sparse publications abandoned before commit for a whole-range replacement.
    public static final int SPARSE_PUBLICATION_FALLBACK_COUNT = 36;
    // Stored rows sparse publications left in place.
    public static final int SPARSE_PUBLICATION_ROWS_KEPT = 37;
    // Attempts to take back the symbol ids a discarded pass assigned and no flush committed,
    // each one a call of LiveViewInMemoryTier.tryRewindSymbolCache.
    public static final int SYMBOL_REWIND_ATTEMPT_COUNT = 38;
    // Of those attempts, the ones a reader refused by pinning a slot whose symbol horizon
    // reaches the ids the rewind would take back.
    public static final int SYMBOL_REWIND_REFUSAL_COUNT = 39;
    // Keys keyed resumes handed back to the primary runtime.
    public static final int TRANSPLANTED_KEY_COUNT = 40;
    private static final int STAT_COUNT = 41;
    // Completed repairs by route, one slot per LiveViewCheckpointRepairRoute.ROUTE_*, after the
    // counters above. A repair counts once, on the turn that publishes it.
    private final long[] values = new long[STAT_COUNT + LiveViewCheckpointRepairRoute.ROUTE_COUNT];

    public void add(int stat, long delta) {
        values[stat] += delta;
    }

    public long get(int stat) {
        return values[stat];
    }

    /**
     * @param route one of {@code LiveViewCheckpointRepairRoute.ROUTE_*}
     * @return completed repairs that published on that route
     */
    public long getRouteCount(int route) {
        return values[STAT_COUNT + route];
    }

    public void inc(int stat) {
        values[stat]++;
    }

    public void incRoute(int route) {
        values[STAT_COUNT + route]++;
    }

    public void max(int stat, long value) {
        if (value > values[stat]) {
            values[stat] = value;
        }
    }
}
