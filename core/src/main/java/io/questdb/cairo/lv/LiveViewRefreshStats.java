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
    // Cycles without rows that put a cleared head back on the root a repair kept at the view's
    // newest row, at the cycle's commit: the repair's re-stamp of the head on that root had
    // declined.
    public static final int CLEARED_HEAD_PUBLICATION_COUNT = 1;
    // Zero-row end-of-table head-miss rebuilds that restored the newest complete root below
    // the view's new frontier and folded the base rows above it to derive the frontier's state.
    public static final int HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT = 2;
    // Zero-row end-of-table head-miss rebuilds that derived the frontier's state by folding
    // the base rows from the frontier's dependency floor up to it into an identity runtime.
    public static final int HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT = 3;
    // Roots an end-of-table head miss's chain had frozen before its key budget refused a later
    // boundary, all discarded with the chain.
    public static final int HEAD_MISS_SPLICE_DISCARDED_ROOT_COUNT = 4;
    // Head-miss rebuilds whose end-of-table splice kept roots above the view's new frontier,
    // so they truncated the spliced timeline at that frontier.
    public static final int HEAD_MISS_SPLICE_FRONTIER_TRUNCATE_COUNT = 5;
    // End-of-table head misses whose chain the key budget refused at a boundary, so they
    // truncated instead: the keys the boundaries would image whole passed the larger of the
    // replay's estimated base rows and cairo.live.view.checkpoint.repair.scan.max.keys.
    public static final int HEAD_MISS_SPLICE_KEY_BUDGET_DECLINE_COUNT = 6;
    // Head-miss rebuilds whose end-of-table splice emitted no row and kept the spliced
    // timeline as it stood, its newest root sitting at the view's new frontier.
    public static final int HEAD_MISS_SPLICE_ZERO_ROW_KEEP_COUNT = 7;
    // Head-miss rebuilds whose end-of-table splice emitted no row with no root at the view's
    // new frontier a restart could restore and no state derived there, so they retired the
    // timeline.
    public static final int HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT = 8;
    // Zero-row end-of-table head-miss rebuilds holding no capture that put the head on a root
    // at the view's new frontier at the repair commit, instead of retiring the truncated
    // timeline.
    public static final int HEAD_MISS_TRUNCATE_ZERO_ROW_KEEP_COUNT = 9;
    // Zero-row end-of-table head-miss rebuilds whose seal of the state derived at the view's new
    // frontier landed a root there, instead of retiring the timeline.
    public static final int HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT = 10;
    // Replay turns run on the isolated repair runtime rather than through the primary one.
    public static final int ISOLATED_REPLAY_TURN_COUNT = 11;
    // Key-domain splices declined because the interval held a root a partial publication
    // cannot build on.
    public static final int KEY_DOMAIN_SPLICE_DECLINE_COUNT = 12;
    // Stored output rows keyed closed-segment repairs copied forward instead of recomputing.
    public static final int KEYED_REPLAY_MERGED_ROWS = 13;
    // Closed segments whose repair started following the correction's keys.
    public static final int KEYED_REPLAY_SEGMENT_COUNT = 14;
    // Priced closed segments whose keyed scan reads less than the whole segment.
    public static final int KEYED_SCAN_CHEAPER_COUNT = 15;
    // Posting rows a keyed scan of every priced closed segment would read; a floor where the
    // count stopped at the whole-range price.
    public static final int KEYED_SCAN_POSTING_ROWS = 16;
    public static final int KEYED_SCAN_PRICED_COUNT = 17;
    public static final int KEYED_SCAN_UNPRICED_COUNT = 18;
    // Base rows a whole-segment scan of every priced closed segment reads.
    public static final int KEYED_SCAN_WHOLE_RANGE_ROWS = 19;
    // Keyed resumes and cold keyed replays that derived checkpoint row positions from the
    // exact insert delta instead of scanning stored rows.
    public static final int OPEN_SEGMENT_ARITHMETIC_ROW_POSITION_COUNT = 20;
    // Open-segment keyed routes declined because a base row at or above the repair's floor may
    // be one an ATTACH PARTITION brought in, which the stored-row count cannot tell from a row
    // the view consumed.
    public static final int OPEN_SEGMENT_ATTACH_DECLINE_COUNT = 21;
    // The OPEN_SEGMENT_COLD_KEYED_* slots price and count the open segment from a cold
    // anchor-segment origin, when no checkpoint root sits below the correction.
    public static final int OPEN_SEGMENT_COLD_KEYED_CHEAPER_COUNT = 22;
    public static final int OPEN_SEGMENT_COLD_KEYED_POSTING_ROWS = 23;
    public static final int OPEN_SEGMENT_COLD_KEYED_PRICED_COUNT = 24;
    // Cold keyed head-miss replays started.
    public static final int OPEN_SEGMENT_COLD_KEYED_REPLAY_COUNT = 25;
    public static final int OPEN_SEGMENT_COLD_KEYED_UNPRICED_COUNT = 26;
    public static final int OPEN_SEGMENT_COLD_KEYED_WHOLE_RANGE_ROWS = 27;
    // The OPEN_SEGMENT_KEYED_* slots price the open segment's resume from a checkpoint root.
    public static final int OPEN_SEGMENT_KEYED_CHEAPER_COUNT = 28;
    // Posting rows the priced resumes' keyed scans would read; a floor where the count
    // stopped at its budget.
    public static final int OPEN_SEGMENT_KEYED_POSTING_ROWS = 29;
    public static final int OPEN_SEGMENT_KEYED_PRICED_COUNT = 30;
    // Resumes started following the correction's keys rather than reading every row above
    // the anchor.
    public static final int OPEN_SEGMENT_KEYED_RESUME_COUNT = 31;
    public static final int OPEN_SEGMENT_KEYED_UNPRICED_COUNT = 32;
    public static final int OPEN_SEGMENT_KEYED_WHOLE_RANGE_ROWS = 33;
    // Priced resumes the keyed scan won only once the root restore beside it was counted.
    public static final int OPEN_SEGMENT_RESTORE_AWARE_CHEAPER_COUNT = 34;
    // Keyed resumes that published as an upsert on the view's own dedup keys.
    public static final int OPEN_SEGMENT_SPARSE_RESUME_COUNT = 35;
    // The OUTPUT_UNIQUENESS_* slots describe the (timestamp, projected key) check a segment
    // repair's output went through.
    public static final int OUTPUT_UNIQUENESS_CHECKED_REPAIRS = 36;
    public static final int OUTPUT_UNIQUENESS_CHECKED_ROWS = 37;
    public static final int OUTPUT_UNIQUENESS_DUPLICATE_ROWS = 38;
    // The widest equal-timestamp group any checked repair emitted; a maximum, not a sum.
    public static final int OUTPUT_UNIQUENESS_MAX_GROUP_ROWS = 39;
    // Repairs whose output carries no key the check can name.
    public static final int OUTPUT_UNIQUENESS_UNCHECKED_REPAIRS = 40;
    public static final int OUTPUT_UNIQUENESS_UNIQUE_REPAIRS = 41;
    // Repair roots a seal removed from below the previous root: roots a repair's closing seal
    // wrote only because it owed one at its frontier, once two newer roots stood above them.
    public static final int REPAIR_ROOT_REMOVAL_COUNT = 42;
    // Roots a resume's chain had frozen before its key budget refused a later boundary, all
    // discarded with the chain.
    public static final int RESUME_CHAIN_DISCARDED_ROOT_COUNT = 43;
    // Anchor resumes whose chain the key budget refused at a boundary, so they truncated
    // instead: the keys the boundaries would image whole passed the larger of the resume's
    // estimated base rows and cairo.live.view.checkpoint.repair.scan.max.keys.
    public static final int RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT = 44;
    // Anchor resumes that emitted no row from an anchor above the view's new frontier and put
    // the frontier's state in the runtime by restoring a root at or below the frontier, then
    // folding the base rows above that root up to the frontier.
    public static final int RESUME_FRONTIER_STATE_RESTORE_COUNT = 45;
    // Anchor resumes that emitted no row from an anchor above the view's new frontier and
    // derived the frontier's state by folding the base rows from the frontier's dependency
    // floor, or from the view's START FROM boundary, up to it into an identity runtime.
    public static final int RESUME_FRONTIER_STATE_WARM_UP_COUNT = 46;
    // O3 resumes served from the live window state instead of restoring the same root.
    public static final int RUNTIME_ANCHOR_REUSE_COUNT = 47;
    // Seals declined because their boundary sits above the newest row the view's table holds,
    // which no restart can restore a root from.
    public static final int SEAL_ABOVE_DURABLE_FRONTIER_DECLINE_COUNT = 48;
    // Base rows the change-set decomposition read one by one out of their WAL segments,
    // rather than folding their commit's span into the residual whole.
    public static final int SEGMENT_CLASSIFY_WALKED_ROWS = 49;
    // Closed anchor segments repaired over their own range whose replacement applied.
    public static final int SEGMENT_REPAIR_COUNT = 50;
    // Segment replays parked on the refresh turn's budget.
    public static final int SEGMENT_YIELD_COUNT = 51;
    // Segment repairs and keyed resumes published as an upsert on the view's dedup keys.
    public static final int SPARSE_PUBLICATION_COUNT = 52;
    // Sparse publications abandoned before commit for a whole-range replacement.
    public static final int SPARSE_PUBLICATION_FALLBACK_COUNT = 53;
    // Stored rows sparse publications left in place.
    public static final int SPARSE_PUBLICATION_ROWS_KEPT = 54;
    // Anchor resumes whose output ended below the newest root their splice kept, so they
    // truncated the spliced timeline at the view's new frontier before the head seal.
    public static final int SPLICE_FRONTIER_TRUNCATE_COUNT = 55;
    // Anchor resumes that emitted no row over a splice whose newest root sits at the view's
    // new frontier, and re-stamped the head on that root.
    public static final int SPLICE_ZERO_ROW_KEEP_COUNT = 56;
    // Attempts to take back the symbol ids a discarded pass assigned and no flush committed,
    // each one a call of LiveViewInMemoryTier.tryRewindSymbolCache.
    public static final int SYMBOL_REWIND_ATTEMPT_COUNT = 57;
    // Of those attempts, the ones a reader refused by pinning a slot whose symbol horizon
    // reaches the ids the rewind would take back.
    public static final int SYMBOL_REWIND_REFUSAL_COUNT = 58;
    // Keys keyed resumes handed back to the primary runtime.
    public static final int TRANSPLANTED_KEY_COUNT = 59;
    // Anchor resumes holding no capture that emitted no row and put the head on a root at the
    // view's new frontier at the repair commit, instead of retiring the truncated timeline.
    public static final int TRUNCATE_ZERO_ROW_KEEP_COUNT = 60;
    private static final int STAT_COUNT = 61;
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
