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

package io.questdb.cairo.wal;

import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;

import java.util.Arrays;

/**
 * Transaction clustering: it decides WHERE to pre-split a partition, from the shape of the
 * incoming work rather than from any one batch.
 * <p>
 * A WAL block apply merging K transactions into an existing partition often carries data that is
 * dense in a few time strides and empty between them. This class bins the partition's data range
 * into fixed-duration strides, marks every bin covered by any incoming transaction's
 * {@code [minTs, maxTs]} range as HOT (a coverage histogram built with a difference array -
 * O(1) per transaction regardless of range width), and returns cut timestamps at the edges of the
 * COLD gaps that are worth keeping as zero-copy pieces. The caller pre-splits the partition at
 * those cuts, so the O3 merge rewrites only the hot strides while the cold gaps between them stay
 * untouched offset views of the shared column files - pieces that cost nothing to keep.
 * <p>
 * A cold gap qualifies for cutting only when it is estimated (uniform-density assumption) to hold
 * at least {@code minGapRows} existing rows; smaller gaps are folded into the neighbouring hot
 * stride (merged through). The estimate drives only the cut DECISION - the caller converts each
 * cut timestamp into an exact row offset by binary-searching the designated timestamp column.
 * <p>
 * O(K + B) time, zero allocation in steady state (scratch buffers are reused).
 */
public class WalTxnClusterer implements Mutable {
    private final LongList cutTimestamps = new LongList();
    // Flat triples per qualifying cold gap: [estimated rows, first cut ts, second cut ts or Long.MIN_VALUE].
    private final LongList gapScratch = new LongList();
    // Flat pairs: incoming txn [minTs, maxTs], clipped by the caller to the partition's data range.
    private final LongList txnRanges = new LongList();
    private int[] cover = new int[0];

    /**
     * Buffers one incoming transaction's timestamp range. The caller clips the range to the
     * partition's data range and skips transactions that do not intersect it.
     */
    public void addTxnRange(long minTs, long maxTs) {
        txnRanges.add(minTs, maxTs);
    }

    @Override
    public void clear() {
        txnRanges.clear();
    }

    /**
     * Computes cut timestamps for one partition against the buffered transaction ranges.
     *
     * @param t0                first existing row timestamp of the partition (piece)
     * @param t1                last existing row timestamp, inclusive; {@code t1 >= t0}
     * @param minBinDuration    finest bin duration (e.g. one minute in the table's timestamp
     *                          resolution); widened when the span exceeds {@code maxBins} bins
     * @param maxBins           bin-count cap, bounding work and cut precision
     * @param minGapRows        minimum estimated existing rows for a cold gap to be worth a cut
     * @param partitionRowCount existing rows in [t0, t1], for the uniform-density estimate
     * @param maxCuts           cut budget; when exceeded, the largest gaps win
     * @return ascending, de-duplicated cut timestamps; a cut at ts {@code X} puts rows
     * {@code < X} left of the cut and rows {@code >= X} right of it. Empty when the block
     * degenerates to a single merge (no qualifying cold gap).
     */
    public LongList computeCuts(
            long t0,
            long t1,
            long minBinDuration,
            int maxBins,
            long minGapRows,
            long partitionRowCount,
            int maxCuts
    ) {
        cutTimestamps.clear();
        if (t1 <= t0 || txnRanges.size() == 0 || maxCuts <= 0 || partitionRowCount <= 0) {
            return cutTimestamps;
        }
        final long span = t1 - t0 + 1;
        final long binDuration = Math.max(minBinDuration, (span + maxBins - 1) / maxBins);
        final int binCount = (int) ((span + binDuration - 1) / binDuration);
        if (binCount < 2) {
            return cutTimestamps;
        }
        if (cover.length < binCount + 1) {
            cover = new int[Numbers.ceilPow2(binCount + 1)];
        }
        Arrays.fill(cover, 0, binCount + 1, 0);

        // Coverage histogram: O(1) per txn via a difference array over the bins.
        for (int i = 0, n = txnRanges.size(); i < n; i += 2) {
            final long rangeLo = Math.max(txnRanges.getQuick(i), t0);
            final long rangeHi = Math.min(txnRanges.getQuick(i + 1), t1);
            if (rangeLo > rangeHi) {
                continue;
            }
            cover[(int) ((rangeLo - t0) / binDuration)]++;
            cover[(int) ((rangeHi - t0) / binDuration) + 1]--;
        }

        // Walk maximal cold runs (prefix sum == 0). An interior gap contributes two cuts (both
        // edges), a leading/trailing gap one (its hot-side edge); each is gated on the estimated
        // existing rows it holds. seenHot tracks whether a run is leading or interior.
        gapScratch.clear();
        int running = 0;
        int coldRunStart = -1;
        boolean seenHot = false;
        for (int b = 0; b <= binCount; b++) {
            final boolean hot = b < binCount && (running += cover[b]) > 0;
            if (!hot && b < binCount) {
                if (coldRunStart < 0) {
                    coldRunStart = b;
                }
                continue;
            }
            if (coldRunStart >= 0) {
                final int runBins = b - coldRunStart;
                final long estRows = (long) ((double) runBins * binDuration * partitionRowCount / span);
                if (estRows >= minGapRows) {
                    final boolean leading = !seenHot;
                    final boolean trailing = b == binCount;
                    final long gapStartTs = t0 + coldRunStart * binDuration;
                    final long gapEndTs = t0 + (long) b * binDuration;
                    if (leading && !trailing) {
                        gapScratch.add(estRows, gapEndTs);
                        gapScratch.add(Long.MIN_VALUE);
                    } else if (trailing && !leading) {
                        gapScratch.add(estRows, gapStartTs);
                        gapScratch.add(Long.MIN_VALUE);
                    } else if (!leading) {
                        gapScratch.add(estRows, gapStartTs);
                        gapScratch.add(gapEndTs);
                    }
                    // leading && trailing: the whole range is cold (no hot bin at all) - no cuts;
                    // the caller only invokes this with at least one intersecting txn, but a txn
                    // range clipped to a single bin boundary can still leave every bin cold.
                }
                coldRunStart = -1;
            }
            if (hot) {
                seenHot = true;
            }
        }

        // Spend the cut budget on the largest gaps first (selection over a handful of triples).
        int budget = maxCuts;
        while (budget > 0 && gapScratch.size() > 0) {
            int best = 0;
            for (int i = 3, n = gapScratch.size(); i < n; i += 3) {
                if (gapScratch.getQuick(i) > gapScratch.getQuick(best)) {
                    best = i;
                }
            }
            final long cutA = gapScratch.getQuick(best + 1);
            final long cutB = gapScratch.getQuick(best + 2);
            final int cost = cutB == Long.MIN_VALUE ? 1 : 2;
            if (cost <= budget) {
                cutTimestamps.add(cutA);
                if (cutB != Long.MIN_VALUE) {
                    cutTimestamps.add(cutB);
                }
                budget -= cost;
            }
            // A 2-cut interior gap that does not fit the remaining budget is dropped; a smaller
            // 1-cut edge gap later in the list may still fit.
            gapScratch.removeIndexBlock(best, 3);
        }
        cutTimestamps.sort();
        return cutTimestamps;
    }
}
