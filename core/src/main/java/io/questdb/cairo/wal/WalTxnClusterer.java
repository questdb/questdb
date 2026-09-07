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
 * Transaction clustering: it decides WHERE to pre-split a partition, from the shape of the incoming work rather than
 * from any one batch.
 */
public class WalTxnClusterer implements Mutable {
    private final LongList cutTimestamps = new LongList();
    // Flat pairs per qualifying cold gap: [first cut ts, second cut ts or Long.MIN_VALUE].
    private final LongList gapScratch = new LongList();
    // Flat pairs: incoming txn [minTs, maxTs], clipped by the caller to the partition's data range.
    private final LongList txnRanges = new LongList();
    private int[] cover = new int[0];

    public void addTxnRange(long minTs, long maxTs) {
        txnRanges.add(minTs, maxTs);
    }

    @Override
    public void clear() {
        txnRanges.clear();
    }

    /**
     * Computes cut timestamps for one partition against the buffered transaction ranges. Cuts are bounded by the size
     * of the pieces they produce, not by a cut count: every piece the returned cuts carve out holds at least
     * {@code minPieceRows} estimated rows.
     * @param t0 first existing row timestamp of the partition (piece)
     * @param t1 last existing row timestamp, inclusive; {@code t1 >= t0}
     * @param minBinDuration finest bin duration (e.g.
     * @param maxBins bin-count cap, bounding work and cut precision
     * @param minPieceRows minimum estimated existing rows in any piece a cut produces
     * @param partitionRowCount existing rows in [t0, t1], for the uniform-density estimate
     * @return ascending, de-duplicated cut timestamps; a cut at ts {@code X} puts rows {@code < X} left of the cut and
     * rows {@code >= X} right of it.
     */
    public LongList computeCuts(
            long t0,
            long t1,
            long minBinDuration,
            int maxBins,
            long minPieceRows,
            long partitionRowCount
    ) {
        cutTimestamps.clear();
        if (t1 <= t0 || txnRanges.size() == 0 || partitionRowCount <= 0) {
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

        // Walk maximal cold runs (prefix sum == 0).
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
                final long estRows = estimateRows((long) runBins * binDuration, partitionRowCount, span);
                if (estRows >= minPieceRows) {
                    final boolean leading = !seenHot;
                    final boolean trailing = b == binCount;
                    final long gapStartTs = t0 + coldRunStart * binDuration;
                    final long gapEndTs = t0 + (long) b * binDuration;
                    if (leading && !trailing) {
                        gapScratch.add(gapEndTs, Long.MIN_VALUE);
                    } else if (trailing && !leading) {
                        gapScratch.add(gapStartTs, Long.MIN_VALUE);
                    } else if (!leading) {
                        gapScratch.add(gapStartTs, gapEndTs);
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

        for (int i = 0, n = gapScratch.size(); i < n; i += 2) {
            cutTimestamps.add(gapScratch.getQuick(i));
            final long cutB = gapScratch.getQuick(i + 1);
            if (cutB != Long.MIN_VALUE) {
                cutTimestamps.add(cutB);
            }
        }
        cutTimestamps.sort();
        keepCutsLeavingWholePieces(t0, t1, minPieceRows, partitionRowCount, span);
        return cutTimestamps;
    }

    /**
     * Rows a timestamp window of {@code duration} holds, assuming the partition's rows spread evenly over
     * {@code span}.
     */
    private static long estimateRows(long duration, long partitionRowCount, long span) {
        return (long) ((double) duration * partitionRowCount / span);
    }

    /**
     * Drops cuts that would carve out a piece smaller than {@code minPieceRows}. A cold gap earns its own cut only
     * against the neighbouring gap it is measured from, so the gate has to run over the cuts in order rather than per
     * gap: two qualifying gaps either side of a narrow hot stride would otherwise leave that stride as a piece too
     * small to be worth the record and the page frame it costs.
     */
    private void keepCutsLeavingWholePieces(long t0, long t1, long minPieceRows, long partitionRowCount, long span) {
        int keep = 0;
        long pieceStart = t0;
        for (int i = 0, n = cutTimestamps.size(); i < n; i++) {
            final long cut = cutTimestamps.getQuick(i);
            if (estimateRows(cut - pieceStart, partitionRowCount, span) < minPieceRows) {
                continue;
            }
            cutTimestamps.setQuick(keep++, cut);
            pieceStart = cut;
        }
        // The rows past the last cut are a piece as well, so a cut that leaves too few of them goes too.
        while (keep > 0 && estimateRows(t1 + 1 - pieceStart, partitionRowCount, span) < minPieceRows) {
            keep--;
            pieceStart = keep > 0 ? cutTimestamps.getQuick(keep - 1) : t0;
        }
        cutTimestamps.setPos(keep);
    }
}
