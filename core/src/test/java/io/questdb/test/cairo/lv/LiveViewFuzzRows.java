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

package io.questdb.test.cairo.lv;

import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import org.junit.Assert;

/**
 * The logical dataset of one fuzz run: rows of {@code (ts, sym, i, x)} in ascending
 * timestamp order, with timestamp ties across keys.
 * <p>
 * A tied row reuses the previous row's timestamp under a key its tie group does not hold
 * yet, the NULL key included, so {@code (ts, sym)} stays unique. Within one key's partition
 * the timestamp is then still a total order, which keeps a window partitioned by the key a
 * deterministic function of the row set, and the recompute oracle exact. An un-partitioned
 * window over tied rows is not, so a run whose window is not partitioned by the key draws
 * no ties.
 * <p>
 * {@link #addExactDuplicates} breaks the uniqueness on purpose: it copies whole rows, every
 * value included. A partitioned window over two identical rows emits the same two output
 * rows whichever it reads first, so the oracle still holds when both sides sort on every
 * output column.
 */
final class LiveViewFuzzRows {
    // A row's key in symIdx: -1 is the NULL key, otherwise an index into the run's symbols.
    static final int NULL_KEY = -1;
    // The tie group's bit for the NULL key; symbols take bits 0 to 15.
    private static final int NULL_KEY_BIT = 1 << 16;
    final long[] iv;
    final int rowCount;
    final int[] symIdx;
    final long[] tsv;
    final double[] xv;
    final boolean[] xNull;
    private int duplicateCount;
    private int tieCount;

    private LiveViewFuzzRows(int rowCount) {
        this.rowCount = rowCount;
        tsv = new long[rowCount];
        symIdx = new int[rowCount];
        iv = new long[rowCount];
        xv = new double[rowCount];
        xNull = new boolean[rowCount];
    }

    /**
     * Draws {@code rowCount} rows from {@code startMicros} on. Each new timestamp steps up by
     * at most {@code stepMax}, plus a whole day every {@code dayJumpEvery} rows on average, at
     * most 30 times. {@code tiePct} percent of the rows try to tie the previous row's
     * timestamp; a tie group that holds every key already gives up the tie.
     */
    static LiveViewFuzzRows generate(
            Rnd rnd,
            int rowCount,
            int symCount,
            long startMicros,
            int stepMax,
            int dayJumpEvery,
            int tiePct
    ) {
        Assert.assertTrue("symbols must fit the tie group mask", symCount <= 16);
        final LiveViewFuzzRows rows = new LiveViewFuzzRows(rowCount);
        long ts = startMicros;
        int groupMask = 0;
        int dayJumps = 0;
        for (int k = 0; k < rowCount; k++) {
            int s = rnd.nextInt(20) == 0 ? NULL_KEY : rnd.nextInt(symCount);
            boolean isTie = k > 0 && tiePct > 0 && rnd.nextInt(100) < tiePct;
            if (isTie && (groupMask & keyBit(s)) != 0) {
                // The drawn key is in the group already: take the first key that is not.
                s = freeKey(groupMask, symCount);
                isTie = s != Integer.MIN_VALUE;
                if (!isTie) {
                    s = rnd.nextInt(symCount);
                }
            }
            if (isTie) {
                rows.tieCount++;
            } else {
                ts += 1 + rnd.nextInt(stepMax);
                if (dayJumps < 30 && rnd.nextInt(dayJumpEvery) == 0) {
                    ts += 86_400_000_000L;
                    dayJumps++;
                }
                groupMask = 0;
            }
            groupMask |= keyBit(s);
            rows.tsv[k] = ts;
            rows.symIdx[k] = s;
            rows.iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            rows.xNull[k] = rnd.nextInt(20) == 0;
            rows.xv[k] = rnd.nextDouble() * 1000.0;
        }
        return rows;
    }

    /**
     * Moves some of the interior commit bounds of {@code bounds} - offsets into
     * {@code order}, ascending, first 0 and last {@code order.length} - so they cut between
     * two rows of one tie group, where a nearby pair of tied rows sits next to each other in
     * the emission order. Each bound moves with probability {@code pct} percent, and never
     * past its neighbours. Returns how many bounds it moved.
     */
    int cutInsideTieGroups(Rnd rnd, int[] order, int[] bounds, int pct) {
        int moved = 0;
        for (int b = 1; b < bounds.length - 1; b++) {
            if (rnd.nextInt(100) >= pct) {
                continue;
            }
            // The first adjacent tied pair at or after the bound, short of the next bound.
            for (int p = Math.max(bounds[b], bounds[b - 1] + 1); p < bounds[b + 1]; p++) {
                if (p > 0 && tsv[order[p - 1]] == tsv[order[p]]) {
                    bounds[b] = p;
                    moved++;
                    break;
                }
            }
        }
        return moved;
    }

    /**
     * Turns {@code pct} percent of the rows into exact copies of the row before them in
     * timestamp order: same timestamp, key and values. Keeps the rows sorted by timestamp.
     */
    void addExactDuplicates(Rnd rnd, int pct) {
        for (int k = 1; k < rowCount; k++) {
            if (rnd.nextInt(100) < pct) {
                tsv[k] = tsv[k - 1];
                symIdx[k] = symIdx[k - 1];
                iv[k] = iv[k - 1];
                xv[k] = xv[k - 1];
                xNull[k] = xNull[k - 1];
                duplicateCount++;
            }
        }
    }

    int getDuplicateCount() {
        return duplicateCount;
    }

    int getTieCount() {
        return tieCount;
    }

    private static int freeKey(int groupMask, int symCount) {
        for (int c = 0; c < symCount; c++) {
            if ((groupMask & (1 << c)) == 0) {
                return c;
            }
        }
        return (groupMask & NULL_KEY_BIT) == 0 ? NULL_KEY : Integer.MIN_VALUE;
    }

    private static int keyBit(int s) {
        return s == NULL_KEY ? NULL_KEY_BIT : 1 << s;
    }
}
