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

package io.questdb.griffin.engine.table;

import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

/**
 * Selectivity statistics for late materialization heuristics.
 * <p>
 * This class tracks the historical selectivity (ratio of filtered rows to total rows)
 * using exponential moving average (EMA). Late materialization is enabled only when
 * the selectivity is below a threshold (20%), meaning most rows are filtered out.
 * <p>
 * Late materialization benefits:
 * - Low selectivity -> most rows filtered -> avoid decoding unused column data
 * - Only applicable to Parquet format (columnar storage requiring decoding)
 * <p>
 * A thread-safe filter hands out no per-worker slots, so every reducing worker, the query owner
 * and any work-stealing thread share one instance. {@link #update(long, long)} therefore has to
 * tolerate concurrent callers. It runs once per page frame, never per row, so a CAS loop costs
 * nothing measurable here.
 * <p>
 * The sample count and the average live in a single word. That keeps the pair consistent for a
 * reader without a second barrier, and it leaves the average impossible to tear. The average is
 * held as a float: a 24-bit mantissa is far more than a ratio compared against a 20% threshold
 * needs. The count saturates at {@link #MIN_SAMPLES}, which is the most the warm-up gate ever
 * reads, so it cannot overflow into the average.
 */
public class SelectivityStats implements Mutable {
    // EMA smoothing factor (0.3 favors recent results)
    private static final double ALPHA = 0.3;
    private static final int MIN_SAMPLES = 2;
    // Selectivity threshold (20%) - enable late materialization when selectivity is below this
    private static final double SELECTIVITY_THRESHOLD = 0.2;
    private static final long STATE_OFFSET = Unsafe.getFieldOffset(SelectivityStats.class, "state");
    // Packed [sampleCount:32][avgSelectivity float bits:32].
    private volatile long state = 0;

    @Override
    public void clear() {
        state = 0;
    }

    public boolean shouldUseLateMaterialization() {
        final long state = this.state;
        if (sampleCountOf(state) < MIN_SAMPLES) {
            return true;
        }
        return avgSelectivityOf(state) < SELECTIVITY_THRESHOLD;
    }

    public void update(long filteredRowCount, long totalRowCount) {
        if (totalRowCount <= 0) {
            return;
        }

        double selectivity = (double) filteredRowCount / totalRowCount;

        long prev;
        long next;
        do {
            prev = state;
            int sampleCount = sampleCountOf(prev);
            double avgSelectivity;
            if (sampleCount == 0) {
                avgSelectivity = selectivity;
            } else {
                // Exponential moving average: EMA = α * current + (1 - α) * EMA_prev
                avgSelectivity = ALPHA * selectivity + (1 - ALPHA) * avgSelectivityOf(prev);
            }
            next = pack(Math.min(sampleCount + 1, MIN_SAMPLES), avgSelectivity);
        } while (!Unsafe.cas(this, STATE_OFFSET, prev, next));
    }

    private static double avgSelectivityOf(long state) {
        return Float.intBitsToFloat((int) state);
    }

    private static long pack(int sampleCount, double avgSelectivity) {
        return ((long) sampleCount << 32) | (Float.floatToRawIntBits((float) avgSelectivity) & 0xffff_ffffL);
    }

    private static int sampleCountOf(long state) {
        return (int) (state >>> 32);
    }
}
