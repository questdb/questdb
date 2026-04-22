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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import io.questdb.std.Unsafe;

/**
 * MinMax downsampling algorithm: selects up to 2 points per time bucket
 * (min and max value).
 * <p>
 * Uses time-based buckets (equal time intervals), like M4 but without
 * first/last tracking. Lighter than M4 (2 comparisons per row instead of
 * 4 values tracked), and the output is half the size. Best for simple
 * envelope visualization where first/last positions within the bucket
 * don't matter.
 * <p>
 * Naturally preserves gaps in the data (empty time intervals produce no
 * output).
 *
 * @see SubsampleAlgorithm
 */
class MinMaxAlgorithm implements SubsampleAlgorithm {
    static final MinMaxAlgorithm INSTANCE = new MinMaxAlgorithm();

    @Override
    public void select(long buffer, int bufferSize, int targetPoints,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        int numBuckets = targetPoints / 2;
        if (numBuckets < 1) {
            numBuckets = 1;
        }

        long minTs = SubsampleAlgorithm.getTimestamp(buffer, 0);
        long maxTs = SubsampleAlgorithm.getTimestamp(buffer, bufferSize - 1);
        if (minTs == maxTs) {
            // All same timestamp - cap at targetPoints
            int count = Math.min(bufferSize, targetPoints);
            for (int i = 0; i < count; i++) {
                selectedIndices.add(i);
            }
            return;
        }

        double timeRange = (double) (maxTs - minTs);
        double bucketWidth = timeRange / numBuckets;

        int dataIdx = 0;
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            long bucketStartTs = minTs + (long) (bucket * bucketWidth);
            long bucketEndTs = (bucket < numBuckets - 1) ? minTs + (long) ((bucket + 1) * bucketWidth) : Long.MAX_VALUE;

            int minIdx = -1;
            int maxIdx = -1;
            double minVal = Double.POSITIVE_INFINITY;
            double maxVal = Double.NEGATIVE_INFINITY;

            while (dataIdx < bufferSize) {
                long ts = Unsafe.getUnsafe().getLong(buffer + (long) dataIdx * ENTRY_SIZE + 8);
                // Final bucket processes all remaining rows (no end boundary)
                if (bucket < numBuckets - 1 && ts >= bucketEndTs) {
                    break;
                }
                if (ts >= bucketStartTs) {
                    double v = SubsampleAlgorithm.getValue(buffer, dataIdx);
                    if (v < minVal) {
                        minVal = v;
                        minIdx = dataIdx;
                    }
                    if (v > maxVal) {
                        maxVal = v;
                        maxIdx = dataIdx;
                    }
                }
                dataIdx++;
            }

            // Empty bucket (gap in data) - skip
            if (minIdx == -1) {
                continue;
            }

            // Emit in timestamp order, deduplicated
            if (minIdx == maxIdx) {
                selectedIndices.add(minIdx);
            } else if (minIdx < maxIdx) {
                selectedIndices.add(minIdx);
                selectedIndices.add(maxIdx);
            } else {
                selectedIndices.add(maxIdx);
                selectedIndices.add(minIdx);
            }
        }
        // Cap output to targetPoints. With small targets (e.g., 2),
        // a single bucket can emit more rows than the target.
        if (selectedIndices.size() > targetPoints) {
            selectedIndices.setPos(targetPoints);
        }
    }
}
