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
 * M4 downsampling algorithm: selects up to 4 points per time bucket
 * (first, last, min, max).
 * <p>
 * Uses time-based buckets (equal time intervals), not row-count buckets.
 * This guarantees pixel-accurate min/max envelope and naturally preserves
 * gaps in the data (empty time intervals produce no output).
 * <p>
 * Reference: Jugel, U. et al. (2014). "M4: A Visualization-Oriented Time
 * Series Data Aggregation." PVLDB Vol. 7, No. 10.
 *
 * @see SubsampleAlgorithm
 */
class M4Algorithm implements SubsampleAlgorithm {
    static final M4Algorithm INSTANCE = new M4Algorithm();

    @Override
    public void select(long buffer, int bufferSize, int targetPoints,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (bufferSize <= 0 || targetPoints <= 0) {
            return;
        }
        int numBuckets = targetPoints / 4;
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

        double timeRange = (double) maxTs - (double) minTs;
        double bucketWidth = timeRange / numBuckets;

        int dataIdx = 0;
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            long bucketStartTs = (long) ((double) minTs + bucket * bucketWidth);
            long bucketEndTs = (bucket < numBuckets - 1) ? (long) ((double) minTs + (bucket + 1) * bucketWidth) : Long.MAX_VALUE;

            int firstIdx = -1;
            int lastIdx = -1;
            int minIdx = -1;
            int maxIdx = -1;
            double minVal = 0;
            double maxVal = 0;
            boolean hasData = false;

            while (dataIdx < bufferSize) {
                if ((dataIdx & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                long ts = Unsafe.getUnsafe().getLong(buffer + (long) dataIdx * ENTRY_SIZE + 8);
                // Final bucket processes all remaining rows (no end boundary)
                if (bucket < numBuckets - 1 && ts >= bucketEndTs) {
                    break;
                }
                if (ts >= bucketStartTs) {
                    if (firstIdx == -1) {
                        firstIdx = dataIdx;
                    }
                    lastIdx = dataIdx;
                    double v = SubsampleAlgorithm.getValue(buffer, dataIdx);
                    if (!hasData) {
                        minVal = v;
                        minIdx = dataIdx;
                        maxVal = v;
                        maxIdx = dataIdx;
                        hasData = true;
                    } else {
                        if (v < minVal) {
                            minVal = v;
                            minIdx = dataIdx;
                        }
                        if (v > maxVal) {
                            maxVal = v;
                            maxIdx = dataIdx;
                        }
                    }
                }
                dataIdx++;
            }

            if (firstIdx == -1) {
                continue;
            }

            // Sort 4 indices with a sorting network (5 comparisons, 0 allocations)
            // and deduplicate.
            assert minIdx >= 0 && maxIdx >= 0 : "selected indices must not be negative";
            emitSorted4(selectedIndices, firstIdx, minIdx, maxIdx, lastIdx);
        }
        // Cap output to targetPoints. With small targets (e.g., 2 or 3),
        // a single bucket can emit up to 4 rows, exceeding the target.
        if (selectedIndices.size() > targetPoints) {
            selectedIndices.setPos(targetPoints);
        }
    }

    /**
     * Sort 4 values with a sorting network and add unique values to the list.
     */
    static void emitSorted4(DirectLongList out, int a, int b, int c, int d) {
        if (a > b) {
            int t = a;
            a = b;
            b = t;
        }
        if (c > d) {
            int t = c;
            c = d;
            d = t;
        }
        if (a > c) {
            int t = a;
            a = c;
            c = t;
        }
        if (b > d) {
            int t = b;
            b = d;
            d = t;
        }
        if (b > c) {
            int t = b;
            b = c;
            c = t;
        }
        out.add(a);
        if (b != a) {
            out.add(b);
        }
        if (c != b) {
            out.add(c);
        }
        if (d != c) {
            out.add(d);
        }
    }

}
