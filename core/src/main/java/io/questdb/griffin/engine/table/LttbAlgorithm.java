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
 * Largest Triangle Three Buckets (LTTB) downsampling algorithm.
 * <p>
 * Divides data into equal row-count buckets and selects the point in each
 * bucket that forms the largest triangle with the previously selected point
 * and the average of the next bucket. First and last points are always kept.
 * <p>
 * Supports gap-preserving mode: when gapThresholdMicros > 0, the data is
 * split into contiguous segments where consecutive timestamps are within
 * the threshold, and each segment is downsampled independently.
 * <p>
 * Reference: Steinarsson, S. (2013). "Downsampling Time Series for Visual
 * Representation." University of Iceland MSc thesis.
 *
 * @see SubsampleAlgorithm
 */
class LttbAlgorithm implements SubsampleAlgorithm {
    private final long gapThresholdMicros;

    LttbAlgorithm(long gapThresholdMicros) {
        this.gapThresholdMicros = gapThresholdMicros;
    }

    @Override
    public void select(long buffer, int bufferSize, int targetPoints,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (gapThresholdMicros > 0) {
            selectGapPreserving(buffer, bufferSize, targetPoints, selectedIndices, circuitBreaker);
        } else {
            selectOnRange(buffer, 0, bufferSize, targetPoints, selectedIndices, circuitBreaker);
        }
    }

    private void selectGapPreserving(long buffer, int n, int totalPoints,
                                     DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        int segStart = 0;
        for (int i = 1; i <= n; i++) {
            boolean isGap = false;
            if (i < n) {
                long prevTs = Unsafe.getUnsafe().getLong(buffer + (long) (i - 1) * ENTRY_SIZE + 8);
                long currTs = Unsafe.getUnsafe().getLong(buffer + (long) i * ENTRY_SIZE + 8);
                isGap = (currTs - prevTs > gapThresholdMicros);
            }

            if (isGap || i == n) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                int segEnd = i;
                int segSize = segEnd - segStart;

                int segTarget = Math.max(2, (int) ((long) segSize * totalPoints / n));
                if (segTarget > segSize) {
                    segTarget = segSize;
                }

                if (segSize <= segTarget) {
                    for (int j = segStart; j < segEnd; j++) {
                        selectedIndices.add(j);
                    }
                } else {
                    selectOnRange(buffer, segStart, segEnd, segTarget, selectedIndices, circuitBreaker);
                }

                segStart = i;
            }
        }
    }

    /**
     * Run LTTB on a sub-range [start, end) of the buffer.
     */
    private static void selectOnRange(long buffer, int start, int end, int m,
                                      DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        int n = end - start;

        selectedIndices.add(start);

        double bucketSize = (double) (n - 2) / (m - 2);
        int prevSelected = start;

        for (int bucket = 0; bucket < m - 2; bucket++) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            int bucketStart = start + (int) ((bucket) * bucketSize) + 1;
            int bucketEnd = start + (int) ((bucket + 1) * bucketSize) + 1;
            if (bucketEnd > end - 1) {
                bucketEnd = end - 1;
            }

            int nextBucketStart = bucketEnd;
            int nextBucketEnd = start + (int) ((bucket + 2) * bucketSize) + 1;
            if (nextBucketEnd > end - 1 || bucket == m - 3) {
                nextBucketEnd = end;
            }

            double avgX = 0;
            double avgY = 0;
            int nextBucketLen = nextBucketEnd - nextBucketStart;
            for (int j = nextBucketStart; j < nextBucketEnd; j++) {
                avgX += (double) SubsampleAlgorithm.getTimestamp(buffer, j);
                avgY += SubsampleAlgorithm.getValue(buffer, j);
            }
            if (nextBucketLen > 0) {
                avgX /= nextBucketLen;
                avgY /= nextBucketLen;
            }

            double ax = (double) SubsampleAlgorithm.getTimestamp(buffer, prevSelected);
            double ay = SubsampleAlgorithm.getValue(buffer, prevSelected);

            double maxArea = -1;
            int maxAreaIndex = bucketStart;
            for (int j = bucketStart; j < bucketEnd; j++) {
                double bx = (double) SubsampleAlgorithm.getTimestamp(buffer, j);
                double by = SubsampleAlgorithm.getValue(buffer, j);
                double area = Math.abs(ax * (by - avgY) + bx * (avgY - ay) + avgX * (ay - by));
                if (area > maxArea) {
                    maxArea = area;
                    maxAreaIndex = j;
                }
            }

            selectedIndices.add(maxAreaIndex);
            prevSelected = maxAreaIndex;
        }

        selectedIndices.add(end - 1);
    }
}
