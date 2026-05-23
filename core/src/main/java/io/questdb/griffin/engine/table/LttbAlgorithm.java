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
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
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
 * <b>Gap-preserving mode uses soft target semantics:</b> target_points is a
 * goal, not a hard maximum. Each segment receives at least
 * min(2, segmentSize) points to preserve gap structure. When many segments
 * are detected, the output may exceed target_points. Non-gap LTTB and
 * M4/MinMax treat target_points as a hard maximum.
 * <p>
 * Reference: Steinarsson, S. (2013). "Downsampling Time Series for Visual
 * Representation." University of Iceland MSc thesis.
 *
 * @see SubsampleAlgorithm
 */
class LttbAlgorithm implements SubsampleAlgorithm {
    private final long gapThresholdMicros;
    // Reusable native lists for segment bookkeeping. Stored as cursor-lifetime
    // fields to avoid per-execution allocation. Cleared per execution.
    private DirectLongList segments;
    private DirectIntList targets;

    LttbAlgorithm(long gapThresholdMicros) {
        this.gapThresholdMicros = gapThresholdMicros;
    }

    void close() {
        if (segments != null) {
            segments.close();
            segments = null;
        }
        if (targets != null) {
            targets.close();
            targets = null;
        }
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

    /**
     * Gap-preserving LTTB: split data into contiguous segments, downsample
     * each independently with proportional point budget.
     * <p>
     * Two-pass approach with reusable native bookkeeping:
     * <ol>
     *   <li>Pass 1: identify segments (start, size) using gap threshold.</li>
     *   <li>Compute proportional targets per segment. Each segment gets at
     *       least min(2, segSize) points. If the total exceeds targetPoints,
     *       scale down proportional allocations while preserving the floor.
     *       The total may still exceed targetPoints when the floor alone
     *       exceeds it (soft target semantics).</li>
     *   <li>Pass 2: run LTTB on each segment with its budgeted target.</li>
     * </ol>
     */
    private void selectGapPreserving(long buffer, int n, int totalPoints,
                                     DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        // Pass 1: identify segments
        if (segments == null) {
            segments = new DirectLongList(64, MemoryTag.NATIVE_FUNC_RSS);
        }
        segments.clear();

        int segStart = 0;
        for (int i = 1; i <= n; i++) {
            if ((i & 0xFFF) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            boolean isGap = false;
            if (i < n) {
                long prevTs = Unsafe.getUnsafe().getLong(buffer + (long) (i - 1) * ENTRY_SIZE + 8);
                long currTs = Unsafe.getUnsafe().getLong(buffer + (long) i * ENTRY_SIZE + 8);
                // Overflow-safe gap detection: currTs - prevTs can overflow on
                // extreme timestamp ranges. Use currTs > prevTs + threshold
                // when the addition does not overflow. If it overflows, the
                // gap threshold exceeds the representable range past prevTs,
                // so currTs (bounded by Long.MAX_VALUE) cannot exceed it.
                if (prevTs > Long.MAX_VALUE - gapThresholdMicros) {
                    isGap = false;
                } else {
                    isGap = currTs > prevTs + gapThresholdMicros;
                }
            }
            if (isGap || i == n) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                int segSize = i - segStart;
                segments.add(segStart);
                segments.add(segSize);
                segStart = i;
            }
        }

        int segCount = (int) (segments.size() / 2);

        // Compute actual floor: sum(min(2, segSize)) for each segment.
        // One-row segments only need 1 point, not 2.
        int floorTotal = 0;
        for (int s = 0; s < segCount; s++) {
            if ((s & 0xFFF) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            int segSize = (int) segments.get(s * 2 + 1);
            floorTotal += Math.min(2, segSize);
        }

        if (targets == null) {
            targets = new DirectIntList(64, MemoryTag.NATIVE_FUNC_RSS);
        }
        targets.clear();

        if (floorTotal >= totalPoints) {
            // Soft target exceeded by floor alone. Give each segment its floor.
            for (int s = 0; s < segCount; s++) {
                int segSize = (int) segments.get(s * 2 + 1);
                targets.add(Math.min(2, segSize));
            }
        } else {
            // Budget available above floor
            int budgetAboveFloor = totalPoints - floorTotal;
            int totalAllocated = 0;
            for (int s = 0; s < segCount; s++) {
                if ((s & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                int segSize = (int) segments.get(s * 2 + 1);
                int floor = Math.min(2, segSize);
                int extra = (int) ((long) segSize * budgetAboveFloor / n);
                int segTarget = Math.min(floor + extra, segSize);
                targets.add(segTarget);
                totalAllocated += segTarget;
            }

            // Trim excess due to rounding. O(segments) single pass: reduce
            // segments from the last one backward, respecting floor.
            int s = segCount - 1;
            while (totalAllocated > totalPoints && s >= 0) {
                int t = targets.get(s);
                int floor = Math.min(2, (int) segments.get(s * 2 + 1));
                if (t > floor) {
                    int trim = Math.min(t - floor, totalAllocated - totalPoints);
                    targets.set(s, t - trim);
                    totalAllocated -= trim;
                }
                s--;
            }
        }

        // Pass 2: run LTTB per segment with budgeted targets
        for (int s = 0; s < segCount; s++) {
            int start = (int) segments.get(s * 2);
            int size = (int) segments.get(s * 2 + 1);
            int segTarget = targets.get(s);
            if (size <= segTarget) {
                for (int j = start; j < start + size; j++) {
                    if ((j & 0xFFF) == 0) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                    }
                    selectedIndices.add(j);
                }
            } else {
                selectOnRange(buffer, start, start + size, segTarget, selectedIndices, circuitBreaker);
            }
        }
    }

    /**
     * Run LTTB on a sub-range [start, end) of the buffer.
     */
    private static void selectOnRange(long buffer, int start, int end, int m,
                                      DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        int n = end - start;
        if (n < 2) {
            // Single data point or empty range - emit what's there
            for (int j = start; j < end; j++) {
                selectedIndices.add(j);
            }
            return;
        }
        if (m < 2) {
            // Cannot form LTTB buckets with fewer than 2 target points.
            // This should not happen in normal flow (targetPoints >= 2 is
            // validated at compile time), but guard defensively.
            selectedIndices.add(start);
            return;
        }

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
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
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
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
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
