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
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

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
 * Large ranges run as MinMaxLTTB: a cheap MinMax pass over equal row-count
 * bins preselects about {@code PRESELECT_RATIO} points per output point
 * (per-bin minimum and maximum, plus the pinned first and last points), and
 * the LTTB triangle stage then runs over the survivors only. The expensive
 * sequential triangle stage shrinks from n points to ~ratio*m while the
 * selection stays visually near-identical to plain LTTB: preselection can
 * never drop a per-bin extreme, and because the bins are row-count based
 * every bin is non-empty, so survivors always number at least m and the
 * output row count is identical to the plain path. Ranges at or below the
 * activation threshold keep the single-stage path bit-for-bit.
 * <p>
 * References: Steinarsson, S. (2013). "Downsampling Time Series for Visual
 * Representation." University of Iceland MSc thesis. Van Der Donckt, J.,
 * Van Der Donckt, J., Deprost, E., Van Hoecke, S. (2023). "MinMaxLTTB:
 * Leveraging MinMax-Preselection to Scale LTTB" (arXiv:2305.00332).
 *
 * @see SubsampleAlgorithm
 */
public class LttbAlgorithm implements SubsampleAlgorithm {
    // MinMaxLTTB (Van Der Donckt et al., 2023): preselect ~PRESELECT_RATIO * (m - 2)
    // interior points with a MinMax pass before the triangle stage. The paper
    // evaluates ratios 2..8; 4 is its recommended default, visually
    // indistinguishable from plain LTTB. Must be >= 2 so that even full per-bin
    // min==max dedup leaves at least m - 2 interior survivors (bins = ratio/2 *
    // (m - 2) >= m - 2), preserving the exact output row count.
    private static final int PRESELECT_RATIO = 4;
    // Preselect only when the interior outnumbers the worst-case (no-dedup)
    // survivor count by at least this factor. Below the threshold the extra
    // cheap scan saves too little triangle work to matter, and staying on the
    // plain path keeps small-range selections bit-identical to classic LTTB.
    private static final int PRESELECT_MIN_SHRINK = 2;
    private final long gapThresholdMicros;
    // Reusable native lists for segment bookkeeping and MinMaxLTTB preselection.
    // Stored as cursor-lifetime fields to avoid per-execution allocation.
    // Cleared per execution.
    private DirectLongList candidates;
    @Nullable
    private MemoryTracker memoryTracker;
    private DirectLongList segments;
    private DirectLongList targets;

    public LttbAlgorithm(long gapThresholdMicros) {
        this.gapThresholdMicros = gapThresholdMicros;
    }

    public void close() {
        if (segments != null) {
            segments.close();
            segments = null;
        }
        if (targets != null) {
            targets.close();
            targets = null;
        }
        if (candidates != null) {
            candidates.close();
            candidates = null;
        }
    }

    public void setMemoryTracker(@Nullable MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        if (segments != null) {
            segments.setMemoryTracker(memoryTracker);
        }
        if (targets != null) {
            targets.setMemoryTracker(memoryTracker);
        }
        if (candidates != null) {
            candidates.setMemoryTracker(memoryTracker);
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
     * Position {@code pos} to buffer index: identity when {@code candidates} is
     * null (plain LTTB over a contiguous range), otherwise the preselected
     * buffer index stored at {@code pos} (MinMaxLTTB triangle stage).
     */
    private static long at(@Nullable DirectLongList candidates, int pos) {
        return candidates == null ? pos : candidates.get(pos);
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
            segments = new DirectLongList(64, MemoryTag.NATIVE_FUNC_RSS, true);
            segments.setMemoryTracker(memoryTracker);
            segments.reopen();
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
            targets = new DirectLongList(64, MemoryTag.NATIVE_FUNC_RSS, true);
            targets.setMemoryTracker(memoryTracker);
            targets.reopen();
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
                int t = (int) targets.get(s);
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
            int segTarget = (int) targets.get(s);
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
     * Run LTTB on a sub-range [start, end) of the buffer, switching to the
     * two-stage MinMaxLTTB variant when the range is large enough for the
     * preselection to pay off (see class doc).
     */
    private void selectOnRange(long buffer, int start, int end, int m,
                               DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        // Preselection needs at least one interior LTTB bucket (m > 2) and an
        // interior that outnumbers the worst-case survivor count
        // (PRESELECT_RATIO * (m - 2), i.e. 2 per bin) by PRESELECT_MIN_SHRINK.
        // The condition also guarantees every preselection bin holds at least
        // 2 * PRESELECT_MIN_SHRINK rows, so bins are never empty. Long math:
        // both sides fit comfortably, no overflow for any int n, m.
        if (m > 2 && (long) (end - start) - 2 > (long) PRESELECT_MIN_SHRINK * PRESELECT_RATIO * (m - 2)) {
            preselectMinMax(buffer, start, end, m, circuitBreaker);
            lttbCore(buffer, candidates, 0, (int) candidates.size(), m, selectedIndices, circuitBreaker);
        } else {
            lttbCore(buffer, null, start, end, m, selectedIndices, circuitBreaker);
        }
    }

    /**
     * MinMaxLTTB stage 1: fill {@link #candidates} with a strictly ascending
     * preselection of buffer indices from [start, end) - the pinned first and
     * last points plus the min-value and max-value point of each of
     * {@code PRESELECT_RATIO / 2 * (m - 2)} equal row-count bins over the
     * interior. Per-bin extremes make the later triangle stage's selection
     * visually near-identical to running it over every point: LTTB rarely
     * picks a point that is not a local extreme of its bucket.
     * <p>
     * Bins are row-count based (like LTTB's own buckets), so with the
     * activation threshold in {@code selectOnRange} every bin is non-empty and
     * the survivor count is at least {@code bins + 2 >= m}: the triangle stage
     * still emits exactly m points, same as the plain path.
     */
    private void preselectMinMax(long buffer, int start, int end, int m, SqlExecutionCircuitBreaker circuitBreaker) {
        if (candidates == null) {
            candidates = new DirectLongList(64, MemoryTag.NATIVE_FUNC_RSS, true);
            candidates.setMemoryTracker(memoryTracker);
            candidates.reopen();
        }
        candidates.clear();

        final int nInner = end - start - 2;
        // (m - 2) * PRESELECT_RATIO cannot overflow in long; the activation
        // threshold caps bins below nInner / (2 * PRESELECT_MIN_SHRINK), so the
        // int cast is safe.
        final int bins = (int) ((long) (m - 2) * PRESELECT_RATIO / 2);
        final int interiorStart = start + 1;

        candidates.add(start);
        for (int b = 0; b < bins; b++) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            // Exact integer boundaries, same reasoning as LTTB's own buckets;
            // b * nInner fits a long for any int inputs.
            final int binStart = interiorStart + (int) ((long) b * nInner / bins);
            final int binEnd = interiorStart + (int) ((long) (b + 1) * nInner / bins);

            // Seed with the first row of the bin. The buffer never holds NaN
            // values (pass1 drops null/NaN rows before buffering); even if one
            // slipped in, the < and > comparisons below keep the seed, matching
            // MinMaxAlgorithm's seeding behavior, and the triangle stage
            // already tolerates NaN areas.
            int minIdx = binStart;
            int maxIdx = binStart;
            double minVal = SubsampleAlgorithm.getValue(buffer, binStart);
            double maxVal = minVal;
            for (int j = binStart + 1; j < binEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                final double v = SubsampleAlgorithm.getValue(buffer, j);
                if (v < minVal) {
                    minVal = v;
                    minIdx = j;
                }
                if (v > maxVal) {
                    maxVal = v;
                    maxIdx = j;
                }
            }
            // Emit in buffer-index order, deduplicated, so candidates stay
            // strictly ascending (bins are disjoint and exclude the pinned
            // endpoints).
            if (minIdx == maxIdx) {
                candidates.add(minIdx);
            } else if (minIdx < maxIdx) {
                candidates.add(minIdx);
                candidates.add(maxIdx);
            } else {
                candidates.add(maxIdx);
                candidates.add(minIdx);
            }
        }
        candidates.add(end - 1);
    }

    /**
     * LTTB triangle stage over positions [start, end). When {@code candidates}
     * is null, positions are buffer indices (plain LTTB over a contiguous
     * range); otherwise each position maps through the preselected candidate
     * list (MinMaxLTTB stage 2) and [start, end) indexes that list.
     */
    private static void lttbCore(long buffer, @Nullable DirectLongList candidates, int start, int end, int m,
                                 DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        int n = end - start;
        if (n < 2) {
            // Single data point or empty range - emit what's there
            for (int j = start; j < end; j++) {
                selectedIndices.add(at(candidates, j));
            }
            return;
        }
        if (m < 2) {
            // Cannot form LTTB buckets with fewer than 2 target points.
            // This should not happen in normal flow (targetPoints >= 2 is
            // validated at compile time), but guard defensively.
            selectedIndices.add(at(candidates, start));
            return;
        }

        selectedIndices.add(at(candidates, start));

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

            final long axTs = SubsampleAlgorithm.getTimestamp(buffer, at(candidates, prevSelected));
            final double ay = SubsampleAlgorithm.getValue(buffer, at(candidates, prevSelected));

            // Mean of the next bucket with x measured relative to point A. The
            // long subtraction is exact at any epoch; converting the absolute
            // epoch itself to double quantizes nanosecond timestamps to 256ns
            // steps (double ulp near 1.7e18) and cancels the area terms below.
            double avgDx = 0;
            double avgY = 0;
            int nextBucketLen = nextBucketEnd - nextBucketStart;
            for (int j = nextBucketStart; j < nextBucketEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                avgDx += (double) (SubsampleAlgorithm.getTimestamp(buffer, at(candidates, j)) - axTs);
                avgY += SubsampleAlgorithm.getValue(buffer, at(candidates, j));
            }
            if (nextBucketLen > 0) {
                avgDx /= nextBucketLen;
                avgY /= nextBucketLen;
            }

            double maxArea = -1;
            int maxAreaIndex = bucketStart;
            for (int j = bucketStart; j < bucketEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                // Triangle area (x2) with vertex A translated to the origin:
                // the cross product of edges AB and AC. Algebraically equal to
                // the absolute-coordinate determinant, but free of the
                // epoch-magnitude products whose rounding error swamps small
                // time differences.
                double dbx = (double) (SubsampleAlgorithm.getTimestamp(buffer, at(candidates, j)) - axTs);
                double by = SubsampleAlgorithm.getValue(buffer, at(candidates, j));
                double area = Math.abs(dbx * (avgY - ay) - avgDx * (by - ay));
                if (area > maxArea) {
                    maxArea = area;
                    maxAreaIndex = j;
                }
            }

            selectedIndices.add(at(candidates, maxAreaIndex));
            prevSelected = maxAreaIndex;
        }

        selectedIndices.add(at(candidates, end - 1));
    }
}
