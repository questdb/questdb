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
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Largest Triangle Three Buckets (LTTB) downsampling algorithm.
 * <p>
 * Divides data into equal row-count buckets and selects the point in each
 * bucket that forms the largest triangle with the previously selected point
 * and the average of the next bucket. First and last points are always kept.
 * <p>
 * Supports gap-preserving mode: when gapThreshold != 0, the data is
 * split into contiguous segments where consecutive timestamps are within
 * the threshold, and each segment is downsampled independently.
 * <p>
 * The threshold is expressed in the SAME unit as the timestamps it is compared
 * against (micros for TIMESTAMP, nanos for TIMESTAMP_NS) - callers scale the
 * parsed interval into the column's native unit before constructing this
 * algorithm. It is deliberately not named "...Micros": treating it as micros
 * while comparing against nanosecond timestamps makes every threshold 1000x
 * too small. The duration uses an unsigned long: 0 disables gap detection and
 * -1L denotes saturation at unsigned MAX, which no timestamp delta can exceed.
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
    // Exact power-of-two rescale applied to the value axis when a bucket's
    // area arithmetic leaves the finite range on finite inputs. |y| * 2^-100
    // stays below 2^924 and unsigned timestamp deltas below 2^64, so both
    // cross-product terms stay below 2^991 and their difference below 2^992:
    // finite for every finite input. Power-of-two scaling never touches
    // mantissas, so it introduces no rounding and preserves area ordering.
    private static final int AREA_RESCALE_EXP = -100;
    private final long gapThreshold;
    // Reusable native lists for segment bookkeeping and MinMaxLTTB preselection.
    // Stored as cursor-lifetime fields to avoid per-execution allocation.
    // Cleared per execution.
    private DirectLongList candidates;
    @Nullable
    private MemoryTracker memoryTracker;
    private DirectLongList segments;
    private DirectLongList targets;

    public LttbAlgorithm(long gapThreshold) {
        this.gapThreshold = gapThreshold;
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
    public void select(long buffer, int bufferSize, int targetPoints, boolean hasIntegralValues,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (gapThreshold != 0 && gapThreshold != -1L) {
            selectGapPreserving(buffer, bufferSize, targetPoints, hasIntegralValues, selectedIndices, circuitBreaker);
        } else {
            selectOnRange(buffer, 0, bufferSize, targetPoints, hasIntegralValues, selectedIndices, circuitBreaker);
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
     * Reads the buffered value as a double for the preselect and triangle math.
     * Integral entries decode via {@code (double) rawLong} - the identical IEEE
     * round-to-nearest conversion pass1 applied before buffering when the value
     * slot still held a narrowed double - so lttb's geometric selection stays
     * bit-identical to the pre-dual-lane behavior.
     */
    private static double valueAsDouble(long buffer, long index, boolean hasIntegralValues) {
        return SubsampleAlgorithm.getValueAsDouble(buffer, index, hasIntegralValues);
    }

    /**
     * Converts the unsigned duration between ascending signed timestamps to double.
     * Subtract before conversion to retain small nanosecond differences at any epoch.
     */
    private static double timestampDelta(long later, long earlier) {
        assert later >= earlier;
        final long delta = later - earlier;
        // Keep a sticky low bit when halving so round-to-nearest handles ties correctly.
        return delta >= 0 ? (double) delta : 2.0 * (double) ((delta >>> 1) | (delta & 1));
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
    private void selectGapPreserving(long buffer, int n, int totalPoints, boolean hasIntegralValues,
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
                long prevTs = Unsafe.getUnsafe().getLong(buffer + (long) (i - 1) * ENTRY_SIZE);
                long currTs = Unsafe.getUnsafe().getLong(buffer + (long) i * ENTRY_SIZE);
                // Ascending signed timestamps yield an exact unsigned duration.
                isGap = Long.compareUnsigned(currTs - prevTs, gapThreshold) > 0;
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
                selectOnRange(buffer, start, start + size, segTarget, hasIntegralValues, selectedIndices, circuitBreaker);
            }
        }
    }

    /**
     * Run LTTB on a sub-range [start, end) of the buffer, switching to the
     * two-stage MinMaxLTTB variant when the range is large enough for the
     * preselection to pay off (see class doc).
     */
    private void selectOnRange(long buffer, int start, int end, int m, boolean hasIntegralValues,
                               DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        // Preselection needs at least one interior LTTB bucket (m > 2) and an
        // interior that outnumbers the worst-case survivor count
        // (PRESELECT_RATIO * (m - 2), i.e. 2 per bin) by PRESELECT_MIN_SHRINK.
        // The condition also guarantees every preselection bin holds at least
        // 2 * PRESELECT_MIN_SHRINK rows, so bins are never empty. Long math:
        // both sides fit comfortably, no overflow for any int n, m.
        if (m > 2 && (long) (end - start) - 2 > (long) PRESELECT_MIN_SHRINK * PRESELECT_RATIO * (m - 2)) {
            preselectMinMax(buffer, start, end, m, hasIntegralValues, circuitBreaker);
            lttbCore(buffer, candidates, 0, (int) candidates.size(), m, hasIntegralValues, selectedIndices, circuitBreaker);
        } else {
            lttbCore(buffer, null, start, end, m, hasIntegralValues, selectedIndices, circuitBreaker);
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
    private void preselectMinMax(long buffer, int start, int end, int m, boolean hasIntegralValues, SqlExecutionCircuitBreaker circuitBreaker) {
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

            // Seed with the first row of the bin. Per SubsampleAlgorithm's NULL
            // contract the buffer holds no non-finite value, so plain < and >
            // are sufficient here. The triangle stage cannot lean on that
            // contract alone: its area products can overflow to Infinity/NaN
            // even on finite inputs, which lttbCore repairs with a rescaled
            // replay of the affected bucket.
            int minIdx = binStart;
            int maxIdx = binStart;
            double minVal = valueAsDouble(buffer, binStart, hasIntegralValues);
            double maxVal = minVal;
            for (int j = binStart + 1; j < binEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                final double v = valueAsDouble(buffer, j, hasIntegralValues);
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
            SubsampleAlgorithm.emitAscendingPair(candidates, minIdx, maxIdx);
        }
        candidates.add(end - 1);
    }

    /**
     * LTTB triangle stage over positions [start, end). When {@code candidates}
     * is null, positions are buffer indices (plain LTTB over a contiguous
     * range); otherwise each position maps through the preselected candidate
     * list (MinMaxLTTB stage 2) and [start, end) indexes that list.
     */
    private static void lttbCore(long buffer, @Nullable DirectLongList candidates, int start, int end, int m, boolean hasIntegralValues,
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
            final double ay = valueAsDouble(buffer, at(candidates, prevSelected), hasIntegralValues);

            // Mean of the next bucket with x measured relative to point A. The
            // unsigned delta is exact at any epoch; converting the absolute
            // epoch itself to double quantizes nanosecond timestamps to 256ns
            // steps (double ulp near 1.7e18) and cancels the area terms below.
            double avgDx = 0;
            double avgY = 0;
            int nextBucketLen = nextBucketEnd - nextBucketStart;
            for (int j = nextBucketStart; j < nextBucketEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                avgDx += timestampDelta(SubsampleAlgorithm.getTimestamp(buffer, at(candidates, j)), axTs);
                avgY += valueAsDouble(buffer, at(candidates, j), hasIntegralValues);
            }
            if (nextBucketLen > 0) {
                avgDx /= nextBucketLen;
                avgY /= nextBucketLen;
            }

            double maxArea = -1;
            int maxAreaIndex = bucketStart;
            boolean sawNonFiniteArea = false;
            for (int j = bucketStart; j < bucketEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                // Triangle area (x2) with vertex A translated to the origin:
                // the cross product of edges AB and AC. Algebraically equal to
                // the absolute-coordinate determinant, but free of the
                // epoch-magnitude products whose rounding error swamps small
                // time differences.
                double dbx = timestampDelta(SubsampleAlgorithm.getTimestamp(buffer, at(candidates, j)), axTs);
                double by = valueAsDouble(buffer, at(candidates, j), hasIntegralValues);
                double area = Math.abs(dbx * (avgY - ay) - avgDx * (by - ay));
                // Finite inputs can still overflow this arithmetic: a product
                // (or the value difference itself) past Double.MAX_VALUE makes
                // the area infinite, and two same-signed infinite products
                // make it NaN. Infinite areas tie (the first candidate wins)
                // and NaN never compares greater, so either poisons the
                // argmax; replay the bucket with rescaled arithmetic below.
                sawNonFiniteArea |= !Numbers.isFinite(area);
                if (area > maxArea) {
                    maxArea = area;
                    maxAreaIndex = j;
                }
            }
            if (sawNonFiniteArea) {
                maxAreaIndex = maxAreaIndexRescaled(buffer, candidates, bucketStart, bucketEnd, nextBucketStart,
                        nextBucketEnd, axTs, ay, avgDx, avgY, hasIntegralValues, circuitBreaker);
            }

            selectedIndices.add(at(candidates, maxAreaIndex));
            prevSelected = maxAreaIndex;
        }

        selectedIndices.add(at(candidates, end - 1));
    }

    /**
     * Overflow-proof replay of a single bucket's max-area selection, used when
     * the fast path in {@link #lttbCore} produced a non-finite area. Every
     * value-axis operand is rescaled by 2^{@link #AREA_RESCALE_EXP} before
     * differencing, so neither the differences nor the cross-product terms can
     * leave the finite range, and rescaled areas carry the same mantissa
     * roundings the fast path would have produced without overflow - the
     * selected candidate is the true largest-area point. Values below 2^-922
     * rescale into the subnormal range and lose precision, but a bucket only
     * lands here when a competing magnitude is near 2^900+, which dwarfs any
     * such candidate regardless.
     */
    private static int maxAreaIndexRescaled(long buffer, @Nullable DirectLongList candidates, int bucketStart, int bucketEnd,
                                            int nextBucketStart, int nextBucketEnd, long axTs, double ay, double avgDx,
                                            double avgY, boolean hasIntegralValues, SqlExecutionCircuitBreaker circuitBreaker) {
        double avgYRescaled;
        if (Numbers.isFinite(avgY)) {
            avgYRescaled = Math.scalb(avgY, AREA_RESCALE_EXP);
        } else {
            // The mean's accumulator saturated at +-Infinity even though the
            // mean of finite values is finite. Re-accumulate in the rescaled
            // domain, where the largest addend is below 2^924 and no sum of
            // Integer.MAX_VALUE addends can overflow.
            avgYRescaled = 0;
            for (int j = nextBucketStart; j < nextBucketEnd; j++) {
                if ((j & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                avgYRescaled += Math.scalb(valueAsDouble(buffer, at(candidates, j), hasIntegralValues), AREA_RESCALE_EXP);
            }
            final int nextBucketLen = nextBucketEnd - nextBucketStart;
            if (nextBucketLen > 0) {
                avgYRescaled /= nextBucketLen;
            }
        }
        final double ayRescaled = Math.scalb(ay, AREA_RESCALE_EXP);
        final double avgDy = avgYRescaled - ayRescaled;
        double maxArea = -1;
        int maxAreaIndex = bucketStart;
        for (int j = bucketStart; j < bucketEnd; j++) {
            if ((j & 0xFFF) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            final double dbx = timestampDelta(SubsampleAlgorithm.getTimestamp(buffer, at(candidates, j)), axTs);
            final double dby = Math.scalb(valueAsDouble(buffer, at(candidates, j), hasIntegralValues), AREA_RESCALE_EXP) - ayRescaled;
            final double area = Math.abs(dbx * avgDy - avgDx * dby);
            if (area > maxArea) {
                maxArea = area;
                maxAreaIndex = j;
            }
        }
        return maxAreaIndex;
    }
}
