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

/**
 * Shared walk for the time-bucketed downsampling algorithms (M4 and MinMax).
 * <p>
 * Splits the buffer's timestamp span into {@code targetPoints / pointsPerBucket()}
 * equal time intervals and, for each non-empty bucket, resolves the four
 * candidate rows every time-bucketed algorithm needs - first, min, max and last -
 * in a single fused pass. Subclasses only choose how many of those four to emit
 * (see {@link #emitBucket}), which is the sole behavioural difference between
 * M4 and MinMax: the bucket boundary arithmetic, the dual-lane comparisons, the
 * circuit-breaker cadence and the output cap were previously duplicated verbatim
 * in both.
 * <p>
 * Time-based buckets (rather than row-count buckets) are what guarantee a
 * pixel-accurate envelope and naturally preserve gaps: an empty time interval
 * emits nothing.
 * <p>
 * Values are compared with plain {@code <} / {@code >}; per {@link SubsampleAlgorithm}'s
 * NULL contract the buffer holds no NULL (non-finite) value in either lane, so no
 * guard is needed here.
 *
 * @see SubsampleAlgorithm
 */
abstract class AbstractTimeBucketAlgorithm implements SubsampleAlgorithm {

    @Override
    public final void select(long buffer, int bufferSize, int targetPoints, boolean hasIntegralValues,
                             DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (bufferSize <= 0 || targetPoints <= 0) {
            return;
        }
        int numBuckets = targetPoints / pointsPerBucket();
        if (numBuckets < 1) {
            numBuckets = 1;
        }

        final long minTs = SubsampleAlgorithm.getTimestamp(buffer, 0);
        final long maxTs = SubsampleAlgorithm.getTimestamp(buffer, bufferSize - 1);
        // Ascending signed timestamps can span an unsigned 64-bit duration.
        final long span = maxTs - minTs;
        if (span == 0) {
            // A single bucket handles matching timestamps.
            numBuckets = 1;
        }
        final long bucketWidth = Long.divideUnsigned(span, numBuckets);
        final long bucketRemainder = Long.remainderUnsigned(span, numBuckets);

        int dataIdx = 0;
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            circuitBreaker.statefulThrowExceptionIfTripped();

            final long bucketStartTs = minTs + SubsampleAlgorithm.bucketOffset(bucketWidth, bucketRemainder, bucket, numBuckets);
            final long bucketEndTs = (bucket < numBuckets - 1)
                    ? minTs + SubsampleAlgorithm.bucketOffset(bucketWidth, bucketRemainder, bucket + 1, numBuckets)
                    : Long.MAX_VALUE;

            int firstIdx = -1;
            int lastIdx = -1;
            int minIdx = -1;
            int maxIdx = -1;
            double minVal = 0;
            double maxVal = 0;
            // Integral lane: exact 64-bit comparisons on the raw long values. A double compare
            // collapses LONG values beyond 2^53 and silently drops true extrema. First/last
            // tracking is positional and does not read the value slot.
            long minLong = 0;
            long maxLong = 0;

            while (dataIdx < bufferSize) {
                if ((dataIdx & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                final long ts = SubsampleAlgorithm.getTimestamp(buffer, dataIdx);
                // Final bucket processes all remaining rows (no end boundary)
                if (bucket < numBuckets - 1 && ts >= bucketEndTs) {
                    break;
                }
                if (ts >= bucketStartTs) {
                    if (firstIdx == -1) {
                        // First in-bucket row seeds min/max in both lanes; `firstIdx == -1`
                        // doubles as the "bucket still empty" flag, so no separate hasData
                        // is needed.
                        firstIdx = dataIdx;
                        minIdx = dataIdx;
                        maxIdx = dataIdx;
                        if (hasIntegralValues) {
                            minLong = maxLong = SubsampleAlgorithm.getLongValue(buffer, dataIdx);
                        } else {
                            minVal = maxVal = SubsampleAlgorithm.getValue(buffer, dataIdx);
                        }
                    } else if (hasIntegralValues) {
                        final long v = SubsampleAlgorithm.getLongValue(buffer, dataIdx);
                        if (v < minLong) {
                            minLong = v;
                            minIdx = dataIdx;
                        }
                        if (v > maxLong) {
                            maxLong = v;
                            maxIdx = dataIdx;
                        }
                    } else {
                        final double v = SubsampleAlgorithm.getValue(buffer, dataIdx);
                        if (v < minVal) {
                            minVal = v;
                            minIdx = dataIdx;
                        }
                        if (v > maxVal) {
                            maxVal = v;
                            maxIdx = dataIdx;
                        }
                    }
                    lastIdx = dataIdx;
                }
                dataIdx++;
            }

            // Empty bucket (gap in data) - skip
            if (firstIdx == -1) {
                continue;
            }
            assert minIdx >= 0 && maxIdx >= 0 : "selected indices must not be negative";
            emitBucket(selectedIndices, firstIdx, minIdx, maxIdx, lastIdx);
        }
        // Cap output to targetPoints. With small targets a single bucket can emit
        // up to pointsPerBucket() rows, exceeding the target.
        if (selectedIndices.size() > targetPoints) {
            selectedIndices.setPos(targetPoints);
        }
    }

    /**
     * Appends this bucket's chosen rows to {@code out} in strictly ascending
     * buffer-index order. Called once per non-empty bucket with the bucket's
     * first, min, max and last row indices already resolved; implementations
     * pick the subset they emit and are responsible for deduplicating indices
     * that coincide.
     */
    protected abstract void emitBucket(DirectLongList out, int firstIdx, int minIdx, int maxIdx, int lastIdx);

    /**
     * Maximum rows this algorithm emits per bucket; divides {@code targetPoints}
     * to size the bucket grid.
     */
    protected abstract int pointsPerBucket();
}
