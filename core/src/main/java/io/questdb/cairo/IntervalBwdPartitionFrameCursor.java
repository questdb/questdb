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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

public class IntervalBwdPartitionFrameCursor extends AbstractIntervalPartitionFrameCursor {
    private static final Log LOG = LogFactory.getLog(IntervalBwdPartitionFrameCursor.class);

    /**
     * Cursor for partition frames that chronologically intersect collection of intervals.
     * Partition frame low and high row will be within intervals inclusive of edges.
     * Intervals themselves are pairs of microsecond time.
     *
     * @param configuration  engine configuration used to resolve the partition parquet decoder
     * @param intervalModel  pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalBwdPartitionFrameCursor(CairoConfiguration configuration, RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        super(configuration, intervalModel, timestampIndex);
    }

    /**
     * NOTE on the composite sibling-cell handling below, mirrored from {@link #next(long)}: no COMPOSITE
     * query reaches this method today. The composite cross-cell merge cursor wraps this one and counts by
     * iterating {@code next()} rather than delegating here -- measured, by instrumenting this method and
     * finding that {@code SELECT count()} with a timestamp filter reaches it on a PLAIN table and on no
     * composite query tried. The mirrored logic is kept deliberately rather than left out: counting by
     * iteration is an obvious thing to optimise later, and a delegation added then would silently
     * reintroduce the dropped-rows defect this fix exists for. It is unreachable-for-composite
     * futureproofing, NOT a live fix -- the live fix is in {@code next()}.
     */
    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        // Mirrors next(): walks partitions and intervals from the top down,
        // accumulating the row count of every frame next() would yield from the
        // current position, without mutating the cursor's iteration state.
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long partitionLimit1 = this.partitionLimit;
        long size = this.sizeSoFar;

        while (intervalsLo1 < intervalsHi1 && partitionLo1 < partitionHi1) {
            final int currentInterval = intervalsHi1 - 1;
            final int currentPartition = partitionHi1 - 1;
            // Task 5b: a cell excluded by a composite dimension predicate is skipped WITHOUT consuming
            // the current interval -- mirrors the "whole partition"/"sibling cell" resets below (both
            // reset partitionLimit1 to -1, this method's own "no residual limit" sentinel, and retreat
            // partitionHi1 alone), so an earlier (lower-cellKey) sibling cell of the SAME day still gets
            // its own chance against this interval on the next iteration. isCellAllowed() short-circuits
            // true (zero cost) when no pruning is in effect.
            if (!isCellAllowed(currentPartition)) {
                partitionHi1 = currentPartition;
                partitionLimit1 = -1;
                continue;
            }
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            final long rowCount = reader.getPartitionRowCountFromMetadata(currentPartition);
            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(currentPartition, rowCount);

                final long intervalLo = intervals.getQuick(currentInterval * 2);
                final long intervalHi = intervals.getQuick(currentInterval * 2 + 1);

                final long limitHi = partitionLimit1 == -1 ? rowCount - 1 : partitionLimit1 - 1;

                final long partitionTimestampLoApprox = timestampFinder.minTimestampApproxFromMetadata();
                // interval is wholly above partition, skip partition
                if (partitionTimestampLoApprox > intervalHi) {
                    partitionHi1 = currentPartition;
                    partitionLimit1 = -1;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // interval is wholly below partition, skip interval -- unless a same-day sibling cell
                // follows, in which case fall through to the exact checks (see next()'s twin comment).
                if (partitionTimestampHiApprox < intervalLo && !hasSameDaySiblingBelow(currentPartition, partitionLo1)) {
                    partitionLimit1 = limitHi + 1;
                    intervalsHi1 = currentInterval;
                    continue;
                }

                reader.openPartition(currentPartition);
                timestampFinder.prepare();

                final long partitionTimestampHiExact = timestampFinder.timestampAt(limitHi);
                // calculate intersection for inclusive intervals "intervalLo" and "intervalHi"
                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                // interval is wholly below partition, skip interval
                if (partitionTimestampHiExact < intervalLo) {
                    if (hasSameDaySiblingBelow(currentPartition, partitionLo1)) {
                        if (currentInterval - 1 >= intervalsLo1
                                && intervals.getQuick((currentInterval - 1) * 2 + 1) >= partitionTimestampLoExact) {
                            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                        }
                        partitionHi1 = currentPartition;
                        partitionLimit1 = -1;
                        continue;
                    }
                    partitionLimit1 = limitHi + 1;
                    intervalsHi1 = currentInterval;
                    continue;
                }

                final long lo;
                if (partitionTimestampLoExact < intervalLo) {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, 0, limitHi) + 1;
                } else {
                    lo = 0;
                }

                final long hi;
                if (partitionTimestampHiExact > intervalHi) {
                    hi = timestampFinder.findTimestamp(intervalHi, lo, limitHi) + 1;
                } else {
                    hi = limitHi + 1;
                }

                if (lo == 0) {
                    // whole partition, skip to next one
                    partitionHi1 = currentPartition;
                    partitionLimit1 = -1;
                } else if (currentPartition - 1 >= partitionLo1
                        && reader.getPartitionTimestampByIndex(currentPartition - 1) == reader.getPartitionTimestampByIndex(currentPartition)) {
                    // Fragment, but a sibling cell of the same day (composite table) still needs its
                    // own chance to be checked against this SAME interval -- mirrors next()'s own fix
                    // (Task 6c finding), see this class's javadoc.
                    // Task 6c review Part A: symmetric with next() -- gate the unsupported
                    // multiple-sub-day-intervals-over-one-multi-cell-day shape so count() agrees with the
                    // backward row scan (both throw) rather than silently miscounting the dropped rows.
                    if (currentInterval - 1 >= intervalsLo1
                            && intervals.getQuick((currentInterval - 1) * 2 + 1) >= partitionTimestampLoExact) {
                        throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                    }
                    partitionHi1 = currentPartition;
                    partitionLimit1 = -1;
                } else {
                    // only fragment, skip to next interval
                    partitionLimit1 = lo;
                    intervalsHi1 = currentInterval;
                }

                if (lo < hi) {
                    size += hi - lo;
                }
            } else {
                // partition was empty, just skip to next
                partitionLimit1 = -1;
                partitionHi1 = currentPartition;
            }
        }

        counter.add(size - this.sizeSoFar);
    }

    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            final int currentInterval = intervalsHi - 1;
            final int currentPartition = partitionHi - 1;
            // Task 5b: see calculateSize()'s identical comment. Reuses the existing skipPartition()
            // helper -- exactly the "whole partition"/"sibling cell" reset shape below (partitionHi
            // retreats, partitionLimit resets to -1), so an earlier sibling cell of the same day still
            // gets its own chance against this interval, and the interval itself is not consumed.
            if (!isCellAllowed(currentPartition)) {
                skipPartition(currentPartition);
                continue;
            }
            long rowCount = reader.getPartitionRowCountFromMetadata(currentPartition);
            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(currentPartition, rowCount);

                final long intervalLo = intervals.getQuick(currentInterval * 2);
                final long intervalHi = intervals.getQuick(currentInterval * 2 + 1);

                final long limitHi;
                if (partitionLimit == -1) {
                    limitHi = rowCount - 1;
                } else {
                    limitHi = partitionLimit - 1;
                }

                LOG.debug()
                        .$("next [partition=").$(currentPartition)
                        .$(", intervalLo=").$ts(intervalModel.getTimestampDriver(), intervalLo)
                        .$(", intervalHi=").$ts(intervalModel.getTimestampDriver(), intervalHi)
                        .$(", limitHi=").$(limitHi)
                        .$(", rowCount=").$(rowCount)
                        .$(", currentInterval=").$(currentInterval)
                        .I$();

                final long partitionTimestampLoApprox = timestampFinder.minTimestampApproxFromMetadata();
                // interval is wholly above partition, skip partition
                if (partitionTimestampLoApprox > intervalHi) {
                    skipPartition(currentPartition);
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // Interval is wholly below partition, skip interval -- UNLESS a sibling cell of the same
                // day follows in this backward walk (see retireIntervalOrVisitSibling). Falling through
                // to the exact checks handles that case uniformly; the extra partition open is paid only
                // by a composite multi-cell day, and this is unreachable for a plain table.
                if (partitionTimestampHiApprox < intervalLo && !hasSameDaySiblingBelow(currentPartition, partitionLo)) {
                    skipInterval(currentInterval, limitHi + 1);
                    continue;
                }

                reader.openPartition(currentPartition);
                timestampFinder.prepare();

                final long partitionTimestampHiExact = timestampFinder.timestampAt(limitHi);
                // calculate intersection for inclusive intervals "intervalLo" and "intervalHi"
                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
                // interval is wholly below partition, skip interval
                if (partitionTimestampHiExact < intervalLo) {
                    if (retireIntervalOrVisitSibling(currentPartition, currentInterval, partitionTimestampLoExact)) {
                        continue;
                    }
                    skipInterval(currentInterval, limitHi + 1);
                    continue;
                }

                final long lo;
                if (partitionTimestampLoExact < intervalLo) {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, 0, limitHi) + 1;
                } else {
                    lo = 0;
                }

                final long hi;
                if (partitionTimestampHiExact > intervalHi) {
                    hi = timestampFinder.findTimestamp(intervalHi, lo, limitHi) + 1;
                } else {
                    hi = limitHi + 1;
                }

                if (lo == 0) {
                    // interval yielded empty partition frame, skip partition
                    skipPartition(currentPartition);
                } else if (currentPartition - 1 >= partitionLo
                        && reader.getPartitionTimestampByIndex(currentPartition - 1) == reader.getPartitionTimestampByIndex(currentPartition)) {
                    // Fragment (interval's LOW bound reached mid-partition), but a SIBLING cell of the
                    // SAME day (composite table, lower cellKey, not yet visited in this backward scan)
                    // still needs its own chance to be checked against this SAME interval -- do NOT
                    // retire the interval yet. Task 6c (read-side differential capstone) finding:
                    // symmetric counterpart of IntervalFwdPartitionFrameCursor#next()'s own fix -- see
                    // that class's javadoc for the full mechanism (here it is the LOW bound / lower
                    // cellKey side of the same underlying defect, since backward scan visits a day's
                    // cells highest-cellKey-first). Provably byte-identical to the prior behaviour for a
                    // plain table (currentPartition - 1 is always the PREVIOUS DAY there, never a
                    // same-timestamp sibling) -- kept unconditional, mirroring 233532984f's own
                    // precedent.
                    //
                    // Task 6c review Part A (symmetric with the forward cursor): 2+ intervals over this
                    // SAME multi-cell day are not yet supported. Skipping to the sibling ABANDONS this
                    // fragmented cell's leftover rows (those BELOW lo, i.e. below intervalLo), and the
                    // monotonic backward walk can never revisit it for a LOWER (later-in-scan) interval.
                    // If that lower interval reaches into this cell's own span (its hi >= this cell's exact
                    // MIN ts) those leftover rows would be SILENTLY dropped -- gate loudly instead.
                    if (currentInterval - 1 >= intervalsLo
                            && intervals.getQuick((currentInterval - 1) * 2 + 1) >= partitionTimestampLoExact) {
                        throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
                    }
                    skipPartition(currentPartition);
                } else {
                    // only fragment, no sibling cell left to check -- this interval is now fully
                    // satisfied, exactly as before this fix.
                    skipInterval(currentInterval, lo);
                }

                if (lo < hi) {
                    frame.partitionIndex = currentPartition;
                    frame.rowLo = lo;
                    frame.rowHi = hi;
                    sizeSoFar += hi - lo;

                    final byte format = reader.getPartitionFormat(currentPartition);
                    if (format == PartitionFormat.PARQUET) {
                        frame.format = PartitionFormat.PARQUET;
                        frame.parquetMetaDecoder = reader.getAndInitParquetPartitionDecoder(currentPartition);
                    } else {
                        assert format == PartitionFormat.NATIVE;
                        frame.format = PartitionFormat.NATIVE;
                        frame.parquetMetaDecoder = null;
                    }

                    return frame;
                }
            } else {
                // partition was empty, just skip to next
                partitionLimit = -1;
                partitionHi = currentPartition;
            }
        }
        return null;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = -1;
    }

    /**
     * Whether the partition BELOW {@code partitionIndex} — the next one this backward walk will visit —
     * is a SIBLING CELL of the same day rather than the previous day, i.e. whether the two share a
     * partition timestamp. Only a composite table's multi-cell day produces this; for a plain table it is
     * always {@code false}, which is what keeps every caller byte-identical for plain tables.
     */
    private boolean hasSameDaySiblingBelow(int partitionIndex, int partitionLoBound) {
        return partitionIndex - 1 >= partitionLoBound
                && reader.getPartitionTimestampByIndex(partitionIndex - 1)
                == reader.getPartitionTimestampByIndex(partitionIndex);
    }

    /**
     * Shared tail for {@link #next(long)}'s "cell wholly below the interval" exit. Returns {@code true}
     * when the caller should {@code continue} at the next same-day sibling cell instead of retiring the
     * interval.
     * <p>
     * Retiring the interval there is correct for a plain table, where {@code currentPartition - 1} is
     * always the PREVIOUS DAY. It is wrong for a composite multi-cell day, where it can be a SIBLING
     * CELL — a separate cell whose rows may fall inside the interval this one sits entirely below.
     * Retiring the interval drops them silently: a backward scan (any {@code ORDER BY ts DESC} with a
     * timestamp filter) returned NO rows where the forward scan of the same data returned one.
     * <p>
     * This mirrors the forward cursor's fix. It carries the same trade and the same guard: advancing
     * abandons this cell for any LOWER interval the backward walk visits next, so if such an interval
     * reaches into this cell's own span, fail loudly rather than drop rows.
     */
    private boolean retireIntervalOrVisitSibling(int currentPartition, int currentInterval, long partitionTimestampLoExact) {
        if (!hasSameDaySiblingBelow(currentPartition, partitionLo)) {
            return false;
        }
        if (currentInterval - 1 >= intervalsLo
                && intervals.getQuick((currentInterval - 1) * 2 + 1) >= partitionTimestampLoExact) {
            throw multipleSubDayIntervalsOverMultiCellDayUnsupported();
        }
        skipPartition(currentPartition);
        return true;
    }

    private void skipInterval(int intervalIndex, long limit) {
        LOG.debug().$("next skips interval [partitionLimit=").$(limit).$(", intervalsHi=").$(intervalIndex).$(']').$();
        partitionLimit = limit; // use "limit" for max
        intervalsHi = intervalIndex;
    }

    private void skipPartition(int currentPartition) {
        LOG.debug().$("next skips partition").$();
        partitionHi = currentPartition;
        partitionLimit = -1;
    }
}
