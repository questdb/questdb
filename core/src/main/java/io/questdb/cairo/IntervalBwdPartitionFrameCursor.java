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

        // 9A: local copies of the run state -- this method must not mutate cursor state.
        int runLo1 = Integer.MAX_VALUE;
        int runIntervalLo1 = 0;
        int runResume1 = 0;

        while (partitionLo1 < partitionHi1 && (intervalsLo1 < intervalsHi1 || partitionHi1 > runLo1)) {
            if (partitionHi1 <= runLo1) {
                runLo1 = backwardRunStart(partitionHi1 - 1, partitionLo1);
                runIntervalLo1 = intervalsHi1;
                runResume1 = intervalsLo1;
            }
            // this cell has consumed every interval -- hand the run on to the next cell down
            if (intervalsLo1 >= intervalsHi1) {
                if (intervalsHi1 > runResume1) {
                    runResume1 = intervalsHi1;
                }
                partitionLimit1 = -1;
                partitionHi1--;
                intervalsHi1 = partitionHi1 <= runLo1 ? runResume1 : runIntervalLo1;
                continue;
            }
            final int currentInterval = intervalsHi1 - 1;
            final int currentPartition = partitionHi1 - 1;
            // Task 5b: a cell excluded by a composite dimension predicate is skipped WITHOUT consuming
            // the current interval -- mirrors the "whole partition"/"sibling cell" resets below (both
            // reset partitionLimit1 to -1, this method's own "no residual limit" sentinel, and retreat
            // partitionHi1 alone), so an earlier (lower-cellKey) sibling cell of the SAME day still gets
            // its own chance against this interval on the next iteration. isCellAllowed() short-circuits
            // true (zero cost) when no pruning is in effect.
            if (!isCellAllowed(currentPartition)) {
                if (intervalsHi1 > runResume1) {
                    runResume1 = intervalsHi1;
                }
                partitionHi1 = currentPartition;
                partitionLimit1 = -1;
                intervalsHi1 = partitionHi1 <= runLo1 ? runResume1 : runIntervalLo1;
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
                // Interval wholly above partition -- the CELL is exhausted (see next()'s twin).
                if (partitionTimestampLoApprox > intervalHi) {
                    if (intervalsHi1 > runResume1) {
                        runResume1 = intervalsHi1;
                    }
                    partitionHi1 = currentPartition;
                    partitionLimit1 = -1;
                    intervalsHi1 = partitionHi1 <= runLo1 ? runResume1 : runIntervalLo1;
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // interval is wholly below partition, skip interval -- unless a same-day sibling cell
                // follows, in which case fall through to the exact checks (see next()'s twin comment).
                if (partitionTimestampHiApprox < intervalLo) {
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
                    // Whole partition consumed, so the CELL is exhausted -- the INTERVAL survives via
                    // runResume1 because it may reach below this day.
                    if (intervalsHi1 > runResume1) {
                        runResume1 = intervalsHi1;
                    }
                    partitionHi1 = currentPartition;
                    partitionLimit1 = -1;
                    intervalsHi1 = partitionHi1 <= runLo1 ? runResume1 : runIntervalLo1;
                } else {
                    // Fragment: the interval's low bound fell inside this cell, so it is finished FOR
                    // THIS CELL. 9A deleted the sibling special-case and its gate.
                    partitionLimit1 = lo;
                    intervalsHi1 = currentInterval;
                }

                if (lo < hi) {
                    size += hi - lo;
                }
            } else {
                // partition was empty, just skip to next
                if (intervalsHi1 > runResume1) {
                    runResume1 = intervalsHi1;
                }
                partitionLimit1 = -1;
                partitionHi1 = currentPartition;
                intervalsHi1 = partitionHi1 <= runLo1 ? runResume1 : runIntervalLo1;
            }
        }

        counter.add(size - this.sizeSoFar);
    }

    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        // 9A: "partitionHi > runLo" is the disjunct that keeps the loop alive for a cell that has
        // consumed every interval, so it can reach retreatBackwardCell() and hand the run on to the next
        // cell DOWN. Without it the walk would stop at the first cell of a day that finished its
        // intervals, dropping every lower-cellKey sibling. runLo's "no run open" sentinel is
        // Integer.MAX_VALUE here (see toTop()), the mirror of the forward cursor's -1 for runHi.
        while (partitionLo < partitionHi && (intervalsLo < intervalsHi || partitionHi > runLo)) {
            // 9A: open a day-run on entry and whenever the previous one completed. The run is entered at
            // its TOP and every cell of it is walked from runIntervalLo downward, so each cell sees EVERY
            // interval. A PLAIN table's run is a single partition, so this collapses to a no-op there.
            if (partitionHi <= runLo) {
                beginBackwardRun();
            }
            // this cell has consumed every interval -- hand the run on to the next cell down
            if (intervalsLo >= intervalsHi) {
                retreatBackwardCell(partitionHi - 1);
                continue;
            }
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            final int currentInterval = intervalsHi - 1;
            final int currentPartition = partitionHi - 1;
            // Task 5b: see calculateSize()'s identical comment. Reuses the existing skipPartition()
            // helper -- exactly the "whole partition"/"sibling cell" reset shape below (partitionHi
            // retreats, partitionLimit resets to -1), so an earlier sibling cell of the same day still
            // gets its own chance against this interval, and the interval itself is not consumed.
            if (!isCellAllowed(currentPartition)) {
                retreatBackwardCell(currentPartition);
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
                // Interval is wholly above partition: this cell's min ts is above the current interval's
                // hi, therefore above every LATER (lower-index) interval's hi too -- the CELL is
                // exhausted, not merely this interval.
                if (partitionTimestampLoApprox > intervalHi) {
                    retreatBackwardCell(currentPartition);
                    continue;
                }

                final long partitionTimestampHiApprox = timestampFinder.maxTimestampApproxFromMetadata();
                // Interval is wholly below partition, retire the interval FOR THIS CELL. 9A dropped the
                // sibling fall-through that used to guard this: siblings walk intervals from
                // runIntervalLo themselves, so retiring it here abandons nothing.
                if (partitionTimestampHiApprox < intervalLo) {
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
                    // Whole partition consumed down to row 0, so the CELL is exhausted -- but the
                    // INTERVAL is not. It may reach below this day, so it survives via runResume rather
                    // than being retired here.
                    retreatBackwardCell(currentPartition);
                } else {
                    // Fragment: the interval's LOW bound fell inside this cell, so this interval is
                    // finished FOR THIS CELL. 9A deleted the sibling special-case that used to sit here,
                    // along with the gate it carried: a sibling no longer depends on this cell declining
                    // to retire the interval, because it walks intervals from runIntervalLo itself.
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
                retreatBackwardCell(currentPartition);
            }
        }
        return null;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = -1;
        // 9A: the backward walk tests "partitionHi <= runLo" for "open a new run", so its no-run-open
        // sentinel must be ABOVE every partition index -- the mirror of the forward cursor's runHi = -1.
        runLo = Integer.MAX_VALUE;
    }

    /**
     * Ends the current cell and moves to the next one DOWN, folding this cell's reached interval bound
     * into the run's resume point.
     * <p>
     * {@code runResume} is the MAXIMUM bound any cell of the run reached, and becomes the global
     * {@code intervalsHi} once the run completes. The maximum, because walking downward an interval that
     * reaches BELOW this day must stay live for the next (earlier) day; taking the last cell's bound
     * would retire it early and silently drop its rows.
     */
    private void retreatBackwardCell(int currentPartition) {
        if (intervalsHi > runResume) {
            runResume = intervalsHi;
        }
        partitionLimit = -1;
        partitionHi = currentPartition;
        // run complete -> publish the resume point; otherwise the next cell of the SAME day restarts at
        // this run's first interval bound
        intervalsHi = partitionHi <= runLo ? runResume : runIntervalLo;
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
