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

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        // Mirrors next(): walks partitions and intervals from the top down,
        // accumulating the row count of every frame next() would yield from the
        // current position, without mutating the cursor's iteration state.
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long size = 0;

        while (intervalsLo1 < intervalsHi1 && partitionLo1 < partitionHi1) {
            final int currentInterval = intervalsHi1 - 1;
            final int currentPartition = partitionHi1 - 1;
            final long intervalLo = intervals.getQuick(currentInterval * 2);
            final long intervalHi = intervals.getQuick(currentInterval * 2 + 1);
            if (hasAnyDelta()) {
                final long calendarLo = getPartitionCalendarLo(currentPartition);
                if (calendarLo > intervalHi) {
                    partitionHi1 = currentPartition;
                    continue;
                }
                final long calendarHi = getPartitionCalendarHi(currentPartition);
                if (calendarHi != Long.MAX_VALUE && calendarHi <= intervalLo) {
                    intervalsHi1 = currentInterval;
                    continue;
                }
            }

            final long baseRowCount = reader.getPartitionRowCountFromMetadata(currentPartition);
            final boolean hasDelta = reader.getTxFile().getPartitionHasDelta(currentPartition);
            if (baseRowCount == 0 && !hasDelta) {
                partitionHi1 = currentPartition;
                continue;
            }
            final TimestampFinder timestampFinder = initTimestampFinder(currentPartition, baseRowCount);
            final long logicalRowCount = getCurrentLogicalRowCount();
            if (logicalRowCount == 0) {
                partitionHi1 = currentPartition;
                continue;
            }
            if (getCurrentPartitionFrameState() == 0) {
                if (timestampFinder.minTimestampLowerBound() > intervalHi) {
                    partitionHi1 = currentPartition;
                    continue;
                }
                if (timestampFinder.maxTimestampUpperBound() < intervalLo) {
                    intervalsHi1 = currentInterval;
                    continue;
                }
            }

            reader.openPartition(currentPartition);
            timestampFinder.prepare();
            final long lo = timestampFinder.countBefore(intervalLo);
            final long hi = timestampFinder.countThrough(intervalHi);
            validateIntervalBounds(currentPartition, lo, hi);
            if (lo < hi) {
                size = Math.addExact(size, hi - lo);
            }
            if (lo == 0) {
                partitionHi1 = currentPartition;
            } else {
                intervalsHi1 = currentInterval;
            }
        }

        counter.add(size);
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
            final long intervalLo = intervals.getQuick(currentInterval * 2);
            final long intervalHi = intervals.getQuick(currentInterval * 2 + 1);
            if (hasAnyDelta()) {
                final long calendarLo = getPartitionCalendarLo(currentPartition);
                if (calendarLo > intervalHi) {
                    partitionHi = currentPartition;
                    continue;
                }
                final long calendarHi = getPartitionCalendarHi(currentPartition);
                if (calendarHi != Long.MAX_VALUE && calendarHi <= intervalLo) {
                    intervalsHi = currentInterval;
                    continue;
                }
            }

            final long baseRowCount = reader.getPartitionRowCountFromMetadata(currentPartition);
            final boolean hasDelta = reader.getTxFile().getPartitionHasDelta(currentPartition);
            if (baseRowCount == 0 && !hasDelta) {
                partitionHi = currentPartition;
                continue;
            }
            final TimestampFinder timestampFinder = initTimestampFinder(currentPartition, baseRowCount);
            final long logicalRowCount = getCurrentLogicalRowCount();
            if (logicalRowCount == 0) {
                partitionHi = currentPartition;
                continue;
            }
            if (getCurrentPartitionFrameState() == 0) {
                if (timestampFinder.minTimestampLowerBound() > intervalHi) {
                    partitionHi = currentPartition;
                    continue;
                }
                if (timestampFinder.maxTimestampUpperBound() < intervalLo) {
                    intervalsHi = currentInterval;
                    continue;
                }
            }

            LOG.debug()
                    .$("next [partition=").$(currentPartition)
                    .$(", intervalLo=").$ts(intervalModel.getTimestampDriver(), intervalLo)
                    .$(", intervalHi=").$ts(intervalModel.getTimestampDriver(), intervalHi)
                    .$(", rowCount=").$(logicalRowCount)
                    .$(", currentInterval=").$(currentInterval)
                    .I$();

            reader.openPartition(currentPartition);
            timestampFinder.prepare();
            final long lo = timestampFinder.countBefore(intervalLo);
            final long hi = timestampFinder.countThrough(intervalHi);
            validateIntervalBounds(currentPartition, lo, hi);
            if (lo == 0) {
                partitionHi = currentPartition;
            } else {
                intervalsHi = currentInterval;
            }
            if (lo < hi) {
                populateFrame(currentPartition, lo, hi);
                sizeSoFar = Math.addExact(sizeSoFar, hi - lo);
                return frame;
            }
        }
        return null;
    }
}
