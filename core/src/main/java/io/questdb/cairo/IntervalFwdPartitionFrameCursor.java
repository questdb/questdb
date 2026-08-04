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

public class IntervalFwdPartitionFrameCursor extends AbstractIntervalPartitionFrameCursor {
    private static final Log LOG = LogFactory.getLog(IntervalFwdPartitionFrameCursor.class);

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
    public IntervalFwdPartitionFrameCursor(CairoConfiguration configuration, RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        super(configuration, intervalModel, timestampIndex);
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        int intervalsLo1 = this.intervalsLo;
        int intervalsHi1 = this.intervalsHi;
        int partitionLo1 = this.partitionLo;
        int partitionHi1 = this.partitionHi;
        long size = 0;

        while (intervalsLo1 < intervalsHi1 && partitionLo1 < partitionHi1) {
            final long intervalLo = intervals.getQuick(intervalsLo1 * 2);
            final long intervalHi = intervals.getQuick(intervalsLo1 * 2 + 1);
            if (hasAnyDelta()) {
                final long calendarLo = getPartitionCalendarLo(partitionLo1);
                if (calendarLo > intervalHi) {
                    intervalsLo1++;
                    continue;
                }
                final long calendarHi = getPartitionCalendarHi(partitionLo1);
                if (calendarHi != Long.MAX_VALUE && calendarHi <= intervalLo) {
                    partitionLo1++;
                    continue;
                }
            }

            final long baseRowCount = reader.getPartitionRowCountFromMetadata(partitionLo1);
            final boolean hasDelta = reader.getTxFile().getPartitionHasDelta(partitionLo1);
            if (baseRowCount == 0 && !hasDelta) {
                partitionLo1++;
                continue;
            }
            final TimestampFinder timestampFinder = initTimestampFinder(partitionLo1, baseRowCount);
            final long logicalRowCount = getCurrentLogicalRowCount();
            if (logicalRowCount == 0) {
                partitionLo1++;
                continue;
            }
            if (getCurrentPartitionFrameState() == 0) {
                if (timestampFinder.minTimestampLowerBound() > intervalHi) {
                    intervalsLo1++;
                    continue;
                }
                if (timestampFinder.maxTimestampUpperBound() < intervalLo) {
                    partitionLo1++;
                    continue;
                }
            }

            reader.openPartition(partitionLo1);
            timestampFinder.prepare();
            final long lo = timestampFinder.countBefore(intervalLo);
            final long hi = timestampFinder.countThrough(intervalHi);
            validateIntervalBounds(partitionLo1, lo, hi);
            if (lo < hi) {
                size = Math.addExact(size, hi - lo);
            }
            if (hi == logicalRowCount) {
                partitionLo1++;
            } else {
                intervalsLo1++;
            }
        }

        counter.add(size);
    }

    @Override
    public PartitionFrame next(long skipTarget) {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            final long intervalLo = intervals.getQuick(intervalsLo * 2);
            final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);
            if (hasAnyDelta()) {
                final long calendarLo = getPartitionCalendarLo(partitionLo);
                if (calendarLo > intervalHi) {
                    intervalsLo++;
                    continue;
                }
                final long calendarHi = getPartitionCalendarHi(partitionLo);
                if (calendarHi != Long.MAX_VALUE && calendarHi <= intervalLo) {
                    partitionLo++;
                    continue;
                }
            }

            final long baseRowCount = reader.getPartitionRowCountFromMetadata(partitionLo);
            final boolean hasDelta = reader.getTxFile().getPartitionHasDelta(partitionLo);
            if (baseRowCount == 0 && !hasDelta) {
                partitionLo++;
                continue;
            }
            final TimestampFinder timestampFinder = initTimestampFinder(partitionLo, baseRowCount);
            final long logicalRowCount = getCurrentLogicalRowCount();
            if (logicalRowCount == 0) {
                partitionLo++;
                continue;
            }
            if (getCurrentPartitionFrameState() == 0) {
                if (timestampFinder.minTimestampLowerBound() > intervalHi) {
                    intervalsLo++;
                    continue;
                }
                if (timestampFinder.maxTimestampUpperBound() < intervalLo) {
                    partitionLo++;
                    continue;
                }
            }

            LOG.debug()
                    .$("next [partition=").$(partitionLo)
                    .$(", intervalLo=").$ts(intervalModel.getTimestampDriver(), intervalLo)
                    .$(", intervalHi=").$ts(intervalModel.getTimestampDriver(), intervalHi)
                    .$(", rowCount=").$(logicalRowCount)
                    .I$();

            reader.openPartition(partitionLo);
            timestampFinder.prepare();
            final long lo = timestampFinder.countBefore(intervalLo);
            final long hi = timestampFinder.countThrough(intervalHi);
            validateIntervalBounds(partitionLo, lo, hi);
            final int framePartition = partitionLo;
            if (hi == logicalRowCount) {
                partitionLo++;
            } else {
                intervalsLo++;
            }
            if (lo < hi) {
                populateFrame(framePartition, lo, hi);
                sizeSoFar = Math.addExact(sizeSoFar, hi - lo);
                return frame;
            }
        }
        return null;
    }
}
