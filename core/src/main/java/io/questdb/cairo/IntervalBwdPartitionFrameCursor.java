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
                // interval is wholly below partition, skip interval
                if (partitionTimestampHiApprox < intervalLo) {
                    partitionLimit1 = limitHi + 1;
                    intervalsHi1 = currentInterval;
                    continue;
                }

                reader.openPartition(currentPartition);
                timestampFinder.prepare();

                // interval is wholly below partition, skip interval
                final long partitionTimestampHiExact = timestampFinder.timestampAt(limitHi);
                if (partitionTimestampHiExact < intervalLo) {
                    partitionLimit1 = limitHi + 1;
                    intervalsHi1 = currentInterval;
                    continue;
                }

                // calculate intersection for inclusive intervals "intervalLo" and "intervalHi"
                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
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
                // interval is wholly below partition, skip interval
                if (partitionTimestampHiApprox < intervalLo) {
                    skipInterval(currentInterval, limitHi + 1);
                    continue;
                }

                reader.openPartition(currentPartition);
                timestampFinder.prepare();

                // interval is wholly below partition, skip interval
                final long partitionTimestampHiExact = timestampFinder.timestampAt(limitHi);
                if (partitionTimestampHiExact < intervalLo) {
                    skipInterval(currentInterval, limitHi + 1);
                    continue;
                }

                // calculate intersection for inclusive intervals "intervalLo" and "intervalHi"
                final long partitionTimestampLoExact = timestampFinder.minTimestampExact();
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
                } else {
                    // only fragment, need to skip to next interval
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
