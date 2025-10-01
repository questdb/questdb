/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
     * @param intervalModel  pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalBwdPartitionFrameCursor(RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        super(intervalModel, timestampIndex);
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
                        assert parquetDecoder.getFileAddr() != -1 : "parquet decoder is not initialized";
                        frame.format = PartitionFormat.PARQUET;
                        frame.parquetDecoder = parquetDecoder;
                    } else {
                        assert format == PartitionFormat.NATIVE;
                        frame.format = PartitionFormat.NATIVE;
                        frame.parquetDecoder = null;
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
