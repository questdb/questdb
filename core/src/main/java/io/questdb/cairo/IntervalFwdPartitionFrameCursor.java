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

public class IntervalFwdPartitionFrameCursor extends AbstractIntervalPartitionFrameCursor {
    private static final Log LOG = LogFactory.getLog(IntervalFwdPartitionFrameCursor.class);

    /**
     * Cursor for partition frames that chronologically intersect collection of intervals.
     * Partition frame low and high row will be within intervals inclusive of edges.
     * Intervals themselves are pairs of microsecond time.
     *
     * @param intervalModel  pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalFwdPartitionFrameCursor(RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        super(intervalModel, timestampIndex);
    }

    @Override
    public PartitionFrame next() {
        // order of logical operations is important
        // we are not calculating partition ranges when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount = reader.openPartition(partitionLo);
            if (rowCount > 0) {
                final TimestampFinder timestampFinder = initTimestampFinder(partitionLo, rowCount);

                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);

                final long partitionTimestampLo = timestampFinder.minTimestamp();
                // interval is wholly above partition, skip interval
                if (partitionTimestampLo > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHi = timestampFinder.maxTimestamp();

                LOG.debug()
                        .$("next [partition=").$(partitionLo)
                        .$(", intervalLo=").microTime(intervalLo)
                        .$(", intervalHi=").microTime(intervalHi)
                        .$(", partitionHi=").microTime(partitionTimestampHi)
                        .$(", partitionLimit=").$(partitionLimit)
                        .$(", rowCount=").$(rowCount)
                        .I$();

                // interval is wholly below partition, skip partition
                if (partitionTimestampHi < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo;
                if (partitionTimestampLo < intervalLo) {
                    // intervalLo is inclusive of value. We will look for bottom index of intervalLo - 1
                    // and then do index + 1 to skip to top of where we need to be.
                    // We are not scanning up on the exact value of intervalLo because it may not exist. In which case
                    // the search function will scan up to top of the lower value.
                    lo = timestampFinder.findTimestamp(intervalLo - 1, partitionLimit, rowCount - 1) + 1;
                } else {
                    lo = 0;
                }

                final long hi;
                if (partitionTimestampHi > intervalHi) {
                    hi = timestampFinder.findTimestamp(intervalHi, lo, rowCount - 1) + 1;
                } else {
                    hi = rowCount;
                }

                if (lo < hi) {
                    frame.partitionIndex = partitionLo;
                    frame.rowLo = lo;
                    frame.rowHi = hi;
                    sizeSoFar += (hi - lo);

                    final byte format = reader.getPartitionFormat(partitionLo);
                    if (format == PartitionFormat.PARQUET) {
                        assert parquetDecoder.getFileAddr() != -1 : "parquet decoder is not initialized";
                        frame.format = PartitionFormat.PARQUET;
                        frame.parquetDecoder = parquetDecoder;
                    } else {
                        assert format == PartitionFormat.NATIVE;
                        frame.format = PartitionFormat.NATIVE;
                        frame.parquetDecoder = null;
                    }

                    // we do have whole partition of fragment?
                    if (hi == rowCount) {
                        // whole partition, will need to skip to next one
                        partitionLimit = 0;
                        partitionLo++;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionLimit = hi;
                        intervalsLo++;
                    }

                    return frame;
                }
                // interval yielded empty partition frame
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }
        return null;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = 0;
    }
}
