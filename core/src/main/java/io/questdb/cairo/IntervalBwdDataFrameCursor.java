/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LongList;
import io.questdb.std.Transient;

public class IntervalBwdDataFrameCursor extends AbstractIntervalDataFrameCursor {

    private static final Log LOG = LogFactory.getLog(IntervalBwdDataFrameCursor.class);

    /**
     * Cursor for data frames that chronologically intersect collection of intervals.
     * Data frame low and high row will be within intervals inclusive of edges. Intervals
     * themselves are pairs of microsecond time.
     *
     * @param intervals      pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalBwdDataFrameCursor(@Transient LongList intervals, int timestampIndex) {
        super(intervals, timestampIndex);
    }

    @Override
    public DataFrame next() {
        // order of logical operations is important
        // we are not calculating partition rages when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            final int currentInterval = intervalsHi - 1;
            final int currentPartition = partitionHi - 1;
            long rowCount = reader.openPartition(currentPartition);
            if (rowCount > 0) {

                final ReadOnlyColumn column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(currentPartition), timestampIndex));
                final long intervalLo = intervals.getQuick(currentInterval * 2);
                final long intervalHi = intervals.getQuick(currentInterval * 2 + 1);

                final long limitHi;
                if (partitionLimit == -1) {
                    limitHi = rowCount - 1;
                } else {
                    limitHi = partitionLimit - 1;
                }

                final long partitionTimestampLo = column.getLong(0);

                LOG.debug()
                        .$("next [partition=").$(currentPartition)
                        .$(", intervalLo=").microTime(intervalLo)
                        .$(", intervalHi=").microTime(intervalHi)
                        .$(", partitionLo=").microTime(partitionTimestampLo)
                        .$(", limitHi=").$(limitHi)
                        .$(", rowCount=").$(rowCount)
                        .$(", currentInterval=").$(currentInterval)
                        .$(']').$();

                // interval is wholly above partition, skip partition
                if (partitionTimestampLo > intervalHi) {
                    skipPartition(currentPartition);
                    continue;
                }

                // interval is wholly below partition, skip interval
                final long partitionTimestampHi = column.getLong(limitHi * Long.BYTES);
                if (partitionTimestampHi < intervalLo) {
                    skipInterval(currentInterval, limitHi + 1);
                    continue;
                }

                // calculate intersection for inclusive intervals "intervalLo" and "intervalHi"
                final long lo;
                if (partitionTimestampLo < intervalLo) {
                    lo = BinarySearch.find(column, intervalLo - 1, 0, limitHi, BinarySearch.SCAN_DOWN) + 1;
                } else {
                    lo = 0;
                }

                final long hi;
                if (partitionTimestampHi > intervalHi) {
                    hi = BinarySearch.find(column, intervalHi, lo, limitHi, BinarySearch.SCAN_DOWN) + 1;
                } else {
                    hi = limitHi + 1;
                }

                if (lo == 0) {
                    // interval yielded empty data frame, skip partition
                    skipPartition(currentPartition);
                } else {
                    // only fragment, need to skip to next interval
                    skipInterval(currentInterval, lo);
                }

                if (lo < hi) {
                    dataFrame.partitionIndex = currentPartition;
                    dataFrame.rowLo = lo;
                    dataFrame.rowHi = hi;
                    sizeSoFar += hi - lo;
                    return dataFrame;
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
