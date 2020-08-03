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
import io.questdb.std.LongList;

public class IntervalFwdDataFrameCursor extends AbstractIntervalDataFrameCursor {
    /**
     * Cursor for data frames that chronologically intersect collection of intervals.
     * Data frame low and high row will be within intervals inclusive of edges. Intervals
     * themselves are pairs of microsecond time.
     *
     * @param intervals      pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                       edges.
     * @param timestampIndex index of timestamp column in the readr that is used by this cursor
     */
    public IntervalFwdDataFrameCursor(LongList intervals, int timestampIndex) {
        super(intervals, timestampIndex);
    }

    @Override
    public DataFrame next() {
        // order of logical operations is important
        // we are not calculating partition rages when intervals are empty
        while (intervalsLo < intervalsHi && partitionLo < partitionHi) {
            // We don't need to worry about column tops and null column because we
            // are working with timestamp. Timestamp column cannot be added to existing table.
            long rowCount = reader.openPartition(partitionLo);
            if (rowCount > 0) {

                final ReadOnlyColumn column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionLo), timestampIndex));
                final long intervalLo = intervals.getQuick(intervalsLo * 2);
                final long intervalHi = intervals.getQuick(intervalsLo * 2 + 1);


                final long partitionTimestampLo = column.getLong(0);
                // interval is wholly above partition, skip interval
                if (partitionTimestampLo > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                final long partitionTimestampHi = column.getLong((rowCount - 1) * 8);
                // interval is wholly below partition, skip partition
                if (partitionTimestampHi < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo;
                if (partitionTimestampLo == intervalLo) {
                    lo = 0;
                } else {
                    lo = BinarySearch.findOrEmplace(column, intervalLo, partitionLimit, rowCount, BinarySearch.SCAN_UP);
                    if (lo < 0) {
                        lo = -lo - 1;
                    }
                }

                long hi = BinarySearch.findOrEmplace(column, intervalHi, lo, rowCount, BinarySearch.SCAN_DOWN);

                if (hi < 0) {
                    hi = -hi - 1;
                } else {
                    // We have direct hit. Interval is inclusive of edges and we have to
                    // bump to high bound because it is non-inclusive
                    hi++;
                }

                if (lo < hi) {
                    dataFrame.partitionIndex = partitionLo;
                    dataFrame.rowLo = lo;
                    dataFrame.rowHi = hi;
                    sizeSoFar += (hi - lo);

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

                    return dataFrame;
                }
                // interval yielded empty data frame
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
