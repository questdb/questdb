/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.std.LongList;

public class IntervalBwdDataFrameCursor extends AbstractIntervalDataFrameCursor {

    /**
     * Cursor for data frames that chronologically intersect collection of intervals.
     * Data frame low and high row will be within intervals inclusive of edges. Intervals
     * themselves are pairs of microsecond time.
     *
     * @param intervals pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                  edges.
     */
    public IntervalBwdDataFrameCursor(LongList intervals) {
        super(intervals);
    }

    @Override
    public boolean hasNext() {
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


                // interval is wholly above partition, skip interval
                if (column.getLong(0) > intervalHi) {
                    partitionHi = currentPartition;
                    partitionLimit = -1;
                    continue;
                }

                // interval is wholly below partition, skip partition
                if (column.getLong((rowCount - 1) * 8) < intervalLo) {
                    partitionLimit = -1;
                    intervalsHi = currentInterval;
                    continue;
                }

                // calculate intersection

                long lo = search(column, intervalLo, 0, partitionLimit == -1 ? rowCount : partitionLimit);
                if (lo < 0) {
                    lo = -lo - 1;
                }

                long hi = search(column, intervalHi, lo, rowCount);

                if (hi < 0) {
                    hi = -hi - 1;
                } else {
                    // We have direct hit. Interval is inclusive of edges and we have to
                    // bump to high bound because it is non-inclusive
                    hi++;
                }

                if (lo < hi) {
                    dataFrame.partitionIndex = currentPartition;
                    dataFrame.rowLo = lo;
                    dataFrame.rowHi = hi;

                    // we do have whole partition of fragment?
                    if (lo == 0) {
                        // whole partition, will need to skip to next one
                        partitionLimit = -1; // use row count next time
                        partitionHi = currentPartition;
                    } else {
                        // only fragment, need to skip to next interval
                        partitionLimit = lo; // use "lo" for max
                        intervalsHi = currentInterval;
                    }

                    return true;
                }
                // interval yielded empty data frame
                partitionLimit = lo;
                intervalsHi = currentInterval;
            } else {
                // partition was empty, just skip to next
                partitionHi = currentPartition;
            }
        }
        return false;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = -1;
    }
}
