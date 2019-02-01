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

public class IntervalFwdDataFrameCursor extends AbstractIntervalDataFrameCursor {

    /**
     * Cursor for data frames that chronologically intersect collection of intervals.
     * Data frame low and high row will be within intervals inclusive of edges. Intervals
     * themselves are pairs of microsecond time.
     *
     * @param intervals pairs of microsecond interval values, as in "low" and "high" inclusive of
     *                  edges.
     */
    public IntervalFwdDataFrameCursor(LongList intervals) {
        super(intervals);
    }

    @Override
    public boolean hasNext() {
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


                // interval is wholly above partition, skip interval
                if (column.getLong(0) > intervalHi) {
                    intervalsLo++;
                    continue;
                }

                // interval is wholly below partition, skip partition
                if (column.getLong((rowCount - 1) * 8) < intervalLo) {
                    partitionLimit = 0;
                    partitionLo++;
                    continue;
                }

                // calculate intersection

                long lo = search(column, intervalLo, partitionLimit, rowCount);
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
                    dataFrame.partitionIndex = partitionLo;
                    dataFrame.rowLo = lo;
                    dataFrame.rowHi = hi;

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

                    return true;
                }
                // interval yielded empty data frame
                partitionLimit = hi;
                intervalsLo++;
            } else {
                // partition was empty, just skip to next
                partitionLo++;
            }
        }
        return false;
    }

    @Override
    public void toTop() {
        super.toTop();
        partitionLimit = 0;
    }
}
