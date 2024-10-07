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

import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import org.jetbrains.annotations.Nullable;

public class LimitPartitionFrameCursor extends AbstractFullPartitionFrameCursor {
    // User-defined limit. If negative, then
    // reading begins from the nth row from the end; if positive, then
    // reading begins from the nth row from the beginning. Currently only
    // negative limits are supported.
    private long limit = 0;

    // First partition from which to begin reading
    private int partitionLo = 0;

    // Row at which to begin reading (if partitionIndex == partitionLo).
    // If limit is non-zero, this number is set to -1, which cause the implementation
    // to calculate the actual offset.
    private long firstRowLo = 0;

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        calculateFirstPartitionAndRow();

        while (partitionIndex < partitionHi) {
            long hi = getTableReader().openPartition(partitionIndex);
            if (hi > 0) {
                if (partitionIndex == partitionLo) {
                    hi -= firstRowLo;
                }
                counter.add(hi);
            }
            partitionIndex++;
        }
    }

    @Override
    public long size() {
        long size = super.size();
        size = Math.min(size, Math.abs(limit));
        return size;
    }

    /**
     * Sets the limit, either positive or negative.
     * If limit is negative, then it is counted from the end
     */
    public void setLimit(long n) {
        if (n > 0) {
            throw new UnsupportedOperationException("only negative limits for now!");
        }
        if (n == 0) {
            limit = firstRowLo = 0;
        } else if (n != limit) {
            // our limit has changed, signal that we need to recalculate the offsets
            limit = n;
            firstRowLo = -1;
        }
    }

    public long getLimit() {
        return limit;
    }

    private void calculateFirstPartitionAndRow() {
        if (firstRowLo > -1) {
            return; // already calculated
        }

        int curPart = partitionHi - 1; // iterator
        long readableRows = -limit;

        while (curPart > -1) {
            long numRows = getTableReader().openPartition(curPart);

            if (numRows < 1) {
                // partition unreadable
                curPart--;
                continue;
            }

            if (readableRows > numRows) {
                // e.g. numRows = 100, readableRows = 500
                readableRows -= numRows;
                curPart--;
            } else {
                // offset begins in this partition, somewhere;
                // e.g. numRows = 100; readableRows = 5; which means that we start reading
                // at row 95, or numRows - readableRows
                firstRowLo = numRows - readableRows;
                partitionIndex = partitionLo = curPart;
                return;
            }
        }

        if (readableRows > 0) {
            // more results than rows?
            firstRowLo = 0;
            partitionLo = 0;
        }
    }

    public @Nullable PartitionFrame next() {
        calculateFirstPartitionAndRow();

        while (partitionIndex < partitionHi) {
            final long hi = getTableReader().openPartition(partitionIndex);
            if (hi < 1) {
                // this partition is missing, skip
                partitionIndex++;
            } else {
                frame.partitionIndex = partitionIndex;
                frame.rowLo = partitionIndex == partitionLo ? firstRowLo : 0;
                frame.rowHi = hi;
                partitionIndex++;
                return frame;
            }
        }
        return null;
    }

    @Override
    public void toTop() {
        partitionIndex = partitionLo;
    }
}
