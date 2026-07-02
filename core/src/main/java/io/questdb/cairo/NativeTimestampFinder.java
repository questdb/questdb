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

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Mutable;
import io.questdb.std.Vect;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;

public class NativeTimestampFinder implements TimestampFinder, Mutable {
    private MemoryR column;
    private long maxTimestampApprox;
    private long minTimestampApprox;
    private int partitionIndex;
    // File-row offset of a zero-copy split suffix child's logical row 0 (0 for every contiguous partition).
    // The designated timestamp column never has a column top, so file_row = logical + partitionTop.
    private long partitionTop;
    private TableReader reader;
    private long rowCount;
    private int timestampColumnOffset;

    @Override
    public void clear() {
        column = null;
        rowCount = 0;
        partitionTop = 0;
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        // Search the physical window [rowLo+partitionTop, rowHi+partitionTop], then map the resolved
        // physical index back to logical space by subtracting partitionTop.
        long idx = Vect.binarySearch64Bit(column.getPageAddress(0), value, rowLo + partitionTop, rowHi + partitionTop, BIN_SEARCH_SCAN_DOWN);
        if (idx < 0) {
            return -idx - 2 - partitionTop;
        }
        return idx - partitionTop;
    }

    @Override
    public long maxTimestampApproxFromMetadata() {
        return maxTimestampApprox;
    }

    @Override
    public long maxTimestampExact() {
        return column.getLong((partitionTop + rowCount - 1) * 8);
    }

    @Override
    public long minTimestampApproxFromMetadata() {
        return minTimestampApprox;
    }

    @Override
    public long minTimestampExact() {
        return column.getLong(partitionTop * 8);
    }

    public NativeTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex, long rowCount) {
        this.timestampColumnOffset = TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), timestampIndex);
        this.reader = reader;
        this.partitionIndex = partitionIndex;
        this.rowCount = rowCount;
        this.minTimestampApprox = reader.getPartitionMinTimestampFromMetadata(partitionIndex);
        this.maxTimestampApprox = reader.getPartitionMaxTimestampFromMetadata(partitionIndex);
        return this;
    }

    @Override
    public void prepare() {
        this.column = reader.getColumn(timestampColumnOffset);
        // Read the partition top here, NOT in of(): callers invoke of() before reader.openPartition(),
        // when the cached slot still holds 0; prepare() runs immediately after openPartition().
        this.partitionTop = reader.getPartitionTop(partitionIndex);
    }

    @Override
    public long timestampAt(long rowIndex) {
        return column.getLong((rowIndex + partitionTop) * 8);
    }
}
