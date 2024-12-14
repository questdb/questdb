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

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.Mutable;
import io.questdb.std.Vect;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_DOWN;

public class NativeTimestampFinder implements TimestampFinder, Mutable {
    private MemoryR column;
    private long rowCount;

    @Override
    public void clear() {
        column = null;
        rowCount = 0;
    }

    @Override
    public long findTimestamp(long value, long rowLo, long rowHi) {
        long idx = Vect.binarySearch64Bit(column.getPageAddress(0), value, rowLo, rowHi, BIN_SEARCH_SCAN_DOWN);
        if (idx < 0) {
            return -idx - 2;
        }
        return idx;
    }

    @Override
    public long maxTimestamp() {
        return column.getLong((rowCount - 1) * 8);
    }

    @Override
    public long minTimestamp() {
        return column.getLong(0);
    }

    public NativeTimestampFinder of(TableReader reader, int partitionIndex, int timestampIndex, long rowCount) {
        this.column = reader.getColumn(TableReader.getPrimaryColumnIndex(reader.getColumnBase(partitionIndex), timestampIndex));
        this.rowCount = rowCount;
        return this;
    }

    @Override
    public long timestampAt(long rowIndex) {
        return column.getLong(rowIndex * 8);
    }
}
