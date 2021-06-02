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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.sql.DataFrame;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedRecordCursor extends AbstractRecordListCursor {
    private final int columnIndex;

    private long bias = 0;
    private long aIndex;
    private long aLimit;

    public LatestByAllIndexedRecordCursor(int columnIndex, DirectLongList rows, @NotNull IntList columnIndexes) {
        super(rows, columnIndexes);
        this.columnIndex = columnIndex;
    }

    @Override
    public boolean hasNext() {
        if (aIndex < aLimit) {
            long row = rows.get(aIndex++) - 1; // we added 1 on cpp side
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        aIndex = bias;
    }

    @Override
    public long size() {
        return aLimit - bias;
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        int keyCount = getSymbolTable(columnIndex).size() + 1;
        int keyLo = 0;
        int keyHi = keyCount;

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        rows.zero(0);

        long rowCount = 0;
        long argsAddress = ArgumentsWrapper.allocateMemory();
        ArgumentsWrapper.setRowsAddress(argsAddress, rows.getAddress());
        ArgumentsWrapper.setRowsCapacity(argsAddress, rows.getCapacity());

        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && rowCount < keyCount) {
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            ArgumentsWrapper.setKeyLo(argsAddress, keyLo);
            ArgumentsWrapper.setKeyHi(argsAddress, keyHi);
            ArgumentsWrapper.setRowsSize(argsAddress, rows.size());

            BitmapIndexUtilsNative.latestScanBackward(
                    indexReader.getKeyBaseAddress(),
                    indexReader.getKeyMemorySize(),
                    indexReader.getValueBaseAddress(),
                    indexReader.getValueMemorySize(),
                    argsAddress,
                    indexReader.getUnIndexedNullCount(),
                    rowHi, rowLo,
                    frame.getPartitionIndex(), indexReader.getValueBlockCapacity()
            );

            rowCount += ArgumentsWrapper.getRowsSize(argsAddress);
            keyLo = (int)ArgumentsWrapper.getKeyLo(argsAddress);
            keyHi = (int)ArgumentsWrapper.getKeyHi(argsAddress) + 1;
        }
        ArgumentsWrapper.releaseMemory(argsAddress);

        // we have to sort rows because multiple symbols
        // are liable to be looked up out of order
        rows.setPos(rows.getCapacity());
        rows.sortAsUnsigned();

        //skip "holes"
        while(rows.get(bias) <= 0) {
            bias++;
        }

        aLimit = rows.size();
        aIndex = bias;
    }

    static final class ArgumentsWrapper {
        private static final long KEY_LO_OFFSET = 0*8;
        private static final long KEY_HI_OFFSET = 1*8;
        private static final long ROWS_ADDRESS_OFFSET = 2*8;
        private static final long ROWS_CAPACITY_OFFSET = 3*8;
        private static final long ROWS_SIZE_OFFSET = 4*8;
        private static final long MEMORY_SIZE = 5*8;

        public static long allocateMemory() {
            return Unsafe.calloc(MEMORY_SIZE);
        }

        public static void releaseMemory(long address) {
            Unsafe.free(address, MEMORY_SIZE);
        }

        public static long getKeyLo(long address) {
            return Unsafe.getUnsafe().getLong(address + KEY_LO_OFFSET);
        }
        public static long getKeyHi(long address) {
            return Unsafe.getUnsafe().getLong(address + KEY_HI_OFFSET);
        }
        public static long getRowsAddress(long address) {
            return Unsafe.getUnsafe().getLong(address + ROWS_ADDRESS_OFFSET);
        }
        public static long getRowsCapacity(long address) {
            return Unsafe.getUnsafe().getLong(address + ROWS_CAPACITY_OFFSET);
        }
        public static long getRowsSize(long address) {
            return Unsafe.getUnsafe().getLong(address + ROWS_SIZE_OFFSET);
        }
        public static void setKeyLo(long address, long lo) {
            Unsafe.getUnsafe().putLong(address + KEY_LO_OFFSET, lo);
        }
        public static void setKeyHi(long address, long up) {
            Unsafe.getUnsafe().putLong(address + KEY_HI_OFFSET, up);
        }
        public static void setRowsAddress(long address, long addr) {
            Unsafe.getUnsafe().putLong(address + ROWS_ADDRESS_OFFSET, addr);
        }
        public static void setRowsCapacity(long address, long cap) {
            Unsafe.getUnsafe().putLong(address + ROWS_CAPACITY_OFFSET, cap);
        }
        public static void setRowsSize(long address, long size) {
            Unsafe.getUnsafe().getLong(address + ROWS_SIZE_OFFSET, size);
        }
    }
}