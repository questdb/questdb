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

    private long indexShift = 0;
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
        aIndex = indexShift;
    }

    @Override
    public long size() {
        return aLimit - indexShift;
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        int keyCount = getSymbolTable(columnIndex).size() + 1;

        long keyLo = 0;
        long keyHi = keyCount;

        rows.extend(keyCount);
        rows.setPos(rows.getCapacity());
        rows.zero(0);

        long rowCount = 0;
        long argsAddress = LatestByArguments.allocateMemory();
        LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
        LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());

        DataFrame frame;
        // frame metadata is based on TableReader, which is "full" metadata
        // this cursor works with subset of columns, which warrants column index remap
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && rowCount < keyCount) {
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            LatestByArguments.setKeyLo(argsAddress, keyLo);
            LatestByArguments.setKeyHi(argsAddress, keyHi);
            LatestByArguments.setRowsSize(argsAddress, rows.size());

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

            rowCount += LatestByArguments.getRowsSize(argsAddress);
            keyLo = LatestByArguments.getKeyLo(argsAddress);
            keyHi = LatestByArguments.getKeyHi(argsAddress) + 1;
        }
        LatestByArguments.releaseMemory(argsAddress);

        // we have to sort rows because multiple symbols
        // are liable to be looked up out of order
        rows.setPos(rows.getCapacity());
        rows.sortAsUnsigned();

        //skip "holes"
        while(rows.get(indexShift) <= 0) {
            indexShift++;
        }

        aLimit = rows.size();
        aIndex = indexShift;
    }
}