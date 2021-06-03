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
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedFilteredRecordCursor extends AbstractRecordListCursor {

    private final int columnIndex;
    private final Function filter;
    private long indexShift = 0;
    private long aIndex;
    private long aLimit;

    public LatestByAllIndexedFilteredRecordCursor(
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull Function filter,
            @NotNull IntList columnIndexes
    ) {
        super(rows, columnIndexes);
        this.columnIndex = columnIndex;
        this.filter = filter;
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
    public void close() {
        filter.close();
        super.close();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        filter.init(this, executionContext);

        final int keyCount = getSymbolTable(columnIndex).size() + 1;

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
        int frameColumnIndex = columnIndexes.getQuick(columnIndex);
        while ((frame = this.dataFrameCursor.next()) != null && rowCount < keyCount) {
            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(frameColumnIndex, BitmapIndexReader.DIR_BACKWARD);
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;
            final int partitionIndex = frame.getPartitionIndex();

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
                    partitionIndex, indexReader.getValueBlockCapacity()
            );
            rowCount += LatestByArguments.getRowsSize(argsAddress);
            keyLo = LatestByArguments.getKeyLo(argsAddress);
            keyHi = LatestByArguments.getKeyHi(argsAddress) + 1;
        }
        LatestByArguments.releaseMemory(argsAddress);

        rows.setPos(rows.getCapacity());
        for(long r = 0; r < rows.getCapacity(); ++r) {
            long row = rows.get(r) - 1;
            if (row >= 0) {
                int partitionIndex = Rows.toPartitionIndex(row);
                recordA.jumpTo(partitionIndex, 0);
                recordA.setRecordIndex(Rows.toLocalRowID(row));
                if (!filter.getBool(recordA)) {
                    rows.set(r, 0); // clear row id
                }
            }
        }
        rows.sortAsUnsigned();
        while(rows.get(indexShift) <= 0) {
            indexShift++;
        }
        aLimit = rows.size();
        aIndex = indexShift;
    }
}
