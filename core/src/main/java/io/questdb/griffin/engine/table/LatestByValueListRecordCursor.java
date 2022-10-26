/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValueListRecordCursor extends AbstractDataFrameRecordCursor {

    private final int shrinkToCapacity;
    private final int columnIndex;
    private final Function filter;
    private IntHashSet foundKeys;
    private IntHashSet symbolKeys;
    private final boolean restrictedByValues;
    private DirectLongList rowIds;
    private int currentRow;

    public LatestByValueListRecordCursor(int columnIndex, @Nullable Function filter, @NotNull IntList columnIndexes, int shrinkToCapacity, boolean restrictedByValues) {
        super(columnIndexes);
        this.shrinkToCapacity = shrinkToCapacity;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.restrictedByValues = restrictedByValues;
        if (restrictedByValues) {
            this.symbolKeys = new IntHashSet(shrinkToCapacity);
        }
        this.foundKeys = new IntHashSet(shrinkToCapacity);
        this.rowIds = new DirectLongList(shrinkToCapacity, MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        super.close();
        if (rowIds.size() > shrinkToCapacity) {
            rowIds = Misc.free(rowIds);
            rowIds = new DirectLongList(shrinkToCapacity, MemoryTag.NATIVE_LONG_LIST);
            foundKeys = new IntHashSet(shrinkToCapacity);
            // symbolKeys is unlikely to take too much memory
            // because every value is associated with a value from `in (...)` WHERE filter and
            // the list of parsed functions is of bigger size than symbolKeys hash set.
        }
    }

    @Override
    void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        this.dataFrameCursor = dataFrameCursor;
        this.recordA.of(dataFrameCursor.getTableReader());
        this.recordB.of(dataFrameCursor.getTableReader());
        dataFrameCursor.toTop();
        foundKeys.clear();
        rowIds.clear();

        // Find all record IDs and save in rowIds in descending order
        // return then row by row in ascending timestamp order
        // since most of the time factory is supposed to return in ASC timestamp order
        // It can be optimised later on to not buffer row IDs and return in desc order.
        if (restrictedByValues) {
            if (symbolKeys.size() > 0) {
                // Find only restricted set of symbol keys
                rowIds.setCapacity(symbolKeys.size());
                if (filter != null) {
                    filter.init(this, executionContext);
                    filter.toTop();
                    findRestrictedWithFilter(filter, symbolKeys, circuitBreaker);
                } else {
                    findRestrictedNoFilter(symbolKeys, circuitBreaker);
                }
            }
        } else {
            // Find latest by all distinct symbol values
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
            int distinctSymbols = symbolTable.getSymbolCount();
            if (symbolTable.containsNullValue()) {
                distinctSymbols++;
            }

            rowIds.setCapacity(distinctSymbols);
            if (distinctSymbols > 0) {
                if (filter != null) {
                    filter.init(this, executionContext);
                    filter.toTop();
                    findAllWithFilter(filter, distinctSymbols, circuitBreaker);
                } else {
                    findAllNoFilter(distinctSymbols, circuitBreaker);
                }
            }
        }
        toTop();
    }

    IntHashSet getSymbolKeys() {
        return symbolKeys;
    }

    public void destroy() {
        // After close() the instance is designed to be re-usable.
        // Destroy makes it non-reusable
        rowIds = Misc.free(rowIds);
    }

    @Override
    public long size() {
        return rowIds.size();
    }

    @Override
    public boolean hasNext() {
        if (currentRow-- > 0) {
            long rowId = rowIds.get(currentRow);
            recordAt(recordA, rowId);
            return true;
        }
        return false;
    }

    private void findAllNoFilter(int distinctCount, SqlExecutionCircuitBreaker circuitBreaker) {
        DataFrame frame = dataFrameCursor.next();
        int foundSize = 0;
        while (frame != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
            frame = dataFrameCursor.next();
        }
    }

    private void findAllWithFilter(Function filter, int distinctCount, SqlExecutionCircuitBreaker circuitBreaker) {
        DataFrame frame = dataFrameCursor.next();
        int foundSize = 0;
        while (frame != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
            frame = dataFrameCursor.next();
        }
    }

    private void findRestrictedNoFilter(IntHashSet symbolKeys, SqlExecutionCircuitBreaker circuitBreaker) {
        DataFrame frame = dataFrameCursor.next();
        int searchSize = symbolKeys.size();
        int foundSize = 0;
        while (frame != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (symbolKeys.contains(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
            frame = dataFrameCursor.next();
        }
    }

    private void findRestrictedWithFilter(Function filter, IntHashSet symbolKeys, SqlExecutionCircuitBreaker circuitBreaker) {
        DataFrame frame = dataFrameCursor.next();
        int searchSize = symbolKeys.size();
        int foundSize = 0;
        while (frame != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && symbolKeys.contains(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
            frame = dataFrameCursor.next();
        }
    }

    @Override
    public void toTop() {
        currentRow = (int) rowIds.size();
    }
}
