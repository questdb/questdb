/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValueListRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final Function filter;
    private final boolean restrictedByExcludedValues;
    private final boolean restrictedByIncludedValues;
    private final int shrinkToCapacity;
    private boolean areRecordsFound;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private int currentRow;
    private IntHashSet excludedSymbolKeys;
    private IntHashSet foundKeys;
    private int foundSize;
    private IntHashSet includedSymbolKeys;
    private DirectLongList rowIds;

    public LatestByValueListRecordCursor(
            int columnIndex,
            @Nullable Function filter,
            @NotNull IntList columnIndexes,
            int shrinkToCapacity,
            boolean restrictedByIncludedValues,
            boolean restrictedByExcludedValues
    ) {
        super(columnIndexes);
        this.shrinkToCapacity = shrinkToCapacity;
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.restrictedByIncludedValues = restrictedByIncludedValues;
        this.restrictedByExcludedValues = restrictedByExcludedValues;
        if (restrictedByIncludedValues || restrictedByExcludedValues) {
            this.includedSymbolKeys = new IntHashSet(shrinkToCapacity);
            this.excludedSymbolKeys = new IntHashSet(shrinkToCapacity);
        }
        this.foundKeys = new IntHashSet(shrinkToCapacity);
        this.rowIds = new DirectLongList(shrinkToCapacity, MemoryTag.NATIVE_LONG_LIST);
    }

    @Override
    public void close() {
        super.close();
        if (rowIds.getCapacity() > shrinkToCapacity) {
            rowIds = Misc.free(rowIds);
            rowIds = new DirectLongList(shrinkToCapacity, MemoryTag.NATIVE_LONG_LIST);
            foundKeys = new IntHashSet(shrinkToCapacity);
            // symbolKeys is unlikely to take too much memory
            // because every value is associated with a value from `in (...)` WHERE filter and
            // the list of parsed functions is of bigger size than symbolKeys hash set.
        }
    }

    public void destroy() {
        // After close() the instance is designed to be re-usable.
        // Destroy makes it non-reusable
        rowIds = Misc.free(rowIds);
    }

    @Override
    public boolean hasNext() {
        if (!areRecordsFound) {
            findRecords();
            toTop();
            areRecordsFound = true;
        }
        if (currentRow-- > 0) {
            long rowId = rowIds.get(currentRow);
            recordAt(recordA, rowId);
            return true;
        }
        return false;
    }

    @Override
    public void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        recordA.of(dataFrameCursor.getTableReader());
        recordB.of(dataFrameCursor.getTableReader());
        circuitBreaker = executionContext.getCircuitBreaker();
        dataFrameCursor.toTop();
        foundSize = 0;
        foundKeys.clear();
        rowIds.clear();
        if (filter != null) {
            filter.init(this, executionContext);
            filter.toTop();
        }
        if (restrictedByIncludedValues) {
            if (includedSymbolKeys.size() > 0) {
                // Find only restricted set of symbol keys
                rowIds.setCapacity(includedSymbolKeys.size());
            }
        } else if (restrictedByExcludedValues) {
            // Find all, but excluded set of symbol keys
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
            int distinctSymbols = symbolTable.getSymbolCount();
            if (symbolTable.containsNullValue()) {
                distinctSymbols++;
            } else if (excludedSymbolKeys.contains(SymbolTable.VALUE_IS_NULL)) {
                // The excluded set contains a null while the symbol table doesn't.
                // Increment the counter to avoid miscalculation.
                distinctSymbols++;
            }
            distinctSymbols = Math.max(0, distinctSymbols - excludedSymbolKeys.size());
            rowIds.setCapacity(distinctSymbols);
        } else {
            // Find latest by all distinct symbol values
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
            int distinctSymbols = symbolTable.getSymbolCount();
            if (symbolTable.containsNullValue()) {
                distinctSymbols++;
            }
            rowIds.setCapacity(distinctSymbols);
        }
        areRecordsFound = false;
    }

    @Override
    public long size() {
        return areRecordsFound ? rowIds.size() : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("FilterOnValueList").meta("on").putColumnName(columnIndex);
    }

    @Override
    public void toTop() {
        currentRow = (int) rowIds.size();
    }

    private void findAllNoFilter(int distinctCount) {
        assert filter == null;
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
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
        }
    }

    private void findAllWithFilter(int distinctCount) {
        assert filter != null;
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
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
        }
    }

    private void findRecords() {
        // Find all record IDs and save in rowIds in descending order.
        // Then return row by row in ascending timestamp order
        // since most of the time factory is supposed to return in ASC timestamp order.
        // It can be optimised later on to not buffer row IDs and return in desc order.
        if (restrictedByIncludedValues) {
            if (includedSymbolKeys.size() > 0) {
                // Find only restricted set of symbol keys
                if (filter != null) {
                    findRestrictedWithFilter();
                } else {
                    findRestrictedNoFilter();
                }
            }
        } else if (restrictedByExcludedValues) {
            // Find all, but excluded set of symbol keys
            int distinctSymbols = (int) rowIds.getCapacity();
            if (filter != null) {
                findRestrictedExcludedOnlyWithFilter(distinctSymbols);
            } else {
                findRestrictedExcludedOnlyNoFilter(distinctSymbols);
            }
        } else {
            // Find latest by all distinct symbol values
            int distinctSymbols = (int) rowIds.getCapacity();
            if (distinctSymbols > 0) {
                if (filter != null) {
                    findAllWithFilter(distinctSymbols);
                } else {
                    findAllNoFilter(distinctSymbols);
                }
            }
        }
    }

    private void findRestrictedExcludedOnlyNoFilter(int distinctCount) {
        assert filter == null;
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedExcludedOnlyWithFilter(int distinctCount) {
        assert filter != null;
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == distinctCount) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedNoFilter() {
        assert filter == null;
        int searchSize = includedSymbolKeys.size();
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (includedSymbolKeys.contains(key) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
        }
    }

    private void findRestrictedWithFilter() {
        assert filter != null;
        int searchSize = includedSymbolKeys.size();
        DataFrame frame;
        while ((frame = dataFrameCursor.next()) != null) {
            long rowLo = frame.getRowLo();
            long row = frame.getRowHi();
            recordA.jumpTo(frame.getPartitionIndex(), 0);

            while (row-- > rowLo) {
                circuitBreaker.statefulThrowExceptionIfTripped();
                recordA.setRecordIndex(row);
                int key = recordA.getInt(columnIndex);
                if (filter.getBool(recordA) && includedSymbolKeys.contains(key) && excludedSymbolKeys.excludes(key) && foundKeys.add(key)) {
                    rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                    if (++foundSize == searchSize) {
                        return;
                    }
                }
            }
        }
    }

    IntHashSet getExcludedSymbolKeys() {
        return excludedSymbolKeys;
    }

    IntHashSet getIncludedSymbolKeys() {
        return includedSymbolKeys;
    }
}
