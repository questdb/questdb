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

import io.questdb.cairo.sql.DataFrame;
import io.questdb.cairo.sql.DataFrameCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByValueListRecordCursor extends AbstractDataFrameRecordCursor {

    private final int columnIndex;
    private final Function filter;
    private final IntHashSet foundKeys;
    private final IntHashSet symbolKeys;
    private final LongList rowIds = new LongList();
    int currentRow;

    public LatestByValueListRecordCursor(int columnIndex, @Nullable Function filter, IntHashSet symbolKeys, @NotNull IntList columnIndexes) {
        super(columnIndexes);
        this.columnIndex = columnIndex;
        this.filter = filter;
        this.symbolKeys = symbolKeys;
        this.foundKeys = new IntHashSet();
    }

    @Override
    public boolean hasNext() {
        if (currentRow-- > 0) {
            long rowId = rowIds.getQuick(currentRow);
            recordAt(recordA, rowId);
            return true;
        }
        return false;
    }

    @Override
    public void toTop() {
        currentRow = rowIds.size();
    }

    @Override
    public long size() {
        return rowIds.size();
    }

    private void findAllNoFilter(int distinctCount) {
        DataFrame frame = dataFrameCursor.next();
        do {
            if (frame != null) {
                long rowLo = frame.getRowLo();
                long row = frame.getRowHi();
                recordA.jumpTo(frame.getPartitionIndex(), 0);

                while (row-- > rowLo) {
                    recordA.setRecordIndex(row);
                    int key = recordA.getInt(columnIndex);
                    if (foundKeys.add(key)) {
                        rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                        if (rowIds.size() == distinctCount) {
                            return;
                        }
                    }
                }
                frame = dataFrameCursor.next();
            }
        } while (frame != null);
    }

    private void findAllWithFilter(Function filter, int distinctCount) {
        DataFrame frame = dataFrameCursor.next();
        do {
            if (frame != null) {
                long rowLo = frame.getRowLo();
                long row = frame.getRowHi();
                recordA.jumpTo(frame.getPartitionIndex(), 0);

                while (row-- > rowLo) {
                    recordA.setRecordIndex(row);
                    int key = recordA.getInt(columnIndex);
                    if ((filter == null || filter.getBool(recordA)) && foundKeys.add(key)) {
                        rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                        if (rowIds.size() == distinctCount) {
                            return;
                        }
                    }
                }
                frame = dataFrameCursor.next();
            }
        } while (frame != null);
    }

    private void findRestrictedNoFilter() {
        DataFrame frame = dataFrameCursor.next();
        do {
            if (frame != null) {
                long rowLo = frame.getRowLo();
                long row = frame.getRowHi();
                recordA.jumpTo(frame.getPartitionIndex(), 0);

                while (row-- > rowLo) {
                    recordA.setRecordIndex(row);
                    int key = recordA.getInt(columnIndex);
                    if (symbolKeys.contains(key) && foundKeys.add(key)) {
                        rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                        if (rowIds.size() == symbolKeys.size()) {
                            return;
                        }
                    }
                }
                frame = dataFrameCursor.next();
            }
        } while (frame != null);
    }

    private void findRestrictedWithFilter(Function filter) {
        DataFrame frame = dataFrameCursor.next();
        do {
            if (frame != null) {
                long rowLo = frame.getRowLo();
                long row = frame.getRowHi();
                recordA.jumpTo(frame.getPartitionIndex(), 0);

                while (row-- > rowLo) {
                    recordA.setRecordIndex(row);
                    int key = recordA.getInt(columnIndex);
                    if ((filter == null || filter.getBool(recordA)) && symbolKeys.contains(key) && foundKeys.add(key)) {
                        rowIds.add(Rows.toRowID(frame.getPartitionIndex(), row));
                        if (rowIds.size() == symbolKeys.size()) {
                            return;
                        }
                    }
                }
                frame = dataFrameCursor.next();
            }
        } while (frame != null);
    }

    @Override
    void of(DataFrameCursor dataFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        this.dataFrameCursor = dataFrameCursor;
        this.recordA.of(dataFrameCursor.getTableReader());
        this.recordB.of(dataFrameCursor.getTableReader());
        dataFrameCursor.toTop();
        foundKeys.clear();
        rowIds.clear();

        if (symbolKeys != null) {
            if (symbolKeys.size() > 0) {
                // Find only restricted set of symbol keys
                if (filter != null) {
                    filter.init(this, executionContext);
                    filter.toTop();
                    findRestrictedWithFilter(filter);
                } else {
                    findRestrictedNoFilter();
                }
            }
        } else {
            // Find latest by all distinct symbol values
            StaticSymbolTable symbolTable = dataFrameCursor.getSymbolTable(columnIndexes.getQuick(columnIndex));
            int distinctSymbols = symbolTable.getSymbolCount();
            if (symbolTable.containsNullValue()) {
                distinctSymbols++;
            }

            if (distinctSymbols > 0) {
                if (filter != null) {
                    filter.init(this, executionContext);
                    filter.toTop();
                    findAllWithFilter(filter, distinctSymbols);
                } else {
                    findAllNoFilter(distinctSymbols);
                }
            }
        }
        toTop();
    }
}
