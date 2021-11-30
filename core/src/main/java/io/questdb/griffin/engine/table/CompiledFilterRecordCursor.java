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

import io.questdb.cairo.TableReaderSelectedColumnRecord;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.vm.api.MemoryAR;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.FiltersCompiler;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

import java.util.function.BooleanSupplier;

class CompiledFilterRecordCursor implements RecordCursor {

    private final TableReaderSelectedColumnRecord recordA;
    private final TableReaderSelectedColumnRecord recordB;

    private PageFrameCursor pageFrameCursor;
    private RecordMetadata metadata;

    private DirectLongList rows;
    private DirectLongList columns;

    private long hi;
    private long current;
    private int partitionIndex;

    private BooleanSupplier next;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextPage = this::nextPage;
    private long filterFnAddress;
    private final long filterAddress;
    private final long filterSize;

    public CompiledFilterRecordCursor(@NotNull IntList columnIndexes, MemoryAR filter) {
        this.recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        this.recordB = new TableReaderSelectedColumnRecord(columnIndexes);
        //todo: what if filter changed outside?
        filterSize = filter.getAppendOffset();
        filter.jumpTo(0);
        filterAddress = filter.getPageAddress(0);
    }

    void of(RecordCursorFactory factory, DirectLongList rows, DirectLongList columns, SqlExecutionContext executionContext, int options) throws SqlException {
        this.rows = rows;
        this.columns = columns;
        this.pageFrameCursor = factory.getPageFrameCursor(executionContext);
        this.recordA.of(pageFrameCursor.getTableReader());
        this.recordB.of(pageFrameCursor.getTableReader());
        this.metadata = factory.getMetadata();
        this.next = nextPage;
        //todo: error reporting
        //todo: how not to recompile it?
        this.filterFnAddress = FiltersCompiler.compileFunction(filterAddress, filterSize, options);
    }

    @Override
    public void close() {
        pageFrameCursor.close();
        FiltersCompiler.freeFunction(filterFnAddress);
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long rowId) {
        ((TableReaderSelectedColumnRecord) record).jumpTo(Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
    }

    @Override
    public void toTop() {
        pageFrameCursor.toTop();
        next = nextPage;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return pageFrameCursor.getSymbolMapReader(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return next.getAsBoolean();
    }

    @Override
    public long size() {
        return -1;
    }

    private boolean nextRow() {
        if (current < hi) {
            recordA.jumpTo(partitionIndex, rows.get(current++));
            return true;
        }
        return nextPage();
    }

    private boolean nextPage() {
        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            int sz = metadata.getColumnCount();
            columns.extend(sz);
            columns.clear();
            for (int columnIndex = 0; columnIndex < sz; columnIndex++) {
                final long columnBaseAddress = frame.getPageAddress(columnIndex);
                columns.add(columnBaseAddress);
            }
            final long rowCount = frame.getPartitionHi() - frame.getPartitionLo();
            rows.extend(rowCount);

            partitionIndex = frame.getPartitionIndex();
            current = 0;
            hi = FiltersCompiler.callFunction(
                    filterFnAddress,
                    columns.getAddress(),
                    columns.size(),
                    rows.getAddress(),
                    rowCount,
                    frame.getPartitionLo());

            if (current < hi) {
                recordA.jumpTo(partitionIndex, rows.get(current));
                recordB.jumpTo(partitionIndex, rows.get(current));
                current += 1;
                next = nextRow;
                return true;
            }
        }
        return false;
    }
}
