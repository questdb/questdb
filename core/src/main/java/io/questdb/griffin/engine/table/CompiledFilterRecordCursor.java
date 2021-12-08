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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderSelectedColumnRecord;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

import java.util.function.BooleanSupplier;

class CompiledFilterRecordCursor implements RecordCursor {

    private final IntList columnIndexes;
    private final TableReaderSelectedColumnRecord recordA;
    private final TableReaderSelectedColumnRecord recordB;

    private PageFrameCursor pageFrameCursor;
    private RecordMetadata metadata;

    // Java based filter; used for page frames with present column tops
    private Function colTopsFilter;
    // JIT compiled filter; used for dense page frames (no column tops)
    private CompiledFilter compiledFilter;
    private DirectLongList rows;
    private DirectLongList columns;

    private int partitionIndex;
    // The following fields are used for table iteration:
    // when compiled filter is in use, they store rows array indexes;
    // when Java filter is in use, they store rowids
    private long hi;
    private long current;

    private BooleanSupplier next;
    private final BooleanSupplier nextColTopsRow = this::nextColTopsRow;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextPage = this::nextPage;

    public CompiledFilterRecordCursor(@NotNull IntList columnIndexes) {
        this.columnIndexes = columnIndexes;
        this.recordA = new TableReaderSelectedColumnRecord(columnIndexes);
        this.recordB = new TableReaderSelectedColumnRecord(columnIndexes);
    }

    public void of(
            RecordCursorFactory factory,
            Function filter,
            CompiledFilter compiledFilter,
            DirectLongList rows,
            DirectLongList columns,
            SqlExecutionContext executionContext
    ) throws SqlException {
        this.colTopsFilter = filter;
        this.compiledFilter = compiledFilter;
        this.rows = rows;
        this.columns = columns;
        this.pageFrameCursor = factory.getPageFrameCursor(executionContext);
        this.recordA.of(pageFrameCursor.getTableReader());
        this.recordB.of(pageFrameCursor.getTableReader());
        this.metadata = factory.getMetadata();
        this.next = nextPage;
        colTopsFilter.init(this, executionContext);
    }

    @Override
    public void close() {
        pageFrameCursor.close();
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
        colTopsFilter.toTop();
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

    private boolean nextColTopsRow() {
        seekNextColTopsRow();
        if (current < hi) {
            return true;
        }
        return nextPage();
    }

    private void seekNextColTopsRow() {
        current += 1;
        while (current < hi) {
            recordA.jumpTo(partitionIndex, current);
            if (colTopsFilter.getBool(recordA)) {
                return;
            }
            current += 1;
        }
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
        final TableReader reader = pageFrameCursor.getTableReader();
        final int columnCount = metadata.getColumnCount();
        while ((frame = pageFrameCursor.next()) != null) {
            partitionIndex = frame.getPartitionIndex();

            boolean hasColumnTops = false;
            final int base = reader.getColumnBase(partitionIndex);
            for (int i = 0; i < columnCount; i++) {
                final int columnIndex = columnIndexes.getQuick(i);
                final long top = reader.getColumnTop(base, columnIndex);
                if (top > frame.getPartitionLo()) {
                    hasColumnTops = true;
                    break;
                }
            }

            if (hasColumnTops) {
                // Use Java filter implementation in case of a page frame with column tops.

                current = frame.getPartitionLo() - 1;
                hi = frame.getPartitionHi();
                seekNextColTopsRow();

                if (current < hi) {
                    recordB.jumpTo(partitionIndex, current);
                    next = nextColTopsRow;
                    return true;
                }
                continue;
            }

            // Use compiled filter in case of a dense page frame.

            columns.extend(columnCount);
            columns.clear();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                final long columnBaseAddress = frame.getPageAddress(columnIndex);
                columns.add(columnBaseAddress);
            }
            final long rowCount = frame.getPartitionHi() - frame.getPartitionLo();
            // TODO: page frames may be quite large; we may want to break them into smaller subframes
            rows.extend(rowCount);

            current = 0;
            hi = compiledFilter.call(
                    columns.getAddress(),
                    columns.size(),
                    rows.getAddress(),
                    rowCount,
                    frame.getPartitionLo()
            );

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
