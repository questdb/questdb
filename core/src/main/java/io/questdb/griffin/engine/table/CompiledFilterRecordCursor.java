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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.CompiledFilterSymbolBindVariable;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.DirectLongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;

import java.util.function.BooleanSupplier;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ANY;

class CompiledFilterRecordCursor implements RecordCursor {

    private final PageAddressCacheRecord recordA = new PageAddressCacheRecord();
    private final PageAddressCacheRecord recordB = new PageAddressCacheRecord();
    private final PageAddressCache pageAddressCache;

    private PageFrameCursor pageFrameCursor;
    private RecordMetadata metadata;

    // Java based filter; used for page frames with present column tops
    private Function colTopsFilter;
    // JIT compiled filter; used for dense page frames (no column tops)
    private CompiledFilter compiledFilter;

    private final int rowsCapacityThreshold;
    private DirectLongList rows;
    private DirectLongList columns;
    private MemoryCARW bindVarMemory;
    private int bindVarCount;

    // Important invariant: only nextPage and other cursor iteration methods
    // are allowed to modify the below three fields
    private int pageFrameIndex;
    // The following fields are used for table iteration:
    // when compiled filter is in use, they store rows array indexes;
    // when Java filter is in use, they store row ids
    private long hi;
    private long current;

    private BooleanSupplier next;

    private final BooleanSupplier nextPage = this::nextPage;
    private final BooleanSupplier nextColTopsRow = this::nextColTopsRow;
    private final BooleanSupplier nextRow = this::nextRow;
    private final BooleanSupplier nextReenterPageFrame = this::nextReenterPageFrame;
    private final boolean hasDescendingOrder;

    public CompiledFilterRecordCursor(CairoConfiguration configuration, boolean hasDescendingOrder) {
        this.hasDescendingOrder = hasDescendingOrder;
        rowsCapacityThreshold = configuration.getSqlJitRowsThreshold() / Long.BYTES;
        pageAddressCache = new PageAddressCache(configuration);
    }

    public void of(
            RecordCursorFactory factory,
            Function filter,
            CompiledFilter compiledFilter,
            DirectLongList rows,
            DirectLongList columns,
            ObjList<Function> bindVarFunctions,
            MemoryCARW bindVarMemory,
            SqlExecutionContext executionContext
    ) throws SqlException {
        this.pageFrameIndex = -1;
        this.colTopsFilter = filter;
        this.compiledFilter = compiledFilter;
        this.rows = rows;
        this.columns = columns;
        this.metadata = factory.getMetadata();
        pageAddressCache.of(metadata);
        this.pageFrameCursor = factory.getPageFrameCursor(executionContext, ORDER_ANY);
        recordA.of(pageFrameCursor, pageAddressCache);
        recordB.of(pageFrameCursor, pageAddressCache);
        this.next = nextPage;
        this.bindVarMemory = bindVarMemory;
        this.bindVarCount = bindVarFunctions.size();
        colTopsFilter.init(this, executionContext);
        prepareBindVarMemory(bindVarFunctions, executionContext);
    }

    private void prepareBindVarMemory(ObjList<Function> functions, SqlExecutionContext executionContext) throws SqlException {
        bindVarMemory.truncate();
        for (int i = 0, n = functions.size(); i < n; i++) {
            Function function = functions.getQuick(i);
            writeBindVarFunction(function, executionContext);
        }
    }

    @Override
    public void close() {
        if (rows.getCapacity() > rowsCapacityThreshold) {
            // This call will shrink down the underlying array
            rows.setCapacity(rowsCapacityThreshold);
        }
        bindVarMemory.truncate();
        pageAddressCache.clear();
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
        final int frameIndex = Rows.toPartitionIndex(rowId);
        final long index = Rows.toLocalRowID(rowId);

        PageAddressCacheRecord pageAddressCacheRecord = (PageAddressCacheRecord) record;
        pageAddressCacheRecord.setFrameIndex(frameIndex);
        pageAddressCacheRecord.setRowIndex(index);
        next = nextReenterPageFrame;
    }

    private boolean nextReenterPageFrame() {
        if (pageFrameIndex == -1) {
            // Cursor iteration -> toTop -> recordAt case
            pageFrameCursor.toTop();
            next = nextPage;
            return next.getAsBoolean();
        }

        // Cursor iteration -> recordAt case
        recordA.setFrameIndex(pageFrameIndex);
        if (pageAddressCache.hasColumnTops(pageFrameIndex)) {
            next = nextColTopsRow;
        } else {
            next = nextRow;
        }
        return next.getAsBoolean();
    }

    @Override
    public void toTop() {
        pageFrameIndex = -1;
        colTopsFilter.toTop();
        pageFrameCursor.toTop();
        next = nextPage;
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return pageFrameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        return next.getAsBoolean();
    }

    @Override
    public long size() {
        return -1;
    }

    private long getCurrentRowIndex() {
        return hasDescendingOrder ? (hi - current - 1) : current;
    }

    private boolean nextColTopsRow() {
        seekNextColTopsRow();
        if (current < hi) {
            return true;
        }
        return nextPage();
    }

    private void seekNextColTopsRow() {
        while (++current < hi) {
            recordA.setRowIndex(getCurrentRowIndex());
            if (colTopsFilter.getBool(recordA)) {
                return;
            }
        }
    }

    private boolean nextRow() {
        if (current < hi) {
            recordA.setRowIndex(rows.get(getCurrentRowIndex()));
            current++;
            return true;
        }
        return nextPage();
    }

    private boolean nextPage() {
        final int columnCount = metadata.getColumnCount();

        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            pageFrameIndex += 1;
            recordA.setFrameIndex(pageFrameIndex);
            pageAddressCache.add(pageFrameIndex, frame);

            final long rowCount = frame.getPartitionHi() - frame.getPartitionLo();

            if (pageAddressCache.hasColumnTops(pageFrameIndex)) {
                // Use Java filter implementation in case of a page frame with column tops.

                current = -1;
                hi = rowCount;
                seekNextColTopsRow();

                if (current < hi) {
                    next = nextColTopsRow;
                    return true;
                }
                continue;
            }

            // Use compiled filter in case of a dense page frame.

            columns.setCapacity(columnCount);
            columns.clear();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                columns.add(pageAddressCache.getPageAddress(pageFrameIndex, columnIndex));
            }
            // TODO: page frames may be quite large; we may want to break them into smaller sub-frames
            if (rows.getCapacity() < rowCount) {
                rows.setCapacity(rowCount);
            }

            current = 0;
            hi = compiledFilter.call(
                    columns.getAddress(),
                    columns.size(),
                    bindVarMemory.getAddress(),
                    bindVarCount,
                    rows.getAddress(),
                    rowCount,
                    0
            );

            if (current < hi) {
                recordA.setRowIndex(rows.get(getCurrentRowIndex()));
                current += 1;
                next = nextRow;
                return true;
            }
        }
        return false;
    }

    private void writeBindVarFunction(Function function, SqlExecutionContext executionContext) throws SqlException {
        final int columnType = function.getType();
        final int columnTypeTag = ColumnType.tagOf(columnType);
        switch (columnTypeTag) {
            case ColumnType.BOOLEAN:
                bindVarMemory.putLong(function.getBool(null) ? 1 : 0);
                return;
            case ColumnType.BYTE:
                bindVarMemory.putLong(function.getByte(null));
                return;
            case ColumnType.GEOBYTE:
                bindVarMemory.putLong(function.getGeoByte(null));
                return;
            case ColumnType.SHORT:
                bindVarMemory.putLong(function.getShort(null));
                return;
            case ColumnType.GEOSHORT:
                bindVarMemory.putLong(function.getGeoShort(null));
                return;
            case ColumnType.CHAR:
                bindVarMemory.putLong(function.getChar(null));
                return;
            case ColumnType.INT:
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.GEOINT:
                bindVarMemory.putLong(function.getGeoInt(null));
                return;
            case ColumnType.SYMBOL:
                assert function instanceof CompiledFilterSymbolBindVariable;
                function.init(this, executionContext);
                bindVarMemory.putLong(function.getInt(null));
                return;
            case ColumnType.FLOAT:
                // compiled filter function will read only the first word
                bindVarMemory.putFloat(function.getFloat(null));
                bindVarMemory.putFloat(Float.NaN);
                return;
            case ColumnType.LONG:
                bindVarMemory.putLong(function.getLong(null));
                return;
            case ColumnType.GEOLONG:
                bindVarMemory.putLong(function.getGeoLong(null));
                return;
            case ColumnType.DATE:
                bindVarMemory.putLong(function.getDate(null));
                return;
            case ColumnType.TIMESTAMP:
                bindVarMemory.putLong(function.getTimestamp(null));
                return;
            case ColumnType.DOUBLE:
                bindVarMemory.putDouble(function.getDouble(null));
                return;
            default:
                throw SqlException.position(0).put("unsupported bind variable type: ").put(ColumnType.nameOf(columnTypeTag));
        }
    }
}
