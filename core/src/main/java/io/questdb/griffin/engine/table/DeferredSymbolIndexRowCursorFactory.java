/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public class DeferredSymbolIndexRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final int columnIndex;
    private final int indexDirection;
    private final ShiftedRowCursor shiftedCursor = new ShiftedRowCursor();
    private final Function symbol;
    private int symbolKey;

    public DeferredSymbolIndexRowCursorFactory(
            int columnIndex,
            Function symbol,
            int indexDirection
    ) {
        this.columnIndex = columnIndex;
        this.symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbol = symbol;
        this.indexDirection = indexDirection;
    }

    @Override
    public void close() {
        Misc.free(symbol);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }

        return pageFrame
                .getIndexReader(columnIndex, indexDirection)
                .getCursor(TableUtils.toIndexKey(symbolKey), pageFrame.getIndexRowLo(), pageFrame.getIndexRowHi() - 1);
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory, long firstRow) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }
        final IndexReader reader = pageFrame.getIndexReader(columnIndex, indexDirection);
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        if (indexDirection == IndexReader.DIR_BACKWARD) {
            return reader.getCursor(TableUtils.toIndexKey(symbolKey), pageFrame.getIndexRowLo(), firstRow + indexShift);
        }
        return shiftedCursor.of(reader.getCursor(TableUtils.toIndexKey(symbolKey), firstRow + indexShift, pageFrame.getIndexRowHi() - 1), firstRow - pageFrame.getPartitionLo());
    }

    @Override
    public Function getFunction() {
        return symbol;
    }

    @Override
    public long getRow(PageFrame pageFrame, long rowLo, long rowHi, long rowIndex) {
        final BitmapIndexFwdReader reader = (BitmapIndexFwdReader) pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD);
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        return reader.getRow(TableUtils.toIndexKey(symbolKey), rowLo + indexShift, rowHi + indexShift, indexDirection, rowIndex) - indexShift;
    }

    @Override
    public long getRowCount(PageFrame pageFrame, long rowLo, long rowHi, long maxCount) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return 0;
        }
        if (!(pageFrame.getIndexReader(columnIndex, IndexReader.DIR_FORWARD) instanceof BitmapIndexFwdReader reader)) {
            return -1;
        }
        final long indexShift = pageFrame.getIndexRowLo() - pageFrame.getPartitionLo();
        return reader.countRows(TableUtils.toIndexKey(symbolKey), rowLo + indexShift, rowHi + indexShift, maxCount);
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        symbol.init(pageFrameCursor, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        symbolKey = pageFrameCursor.getSymbolTable(columnIndex).keyOf(symbol.getSymbol(null), symbolKey);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(IndexReader.nameOf(indexDirection)).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.meta("deferred").val("true");
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(symbol);
    }
}
