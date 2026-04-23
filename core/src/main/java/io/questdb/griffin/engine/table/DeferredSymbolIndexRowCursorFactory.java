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

public class DeferredSymbolIndexRowCursorFactory implements FunctionBasedRowCursorFactory {
    private final int columnIndex;
    private final int indexDirection;
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
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
            return EmptyRowCursor.INSTANCE;
        }

        return pageFrame
                .getIndexReader(columnIndex, indexDirection)
                .getCursor(symbolKey, pageFrame.getPartitionLo(), pageFrame.getPartitionHi() - 1);
    }

    @Override
    public Function getFunction() {
        return symbol;
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
        int symbolKey = pageFrameCursor.getSymbolTable(columnIndex).keyOf(symbol.getSymbol(null));
        this.symbolKey = symbolKey != SymbolTable.VALUE_NOT_FOUND
                ? TableUtils.toIndexKey(symbolKey)
                : SymbolTable.VALUE_NOT_FOUND;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(IndexReader.nameOf(indexDirection)).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.meta("deferred").val("true");
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(symbol);
    }
}
