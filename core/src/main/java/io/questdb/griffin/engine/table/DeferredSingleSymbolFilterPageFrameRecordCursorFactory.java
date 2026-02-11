/*******************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

public class DeferredSingleSymbolFilterPageFrameRecordCursorFactory extends PageFrameRecordCursorFactory {
    private final int symbolColumnIndex;
    private final SingleSymbolFilter symbolFilter;
    private final Function symbolFunc;
    private boolean convertedToFrame;
    private int symbolKey;

    public DeferredSingleSymbolFilterPageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            int symbolColumnIndex,
            Function symbolFunc,
            RowCursorFactory rowCursorFactory,
            RecordMetadata metadata,
            PartitionFrameCursorFactory partitionFrameCursorFactory,
            boolean followsOrderByAdvice,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            boolean supportsRandomAccess
    ) {
        super(
                configuration,
                metadata,
                partitionFrameCursorFactory,
                rowCursorFactory,
                followsOrderByAdvice,
                null,
                false,
                columnIndexes,
                columnSizes,
                supportsRandomAccess,
                false
        );
        this.symbolFunc = symbolFunc;
        symbolKey = SymbolTable.VALUE_NOT_FOUND;
        this.symbolColumnIndex = symbolColumnIndex;

        symbolFilter = new SingleSymbolFilter() {
            @Override
            public int getColumnIndex() {
                return DeferredSingleSymbolFilterPageFrameRecordCursorFactory.this.symbolColumnIndex;
            }

            @Override
            public int getSymbolFilterKey() {
                return symbolKey;
            }
        };
    }

    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        convertedToFrame = true;
        return symbolFilter;
    }

    @Override
    public String getBaseColumnName(int columnIndex) {
        return getMetadata().getColumnName(columnIndex);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        assert convertedToFrame;
        PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, columnIndexes, order);
        initFwdPageFrameCursor(partitionFrameCursor, executionContext);
        try {
            if (symbolKey == SymbolTable.VALUE_NOT_FOUND) {
                final CharSequence symbol = symbolFunc.getStrA(null);
                final StaticSymbolTable symbolMapReader = fwdPageFrameCursor.getSymbolTable(symbolColumnIndex);
                symbolKey = symbolMapReader.keyOf(symbol);
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    symbolKey = TableUtils.toIndexKey(symbolKey);
                }
            }
            return fwdPageFrameCursor;
        } catch (Throwable th) {
            Misc.free(fwdPageFrameCursor);
            throw th;
        }
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        convertedToFrame = false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return convertedToFrame;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DeferredSingleSymbolFilterPageFrame");
        super.toPlanInner(sink);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        assert !convertedToFrame;
        return super.initRecordCursor(pageFrameCursor, executionContext);
    }
}
