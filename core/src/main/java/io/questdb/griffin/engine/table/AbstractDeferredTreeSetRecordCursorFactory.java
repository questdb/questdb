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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDeferredTreeSetRecordCursorFactory extends AbstractTreeSetRecordCursorFactory {
    protected final int columnIndex;
    protected final IntHashSet symbolKeys;
    protected ObjList<Function> symbolFuncs;

    public AbstractDeferredTreeSetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @Transient ObjList<Function> keyValueFuncs,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.columnIndex = columnIndex;
        this.symbolFuncs = new ObjList<>(keyValueFuncs);
        this.symbolKeys = new IntHashSet(keyValueFuncs.size());
    }

    static int resolveSymbolKey(StaticSymbolTable symbolTable, CharSequence value) {
        int key = symbolTable.keyOf(value);
        return key == SymbolTable.VALUE_IS_NULL && !symbolTable.containsNullValue() ? SymbolTable.VALUE_NOT_FOUND : key;
    }

    @Override
    protected void _close() {
        final ObjList<Function> symbolFuncs = this.symbolFuncs;
        this.symbolFuncs = null;
        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeObjListBestEffort(failure, symbolFuncs);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        symbolKeys.clear();
        final StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
        for (int i = 0, n = symbolFuncs.size(); i < n; i++) {
            final Function symbolFunc = symbolFuncs.getQuick(i);
            symbolFunc.init(pageFrameCursor, executionContext);
            final int symbolKey = resolveSymbolKey(symbolTable, symbolFunc.getStrA(null));
            if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                symbolKeys.add(TableUtils.toIndexKey(symbolKey));
            }
        }
        return super.initRecordCursor(pageFrameCursor, executionContext);
    }
}
