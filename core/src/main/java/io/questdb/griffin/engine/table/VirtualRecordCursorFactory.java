/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowStableFunction;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class VirtualRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;
    private final VirtualFunctionRecordCursor cursor;
    private final ObjList<Function> functions;
    private final VirtualRecordCursorFactorySymbolTableSource internalSymbolTableSource;
    private final RecordMetadata priorityMetadata;
    private final boolean supportsRandomAccess;

    public VirtualRecordCursorFactory(
            RecordMetadata virtualMetadata,
            RecordMetadata priorityMetadata,
            ObjList<Function> functions,
            RecordCursorFactory base,
            int virtualColumnReservedSlots
    ) {
        super(virtualMetadata);
        this.base = base;
        this.functions = functions;
        int functionCount = functions.size();
        boolean supportsRandomAccess = base.recordCursorSupportsRandomAccess();
        final ObjList<RowStableFunction> rowStableFunctions = new ObjList<>();
        for (int i = 0; i < functionCount; i++) {
            Function function = functions.getQuick(i);
            if (!function.supportsRandomAccess()) {
                supportsRandomAccess = false;
                break;
            }

            if (function instanceof RowStableFunction) {
                rowStableFunctions.add((RowStableFunction) function);
            }

        }
        this.supportsRandomAccess = supportsRandomAccess;
        this.cursor = new VirtualFunctionRecordCursor(functions, rowStableFunctions, supportsRandomAccess, virtualColumnReservedSlots);
        this.internalSymbolTableSource = new VirtualRecordCursorFactorySymbolTableSource(cursor, virtualColumnReservedSlots);
        this.priorityMetadata = priorityMetadata;
    }

    @Override
    public boolean followedLimitAdvice() {
        return base.followedLimitAdvice();
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return priorityMetadata.getColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = base.getCursor(executionContext);
        try {
            internalSymbolTableSource.of(cursor);
            Function.init(functions, internalSymbolTableSource, executionContext, null);
            this.cursor.of(cursor);
            return this.cursor;
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return supportsRandomAccess;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("VirtualRecord");
        sink.optAttr("functions", functions, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        Misc.freeObjList(functions);
        Misc.free(base);
    }

    private static class VirtualRecordCursorFactorySymbolTableSource implements SymbolTableSource {
        private final RecordCursor own;
        private final int virtualColumnReservedSlots;
        private RecordCursor base;

        public VirtualRecordCursorFactorySymbolTableSource(RecordCursor own, int virtualColumnReservedSlots) {
            this.own = own;
            this.virtualColumnReservedSlots = virtualColumnReservedSlots;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return own.getSymbolTable(columnIndex);
            }
            return base.getSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < virtualColumnReservedSlots) {
                return own.newSymbolTable(columnIndex);
            }
            return base.newSymbolTable(columnIndex - virtualColumnReservedSlots);
        }

        public void of(RecordCursor base) {
            this.base = base;
        }
    }
}
