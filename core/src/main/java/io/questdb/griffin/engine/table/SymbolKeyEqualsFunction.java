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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.FunctionFlags;

/**
 * {@code symbol = value} by the symbol's key, resolved at init: the filter a single-symbol index
 * scan's parallel plan applies to a frame its posting index cannot read (see
 * {@link IndexKeyPageFrameCursor}). It shares the scan's key function, which the scan frees, and
 * reads only the record it is given, so the filtering threads share it.
 */
public class SymbolKeyEqualsFunction extends BooleanFunction {
    private final int columnIndex;
    private final Function symbolFunc;
    private int symbolKey = SymbolTable.VALUE_NOT_FOUND;

    public SymbolKeyEqualsFunction(int columnIndex, Function symbolFunc) {
        this.columnIndex = columnIndex;
        this.symbolFunc = symbolFunc;
    }

    @Override
    public boolean getBool(Record rec) {
        return symbolKey != SymbolTable.VALUE_NOT_FOUND && rec.getInt(columnIndex) == symbolKey;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        symbolFunc.init(symbolTableSource, executionContext);
        // the table's own symbol table: the frames come from a table scan
        symbolKey = ((StaticSymbolTable) symbolTableSource.getSymbolTable(columnIndex)).keyOf(symbolFunc.getStrA(null));
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("symbol key of ").val(symbolFunc);
    }

    @Override
    public void close() {
        // the scan frees the key function
    }

    @Override
    public boolean hasPerCallState() {
        return FunctionFlags.hasPerCallState(symbolFunc);
    }

    @Override
    public boolean isNonDeterministic() {
        return FunctionFlags.isNonDeterministic(symbolFunc);
    }

    @Override
    public boolean isRandom() {
        return FunctionFlags.isRandom(symbolFunc);
    }

    @Override
    public boolean isStableWithinExecution() {
        return FunctionFlags.isStableWithinExecution(symbolFunc);
    }
}
