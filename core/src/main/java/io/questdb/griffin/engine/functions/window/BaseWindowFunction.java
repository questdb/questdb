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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Misc;

public abstract class BaseWindowFunction implements WindowFunction {
    protected final Function arg;
    // Whether arg reads back as a DATE (milliseconds) rather than a TIMESTAMP (ticks). The value
    // window functions are specialized into DATE and TIMESTAMP subclasses, so this is invariant per
    // instance; readArgValue caches it here to avoid re-deriving the tag from arg.getType() per row.
    protected final boolean argIsDate;
    protected int columnIndex;

    public BaseWindowFunction(Function arg) {
        this.arg = arg;
        this.argIsDate = arg != null && ColumnType.tagOf(arg.getType()) == ColumnType.DATE;
    }

    @Override
    public void close() {
        Misc.free(arg);
    }

    @Override
    public void cursorClosed() {
        if (arg != null) {
            arg.cursorClosed();
        }
    }

    @Override
    public abstract String getName();

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        if (arg != null) {
            arg.init(symbolTableSource, executionContext);
        }
    }

    /**
     * Reads a value-window function's argument as a native long. A DATE argument is read as
     * milliseconds; everything else (TIMESTAMP ticks, or a SYMBOL/STRING/VARCHAR parsed to a
     * timestamp) goes through getTimestamp(). The max/min/first_value/last_value/nth_value value
     * functions store and write this native long, and report getType() = arg.getType(), so the
     * cached chain column reads it back at the right scale for both DATE and TIMESTAMP results.
     */
    protected final long readArgValue(Record rec) {
        return argIsDate ? arg.getDate(rec) : arg.getTimestamp(rec);
    }

    @Override
    public void reset() {
    }

    @Override
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getName());
        if (arg != null) {
            sink.val('(').val(arg).val(')');
        } else {
            sink.val("(*)");
        }
        if (isIgnoreNulls()) {
            sink.val(" ignore nulls");
        }
        sink.val(" over ()");
    }

    @Override
    public void toTop() {
        if (arg != null) {
            arg.toTop();
        }
    }
}
