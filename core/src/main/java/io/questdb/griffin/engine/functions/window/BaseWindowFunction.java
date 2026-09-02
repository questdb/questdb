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
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
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
    private LiveViewCheckpointDependency checkpointDependency;
    private LiveViewCheckpointFunctionIdentity checkpointFunctionIdentity;

    public BaseWindowFunction(Function arg) {
        this.arg = arg;
        this.argIsDate = arg != null && ColumnType.tagOf(arg.getType()) == ColumnType.DATE;
    }

    @Override
    public LiveViewCheckpointDependency checkpointDependency() {
        return checkpointDependency;
    }

    @Override
    public LiveViewCheckpointFunctionIdentity checkpointFunctionIdentity() {
        return checkpointFunctionIdentity;
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
     * Rebinds {@code arg} on the live-view incremental refresh path, which skips
     * {@link #init} from the second cycle on so the accumulated window state survives.
     * <p>
     * arg caches cursor-scoped bindings: a SYMBOL column holds the symbol table it
     * resolved against, and a symbol comparison such as {@code side = 'BUY'} caches the
     * int key it resolved the constant to. Each refresh hands the function a fresh
     * WAL-segment-scoped SymbolTableSource whose keys the WAL writer re-assigns per
     * commit, so a binding cached on one cycle names the wrong value on the next and the
     * window silently aggregates the wrong rows. Rebinding every cycle is what
     * {@link WindowFunction#initPartitionBy} exists to do; overrides must call super.
     */
    @Override
    public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
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
    public void setCheckpointCompilerMetadata(
            LiveViewCheckpointFunctionIdentity identity,
            LiveViewCheckpointDependency dependency
    ) {
        if (checkpointFunctionIdentity != null || checkpointDependency != null) {
            throw new IllegalStateException("live view checkpoint compiler metadata already set");
        }
        this.checkpointFunctionIdentity = identity;
        this.checkpointDependency = dependency;
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
