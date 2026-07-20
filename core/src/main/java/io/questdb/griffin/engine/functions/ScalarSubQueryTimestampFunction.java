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

package io.questdb.griffin.engine.functions;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.model.ScalarTimestampBoundHolder;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

public final class ScalarSubQueryTimestampFunction extends TimestampFunction {

    private final Function cursorFunction;
    private final RecordCursorFactory factory;
    private final int position;
    private ScalarTimestampBoundHolder publishHolder;
    private long value = Numbers.LONG_NULL;

    public ScalarSubQueryTimestampFunction(Function cursorFunction, int position) {
        super(getTimestampType(cursorFunction));
        this.cursorFunction = cursorFunction;
        this.factory = cursorFunction.getRecordCursorFactory();
        this.position = position;
        assert factory != null;
    }

    @Override
    public void close() {
        Misc.free(cursorFunction);
    }

    @Override
    public int getComplexity() {
        return cursorFunction.getComplexity();
    }

    @Override
    public long getTimestamp(Record rec) {
        return value;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        cursorFunction.init(symbolTableSource, executionContext);
        value = ScalarSubQueryUtils.readTimestamp(factory, executionContext, position);
        // Publish the single per-execution value so the retained residual filter (and its per-worker
        // clones) read the exact same frozen bound instead of opening the sub-query a second time.
        if (publishHolder != null) {
            publishHolder.publish(value);
        }
    }

    @Override
    public boolean isNonDeterministic() {
        return factory.isNonDeterministic();
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    // Runtime-constant does NOT imply stable here: init() re-opens the wrapped cursor, so
    // stability holds only when the sub-query factory proves it (fail-safe default: unstable).
    @Override
    public boolean isStableWithinExecution() {
        return factory.isStableWithinExecution();
    }

    public void setPublishHolder(ScalarTimestampBoundHolder publishHolder) {
        this.publishHolder = publishHolder;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(cursorFunction);
    }

    private static int getTimestampType(Function cursorFunction) {
        final RecordCursorFactory factory = cursorFunction.getRecordCursorFactory();
        assert factory != null;
        final RecordMetadata metadata = factory.getMetadata();
        assert metadata.getColumnCount() == 1;
        final int timestampType = metadata.getColumnType(0);
        assert ColumnType.isTimestamp(timestampType);
        return timestampType;
    }
}
