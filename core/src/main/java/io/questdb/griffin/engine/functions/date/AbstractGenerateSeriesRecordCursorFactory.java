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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;

public abstract class AbstractGenerateSeriesRecordCursorFactory extends AbstractRecordCursorFactory {
    public final Function endFunc;
    public final Function startFunc;
    public final Function stepFunc;

    public AbstractGenerateSeriesRecordCursorFactory(RecordMetadata metadata, Function startFunc, Function endFunc, Function stepFunc, int position) throws SqlException {
        super(metadata);
        if (!startFunc.isConstantOrRuntimeConstant() || !endFunc.isConstantOrRuntimeConstant() || !stepFunc.isConstantOrRuntimeConstant()) {
            throw SqlException.$(position, "arguments must be constant or bind variable constants");
        }
        if (startFunc.isNullConstant() || endFunc.isNullConstant() || stepFunc.isNullConstant()) {
            throw SqlException.$(position, "arguments cannot be null");
        }
        this.startFunc = startFunc;
        this.endFunc = endFunc;
        this.stepFunc = stepFunc;
    }

    public void init(SqlExecutionContext executionContext) throws SqlException {
        startFunc.init(null, executionContext);
        endFunc.init(null, executionContext);
        stepFunc.init(null, executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("generate_series")
                .meta("start").val(startFunc)
                .meta("end").val(endFunc)
                .meta("step").val(stepFunc);
    }

    public abstract static class AbstractGenerateSeriesRecordCursor implements NoRandomAccessRecordCursor {
        public final Function endFunc;
        public final Function startFunc;
        public final Function stepFunc;

        public AbstractGenerateSeriesRecordCursor(Function startFunc, Function endFunc, Function stepFunc) {
            this.startFunc = startFunc;
            this.endFunc = endFunc;
            this.stepFunc = stepFunc;
        }

        @Override
        public void close() {
        }

        public void of(SqlExecutionContext executionContext) throws SqlException {
            startFunc.init(null, executionContext);
            endFunc.init(null, executionContext);
            stepFunc.init(null, executionContext);
        }

    }
}
