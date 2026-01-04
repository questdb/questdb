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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;


/**
 * Abstract base class for generate_series record cursor factories.
 */
public abstract class AbstractGenerateSeriesRecordCursorFactory extends AbstractRecordCursorFactory {
    /**
     * The end function for the series.
     */
    public final Function endFunc;
    /**
     * The start function for the series.
     */
    public final Function startFunc;
    /**
     * The step function for the series.
     */
    public final Function stepFunc;
    /**
     * The step argument position for error reporting.
     */
    int stepPosition;

    /**
     * Constructs a new generate series record cursor factory.
     *
     * @param metadata     the record metadata
     * @param startFunc    the start function
     * @param endFunc      the end function
     * @param stepFunc     the step function
     * @param argPositions the argument positions for error reporting
     * @throws SqlException if arguments are invalid
     */
    public AbstractGenerateSeriesRecordCursorFactory(RecordMetadata metadata, Function startFunc, Function endFunc, Function stepFunc, IntList argPositions) throws SqlException {
        super(metadata);

        if (!startFunc.isConstantOrRuntimeConstant() || startFunc.isNullConstant()) {
            throw SqlException.$(argPositions.getQuick(0), "start argument must be a non-null constant or bind variable constant");
        }
        if (!endFunc.isConstantOrRuntimeConstant() || endFunc.isNullConstant()) {
            throw SqlException.$(argPositions.getQuick(1), "end argument must be a non-null constant or bind variable constant");
        }
        if (!stepFunc.isConstantOrRuntimeConstant() || stepFunc.isNullConstant()) {
            assert argPositions.size() > 2;
            throw SqlException.$(argPositions.getQuick(2), "step argument must be a non-null constant or bind variable constant");
        }

        this.startFunc = startFunc;
        this.endFunc = endFunc;
        this.stepFunc = stepFunc;
        this.stepPosition = argPositions.size() > 2 ? argPositions.getQuick(2) : 0;
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

    /**
     * Abstract base class for generate_series record cursors.
     */
    public abstract static class AbstractGenerateSeriesRecordCursor implements NoRandomAccessRecordCursor {
        /**
         * The end function for the series.
         */
        public final Function endFunc;
        /**
         * The start function for the series.
         */
        public final Function startFunc;
        /**
         * The step function for the series.
         */
        public final Function stepFunc;

        /**
         * Constructs a new generate series record cursor.
         *
         * @param startFunc the start function
         * @param endFunc   the end function
         * @param stepFunc  the step function
         */
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
