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

package io.questdb.griffin.engine.functions.bind;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;

/**
 * One interval bound of a tick expression with a date variable, in a JIT filter's bind variable
 * slot. The JIT factories call {@link #init} on every cursor open, before they write the slots, so
 * the bound follows the clock. See {@link CompiledFilterTickBounds}.
 */
public class CompiledFilterTickBoundBindVariable extends LongFunction {
    private final CompiledFilterTickBounds bounds;
    private final int index;

    public CompiledFilterTickBoundBindVariable(CompiledFilterTickBounds bounds, int index) {
        assert index >= 0 && index < 2 * bounds.getMaxIntervalCount();
        this.bounds = bounds;
        this.index = index;
    }

    @Override
    public long getLong(Record rec) {
        return bounds.getBound(index);
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
        bounds.init(executionContext);
    }

    @Override
    public boolean isNonDeterministic() {
        return true;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isStableWithinExecution() {
        // init() resolves the bound before execution starts, and it does not change while it runs
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("tick bound");
    }
}
