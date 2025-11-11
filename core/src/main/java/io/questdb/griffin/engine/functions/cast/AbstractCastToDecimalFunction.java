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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.decimal.ToDecimalFunction;

public abstract class AbstractCastToDecimalFunction extends ToDecimalFunction implements UnaryFunction {
    protected final Function arg;
    protected final int position;
    protected final int precision;
    protected final int scale;

    public AbstractCastToDecimalFunction(Function arg, int targetType, int position) {
        super(targetType);
        this.arg = arg;
        this.position = position;
        this.precision = ColumnType.getDecimalPrecision(targetType);
        this.scale = ColumnType.getDecimalScale(targetType);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    public boolean store(Record rec) {
        return cast(rec);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(arg).val("::").val(ColumnType.nameOf(type));
    }

    /**
     * The implementation must fill the decimal with the cast value following the target scale and precision.
     * If the value to cast is null, it must return false without doing additional work.
     *
     * @return whether the result is not null.
     */
    protected abstract boolean cast(Record rec);
}
