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

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractDayIntervalFunction extends IntervalFunction implements FunctionExtension {
    protected final Interval interval = new Interval();
    protected TimestampDriver timestampDriver;

    protected AbstractDayIntervalFunction(int intervalType) {
        super(intervalType);
    }

    @Override
    public FunctionExtension extendedOps() {
        return this;
    }

    @Override
    public int getArrayLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public @NotNull Interval getInterval(Record rec) {
        return interval;
    }

    @Override
    public Record getRecord(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrA(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getStrB(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStrLen(Record rec, int arrayIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        // The `executionContext.getIntervalFunctionType()` is primarily designed to serve
        // `InTimestampIntervalFunctionFactory.Func`. Regular filter functions are created
        // through post-order traversal, and the type of filtered columns is unknown
        // when creating interval functions. Functions like 'today()' depend on interval type
        // to convert to actual interval ranges (relying solely on `TimestampDriver.FixInterval`
        // would lose precision for `end`). For `init` calls that are not from InTimestampIntervalFunctionFactory.Func,
        // the intervalType here must remain consistent with the intervalType used during function creation.
        intervalType = executionContext.getIntervalFunctionType();
        timestampDriver = IntervalUtils.getTimestampDriverByIntervalType(intervalType);
        final long now = executionContext.getNow(timestampDriver.getTimestampType());
        final long start = timestampDriver.startOfDay(now, shiftFromToday());
        final long end = timestampDriver.endOfDay(start);
        interval.of(start, end);
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
    public boolean isThreadSafe() {
        return true;
    }

    protected abstract int shiftFromToday();
}
