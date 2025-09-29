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

package io.questdb.griffin.engine.functions.constants;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.FunctionExtension;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.IntervalFunction;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import org.jetbrains.annotations.NotNull;

public class IntervalConstant extends IntervalFunction implements ConstantFunction, FunctionExtension {
    public static final IntervalConstant RAW_NULL = new IntervalConstant(Numbers.LONG_NULL, Numbers.LONG_NULL, ColumnType.INTERVAL_RAW);
    public static final IntervalConstant TIMESTAMP_MICRO_NULL = new IntervalConstant(Numbers.LONG_NULL, Numbers.LONG_NULL, ColumnType.INTERVAL_TIMESTAMP_MICRO);
    public static final IntervalConstant TIMESTAMP_NANO_NULL = new IntervalConstant(Numbers.LONG_NULL, Numbers.LONG_NULL, ColumnType.INTERVAL_TIMESTAMP_NANO);

    private final Interval interval = new Interval();

    public IntervalConstant(long lo, long hi, int intervalType) {
        super(intervalType);
        interval.of(lo, hi);
    }

    public static IntervalConstant newInstance(long lo, long hi, int intervalType) {
        return lo != Numbers.LONG_NULL || hi != Numbers.LONG_NULL ? new IntervalConstant(lo, hi, intervalType) : getIntervalNull(intervalType);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void cursorClosed() {
        super.cursorClosed();
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
    public boolean isNullConstant() {
        return interval.equals(Interval.NULL);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valInterval(interval, intervalType);
    }

    @Override
    public void toTop() {
        super.toTop();
    }

    static IntervalConstant getIntervalNull(int intervalType) {
        switch (intervalType) {
            case ColumnType.INTERVAL_TIMESTAMP_MICRO:
                return TIMESTAMP_MICRO_NULL;
            case ColumnType.INTERVAL_TIMESTAMP_NANO:
                return TIMESTAMP_NANO_NULL;
            case ColumnType.INTERVAL_RAW:
                return RAW_NULL;
            default:
                return null;
        }
    }
}
