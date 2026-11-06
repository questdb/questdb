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

package io.questdb.griffin.bind;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.std.Numbers;

/**
 * Stands in, while {@link FunctionBinder} creates a call, for a constant it canonicalizes to the precision of a
 * TIMESTAMP operand: the binder reuses it across calls, and replaces it with the {@link TimestampConstant} it
 * {@link #materialize materializes} wherever a function can keep it beyond the call's creation.
 */
final class CanonicalTimestampConstant extends TimestampFunction implements ConstantFunction {
    private long value;

    CanonicalTimestampConstant() {
        super(ColumnType.TIMESTAMP_MICRO);
    }

    @Override
    public long getTimestamp(Record rec) {
        return value;
    }

    @Override
    public boolean isNullConstant() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.valISODate(timestampDriver, value);
    }

    /**
     * The constant this one stands in for.
     */
    Function materialize() {
        return new TimestampConstant(value, timestampType);
    }

    /**
     * Stands in for a constant that is not NULL.
     */
    CanonicalTimestampConstant of(long value, int timestampType) {
        assert value != Numbers.LONG_NULL;
        this.value = value;
        this.timestampType = timestampType;
        this.timestampDriver = ColumnType.getTimestampDriver(timestampType);
        return this;
    }
}
