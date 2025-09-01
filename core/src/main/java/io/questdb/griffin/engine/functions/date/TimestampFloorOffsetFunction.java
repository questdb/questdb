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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Numbers;


public final class TimestampFloorOffsetFunction extends TimestampFunction implements UnaryFunction {
    private final Function arg;
    private final TimestampDriver.TimestampFloorWithOffsetMethod floor;
    private final long offset;
    private final int stride;
    private final char unit;

    public TimestampFloorOffsetFunction(Function arg, char unit, int stride, long offset, int timestampType) {
        super(timestampType);
        this.arg = arg;
        this.stride = stride;
        this.offset = offset;
        this.unit = unit;
        floor = this.timestampDriver.getTimestampFloorWithOffsetMethod(unit);
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public long getTimestamp(Record rec) {
        final long ts = arg.getTimestamp(rec);
        return ts == Numbers.LONG_NULL ? Numbers.LONG_NULL : floor.floor(ts, stride, offset);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(TimestampFloorFunctionFactory.NAME).val("('");
        sink.val(stride);
        sink.val(unit).val("',");
        sink.val(getArg());
        if (offset != 0) {
            sink.val(",'").val(timestampDriver.toMSecString(offset)).val('\'');
        }
        sink.val(')');
    }
}
