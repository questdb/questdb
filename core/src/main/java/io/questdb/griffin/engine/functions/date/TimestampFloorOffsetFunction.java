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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.datetime.CommonUtils;


public final class TimestampFloorOffsetFunction extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
    private final char addUnit;
    private final Function arg;
    private final TimestampDriver.TimestampFloorWithOffsetMethod floor;
    private final boolean isExactlyInvertible;
    private final String name;
    private final long offset;
    private final int stride;
    private final char unit;

    public TimestampFloorOffsetFunction(String name, Function arg, char unit, int stride, long offset, int timestampType) {
        super(timestampType);
        this.arg = arg;
        this.name = name;
        this.stride = stride;
        this.offset = offset;
        this.unit = unit;
        // add()/dateadd use the lowercase microsecond unit while ceil/floor use the uppercase one
        this.addUnit = unit == 'U' ? 'u' : unit;
        floor = this.timestampDriver.getTimestampFloorWithOffsetMethod(unit);
        this.isExactlyInvertible = isExactlyInvertible();
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
    public Function getTimestampArg() {
        return arg;
    }

    @Override
    public int invertTimestampInterval(Interval io) {
        if (!isExactlyInvertible) {
            return NONE;
        }
        long lo = io.getLo();
        long hi = io.getHi();
        // below the origin every timestamp floors to the origin
        if (offset != 0) {
            if (hi != Long.MAX_VALUE && hi < offset) {
                io.of(Long.MAX_VALUE, Numbers.LONG_NULL);
                return EXACT;
            }
            if (lo != Numbers.LONG_NULL && lo <= offset) {
                lo = Numbers.LONG_NULL;
            }
        }
        if (lo != Numbers.LONG_NULL) {
            final long b = floor.floor(lo, stride, offset);
            if (b != lo) {
                final long c = timestampDriver.add(b, addUnit, stride);
                if (c < lo) {
                    return NONE;
                }
                lo = c;
            }
        }
        if (hi != Long.MAX_VALUE) {
            final long c = timestampDriver.add(floor.floor(hi, stride, offset), addUnit, stride);
            if (c <= hi) {
                return NONE;
            }
            hi = c - 1;
        }
        io.of(lo, hi);
        return EXACT;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(name).val("('");
        sink.val(stride);
        sink.val(unit).val("',");
        sink.val(getArg());
        if (offset != 0) {
            sink.val(",'").val(timestampDriver.toMSecString(offset)).val('\'');
        }
        sink.val(')');
    }

    // EXACT only when add() reproduces the floor's bucket boundaries; a sub-resolution
    // stride (e.g. nanoseconds on a micro column) does not, and must stay a row filter.
    private boolean isExactlyInvertible() {
        if (!CommonUtils.isFixedAlignedUnit(unit)) {
            return false;
        }
        final long b0 = floor.floor(offset, stride, offset);
        final long next = timestampDriver.add(b0, addUnit, stride);
        return next > b0 && floor.floor(next, stride, offset) == next && floor.floor(next - 1, stride, offset) == b0;
    }
}
