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

package io.questdb.griffin;

import io.questdb.std.datetime.microtime.Micros;

/**
 * How a function's value depends on the precision of its TIMESTAMP argument, see
 * {@link FunctionFactory#getTimestampPrecisionUse(io.questdb.griffin.model.ExpressionNode)}.
 */
public enum TimestampPrecisionUse {
    /**
     * The value depends on the precision, as the argument's raw value or its text does.
     */
    DEPENDENT,
    /**
     * The value is the same at every precision, as a calendar field of the instant is.
     */
    INVARIANT,
    /**
     * The value is a timestamp of the same instant at every precision, typed at the argument's precision.
     */
    PRESERVING;

    /**
     * The use of a call that floors its TIMESTAMP argument to the epoch-aligned grid of {@code stride} constant time
     * units {@code unit}: {@link #PRESERVING} for a grid of nanoseconds whose stride divides a microsecond or is a
     * whole number of microseconds, which floors an instant to the same instant at microsecond and at nanosecond
     * precision, otherwise as {@link #ofShift} for the unit.
     */
    public static TimestampPrecisionUse ofFloor(long stride, char unit) {
        return unit == 'n' && stride > 0 && (Micros.MICRO_NANOS % stride == 0 || stride % Micros.MICRO_NANOS == 0) ? PRESERVING : ofShift(unit);
    }

    /**
     * The use of a call that shifts its TIMESTAMP argument by a number of constant time units {@code unit}:
     * {@link #PRESERVING} for a microsecond or a longer unit, which every precision counts exactly, and
     * {@link #DEPENDENT} for a nanosecond, as microsecond precision truncates a number of nanoseconds that is not a
     * whole number of microseconds.
     */
    public static TimestampPrecisionUse ofShift(char unit) {
        return unit != 'n' ? PRESERVING : DEPENDENT;
    }
}
