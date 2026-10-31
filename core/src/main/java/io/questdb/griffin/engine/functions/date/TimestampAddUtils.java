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
import io.questdb.std.Numbers;

/**
 * Overflow-checked counterparts of the {@link TimestampDriver} add methods for the {@code dateadd()} functions.
 * The add methods wrap around when the result does not fit a long; these return NULL instead.
 */
public final class TimestampAddUtils {

    private TimestampAddUtils() {
    }

    /**
     * Returns the result of a calendar add, or NULL when the add left the timestamp range. The add wraps
     * around there, which moves the year of the result by hundreds of years (585 for nanoseconds, 584,555
     * for microseconds) from the year the add asked for. A margin of one year absorbs any inaccuracy of
     * {@code getYear()} on the last days of a year.
     */
    static long addCalendarOrNull(TimestampDriver driver, char period, long timestamp, int stride, long result) {
        final long year = driver.getYear(timestamp);
        final long expectedYear = period == 'y'
                ? year + stride
                : Math.floorDiv(year * 12 + driver.getMonthOfYear(timestamp) - 1 + stride, 12);
        return result != Numbers.LONG_NULL && Math.abs(driver.getYear(result) - expectedYear) <= 1 ? result : Numbers.LONG_NULL;
    }

    /**
     * Returns {@code timestamp + delta}, or NULL when the sum does not fit a long or {@code delta} is NULL.
     */
    static long addFixedOrNull(long timestamp, long delta) {
        final long result = timestamp + delta;
        if (delta == Numbers.LONG_NULL || ((timestamp ^ result) & (delta ^ result)) < 0) {
            return Numbers.LONG_NULL;
        }
        return result;
    }

    /**
     * Adds {@code stride} units of {@code period} to {@code timestamp} the way {@code method} does, but returns
     * NULL when the result does not fit the timestamp type. {@code timestamp} must not be NULL.
     */
    public static long addOrNull(TimestampDriver driver, TimestampDriver.TimestampAddMethod method, char period, long timestamp, int stride) {
        if (isCalendarUnit(period)) {
            return addCalendarOrNull(driver, period, timestamp, stride, method.add(timestamp, stride));
        }
        return addFixedOrNull(timestamp, getFixedDelta(method.add(0, 1), method.add(0, stride), stride));
    }

    /**
     * Same as the overload above, for a period that is only known at run time. An unknown period gives NULL.
     */
    static long addOrNull(TimestampDriver driver, char period, long timestamp, int stride) {
        if (isCalendarUnit(period)) {
            return addCalendarOrNull(driver, period, timestamp, stride, driver.add(timestamp, period, stride));
        }
        return addFixedOrNull(timestamp, getFixedDelta(driver.add(0, period, 1), driver.add(0, period, stride), stride));
    }

    /**
     * Returns the span of {@code stride} fixed-length units, or NULL if it does not fit a long. {@code unit} is
     * the length of one unit, and {@code wrappedDelta} is the product the add methods compute, which wraps
     * around when the span does not fit.
     */
    static long getFixedDelta(long unit, long wrappedDelta, int stride) {
        // unit is zero when the unit is shorter than a tick: nanoseconds added to a microsecond timestamp
        if (unit != 0 && Math.multiplyHigh(unit, stride) != wrappedDelta >> 63) {
            return Numbers.LONG_NULL;
        }
        return wrappedDelta;
    }

    static boolean isCalendarUnit(char period) {
        return period == 'M' || period == 'y';
    }
}
