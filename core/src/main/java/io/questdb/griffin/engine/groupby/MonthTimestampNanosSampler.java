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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.datetime.nanotime.Nanos.toNanos;

public class MonthTimestampNanosSampler implements TimestampSampler {
    // the calendar grid counts its months from January 1970, as Nanos.floorMM() does
    private static final long EPOCH_MONTH_INDEX = Nanos.EPOCH_YEAR_0 * 12L;
    private final int stepMonths;
    // The month of a bucket start, as 12 * year + month - 1. The buckets start every stepMonths months from it.
    private long anchorMonthIndex = EPOCH_MONTH_INDEX;
    // Time of day of the bucket starts. A negative value comes from setOffset() and places each bucket start that
    // long before the start of its calendar month, on the last day of the previous month.
    private long dayMod;
    private int startDay;

    public MonthTimestampNanosSampler(int stepMonths) {
        this.stepMonths = stepMonths;
    }

    @Override
    public long getApproxBucketSize() {
        return Nanos.MONTH_NANOS_APPROX * stepMonths;
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_NANO;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        try {
            return addMonth(timestamp, stepMonths);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long nextTimestamp(long timestamp, long numSteps) {
        try {
            return addMonth(timestamp, Math.toIntExact(Math.multiplyExact(numSteps, stepMonths)));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return addMonth(timestamp, -stepMonths);
    }

    @Override
    public long round(long value) {
        // Returns the last bucket start at or before the value. A negative offset moves each bucket start into the
        // month before, so the bucket follows the calendar month of the value shifted by the offset; max() keeps the
        // value when the shift overflows at the end of the timestamp range. The bucket that starts in that month,
        // counted from the anchor month in whole strides, can start after the value, when the value comes earlier in
        // its month than the day and time of the grid. Every later bucket starts in a later month. setStart() keeps
        // the time of day under a day, and setOffset() starts the grid on the first day of the month with an offset
        // far shorter than a month, so one step back finds the bucket.
        final long shifted = dayMod < 0 ? Math.max(value, value - dayMod) : value;
        final int y = Nanos.getYear(shifted);
        final boolean isLeap = CommonUtils.isLeapYear(y);
        final int m = Nanos.getMonthOfYear(shifted, y, isLeap);
        final long monthIndex = 12L * y + m - 1;
        final int monthsIntoBucket = stepMonths == 1 ? 0 : Math.floorMod(monthIndex - anchorMonthIndex, stepMonths);
        final long bucketMonthIndex = monthIndex - monthsIntoBucket;
        final long bucketStart = monthsIntoBucket == 0
                ? toNanos(y, isLeap, m, startDay > 0 ? startDay : 1) + dayMod
                : bucketStart(bucketMonthIndex);
        // the difference stays correct when the bucket start overflows a long at the end of the timestamp range
        return value - bucketStart < 0 ? bucketStart(bucketMonthIndex - stepMonths) : bucketStart;
    }

    @Override
    public void setOffset(long timestamp) {
        // the offset applies to the calendar grid, which starts on the first day of the month
        this.anchorMonthIndex = EPOCH_MONTH_INDEX;
        this.startDay = 0;
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        final int m = Nanos.getMonthOfYear(timestamp, y, leap);
        this.anchorMonthIndex = 12L * y + m - 1;
        this.startDay = Nanos.getDayOfMonth(timestamp, y, m, leap);
        long dayMod = timestamp % Nanos.DAY_NANOS;
        if (dayMod < 0) {
            dayMod += Nanos.DAY_NANOS;
        }
        this.dayMod = dayMod;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("MonthTsSampler");
    }

    private long addMonth(long timestamp, int monthCount) {
        // the calendar month of a bucket start that a negative offset moved into the previous month
        final long shifted = timestamp - Math.min(dayMod, 0);
        int y = Nanos.getYear(shifted);
        final boolean leap = CommonUtils.isLeapYear(y);
        int m = Nanos.getMonthOfYear(shifted, y, leap);

        int _y;
        int _m = m - 1 + monthCount;
        if (_m > -1) {
            _y = y + _m / 12;
            _m = (_m % 12) + 1;
        } else {
            _y = y + _m / 12 - 1;
            _m = -_m % 12;
            if (_m == 0) {
                _m = 12;
            }
            _m = 12 - _m + 1;
            if (_m == 1) {
                _y += 1;
            }
        }
        int _d = startDay;
        if (startDay == 0) {
            _d = 1;
        } else {
            int maxDay = CommonUtils.getDaysPerMonth(_m, CommonUtils.isLeapYear(_y));
            if (_d > maxDay) {
                _d = maxDay;
            }
        }
        return Nanos.toNanos(_y, _m, _d) + dayMod;
    }

    // Returns the start of the bucket in the given month, as 12 * year + month - 1. A start day past the end of the
    // month moves to its last day, as in addMonth().
    private long bucketStart(long monthIndex) {
        final int y = (int) Math.floorDiv(monthIndex, 12);
        final int m = (int) Math.floorMod(monthIndex, 12) + 1;
        return toNanos(y, CommonUtils.isLeapYear(y), m, startDay > 0 ? startDay : 1) + dayMod;
    }
}
