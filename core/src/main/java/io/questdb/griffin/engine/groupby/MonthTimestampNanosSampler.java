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
    // month index (12 * year + month - 1) of January 1970, the default grid anchor, as in Nanos.floorMM()
    private static final long EPOCH_MONTH_INDEX = Nanos.EPOCH_YEAR_0 * 12L;
    private final int stepMonths;
    // Bucket starts fall on the months whose index (12 * year + month - 1) modulo stepMonths equals this value.
    private int anchorPhase;
    // Time of day of the bucket starts. A negative value comes from setOffset() and places each bucket start that
    // long before the start of its calendar month, on the last day of the previous month.
    private long dayMod;
    private int startDay;

    public MonthTimestampNanosSampler(int stepMonths) {
        this.stepMonths = stepMonths;
        this.anchorPhase = Math.floorMod(EPOCH_MONTH_INDEX, stepMonths);
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
        // Returns the last bucket start at or before the value. A negative offset moves bucket starts into the
        // previous month, so use the month of the value shifted by the offset; max() keeps the value as is when
        // the shift overflows near Long.MAX_VALUE. The candidate bucket in that month starts after the value when
        // the value lies earlier in its month than the grid's day and time. In that case, the bucket one stride
        // back is the answer: setStart() keeps the time of day under a day, and setOffset() uses offsets far
        // shorter than a month.
        final long shifted = dayMod < 0 ? Math.max(value, value - dayMod) : value;
        final int y = Nanos.getYear(shifted);
        final boolean isLeap = CommonUtils.isLeapYear(y);
        final int m = Nanos.getMonthOfYear(shifted, y, isLeap);
        // 12 * y + m - 1 - anchorPhase fits an int for any timestamp year; bucketMonth < 1 means that the bucket
        // starts in an earlier year
        final int bucketMonth = stepMonths == 1 ? m : m - Math.floorMod(12 * y + m - 1 - anchorPhase, stepMonths);
        if (bucketMonth > 0) {
            final long bucketStart = toNanos(y, isLeap, bucketMonth, startDay > 0 ? startDay : 1) + dayMod;
            // compare via subtraction, which stays correct when bucketStart overflows near Long.MAX_VALUE
            return value - bucketStart < 0 ? bucketStart(12L * y + bucketMonth - 1 - stepMonths) : bucketStart;
        }
        return roundFromEarlierYear(value, 12L * y + bucketMonth - 1);
    }

    @Override
    public void setOffset(long timestamp) {
        // the offset applies to the calendar grid, which starts on the first day of the month
        this.anchorPhase = Math.floorMod(EPOCH_MONTH_INDEX, stepMonths);
        this.startDay = 0;
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        final int m = Nanos.getMonthOfYear(timestamp, y, leap);
        this.anchorPhase = Math.floorMod(12L * y + m - 1, stepMonths);
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

    // Returns the bucket start in the month with the given index (12 * year + month - 1). A start day past
    // the end of the month is clamped to its last day, as in addMonth().
    private long bucketStart(long monthIndex) {
        final int y = (int) Math.floorDiv(monthIndex, 12);
        final int m = (int) Math.floorMod(monthIndex, 12) + 1;
        return toNanos(y, CommonUtils.isLeapYear(y), m, startDay > 0 ? startDay : 1) + dayMod;
    }

    // Returns the last bucket start at or before the value, given the month index (12 * year + month - 1)
    // of the candidate bucket, which falls in an earlier year than the value.
    private long roundFromEarlierYear(long value, long bucketMonthIndex) {
        final long bucketStart = bucketStart(bucketMonthIndex);
        return value - bucketStart < 0 ? bucketStart(bucketMonthIndex - stepMonths) : bucketStart;
    }
}
