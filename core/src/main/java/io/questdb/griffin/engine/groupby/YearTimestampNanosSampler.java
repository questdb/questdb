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
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class YearTimestampNanosSampler implements TimestampSampler {
    private final int stepYears;
    // grid anchor: buckets start every stepYears years from this year; defaults to 1970, as in Nanos.floorYYYY()
    private int anchorYear = Nanos.EPOCH_YEAR_0;
    // Time of day of the bucket starts. A negative value comes from setOffset() and places each bucket start that
    // long before the start of its calendar year, on the last day of the previous year.
    private long dayMod;
    private int startDay;
    private int startMonth;

    public YearTimestampNanosSampler(int stepYears) {
        this.stepYears = stepYears;
    }

    @Override
    public long getApproxBucketSize() {
        return Nanos.YEAR_NANOS_NONLEAP * stepYears;
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_NANO;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        try {
            return addYears(timestamp, stepYears);
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long nextTimestamp(long timestamp, long numSteps) {
        try {
            return addYears(timestamp, Math.toIntExact(Math.multiplyExact(numSteps, stepYears)));
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return addYears(timestamp, -stepYears);
    }

    @Override
    public long round(long value) {
        // Returns the last bucket start at or before the value. A negative offset moves bucket starts into the
        // previous year, so use the year of the value shifted by the offset; max() keeps the value as is when the
        // shift overflows near Long.MAX_VALUE. The candidate bucket in that year starts after the value when the
        // value lies earlier in its year than the grid's month, day and time. In that case, the bucket one stride
        // back is the answer: setStart() keeps the time of day under a day, and setOffset() uses offsets far
        // shorter than a year.
        final int y = Nanos.getYear(dayMod < 0 ? Math.max(value, value - dayMod) : value);
        final int bucketYear = stepYears == 1 ? y : anchorYear + Math.floorDiv(y - anchorYear, stepYears) * stepYears;
        final long bucketStart = bucketStart(bucketYear);
        // compare via subtraction, which stays correct when bucketStart overflows near Long.MAX_VALUE
        return value - bucketStart < 0 ? bucketStart(bucketYear - stepYears) : bucketStart;
    }

    @Override
    public void setOffset(long timestamp) {
        // the offset applies to the calendar grid, which starts on the first day of the year
        this.anchorYear = Nanos.EPOCH_YEAR_0;
        this.startMonth = 0;
        this.startDay = 0;
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.anchorYear = y;
        this.startMonth = Nanos.getMonthOfYear(timestamp, y, leap);
        this.startDay = Nanos.getDayOfMonth(timestamp, y, startMonth, leap);
        long dayMod = timestamp % Nanos.DAY_NANOS;
        if (dayMod < 0) {
            dayMod += Nanos.DAY_NANOS;
        }
        this.dayMod = dayMod;
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("YearTsSampler");
    }

    private long addYears(long timestamp, int numYears) {
        if (numYears == 0) {
            return timestamp;
        }
        // the calendar year of a bucket start that a negative offset moved into the previous year
        final int y = Nanos.getYear(timestamp - Math.min(dayMod, 0));
        final int newYear = Math.min(y + numYears, NanosTimestampDriver.MAX_NANO_YEAR + 1);
        final boolean leap = CommonUtils.isLeapYear(newYear);
        int month = startMonth > 0 ? startMonth : 1;
        int day = startDay > 0 ? startDay : 1;
        day = Math.min(CommonUtils.getDaysPerMonth(month, leap), day);
        return Nanos.toNanos(newYear, month, day) + dayMod;
    }

    // Returns the bucket start in the given year. A start day past the end of the month is clamped to its
    // last day, as in addYears().
    private long bucketStart(int year) {
        return Nanos.toNanos(year, CommonUtils.isLeapYear(year), startMonth > 0 ? startMonth : 1, startDay > 0 ? startDay : 1) + dayMod;
    }
}
