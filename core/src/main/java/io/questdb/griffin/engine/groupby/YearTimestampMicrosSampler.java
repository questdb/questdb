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
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class YearTimestampMicrosSampler implements TimestampSampler {
    private final int stepYears;
    // The year of a bucket start. The buckets start every stepYears years from it. The calendar grid counts its
    // years from 1970, as Micros.floorYYYY() does.
    private int anchorYear = Micros.EPOCH_YEAR_0;
    // Time of day of the bucket starts. A negative value comes from setOffset() and places each bucket start that
    // long before the start of its calendar year, on the last day of the previous year.
    private long dayMod;
    private int startDay;
    private int startMonth;

    public YearTimestampMicrosSampler(int stepYears) {
        this.stepYears = stepYears;
    }

    @Override
    public long getApproxBucketSize() {
        return Micros.YEAR_MICROS_NONLEAP * stepYears;
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_MICRO;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        return addYears(timestamp, stepYears);
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
        // Returns the last bucket start at or before the value. A negative offset moves each bucket start into the
        // year before, so the bucket follows the calendar year of the value shifted by the offset; max() keeps the
        // value when the shift overflows at the end of the timestamp range. The bucket that starts in that year,
        // counted from the anchor year in whole strides, can start after the value, when the value comes earlier in
        // its year than the month, day and time of the grid. Every later bucket starts in a later year. setStart()
        // keeps the time of day under a day, and setOffset() starts the grid on the first day of the year with an
        // offset far shorter than a year, so one step back finds the bucket.
        final int y = Micros.getYear(dayMod < 0 ? Math.max(value, value - dayMod) : value);
        final int bucketYear = stepYears == 1 ? y : anchorYear + Math.floorDiv(y - anchorYear, stepYears) * stepYears;
        final long bucketStart = bucketStart(bucketYear);
        // the difference stays correct when the bucket start overflows a long at the end of the timestamp range
        return value - bucketStart < 0 ? bucketStart(bucketYear - stepYears) : bucketStart;
    }

    @Override
    public void setOffset(long timestamp) {
        // the offset applies to the calendar grid, which starts on the first day of the year
        this.anchorYear = Micros.EPOCH_YEAR_0;
        this.startMonth = 0;
        this.startDay = 0;
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Micros.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.anchorYear = y;
        this.startMonth = Micros.getMonthOfYear(timestamp, y, leap);
        this.startDay = Micros.getDayOfMonth(timestamp, y, startMonth, leap);
        long dayMod = timestamp % Micros.DAY_MICROS;
        if (dayMod < 0) {
            dayMod += Micros.DAY_MICROS;
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
        final int y = Micros.getYear(timestamp - Math.min(dayMod, 0));
        final boolean leap = CommonUtils.isLeapYear(y + numYears);
        int month = startMonth > 0 ? startMonth : 1;
        int day = startDay > 0 ? startDay : 1;
        day = Math.min(CommonUtils.getDaysPerMonth(month, leap), day);
        return Micros.toMicros(y + numYears, month, day) + dayMod;
    }

    // Returns the start of the bucket in the given year. A start day past the end of the month moves to its last
    // day, as in addYears().
    private long bucketStart(int year) {
        return Micros.toMicros(year, CommonUtils.isLeapYear(year), startMonth > 0 ? startMonth : 1, startDay > 0 ? startDay : 1) + dayMod;
    }
}
