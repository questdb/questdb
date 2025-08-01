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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class YearTimestampMicrosSampler implements TimestampSampler {
    private final int stepYears;
    private int startDay;
    private int startHour;
    private int startMicros;
    private int startMillis;
    private int startMin;
    private int startMonth;
    private int startSec;

    public YearTimestampMicrosSampler(int stepYears) {
        this.stepYears = stepYears;
    }

    @Override
    public long getApproxBucketSize() {
        return Timestamps.YEAR_MICROS_NONLEAP * stepYears;
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_MICRO;
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
    public long nextTimestamp(long timestamp, int numSteps) {
        try {
            return addYears(timestamp, Math.multiplyExact(numSteps, stepYears));
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
        final int y = Timestamps.getYear(value);
        return Timestamps.toMicros(
                y - y % stepYears,
                CommonUtils.isLeapYear(y),
                startDay,
                startMonth,
                startHour,
                startMin,
                startSec,
                startMillis,
                startMicros
        );
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Timestamps.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.startMonth = Timestamps.getMonthOfYear(timestamp, y, leap);
        this.startDay = Timestamps.getDayOfMonth(timestamp, y, startMonth, leap);
        this.startHour = Timestamps.getHourOfDay(timestamp);
        this.startMin = Timestamps.getMinuteOfHour(timestamp);
        this.startSec = Timestamps.getSecondOfMinute(timestamp);
        this.startMillis = Timestamps.getMillisOfSecond(timestamp);
        this.startMicros = Timestamps.getMicrosOfMilli(timestamp);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("YearTsSampler");
    }

    private long addYears(long timestamp, int numYears) {
        if (numYears == 0) {
            return timestamp;
        }
        final int y = Timestamps.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y + numYears);
        final int maxDay = Math.min(startDay, CommonUtils.getDaysPerMonth(startMonth, leap)) - 1;
        return Timestamps.yearMicros(y + numYears, leap)
                + Timestamps.monthOfYearMicros(startMonth, leap)
                + maxDay * Timestamps.DAY_MICROS
                + startHour * Timestamps.HOUR_MICROS
                + startMin * Timestamps.MINUTE_MICROS
                + startSec * Timestamps.SECOND_MICROS
                + startMillis * Timestamps.MILLI_MICROS
                + startMicros;
    }
}
