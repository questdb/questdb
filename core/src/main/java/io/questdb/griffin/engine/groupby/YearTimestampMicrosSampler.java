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
import io.questdb.std.datetime.microtime.Micros;
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
        int y = Micros.getYear(value);
        y = Micros.EPOCH_YEAR_0 + ((y - Micros.EPOCH_YEAR_0) / stepYears) * stepYears;
        return Micros.toMicros(
                y,
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
        final int y = Micros.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.startMonth = Micros.getMonthOfYear(timestamp, y, leap);
        this.startDay = Micros.getDayOfMonth(timestamp, y, startMonth, leap);
        this.startHour = Micros.getHourOfDay(timestamp);
        this.startMin = Micros.getMinuteOfHour(timestamp);
        this.startSec = Micros.getSecondOfMinute(timestamp);
        this.startMillis = Micros.getMillisOfSecond(timestamp);
        this.startMicros = Micros.getMicrosOfMilli(timestamp);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("YearTsSampler");
    }

    private long addYears(long timestamp, int numYears) {
        if (numYears == 0) {
            return timestamp;
        }
        final int y = Micros.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y + numYears);
        final int maxDay = Math.min(startDay, CommonUtils.getDaysPerMonth(startMonth, leap)) - 1;
        return Micros.yearMicros(y + numYears, leap)
                + Micros.monthOfYearMicros(startMonth, leap)
                + maxDay * Micros.DAY_MICROS
                + startHour * Micros.HOUR_MICROS
                + startMin * Micros.MINUTE_MICROS
                + startSec * Micros.SECOND_MICROS
                + startMillis * Micros.MILLI_MICROS
                + startMicros;
    }
}
