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
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

public class YearTimestampNanosSampler implements TimestampSampler {
    private final int stepYears;
    private int startDay;
    private int startHour;
    private int startMicros;
    private int startMillis;
    private int startMin;
    private int startMonth;
    private int startNanos;
    private int startSec;

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
    public long nextTimestamp(long timestamp, int numSteps) {
        try {
            return addYears(timestamp, numSteps * stepYears);
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
        int y = Nanos.getYear(value);
        y = Nanos.EPOCH_YEAR_0 + ((y - Nanos.EPOCH_YEAR_0) / stepYears) * stepYears;
        return Nanos.toNanos(
                y,
                CommonUtils.isLeapYear(y),
                startDay,
                startMonth,
                startHour,
                startMin,
                startSec,
                startMillis,
                startMicros,
                startNanos
        );
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.startMonth = Nanos.getMonthOfYear(timestamp, y, leap);
        this.startDay = Nanos.getDayOfMonth(timestamp, y, startMonth, leap);
        this.startHour = Nanos.getWallHours(timestamp);
        this.startMin = Nanos.getWallMinutes(timestamp);
        this.startSec = Nanos.getWallSeconds(timestamp);
        this.startMillis = Nanos.getWallMillis(timestamp);
        this.startMicros = Nanos.getWallMicros(timestamp);
        this.startNanos = Nanos.getWallNanos(timestamp);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("YearTsSampler");
    }

    private long addYears(long timestamp, int numYears) {
        if (numYears == 0) {
            return timestamp;
        }
        final int y = Nanos.getYear(timestamp);
        final int newYear = Math.min(y + numYears, NanosTimestampDriver.MAX_NANO_YEAR + 1);
        final boolean leap = CommonUtils.isLeapYear(newYear);
        final int maxDay = Math.min(startDay, CommonUtils.getDaysPerMonth(startMonth, leap)) - 1;
        long result = Nanos.yearNanos(newYear, leap);
        result = Math.addExact(result, Nanos.monthOfYearNanos(startMonth, leap));
        result = Math.addExact(result, Math.multiplyExact(maxDay, Nanos.DAY_NANOS));
        result = Math.addExact(result, Math.multiplyExact(startHour, Nanos.HOUR_NANOS));
        result = Math.addExact(result, Math.multiplyExact(startMin, Nanos.MINUTE_NANOS));
        result = Math.addExact(result, Math.multiplyExact(startSec, Nanos.SECOND_NANOS));
        result = Math.addExact(result, Math.multiplyExact(startMillis, Nanos.MILLI_NANOS));
        result = Math.addExact(result, Math.multiplyExact(startMicros, Nanos.MICRO_NANOS));
        result = Math.addExact(result, startNanos);
        return result;
    }
}
