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
    public long nextTimestamp(long timestamp) {
        return addYears(timestamp, stepYears);
    }

    @Override
    public long nextTimestamp(long timestamp, int numSteps) {
        return addYears(timestamp, numSteps * stepYears);
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return addYears(timestamp, -stepYears);
    }

    @Override
    public long round(long value) {
        final int y = Nanos.getYear(value);
        return Nanos.toNanos(
                y - y % stepYears,
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
        final boolean leap = CommonUtils.isLeapYear(y + numYears);
        final int maxDay = Math.min(startDay, CommonUtils.getDaysPerMonth(startMonth, leap)) - 1;
        return Nanos.yearNanos(y + numYears, leap)
                + Nanos.monthOfYearNanos(startMonth, leap)
                + maxDay * Nanos.DAY_NANOS
                + startHour * Nanos.HOUR_NANOS
                + startMin * Nanos.MINUTE_NANOS
                + startSec * Nanos.SECOND_NANOS
                + startMillis * Nanos.MILLI_NANOS
                + startMicros * Nanos.MICRO_NANOS
                + startNanos;
    }
}