/*******************************************************************************
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
        int y = Micros.getYear(value);
        y = Micros.EPOCH_YEAR_0 + ((y - Micros.EPOCH_YEAR_0) / stepYears) * stepYears;
        int month = startMonth > 0 ? startMonth : 1;
        int day = startDay > 0 ? startDay : 1;
        return Micros.toMicros(y, CommonUtils.isLeapYear(y), month, day) + dayMod;
    }

    @Override
    public void setOffset(long timestamp) {
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Micros.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
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
        final int y = Micros.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y + numYears);
        int month = startMonth > 0 ? startMonth : 1;
        int day = startDay > 0 ? startDay : 1;
        day = Math.min(CommonUtils.getDaysPerMonth(month, leap), day);
        return Micros.toMicros(y + numYears, month, day) + dayMod;
    }
}
