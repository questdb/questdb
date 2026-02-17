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
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.datetime.nanotime.Nanos.toNanos;

public class MonthTimestampNanosSampler implements TimestampSampler {
    private final int stepMonths;
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
        int y = Nanos.getYear(value);
        final boolean leap = CommonUtils.isLeapYear(y);
        int m = Nanos.getMonthOfYear(value, y, leap);
        // target month
        int nextMonth = ((m - 1) / stepMonths) * stepMonths + 1;
        int d = startDay > 0 ? startDay : 1;
        return toNanos(y, leap, nextMonth, d) + dayMod;
    }

    @Override
    public void setOffset(long timestamp) {
        this.dayMod = timestamp;
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        this.startDay = Nanos.getDayOfMonth(timestamp, y, Nanos.getMonthOfYear(timestamp, y, leap), leap);
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
        int y = Nanos.getYear(timestamp);
        final boolean leap = CommonUtils.isLeapYear(y);
        int m = Nanos.getMonthOfYear(timestamp, y, leap);

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
}