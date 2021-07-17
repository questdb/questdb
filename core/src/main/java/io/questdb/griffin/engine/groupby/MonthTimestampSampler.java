/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.std.datetime.microtime.Timestamps.toMicros;

class MonthTimestampSampler implements TimestampSampler {
    private final int monthCount;
    private int startDay;
    private int startHour;
    private int startMin;
    private int startSec;
    private int startMillis;
    private int startMicros;

    MonthTimestampSampler(int monthCount) {
        this.monthCount = monthCount;
    }

    @Override
    public long nextTimestamp(long timestamp) {
        return addMonth(timestamp, monthCount);
    }

    @Override
    public long previousTimestamp(long timestamp) {
        return addMonth(timestamp, -monthCount);
    }

    @Override
    public long round(long value) {
        int y = Timestamps.getYear(value);
        final boolean leap = Timestamps.isLeapYear(y);
        int m = Timestamps.getMonthOfYear(value, y, leap);
        // target month
        int nextMonth = ((m - 1) / monthCount) * monthCount + 1;
        return toMicros(y, leap, startDay, nextMonth, startHour, startMin, startSec, startMillis, startMicros);
    }

    @Override
    public void setStart(long timestamp) {
        final int y = Timestamps.getYear(timestamp);
        final boolean leap = Timestamps.isLeapYear(y);
        this.startDay = Timestamps.getDayOfMonth(timestamp, y, Timestamps.getMonthOfYear(timestamp, y, leap), leap);
        this.startHour = Timestamps.getHourOfDay(timestamp);
        this.startMin = Timestamps.getMinuteOfHour(timestamp);
        this.startSec = Timestamps.getSecondOfMinute(timestamp);
        this.startMillis = Timestamps.getMillisOfSecond(timestamp);
        this.startMicros = Timestamps.getMicrosOfSecond(timestamp);
    }

    private long addMonth(long timestamp, int monthCount) {
        int y = Timestamps.getYear(timestamp);
        final boolean leap = Timestamps.isLeapYear(y);
        int m = Timestamps.getMonthOfYear(timestamp, y, leap);

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
        int maxDay = Timestamps.getDaysPerMonth(_m, Timestamps.isLeapYear(_y));
        if (_d > maxDay) {
            _d = maxDay;
        }
        return Timestamps.toMicros(_y, _m, _d) +
                +startHour * Timestamps.HOUR_MICROS
                + startMin * Timestamps.MINUTE_MICROS
                + startSec * Timestamps.SECOND_MICROS
                + startMillis * Timestamps.MILLI_MICROS
                + startMicros;
    }
}
