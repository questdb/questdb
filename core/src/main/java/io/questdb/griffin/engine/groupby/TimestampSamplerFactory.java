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

import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;

public final class TimestampSamplerFactory {

    /**
     * Find the end of the interval token in the input string. The interval token is expected to be a number followed by
     * a single letter qualifier.
     *
     * @param cs       input string
     * @param position position in SQL text to report error against
     * @return index of the first character after the interval token
     * @throws SqlException when input string is not a valid interval token
     */
    public static int findIntervalEndIndex(CharSequence cs, int position) throws SqlException {
        int k = -1;

        if (cs == null) {
            throw SqlException.$(position, "missing interval");
        }

        final int len = cs.length();
        if (len == 0) {
            throw SqlException.$(position, "expected interval qualifier");
        }

        // look for end of digits
        for (int i = 0; i < len; i++) {
            char c = cs.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == 0 && cs.charAt(0) == '-') {
            throw SqlException.$(position, "negative interval is not allowed");
        }

        if (k == -1) {
            throw SqlException.$(position + len, "expected interval qualifier");
        }

        // expect 1 letter qualifier
        if (k + 1 < len) {
            throw SqlException.$(position + k, "expected single letter qualifier");
        }

        return k;
    }

    public static TimestampSampler getInstance(long period, CharSequence units, int position) throws SqlException {
        if (units.length() == 1) {
            return getInstance(period, units.charAt(0), position);
        }
        // Just in case SqlParser will allow this in the future
        throw SqlException.$(position, "expected one character interval qualifier");
    }

    @NotNull
    public static TimestampSampler getInstance(long interval, char timeUnit, int position) throws SqlException {
        switch (timeUnit) {
            case 'U':
                // micros
                return new MicroTimestampSampler(interval);
            case 'T':
                // millis
                return new MicroTimestampSampler(Timestamps.MILLI_MICROS * interval);
            case 's':
                // seconds
                return new MicroTimestampSampler(Timestamps.SECOND_MICROS * interval);
            case 'm':
                // minutes
                return new MicroTimestampSampler(Timestamps.MINUTE_MICROS * interval);
            case 'h':
                // hours
                return new MicroTimestampSampler(Timestamps.HOUR_MICROS * interval);
            case 'd':
                // days
                return new MicroTimestampSampler(Timestamps.DAY_MICROS * interval);
            case 'w':
                // weeks
                return new MicroTimestampSampler(Timestamps.WEEK_MICROS * interval);
            case 'M':
                // months
                return new MonthTimestampSampler((int) interval);
            case 'y':
                return new YearTimestampSampler((int) interval);
            default:
                // Just in case SqlParser will allow this in the future
                throw SqlException.$(position, "unsupported interval qualifier");

        }
    }

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs       the key
     * @param position position in SQL text to report error against
     * @return instance of appropriate TimestampSampler
     * @throws SqlException when input string is invalid
     */
    public static TimestampSampler getInstance(CharSequence cs, int position) throws SqlException {
        int k = findIntervalEndIndex(cs, position);
        assert cs.length() > k;

        long n = parseInterval(cs, k, position);
        return getInstance(n, cs.charAt(k), position + k);
    }

    /**
     * Parse interval value from string. Expected to be called after {@link #findIntervalEndIndex(CharSequence, int)}
     * has been called and returned a valid index. Behavior is undefined if called with invalid index.
     *
     * @param cs          token to parse interval from
     * @param intervalEnd end of interval token, exclusive
     * @param position    position in SQL text to report error against
     * @return parsed interval value
     * @throws SqlException when input string is invalid
     */
    public static long parseInterval(CharSequence cs, int intervalEnd, int position) throws SqlException {
        if (intervalEnd == 0) {
            // 'SAMPLE BY m' is the same as 'SAMPLE BY 1m' etc.
            return 1;
        }
        try {
            int n = Numbers.parseInt(cs, 0, intervalEnd);
            if (n == 0) {
                throw SqlException.$(position, "zero is not a valid sample value");
            }
            return n;
        } catch (NumericException e) {
            throw SqlException.$(position, "invalid sample value [value=").put(cs).put(']');
        }
    }
}
