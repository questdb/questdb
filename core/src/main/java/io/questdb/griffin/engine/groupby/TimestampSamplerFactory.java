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

import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import org.jetbrains.annotations.NotNull;

public final class TimestampSamplerFactory {

    /**
     * Find the end of the interval token in the input string. The interval token is expected to be a number followed by
     * a single letter qualifier.
     *
     * @param cs       input string
     * @param position position in SQL text to report error against
     * @param kind     kind of an interval we are parsing, used for error reporting
     * @return index of the first character after the interval token
     * @throws SqlException when input string is not a valid interval token
     */
    public static int findIntervalEndIndex(CharSequence cs, int position, CharSequence kind) throws SqlException {
        if (cs == null) {
            throw SqlException.$(position, "missing interval");
        }

        final int len = cs.length();
        if (len == 0) {
            throw SqlException.$(position, "expected interval qualifier");
        }

        // look for end of digits
        int k = -1;
        boolean allZeros = true;
        boolean atLeastOneDigit = false;
        for (int i = 0; i < len; i++) {
            char c = cs.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
            atLeastOneDigit = true;
            if (c != '0') {
                allZeros = false;
            }
        }

        if (k == 0 && cs.charAt(0) == '-') {
            throw SqlException.$(position, "negative interval is not allowed");
        }

        if (allZeros && atLeastOneDigit) {
            throw SqlException.$(position, "zero is not a valid ").put(kind).put(" value");
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

    public static TimestampSampler getInstance(TimestampDriver driver, long interval, CharSequence units, int position) throws SqlException {
        if (units.length() == 1) {
            return getInstance(driver, interval, units.charAt(0), position);
        }
        // Just in case SqlParser will allow this in the future
        throw SqlException.$(position, "expected one character interval qualifier");
    }

    @NotNull
    public static TimestampSampler getInstance(TimestampDriver driver, long interval, char timeUnit, int position) throws SqlException {
        return driver.getTimestampSampler(interval, timeUnit, position);
    }

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs       the key
     * @param position position in SQL text to report error against
     * @return instance of appropriate TimestampSampler
     * @throws SqlException when input string is invalid
     */
    public static TimestampSampler getInstance(TimestampDriver driver, CharSequence cs, int position) throws SqlException {
        int k = findIntervalEndIndex(cs, position, "sample");
        assert cs.length() > k;

        long n = parseInterval(cs, k, position, "sample", Numbers.INT_NULL, '?');
        return getInstance(driver, n, cs.charAt(k), position + k);
    }

    /**
     * Parse interval value from string. Expected to be called after {@link #findIntervalEndIndex(CharSequence, int, CharSequence)}
     * has been called and returned a valid index. Behavior is undefined if called with invalid index.
     *
     * @param cs          token to parse interval from
     * @param intervalEnd end of interval token, exclusive
     * @param position    position in SQL text to report error against
     * @param kind        kind of an interval we are parsing, used for error reporting
     * @param maxValue    maximum value for the interval, used for error reporting
     * @param unit        unit qualifier, used for error reporting
     * @return parsed interval value
     * @throws SqlException when input string is invalid
     */
    public static long parseInterval(CharSequence cs, int intervalEnd, int position, String kind, int maxValue, char unit) throws SqlException {
        if (intervalEnd == 0) {
            // 'SAMPLE BY m' is the same as 'SAMPLE BY 1m' etc.
            return 1;
        }
        try {
            int n = Numbers.parseInt(cs, 0, intervalEnd);
            if (n == 0) {
                throw SqlException.$(position, "zero is not a valid ").put(kind).put(" value");
            }
            if (maxValue != Numbers.INT_NULL && n > maxValue) {
                throw SqlException.$(position, kind).put(" value too high for given units [value=").put(cs).put(", maximum=").put(maxValue).put(unit).put(']');
            }
            return n;
        } catch (NumericException e) {
            SqlException ex = SqlException.$(position, "invalid ").put(kind).put(" value [value=").put(cs);
            if (maxValue != Numbers.INT_NULL) {
                ex.put(", maximum=").put(maxValue).put(unit);
            }
            ex.put(']');
            throw ex;
        }
    }
}
