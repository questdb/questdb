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

package io.questdb.cairo;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Utf8Sequence;

public class TimestampUtils {

    public static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };

    /**
     * Days in a given month. This method expects you to know if month is in leap year.
     *
     * @param m    month from 1 to 12
     * @param leap true if this is for leap year
     * @return number of days in month.
     */
    public static int getDaysPerMonth(int m, boolean leap) {
        return leap & m == 2 ? 29 : DAYS_PER_MONTH[m - 1];
    }

    /**
     * Calculates if year is leap year using following algorithm:
     * <p>
     * <a href="http://en.wikipedia.org/wiki/Leap_year">...</a>
     *
     * @param year the year
     * @return true if year is leap
     */
    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    public static long microsToNanos(long micros) {
        if (micros == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        try {
            return Math.multiplyExact(micros, 1000L);
        } catch (ArithmeticException e) {
            throw ImplicitCastException.inconvertibleValue(micros, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO);
        }
    }

    public static long nanosToMicros(long nanos) {
        return nanos == Numbers.LONG_NULL ? Numbers.LONG_NULL : nanos / 1000L;
    }

    protected static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    protected static void checkChar(Utf8Sequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.byteAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    protected static boolean checkLen2(int p, int lim) throws NumericException {
        if (lim - p >= 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    protected static boolean checkLen3(int p, int lim) throws NumericException {
        if (lim - p > 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    protected static boolean checkLenStrict(int p, int lim) throws NumericException {
        if (lim - p == 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    protected static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.INSTANCE;
        }
    }

    protected static void checkSpecialChar(CharSequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.charAt(p) != 'T' && s.charAt(p) != ' ')) {
            throw NumericException.INSTANCE;
        }
    }

    protected static void checkSpecialChar(Utf8Sequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.byteAt(p) != 'T' && s.byteAt(p) != ' ')) {
            throw NumericException.INSTANCE;
        }
    }

    protected static int parseSign(char c) throws NumericException {
        int tzSign;
        switch (c) {
            case '+':
                tzSign = -1;
                break;
            case '-':
                tzSign = 1;
                break;
            default:
                throw NumericException.INSTANCE;
        }
        return tzSign;
    }

    protected static int tenPow(int i) throws NumericException {
        switch (i) {
            case 0:
                return 1;
            case 1:
                return 10;
            case 2:
                return 100;
            case 3:
                return 1000;
            case 4:
                return 10000;
            case 5:
                return 100000;
            case 6:
                return 1000000;
            case 7:
                return 10000000;
            case 8:
                return 100000000;
            default:
                throw NumericException.INSTANCE;
        }
    }

    @FunctionalInterface
    public interface TimestampUnitConverter {
        long convert(long timestamp);
    }
}
