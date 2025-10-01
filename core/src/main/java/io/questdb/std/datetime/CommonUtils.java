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

package io.questdb.std.datetime;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.PartitionBy;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Utf8Sequence;

public class CommonUtils {
    public static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    public static final int DAY_HOURS = 24;
    public static final String DAY_PATTERN = "yyyy-MM-dd";
    public static final String GREEDY_MILLIS1_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.Sz";
    public static final String GREEDY_MILLIS2_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSz";
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_MINUTES = 60;
    public static final String HOUR_PATTERN = "yyyy-MM-ddTHH";
    public static final int HOUR_PM = 1;
    // "2261-12-31 23:59:59.999999999" for nano timestamp
    public static final long MAX_TIMESTAMP = 9214646399999999999L;
    public static final long MINUTE_SECONDS = 60;
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String NSEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz";
    public static final String PG_TIMESTAMP_MILLI_TIME_Z_PATTERN = "y-MM-dd HH:mm:ss.SSSz";
    public static final String SEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ssz";
    public static final byte TIMESTAMP_UNIT_NANOS = 1;
    public static final byte TIMESTAMP_UNIT_MICROS = TIMESTAMP_UNIT_NANOS + 1;
    public static final byte TIMESTAMP_UNIT_MILLIS = TIMESTAMP_UNIT_MICROS + 1;
    public static final byte TIMESTAMP_UNIT_SECONDS = TIMESTAMP_UNIT_MILLIS + 1;
    public static final byte TIMESTAMP_UNIT_MINUTES = TIMESTAMP_UNIT_SECONDS + 1;
    public static final byte TIMESTAMP_UNIT_HOURS = TIMESTAMP_UNIT_MINUTES + 1;
    public static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final String WEEK_PATTERN = "YYYY-Www";
    public static final int YEAR_MONTHS = 12;
    public static final String YEAR_PATTERN = "yyyy";

    public static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.instance();
        }
    }

    public static void checkChar(Utf8Sequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.byteAt(p) != c) {
            throw NumericException.instance();
        }
    }

    public static boolean checkLen2(int p, int lim) throws NumericException {
        if (lim - p >= 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static boolean checkLen3(int p, int lim) throws NumericException {
        if (lim - p > 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static boolean checkLenStrict(int p, int lim) throws NumericException {
        if (lim - p == 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.instance();
        }
    }

    public static void checkSpecialChar(CharSequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.charAt(p) != 'T' && s.charAt(p) != ' ')) {
            throw NumericException.instance();
        }
    }

    public static void checkSpecialChar(Utf8Sequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.byteAt(p) != 'T' && s.byteAt(p) != ' ')) {
            throw NumericException.instance();
        }
    }

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
     * Since ISO weeks don't always start on the first day of the year, there is an offset of days from the 1st day of the year.
     *
     * @param year of timestamp
     * @return difference in the days from the start of the year (January 1st) and the first ISO week
     */
    public static int getIsoYearDayOffset(int year) {
        int dayOfTheWeekOfEndOfPreviousYear = Micros.getDayOfTheWeekOfEndOfYear(year - 1);
        return ((dayOfTheWeekOfEndOfPreviousYear <= 3) ? 0 : 7) - dayOfTheWeekOfEndOfPreviousYear;
    }

    public static int getStrideMultiple(CharSequence str, int position) throws SqlException {
        if (str != null) {
            if (Chars.equals(str, '-')) {
                throw SqlException.position(position).put("positive number expected: ").put(str);
            }
            if (str.length() > 1) {
                try {
                    final int multiple = Numbers.parseInt(str, 0, str.length() - 1);
                    if (multiple <= 0) {
                        throw SqlException.position(position).put("positive number expected: ").put(str);
                    }
                    return multiple;
                } catch (NumericException ignored) {
                }
            }
        }
        return 1;
    }

    public static char getStrideUnit(CharSequence str, int position) throws SqlException {
        assert str.length() > 0;
        final char unit = str.charAt(str.length() - 1);
        switch (unit) {
            case 'M':
            case 'y':
            case 'w':
            case 'd':
            case 'h':
            case 'm':
            case 's':
            case 'T':
            case 'U':
            case 'n':
                return unit;
            default:
                throw SqlException.position(position).put("Invalid unit: ").put(str);
        }
    }

    public static int getWeeks(int y) {
        if (Micros.getDayOfTheWeekOfEndOfYear(y) == 4 || Micros.getDayOfTheWeekOfEndOfYear(y - 1) == 3) {
            return 53;
        }
        return 52;
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
        try {
            return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : Math.multiplyExact(micros, Micros.MICRO_NANOS);
        } catch (ArithmeticException e) {
            throw ImplicitCastException.inconvertibleValue(micros, ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO);
        }
    }

    public static long nanosToMicros(long nanos) {
        return nanos == Numbers.LONG_NULL ? Numbers.LONG_NULL : nanos / 1000L;
    }

    public static int parseSign(char c) throws NumericException {
        int tzSign;
        switch (c) {
            case '+':
                tzSign = -1;
                break;
            case '-':
                tzSign = 1;
                break;
            default:
                throw NumericException.instance();
        }
        return tzSign;
    }

    public static int tenPow(int i) throws NumericException {
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
                throw NumericException.instance();
        }
    }

    /**
     * Returns a duration value in TTL format: if positive, it's in hours; if negative, it's in months (and
     * the actual value is positive)
     *
     * @param value           the number of units, must be a non-negative number
     * @param partitionByUnit the time unit, one of `PartitionBy` constants
     * @param tokenPos        the position of the number token in the SQL string
     * @return the TTL value as described
     * @throws SqlException if the passed value is out of range
     */
    public static int toHoursOrMonths(int value, int partitionByUnit, int tokenPos) throws SqlException {
        if (value < 0) {
            throw new AssertionError("The value must be non-negative");
        }
        if (value == 0) {
            return 0;
        }
        switch (partitionByUnit) {
            case PartitionBy.HOUR:
                return value;
            case PartitionBy.DAY:
                int maxDays = Integer.MAX_VALUE / DAY_HOURS;
                if (value > maxDays) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" days. Max value: ").put(maxDays).put(" days");
                }
                return DAY_HOURS * value;
            case PartitionBy.WEEK:
                int maxWeeks = Integer.MAX_VALUE / Micros.WEEK_DAYS / DAY_HOURS;
                if (value > maxWeeks) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" weeks. Max value: ").put(maxWeeks).put(" weeks");
                }
                return Micros.WEEK_DAYS * DAY_HOURS * value;
            case PartitionBy.MONTH:
                return -value;
            case PartitionBy.YEAR:
                int maxYears = Integer.MAX_VALUE / YEAR_MONTHS;
                if (value > maxYears) {
                    throw SqlException.$(tokenPos, "value out of range: ")
                            .put(value).put(" years. Max value: ").put(maxYears).put(" years");
                }
                return -(YEAR_MONTHS * value);
            default:
                throw new AssertionError("invalid value for partitionByUnit: " + partitionByUnit);
        }
    }

    @FunctionalInterface
    public interface TimestampUnitConverter {
        long convert(long timestamp);
    }
}
