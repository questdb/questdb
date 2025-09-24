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

package io.questdb.std.datetime.microtime;

import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.FixedTimeZoneRule;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.datetime.CommonUtils.DAYS_PER_MONTH;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public final class Micros {
    public static final long DAY_MICROS = 86_400_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long AVG_YEAR_MICROS = (long) (365.2425 * DAY_MICROS);
    public static final long DAY_SECONDS = 86400;
    public static final long FIRST_CENTURY_MICROS = -62135596800000000L;
    public static final long HOUR_MICROS = 3600000000L;
    public static final long HOUR_SECONDS = 3600;
    public static final long MICRO_NANOS = 1000;
    public static final long MILLI_MICROS = 1000;
    public static final long MINUTE_MICROS = 60000000;
    public static final long MINUTE_SECONDS = 60;
    public static final long MONTH_MICROS_APPROX = 30 * DAY_MICROS;
    public static final long SECOND_MICROS = 1000000;
    public static final int SECOND_MILLIS = 1000;
    public static final long SECOND_NANOS = 1000000000;
    public static final long STARTUP_TIMESTAMP;
    public static final int WEEK_DAYS = 7;
    public static final long WEEK_MICROS = 7 * DAY_MICROS;
    public static final long YEAR_10000 = 253_402_300_800_000_000L;
    public static final long YEAR_MICROS_NONLEAP = 365 * DAY_MICROS;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long[] MAX_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long[] MIN_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long YEAR_MICROS_LEAP = 366 * DAY_MICROS;
    public static final int EPOCH_YEAR_0 = getYear(0);

    private Micros() {
    }

    public static long addDays(long micros, int days) {
        return micros + days * DAY_MICROS;
    }

    public static long addHours(long micros, int hours) {
        return micros + hours * HOUR_MICROS;
    }

    public static long addMicros(long micros, int moreMicros) {
        return micros + moreMicros;
    }

    public static long addMillis(long micros, int millis) {
        return micros + millis * MILLI_MICROS;
    }

    public static long addMinutes(long micros, int minutes) {
        return micros + minutes * MINUTE_MICROS;
    }

    public static long addMonths(long micros, int months) {
        if (months == 0) {
            return micros;
        }
        int y = Micros.getYear(micros);
        boolean l = CommonUtils.isLeapYear(y);
        int m = getMonthOfYear(micros, y, l);
        int _y;
        int _m = m - 1 + months;
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
        int _d = getDayOfMonth(micros, y, m, l);
        int maxDay = CommonUtils.getDaysPerMonth(_m, CommonUtils.isLeapYear(_y));
        if (_d > maxDay) {
            _d = maxDay;
        }
        return toMicros(_y, _m, _d) + getTimeMicros(micros);
    }

    public static long addNanos(long micros, int nanos) {
        return micros + nanos / 1000L;
    }

    public static long addPeriod(long lo, char type, int period) {
        switch (type) {
            case 'u':
                return Micros.addMicros(lo, period);
            case 'T':
                return Micros.addMillis(lo, period);
            case 's':
                return Micros.addSeconds(lo, period);
            case 'm':
                return Micros.addMinutes(lo, period);
            case 'h':
                return Micros.addHours(lo, period);
            case 'd':
                return Micros.addDays(lo, period);
            case 'w':
                return Micros.addWeeks(lo, period);
            case 'M':
                return Micros.addMonths(lo, period);
            case 'y':
                return Micros.addYears(lo, period);
            case 'n':
                return Micros.addNanos(lo, period);
            default:
                return Numbers.LONG_NULL;
        }
    }

    public static long addSeconds(long micros, int seconds) {
        return micros + seconds * SECOND_MICROS;
    }

    public static long addWeeks(long micros, int weeks) {
        return micros + weeks * WEEK_MICROS;
    }

    public static long addYears(long micros, int years) {
        if (years == 0) {
            return micros;
        }

        int y = getYear(micros);
        int m;
        boolean leap1 = CommonUtils.isLeapYear(y);
        boolean leap2 = CommonUtils.isLeapYear(y + years);

        return yearMicros(y + years, leap2)
                + monthOfYearMicros(m = getMonthOfYear(micros, y, leap1), leap2)
                + (getDayOfMonth(micros, y, m, leap1) - 1) * DAY_MICROS
                + getTimeMicros(micros);
    }

    public static long ceilDD(long micros) {
        int y = getYear(micros);
        boolean l = CommonUtils.isLeapYear(y);
        int m = getMonthOfYear(micros, y, l);
        return yearMicros(y, l)
                + monthOfYearMicros(m, l)
                + (getDayOfMonth(micros, y, m, l)) * DAY_MICROS;
    }

    public static long ceilHH(long micros) {
        return floorHH(micros) + HOUR_MICROS;
    }

    public static long ceilMC(long micros) {
        return micros;
    }

    public static long ceilMI(long micros) {
        return floorMI(micros) + MINUTE_MICROS;
    }

    public static long ceilMM(long micros) {
        int y = getYear(micros);
        boolean l = CommonUtils.isLeapYear(y);
        int m = getMonthOfYear(micros, y, l);
        return yearMicros(y, l)
                + monthOfYearMicros(m, l)
                + (CommonUtils.getDaysPerMonth(m, l)) * DAY_MICROS;
    }

    public static long ceilMS(long micros) {
        return floorMS(micros) + MILLI_MICROS;
    }

    public static long ceilNS(long micros) {
        return micros;
    }

    public static long ceilSS(long micros) {
        return floorSS(micros) + SECOND_MICROS;
    }

    public static long ceilWW(long micros) {
        return floorWW(micros) + WEEK_MICROS;
    }

    public static long ceilYYYY(long micros) {
        int y = getYear(micros);
        boolean l = CommonUtils.isLeapYear(y);
        return yearMicros(y, l)
                + monthOfYearMicros(12, l)
                + (DAYS_PER_MONTH[11]) * DAY_MICROS;
    }

    public static long endOfYear(int year) {
        return toMicros(year, 12, 31, 23, 59) + 59 * SECOND_MILLIS + 999999L;
    }

    /**
     * Floor timestamp to the first day of the century and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z will be floored to
     * 2001-01-01T00:00:00.000Z
     *
     * @param micros timestamp to floor in microseconds since epoch
     * @return given timestamp floored to the first day of the century with time set to 00:00:00.000
     */
    public static long floorCentury(long micros) {
        int year = getYear(micros);
        int centuryFirstYear = (((year + 99) / 100) * 100) - 99;
        boolean leapYear = CommonUtils.isLeapYear(centuryFirstYear);
        return yearMicros(centuryFirstYear, leapYear);
    }

    public static long floorDD(long micros) {
        return micros - getTimeMicros(micros);
    }

    public static long floorDD(long micros, int stride) {
        return micros - getTimeMicros(micros, stride);
    }

    public static long floorDD(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorDD(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getTimeMicros(micros, stride, offset);
    }

    /**
     * Floor timestamp to Monday of the same week and set time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z (=Tuesday) will be floored to
     * 2007-12-31T00:00:00.000Z (=Monday of the same week)
     *
     * @param micros timestamp to floor in microseconds since epoch
     * @return given timestamp floored to Monday of the same week and time set to 00:00:00.000Z
     */
    public static long floorDOW(long micros) {
        long l = previousOrSameDayOfWeek(micros, 1);
        return floorDD(l);
    }

    /**
     * Floor timestamp to the first day of the decade and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z will be floored to
     * 2000-01-01T00:00:00.000Z
     *
     * @param micros timestamp to floor in microseconds since epoch
     * @return given timestamp floored to the first day of the decade with time set to 00:00:00.000
     */
    public static long floorDecade(long micros) {
        int year = getYear(micros);
        int decadeFirstYear = (year / 10) * 10;
        boolean leapYear = CommonUtils.isLeapYear(decadeFirstYear);
        return yearMicros(decadeFirstYear, leapYear);
    }

    public static long floorHH(long micros) {
        return micros - getRemainderMicros(micros, HOUR_MICROS);
    }

    public static long floorHH(long micros, int stride) {
        return micros - getRemainderMicros(micros, stride * HOUR_MICROS);
    }

    public static long floorHH(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorHH(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getRemainderMicros(micros - offset, stride * HOUR_MICROS);
    }

    public static long floorMC(long micros) {
        return micros;
    }

    /**
     * Floors timestamp value to the nearest microsecond.
     *
     * @param micros the input value to floor
     * @param stride the number of microseconds to floor to.
     * @return floored value.
     */
    public static long floorMC(long micros, int stride) {
        return micros - getRemainderMicros(micros, stride);
    }

    public static long floorMC(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorMC(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getRemainderMicros(micros - offset, stride);
    }

    public static long floorMI(long micros) {
        return micros - getRemainderMicros(micros, MINUTE_MICROS);
    }

    public static long floorMI(long micros, int stride) {
        return micros - getRemainderMicros(micros, stride * MINUTE_MICROS);
    }

    public static long floorMI(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorMI(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getRemainderMicros(micros - offset, stride * MINUTE_MICROS);
    }

    public static long floorMM(long micros) {
        int y = getYear(micros);
        boolean l = CommonUtils.isLeapYear(y);
        return yearMicros(y, l) + monthOfYearMicros(getMonthOfYear(micros, y, l), l);
    }

    @SuppressWarnings("unused")
    public static long floorMM(long micros, long offset) {
        return floorMM(micros, 1, offset);
    }

    public static long floorMM(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorMM(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        final long monthsDiff = getMonthsBetween(micros, offset);
        final long monthsToAdd = monthsDiff - (monthsDiff % stride);
        return addMonths(offset, (int) monthsToAdd);
    }

    public static long floorMM(long micros, int stride) {
        final int y0 = getYear(micros);
        final boolean l0 = CommonUtils.isLeapYear(y0);
        final int m0 = getMonthOfYear(micros, y0, l0);
        final long mdiff = 12L * (y0 - EPOCH_YEAR_0) + (m0 - 1);

        final long m = Math.floorDiv(mdiff, stride) * stride;
        final int y = EPOCH_YEAR_0 + (int) Math.floorDiv(m, 12);
        final int mm = Math.floorMod(m, 12);
        final boolean l = CommonUtils.isLeapYear(y);
        return yearMicros(y, l) + (mm > 0 ? monthOfYearMicros(mm + 1, l) : 0);
    }

    public static long floorMS(long micros) {
        return micros - getRemainderMicros(micros, MILLI_MICROS);
    }

    public static long floorMS(long micros, int stride) {
        return micros - getRemainderMicros(micros, stride * MILLI_MICROS);
    }

    public static long floorMS(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorMS(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getRemainderMicros(micros - offset, stride * MILLI_MICROS);
    }

    /**
     * Floor timestamp to the first day of the millennia and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2108-01-01T04:15:11.123Z will be floored to
     * 2001-01-01T00:00:00.000Z
     *
     * @param micros timestamp to floor in microseconds since epoch
     * @return given timestamp floored to the first day of the millennia with time set to 00:00:00.000
     */
    public static long floorMillennium(long micros) {
        int year = getYear(micros);
        int millenniumFirstYear = (((year + 999) / 1000) * 1000) - 999;
        boolean leapYear = CommonUtils.isLeapYear(millenniumFirstYear);
        return yearMicros(millenniumFirstYear, leapYear);
    }

    public static long floorNS(long micros) {
        return micros;
    }

    public static long floorNS(long micros, int stride) {
        final long nanos = micros * MICRO_NANOS;
        return (nanos - getRemainderMicros(nanos, stride)) / MICRO_NANOS;
    }

    public static long floorNS(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorNS(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        final long nanos = micros * MICRO_NANOS;
        return (nanos - getRemainderMicros(nanos - offset, stride)) / MICRO_NANOS;
    }

    /**
     * Floor timestamp to the first day of the quarter and set time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-05-21T04:15:11.123Z will be floored to
     * 2008-04-01T00:00:00.000Z as that's the 1st day of the same quarter as the original date.
     *
     * @param micros timestamp to floor in microseconds since epoch
     * @return given timestamp floored to the first day of the quarter with time set to 00:00:00.000Z
     */
    public static long floorQuarter(long micros) {
        int year = getYear(micros);
        boolean leapYear = CommonUtils.isLeapYear(year);
        int monthOfYear = getMonthOfYear(micros, year, leapYear);
        int q = (monthOfYear - 1) / 3;
        int month = (3 * q) + 1;
        return yearMicros(year, leapYear) + monthOfYearMicros(month, leapYear);
    }

    public static long floorSS(long micros) {
        return micros - getRemainderMicros(micros, SECOND_MICROS);
    }

    public static long floorSS(long micros, int stride) {
        return micros - getRemainderMicros(micros, stride * SECOND_MICROS);
    }

    public static long floorSS(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorSS(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        return micros - getRemainderMicros(micros - offset, stride * SECOND_MICROS);
    }

    public static long floorWW(long micros) {
        return floorWW(micros, 1);
    }

    public static long floorWW(long micros, int stride) {
        // Epoch 1 Jan 1970 is a Thursday.
        // Shift 3 days to find offset in the week.
        long weekOffset = (micros + 3 * Micros.DAY_MICROS) % (stride * WEEK_MICROS);
        if (weekOffset < 0) {
            // Floor value must be always below or equal to the original value.
            // If offset is negative, we need to add stride to it so that the result is
            // Monday before the original value.
            weekOffset += stride * WEEK_MICROS;
        }
        return micros - weekOffset;
    }

    public static long floorWW(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorWW(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        long numWeeksToAdd = getWeeksBetween(offset, micros);
        long modulo = numWeeksToAdd % stride;
        if (numWeeksToAdd < 1) {
            return offset;
        } else {
            return addWeeks(offset, (int) (numWeeksToAdd - modulo));
        }
    }

    public static long floorYYYY(long micros) {
        final int y = getYear(micros);
        return yearMicros(y, CommonUtils.isLeapYear(y));
    }

    public static long floorYYYY(long micros, int stride) {
        final int y = EPOCH_YEAR_0 + Math.floorDiv(getYear(micros) - EPOCH_YEAR_0, stride) * stride;
        return yearMicros(y, CommonUtils.isLeapYear(y));
    }

    @SuppressWarnings("unused")
    public static long floorYYYY(long micros, long offset) {
        return floorYYYY(micros, 1, offset);
    }

    public static long floorYYYY(long micros, int stride, long offset) {
        if (offset == 0) {
            return floorYYYY(micros, stride);
        }
        if (micros < offset) {
            return offset;
        }
        final long yearsDiff = getYearsBetween(micros, offset);
        final long yearsToAdd = yearsDiff - (yearsDiff % stride);
        return addYears(offset, (int) yearsToAdd);
    }

    public static int getCentury(long micros) {
        final int year = getYear(micros);
        int century = year / 100;

        if (year > century * 100) {
            century++;
        }

        if (micros >= Micros.FIRST_CENTURY_MICROS) {
            return century;
        }
        return century - 1;
    }

    public static int getDayOfMonth(long micros, int year, int month, boolean leap) {
        long yearMicros = yearMicros(year, leap);
        yearMicros += monthOfYearMicros(month, leap);
        return (int) ((micros - yearMicros) / DAY_MICROS) + 1;
    }

    public static int getDayOfTheWeekOfEndOfYear(int year) {
        return (year + Math.abs(year / 4) - Math.abs(year / 100) + Math.abs(year / 400)) % 7;
    }

    public static int getDayOfWeek(long micros) {
        // 1970-01-01 is Thursday.
        long d;
        if (micros > -1) {
            d = micros / DAY_MICROS;
        } else {
            d = (micros - (DAY_MICROS - 1)) / DAY_MICROS;
            if (d < -3) {
                return 7 + (int) ((d + 4) % 7);
            }
        }
        return 1 + (int) ((d + 3) % 7);
    }

    public static int getDayOfWeekSundayFirst(long micros) {
        // 1970-01-01 is Thursday.
        long d;
        if (micros > -1) {
            d = micros / DAY_MICROS;
        } else {
            d = (micros - (DAY_MICROS - 1)) / DAY_MICROS;
            if (d < -4) {
                return 7 + (int) ((d + 5) % 7);
            }
        }
        return 1 + (int) ((d + 4) % 7);
    }

    public static int getDayOfYear(long micros) {
        int year = getYear(micros);
        boolean leap = CommonUtils.isLeapYear(year);
        long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static long getDaysBetween(long a, long b) {
        return Math.abs(a - b) / DAY_MICROS;
    }

    public static int getDecade(long micros) {
        return getYear(micros) / 10;
    }

    public static int getDow(long micros) {
        return getDayOfWeekSundayFirst(micros) - 1;
    }

    public static int getDoy(long micros) {
        final int year = getYear(micros);
        final boolean leap = CommonUtils.isLeapYear(year);
        final long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static int getHourOfDay(long micros) {
        if (micros > -1) {
            return (int) ((micros / HOUR_MICROS) % CommonUtils.DAY_HOURS);
        } else {
            return CommonUtils.DAY_HOURS - 1 + (int) (((micros + 1) / HOUR_MICROS) % CommonUtils.DAY_HOURS);
        }
    }

    public static long getHoursBetween(long a, long b) {
        return Math.abs(a - b) / HOUR_MICROS;
    }

    // Each ISO 8601 week-numbering year begins with the Monday of the week containing the 4th of January,
    // so in early January or late December the ISO year may be different from the Gregorian year.
    // See the getWeek() method for more information.
    public static int getIsoYear(long micros) {
        int w = (10 + getDoy(micros) - getDayOfWeek(micros)) / 7;
        int y = getYear(micros);
        if (w < 1) {
            return y - 1;
        }

        if (w > CommonUtils.getWeeks(y)) {
            return y + 1;
        }

        return y;
    }

    public static long getMicrosBetween(long a, long b) {
        return Math.abs(a - b);
    }

    public static int getMicrosOfMilli(long micros) {
        if (micros > -1) {
            return (int) (micros % MILLI_MICROS);
        } else {
            return (int) (MILLI_MICROS - 1 + ((micros + 1) % MILLI_MICROS));
        }
    }

    public static int getMicrosOfSecond(long micros) {
        if (micros > -1) {
            return (int) (micros % SECOND_MICROS);
        } else {
            return (int) (SECOND_MICROS - 1 + ((micros + 1) % SECOND_MICROS));
        }
    }

    // Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.
    public static int getMillennium(long micros) {
        int year = getYear(micros);
        int millenniumFirstYear = (((year + 999) / 1000) * 1000) - 999;
        return millenniumFirstYear / 1000 + 1;
    }

    public static long getMillisBetween(long a, long b) {
        return Math.abs(a - b) / MILLI_MICROS;
    }

    public static int getMillisOfSecond(long micros) {
        if (micros > -1) {
            return (int) ((micros / MILLI_MICROS) % SECOND_MILLIS);
        } else {
            return SECOND_MILLIS - 1 + (int) (((micros + 1) / MILLI_MICROS) % SECOND_MILLIS);
        }
    }

    public static int getMinuteOfHour(long micros) {
        if (micros > -1) {
            return (int) ((micros / MINUTE_MICROS) % CommonUtils.HOUR_MINUTES);
        } else {
            return CommonUtils.HOUR_MINUTES - 1 + (int) (((micros + 1) / MINUTE_MICROS) % CommonUtils.HOUR_MINUTES);
        }
    }

    public static long getMinutesBetween(long a, long b) {
        return Math.abs(a - b) / MINUTE_MICROS;
    }

    public static int getMonthOfYear(long micros) {
        final int y = Micros.getYear(micros);
        final boolean leap = CommonUtils.isLeapYear(y);
        return getMonthOfYear(micros, y, leap);
    }

    /**
     * Calculates month of year from absolute micros.
     *
     * @param micros micros since 1970
     * @param year   year of month
     * @param leap   true if year was leap
     * @return month of year
     */
    public static int getMonthOfYear(long micros, int year, boolean leap) {
        int i = (int) (((micros - yearMicros(year, leap)) / 1000) >> 10);
        return leap
                ? ((i < 182 * 84375)
                ? ((i < 91 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 60 * 84375) ? 2 : 3)
                : ((i < 121 * 84375) ? 4 : (i < 152 * 84375) ? 5 : 6))
                : ((i < 274 * 84375)
                ? ((i < 213 * 84375) ? 7 : (i < 244 * 84375) ? 8 : 9)
                : ((i < 305 * 84375) ? 10 : (i < 335 * 84375) ? 11 : 12)))
                : ((i < 181 * 84375)
                ? ((i < 90 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 59 * 84375) ? 2 : 3)
                : ((i < 120 * 84375) ? 4 : (i < 151 * 84375) ? 5 : 6))
                : ((i < 273 * 84375)
                ? ((i < 212 * 84375) ? 7 : (i < 243 * 84375) ? 8 : 9)
                : ((i < 304 * 84375) ? 10 : (i < 334 * 84375) ? 11 : 12)));
    }

    public static long getMonthsBetween(long a, long b) {
        if (b < a) {
            return getMonthsBetween(b, a);
        }

        int aYear = getYear(a);
        int bYear = getYear(b);
        boolean aLeap = CommonUtils.isLeapYear(aYear);
        boolean bLeap = CommonUtils.isLeapYear(bYear);
        int aMonth = getMonthOfYear(a, aYear, aLeap);
        int bMonth = getMonthOfYear(b, bYear, bLeap);

        long aResidual = a - yearMicros(aYear, aLeap) - monthOfYearMicros(aMonth, aLeap);
        long bResidual = b - yearMicros(bYear, bLeap) - monthOfYearMicros(bMonth, bLeap);
        long months = 12L * (bYear - aYear) + (bMonth - aMonth);

        if (aResidual > bResidual) {
            return months - 1;
        } else {
            return months;
        }
    }

    public static long getNanosBetween(long a, long b) {
        return Math.abs(a - b) * MICRO_NANOS;
    }

    public static long getPeriodBetween(char type, long start, long end) {
        switch (type) {
            case 'n':
                return Micros.getNanosBetween(start, end);
            case 'u':
                return Micros.getMicrosBetween(start, end);
            case 'T':
                return Micros.getMillisBetween(start, end);
            case 's':
                return Micros.getSecondsBetween(start, end);
            case 'm':
                return Micros.getMinutesBetween(start, end);
            case 'h':
                return Micros.getHoursBetween(start, end);
            case 'd':
                return Micros.getDaysBetween(start, end);
            case 'w':
                return Micros.getWeeksBetween(start, end);
            case 'M':
                return Micros.getMonthsBetween(start, end);
            case 'y':
                return Micros.getYearsBetween(start, end);
            default:
                return Numbers.LONG_NULL;
        }
    }

    // The quarter of the year (1–4) that the date is in
    public static int getQuarter(long micros) {
        final int month = getMonthOfYear(micros);
        return ((month - 1) / 3) + 1;
    }

    public static int getSecondOfMinute(long micros) {
        if (micros > -1) {
            return (int) ((micros / SECOND_MICROS) % MINUTE_SECONDS);
        } else {
            return (int) (MINUTE_SECONDS - 1 + (int) (((micros + 1) / SECOND_MICROS) % MINUTE_SECONDS));
        }
    }

    public static long getSecondsBetween(long a, long b) {
        return Math.abs(a - b) / SECOND_MICROS;
    }

    public static TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone) throws NumericException {
        return getTimezoneRules(locale, timezone, 0, timezone.length());
    }

    public static TimeZoneRules getTimezoneRules(
            DateLocale locale,
            CharSequence timezone,
            int lo,
            int hi
    ) throws NumericException {
        long l = Dates.parseOffset(timezone, lo, hi);
        if (l == Long.MIN_VALUE) {
            return locale.getZoneRules(
                    Numbers.decodeLowInt(locale.matchZone(timezone, lo, hi)),
                    RESOLUTION_MICROS
            );
        }
        return new FixedTimeZoneRule(Numbers.decodeLowInt(l) * MINUTE_MICROS);
    }

    // https://en.wikipedia.org/wiki/ISO_week_date
    public static int getWeek(long micros) {
        int w = (10 + getDoy(micros) - getDayOfWeek(micros)) / 7;
        int y = getYear(micros);
        if (w < 1) {
            return CommonUtils.getWeeks(y - 1);
        }

        if (w > CommonUtils.getWeeks(y)) {
            return 1;
        }

        return w;
    }

    public static int getWeekOfMonth(long micros) {
        int year = getYear(micros);
        boolean leap = CommonUtils.isLeapYear(year);
        return getDayOfMonth(micros, year, getMonthOfYear(micros, year, leap), leap) / 7 + 1;
    }

    public static int getWeekOfYear(long micros) {
        return getDayOfYear(micros) / 7 + 1;
    }

    public static long getWeeksBetween(long a, long b) {
        return Math.abs(a - b) / WEEK_MICROS;
    }

    public static int getYear(long micros) {
        // Initial year estimate relative to 1970
        // Use a reasonable approximation of days per year to avoid overflow
        // 365.25 days per year approximation
        int yearsSinceEpoch = (int) (micros / AVG_YEAR_MICROS);
        int yearEstimate = 1970 + yearsSinceEpoch;

        // Handle negative years appropriately
        if (micros < 0 && yearEstimate >= 1970) {
            yearEstimate = 1969;
        }

        // Calculate year start
        boolean leap = CommonUtils.isLeapYear(yearEstimate);
        long yearStart = yearMicros(yearEstimate, leap);

        // Check if we need to adjust
        long diff = micros - yearStart;

        if (diff < 0) {
            // We're in the previous year
            yearEstimate--;
        } else {
            // Check if we're in the next year
            long yearLength = leap ? YEAR_MICROS_LEAP : YEAR_MICROS_NONLEAP;
            if (diff >= yearLength) {
                yearEstimate++;
            }
        }

        return yearEstimate;
    }

    public static long getYearsBetween(long a, long b) {
        return getMonthsBetween(a, b) / 12;
    }

    public static long monthOfYearMicros(int month, boolean leap) {
        return leap ? MAX_MONTH_OF_YEAR_MICROS[month - 1] : MIN_MONTH_OF_YEAR_MICROS[month - 1];
    }

    public static long nextOrSameDayOfWeek(long millis, int dow) {
        int thisDow = getDayOfWeek(millis);
        if (thisDow == dow) {
            return millis;
        }

        if (thisDow < dow) {
            return millis + (dow - thisDow) * DAY_MICROS;
        } else {
            return millis + (7 - (thisDow - dow)) * DAY_MICROS;
        }
    }

    public static long parseNanosAsMicrosGreedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance();
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || Numbers.notDigit(sequence.charAt(i))) {
            throw NumericException.instance();
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (Numbers.notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance();
            }
            val = r;
        }

        final int len = i - p;

        if (len > 9 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance();
        }

        while (i - p < 9) {
            val *= 10;
            i++;
        }

        val /= 1000;

        return Numbers.encodeLowHighInts(negative ? val : -val, len);
    }

    public static long previousOrSameDayOfWeek(long micros, int dow) {
        int thisDow = getDayOfWeek(micros);
        if (thisDow == dow) {
            return micros;
        }

        if (thisDow < dow) {
            return micros - (7 + (thisDow - dow)) * DAY_MICROS;
        } else {
            return micros - (thisDow - dow) * DAY_MICROS;
        }
    }

    public static long toMicros(int y, int m, int d, int h, int mi) {
        return toMicros(y, CommonUtils.isLeapYear(y), m, d, h, mi);
    }

    public static long toMicros(int y, boolean leap, int m, int d, int h, int mi) {
        return yearMicros(y, leap) + monthOfYearMicros(m, leap) + (d - 1) * DAY_MICROS + h * HOUR_MICROS + mi * MINUTE_MICROS;
    }

    public static long toMicros(
            int y,
            boolean leap,
            int day,
            int month,
            int hour,
            int min,
            int sec,
            int millis,
            int micros
    ) {
        int maxDay = Math.min(day, CommonUtils.getDaysPerMonth(month, leap)) - 1;
        return yearMicros(y, leap)
                + monthOfYearMicros(month, leap)
                + maxDay * DAY_MICROS
                + hour * HOUR_MICROS
                + min * MINUTE_MICROS
                + sec * SECOND_MICROS
                + millis * MILLI_MICROS
                + micros;
    }

    public static long toMicros(int y, int m, int d) {
        boolean l = CommonUtils.isLeapYear(y);
        return yearMicros(y, l) + monthOfYearMicros(m, l) + (d - 1) * DAY_MICROS;
    }

    public static String toString(long micros) {
        Utf16Sink sink = Misc.getThreadLocalSink();
        MicrosFormatUtils.appendDateTime(sink, micros);
        return sink.toString();
    }

    public static long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return toTimezone(utcTimestamp, locale, timezone, 0, timezone.length());
    }

    public static long toTimezone(
            long utc,
            DateLocale locale,
            CharSequence timezone,
            int lo,
            int hi
    ) throws NumericException {
        final long offset;
        long l = Dates.parseOffset(timezone, lo, hi);
        if (l == Long.MIN_VALUE) {
            return utc + locale.getZoneRules(
                    Numbers.decodeLowInt(locale.matchZone(timezone, lo, hi)),
                    RESOLUTION_MICROS
            ).getOffset(utc);
        }
        offset = Numbers.decodeLowInt(l) * MINUTE_MICROS;
        return utc + offset;
    }

    public static String toUSecString(long micros) {
        Utf16Sink sink = Misc.getThreadLocalSink();
        MicrosFormatUtils.appendDateTimeUSec(sink, micros);
        return sink.toString();
    }

    public static long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return toUTC(localTimestamp, locale, timezone, 0, timezone.length());
    }

    public static long toUTC(
            long localTimestamp,
            DateLocale locale,
            CharSequence timezone,
            int lo,
            int hi
    ) throws NumericException {
        long offset;
        long l = Dates.parseOffset(timezone, lo, hi);
        if (l == Long.MIN_VALUE) {
            TimeZoneRules zoneRules = locale.getZoneRules(
                    Numbers.decodeLowInt(locale.matchZone(timezone, lo, hi)),
                    RESOLUTION_MICROS
            );
            offset = zoneRules.getLocalOffset(localTimestamp);
            return localTimestamp - offset;
        }
        offset = Numbers.decodeLowInt(l) * MINUTE_MICROS;
        return localTimestamp - offset;
    }

    public static long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return localTimestamp - zoneRules.getLocalOffset(localTimestamp);
    }

    /**
     * Calculated epoch offset in microseconds of the beginning of the year. For example of year 2008 this is
     * equivalent to parsing "2008-01-01T00:00:00.000Z", except this method is faster.
     *
     * @param year the year
     * @param leap true if given year is leap year
     * @return epoch offset in micros.
     */
    public static long yearMicros(int year, boolean leap) {
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }

        long days = year * 365L + (leapYears - DAYS_0000_TO_1970);
        long micros = days * DAY_MICROS;
        if (days < 0 & micros > 0) {
            return Long.MIN_VALUE;
        }
        return micros;
    }

    private static long getRemainderMicros(long micros, long interval) {
        final long rem = micros % interval;
        return rem < 0 ? interval + rem : rem;
    }

    private static long getTimeMicros(long micros) {
        return getRemainderMicros(micros, DAY_MICROS);
    }

    private static long getTimeMicros(long micros, int strideDays) {
        return getRemainderMicros(micros, strideDays * DAY_MICROS);
    }

    private static long getTimeMicros(long micros, int strideDays, long offset) {
        return getRemainderMicros(micros - offset, strideDays * DAY_MICROS);
    }

    static {
        STARTUP_TIMESTAMP = Os.currentTimeMicros();
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MICROS;
            MIN_MONTH_OF_YEAR_MICROS[i + 1] = minSum;
            maxSum += CommonUtils.getDaysPerMonth(i + 1, true) * DAY_MICROS;
            MAX_MONTH_OF_YEAR_MICROS[i + 1] = maxSum;
        }
    }
}
