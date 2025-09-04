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

package io.questdb.std.datetime.nanotime;

import io.questdb.griffin.SqlException;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.FixedTimeZoneRule;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_NANOS;
import static io.questdb.std.datetime.microtime.Micros.*;

public final class Nanos {
    public static final long DAY_NANOS = 86_400_000_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long AVG_YEAR_NANOS = (long) (365.2425 * DAY_NANOS);
    public static final long HOUR_NANOS = 3_600_000_000_000L;
    public static final long MICRO_NANOS = 1000;
    public static final long MILLI_NANOS = 1_000_000;
    public static final long MINUTE_NANOS = 60_000_000_000L;
    public static final long MONTH_NANOS_APPROX = 30 * DAY_NANOS;
    public static final long SECOND_NANOS = 1_000_000_000;
    public static final long WEEK_NANOS = 7 * DAY_NANOS;
    public static final long YEAR_NANOS_NONLEAP = 365 * DAY_NANOS;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long YEAR_NANOS_LEAP = 366 * DAY_NANOS;
    public static final int EPOCH_YEAR_0 = getYear(0);

    private Nanos() {
    }

    public static long addDays(long nanos, int days) {
        return nanos + days * DAY_NANOS;
    }

    public static long addHours(long nanos, int hours) {
        return nanos + hours * HOUR_NANOS;
    }

    public static long addMicros(long nanos, int moreMicros) {
        return nanos + moreMicros * MICRO_NANOS;
    }

    public static long addMillis(long nanos, int millis) {
        return nanos + millis * MILLI_NANOS;
    }

    public static long addMinutes(long nanos, int minutes) {
        return nanos + minutes * MINUTE_NANOS;
    }

    public static long addMonths(long nanos, int months) {
        if (months == 0) {
            return nanos;
        }
        int y = Nanos.getYear(nanos);
        boolean l = Nanos.isLeapYear(y);
        int m = getMonthOfYear(nanos, y, l);
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
        int _d = getDayOfMonth(nanos, y, m, l);
        int maxDay = CommonUtils.getDaysPerMonth(_m, isLeapYear(_y));
        if (_d > maxDay) {
            _d = maxDay;
        }
        return toNanos(_y, _m, _d) + getTimeNanos(nanos);
    }

    public static long addNanos(long nanos, int moreNanos) {
        return nanos + moreNanos;
    }

    public static long addPeriod(long nanos, char type, int period) {
        switch (type) {
            case 'u':
                return Nanos.addMicros(nanos, period);
            case 'T':
                return Nanos.addMillis(nanos, period);
            case 's':
                return Nanos.addSeconds(nanos, period);
            case 'm':
                return Nanos.addMinutes(nanos, period);
            case 'h':
                return Nanos.addHours(nanos, period);
            case 'd':
                return Nanos.addDays(nanos, period);
            case 'w':
                return Nanos.addWeeks(nanos, period);
            case 'M':
                return Nanos.addMonths(nanos, period);
            case 'y':
                return Nanos.addYears(nanos, period);
            case 'n':
                return Nanos.addNanos(nanos, period);
            default:
                return Numbers.LONG_NULL;
        }
    }

    public static long addSeconds(long nanos, int seconds) {
        return nanos + seconds * SECOND_NANOS;
    }

    public static long addWeeks(long nanos, int weeks) {
        return nanos + weeks * WEEK_NANOS;
    }

    public static long addYears(long nanos, int years) {
        if (years == 0) {
            return nanos;
        }

        // Use the nano version of getYear
        int y = getYear(nanos);
        int m;
        boolean leap1 = isLeapYear(y);
        boolean leap2 = isLeapYear(y + years);

        return yearNanos(y + years, leap2)
                + monthOfYearNanos(m = getMonthOfYear(nanos, y, leap1), leap2)
                + (getDayOfMonth(nanos, y, m, leap1) - 1) * DAY_NANOS
                + getTimeNanos(nanos);
    }

    public static long ceilDD(long nanos) {
        int y = getYear(nanos);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(nanos, y, l);
        return yearNanos(y, l)
                + monthOfYearNanos(m, l)
                + (getDayOfMonth(nanos, y, m, l)) * DAY_NANOS;
    }

    public static long ceilHH(long nanos) {
        return floorHH(nanos) + HOUR_NANOS;
    }

    public static long ceilMC(long nanos) {
        return floorMC(nanos) + MICRO_NANOS;
    }

    public static long ceilMI(long nanos) {
        return floorMI(nanos) + MINUTE_NANOS;
    }

    public static long ceilMM(long nanos) {
        int y = getYear(nanos);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(nanos, y, l);
        return yearNanos(y, l)
                + monthOfYearNanos(m, l)
                + (CommonUtils.getDaysPerMonth(m, l)) * DAY_NANOS;
    }

    public static long ceilMS(long nanos) {
        return floorMS(nanos) + MILLI_NANOS;
    }

    public static long ceilNS(long nanos) {
        return nanos;
    }

    public static long ceilSS(long nanos) {
        return floorSS(nanos) + SECOND_NANOS;
    }

    public static long ceilWW(long nanos) {
        return floorWW(nanos) + WEEK_NANOS;
    }

    public static long ceilYYYY(long nanos) {
        int y = getYear(nanos);
        boolean l = isLeapYear(y);
        return yearNanos(y, l)
                + monthOfYearNanos(12, l)
                + (CommonUtils.getDaysPerMonth(11, false) + 1) * DAY_NANOS;
    }

    public static long endOfYear(int year) {
        return toNanos(year, 12, 31, 23, 59) + 59 * SECOND_NANOS + 999_999_999L;
    }

    /**
     * Floor timestamp to the first day of the century and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z will be floored to
     * 2001-01-01T00:00:00.000Z
     *
     * @param nanos timestamp to floor in nanos since epoch
     * @return given timestamp floored to the first day of the century with time set to 00:00:00.000
     */
    public static long floorCentury(long nanos) {
        int year = getYear(nanos);
        int centuryFirstYear = (((year + 99) / 100) * 100) - 99;
        boolean leapYear = isLeapYear(centuryFirstYear);
        return yearNanos(centuryFirstYear, leapYear);
    }

    public static long floorDD(long nanos) {
        return nanos - getTimeNanos(nanos);
    }

    public static long floorDD(long nanos, int stride) {
        return nanos - getTimeNanos(nanos, stride);
    }

    public static long floorDD(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorDD(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getTimeNanos(nanos, stride, offset);
    }

    /**
     * Floor timestamp to Monday of the same week and set time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z (=Tuesday) will be floored to
     * 2007-12-31T00:00:00.000Z (=Monday of the same week)
     *
     * @param nanos timestamp to floor in nanos since epoch
     * @return given timestamp floored to Monday of the same week and time set to 00:00:00.000Z
     */
    public static long floorDOW(long nanos) {
        long l = previousOrSameDayOfWeek(nanos, 1);
        return floorDD(l);
    }

    /**
     * Floor timestamp to the first day of the decade and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-01-01T04:15:11.123Z will be floored to
     * 2000-01-01T00:00:00.000Z
     *
     * @param nanos timestamp to floor in nanos since epoch
     * @return given timestamp floored to the first day of the decade with time set to 00:00:00.000
     */
    public static long floorDecade(long nanos) {
        int year = getYear(nanos);
        int decadeFirstYear = (year / 10) * 10;
        boolean leapYear = isLeapYear(decadeFirstYear);
        return yearNanos(decadeFirstYear, leapYear);
    }

    public static long floorHH(long nanos) {
        return nanos - getRemainderNanos(nanos, HOUR_NANOS);
    }

    public static long floorHH(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride * HOUR_NANOS);
    }

    public static long floorHH(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorHH(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride * HOUR_NANOS);
    }

    public static long floorMC(long nanos) {
        return nanos - getRemainderNanos(nanos, MICRO_NANOS);
    }

    /**
     * Floors timestamp value to the nearest microsecond.
     *
     * @param nanos  the input value to floor
     * @param stride the number of nanos to floor to.
     * @return floored value.
     */
    public static long floorMC(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride * MICRO_NANOS);
    }

    public static long floorMC(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorMC(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride * MICRO_NANOS);
    }

    public static long floorMI(long nanos) {
        return nanos - getRemainderNanos(nanos, MINUTE_NANOS);
    }

    public static long floorMI(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride * MINUTE_NANOS);
    }

    public static long floorMI(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorMI(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride * MINUTE_NANOS);
    }

    public static long floorMM(long nanos) {
        int y = getYear(nanos);
        boolean l = isLeapYear(y);
        return yearNanos(y, l) + monthOfYearNanos(getMonthOfYear(nanos, y, l), l);
    }

    @SuppressWarnings("unused")
    public static long floorMM(long nanos, long offset) {
        return floorMM(nanos, 1, offset);
    }

    public static long floorMM(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorMM(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        final long monthsDiff = getMonthsBetween(nanos, offset);
        final long monthsToAdd = monthsDiff - (monthsDiff % stride);
        return addMonths(offset, (int) monthsToAdd);
    }

    public static long floorMM(long nanos, int stride) {
        final int y0 = getYear(nanos);
        final boolean l0 = CommonUtils.isLeapYear(y0);
        final int m0 = getMonthOfYear(nanos, y0, l0);
        final long mdiff = 12L * (y0 - EPOCH_YEAR_0) + (m0 - 1);

        final long m = Math.floorDiv(mdiff, stride) * stride;
        final int y = EPOCH_YEAR_0 + (int) Math.floorDiv(m, 12);
        final int mm = Math.floorMod(m, 12);
        final boolean l = CommonUtils.isLeapYear(y);
        return yearNanos(y, l) + (mm > 0 ? monthOfYearNanos(mm + 1, l) : 0);
    }

    public static long floorMS(long nanos) {
        return nanos - getRemainderNanos(nanos, MILLI_NANOS);
    }

    public static long floorMS(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride * MILLI_NANOS);
    }

    public static long floorMS(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorMS(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride * MILLI_NANOS);
    }

    /**
     * Floor timestamp to the first day of the millennia and set to time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2108-01-01T04:15:11.123Z will be floored to
     * 2001-01-01T00:00:00.000Z
     *
     * @param nanos timestamp to floor in nanos since epoch
     * @return given timestamp floored to the first day of the millennia with time set to 00:00:00.000
     */
    public static long floorMillennium(long nanos) {
        int year = getYear(nanos);
        int millenniumFirstYear = (((year + 999) / 1000) * 1000) - 999;
        boolean leapYear = isLeapYear(millenniumFirstYear);
        return yearNanos(millenniumFirstYear, leapYear);
    }

    public static long floorNS(long nanos) {
        return nanos;
    }

    public static long floorNS(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride);
    }

    public static long floorNS(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorNS(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride);
    }

    /**
     * Floor timestamp to the first day of the quarter and set time to 00:00:00.000Z
     * <br>
     * Example: Timestamp representing 2008-05-21T04:15:11.123Z will be floored to
     * 2008-04-01T00:00:00.000Z as that's the 1st day of the same quarter as the original date.
     *
     * @param nanos timestamp to floor in nanos since epoch
     * @return given timestamp floored to the first day of the quarter with time set to 00:00:00.000Z
     */
    public static long floorQuarter(long nanos) {
        int year = getYear(nanos);
        boolean leapYear = isLeapYear(year);
        int monthOfYear = getMonthOfYear(nanos, year, leapYear);
        int q = (monthOfYear - 1) / 3;
        int month = (3 * q) + 1;
        return yearNanos(year, leapYear) + monthOfYearNanos(month, leapYear);
    }

    public static long floorSS(long nanos) {
        return nanos - getRemainderNanos(nanos, SECOND_NANOS);
    }

    public static long floorSS(long nanos, int stride) {
        return nanos - getRemainderNanos(nanos, stride * SECOND_NANOS);
    }

    public static long floorSS(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorSS(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        return nanos - getRemainderNanos(nanos - offset, stride * SECOND_NANOS);
    }

    public static long floorWW(long nanos) {
        return floorWW(nanos, 1);
    }

    public static long floorWW(long nanos, int stride) {
        // Epoch 1 Jan 1970 is a Thursday.
        // Shift 3 days to find offset in the week.
        long weekOffset = (nanos + DAY_NANOS * 3) % (stride * WEEK_NANOS);
        if (weekOffset < 0) {
            // Floor value must be always below or equal to the original value.
            // If offset is negative, we need to add stride to it so that the result is
            // Monday before the original value.
            weekOffset += stride * WEEK_NANOS;
        }
        return nanos - weekOffset;
    }

    public static long floorWW(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorWW(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        long numWeeksToAdd = getWeeksBetween(offset, nanos);
        long modulo = numWeeksToAdd % stride;
        if (numWeeksToAdd < 1) {
            return offset;
        } else {
            return addWeeks(offset, (int) (numWeeksToAdd - modulo));
        }
    }

    public static long floorYYYY(long nanos) {
        final int y = getYear(nanos);
        return yearNanos(y, isLeapYear(y));
    }

    public static long floorYYYY(long nanos, int stride) {
        final int y = EPOCH_YEAR_0 + Math.floorDiv(getYear(nanos) - EPOCH_YEAR_0, stride) * stride;
        return yearNanos(y, isLeapYear(y));
    }

    @SuppressWarnings("unused")
    public static long floorYYYY(long nanos, long offset) {
        return floorYYYY(nanos, 1, offset);
    }

    public static long floorYYYY(long nanos, int stride, long offset) {
        if (offset == 0) {
            return floorYYYY(nanos, stride);
        }
        if (nanos < offset) {
            return offset;
        }
        final long yearsDiff = getYearsBetween(nanos, offset);
        final long yearsToAdd = yearsDiff - (yearsDiff % stride);
        return addYears(offset, (int) yearsToAdd);
    }

    public static int getCentury(long nanos) {
        final int year = Nanos.getYear(nanos);
        int century = year / 100;
        if (year > century * 100) {
            century++;
        }
        return century;
    }

    public static int getDayOfMonth(long nanos, int year, int month, boolean leap) {
        long yearNanos = yearNanos(year, leap);
        yearNanos += monthOfYearNanos(month, leap);
        return (int) ((nanos - yearNanos) / DAY_NANOS) + 1;
    }

    public static int getDayOfWeek(long nanos) {
        // 1970-01-01 is Thursday.
        long d;
        if (nanos > -1) {
            d = nanos / DAY_NANOS;
        } else {
            d = (nanos - (DAY_NANOS - 1)) / DAY_NANOS;
            if (d < -3) {
                return 7 + (int) ((d + 4) % 7);
            }
        }
        return 1 + (int) ((d + 3) % 7);
    }

    public static int getDayOfWeekSundayFirst(long nanos) {
        // 1970-01-01 is Thursday.
        long d;
        if (nanos > -1) {
            d = nanos / DAY_NANOS;
        } else {
            d = (nanos - (DAY_NANOS - 1)) / DAY_NANOS;
            if (d < -4) {
                return 7 + (int) ((d + 5) % 7);
            }
        }
        return 1 + (int) ((d + 4) % 7);
    }

    public static int getDayOfYear(long nanos) {
        int year = getYear(nanos);
        boolean leap = isLeapYear(year);
        long yearStart = yearNanos(year, leap);
        return (int) ((nanos - yearStart) / DAY_NANOS) + 1;
    }

    public static long getDaysBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / DAY_NANOS;
    }

    public static int getDecade(long nanos) {
        return getYear(nanos) / 10;
    }

    public static int getDow(long nanos) {
        return getDayOfWeekSundayFirst(nanos) - 1;
    }

    public static int getDoy(long nanos) {
        final int year = getYear(nanos);
        final boolean leap = isLeapYear(year);
        final long yearStart = yearNanos(year, leap);
        return (int) ((nanos - yearStart) / DAY_NANOS) + 1;
    }

    public static long getHoursBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / HOUR_NANOS;
    }

    // Each ISO 8601 week-numbering year begins with the Monday of the week containing the 4th of January,
    // so in early January or late December the ISO year may be different from the Gregorian year.
    // See the getWeek() method for more information.
    public static int getIsoYear(long nanos) {
        int w = (10 + getDoy(nanos) - getDayOfWeek(nanos)) / 7;
        int y = getYear(nanos);
        if (w < 1) {
            return y - 1;
        }

        if (w > CommonUtils.getWeeks(y)) {
            return y + 1;
        }

        return y;
    }

    public static long getMicrosBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / MICRO_NANOS;
    }

    public static int getMicrosOfMilli(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MICRO_NANOS) % MILLI_MICROS);
        } else {
            return (int) (MILLI_MICROS - 1 + (int) (((nanos + 1) / MICRO_NANOS) % MILLI_MICROS));
        }
    }

    public static int getMicrosOfSecond(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MICRO_NANOS) % SECOND_MICROS);
        } else {
            return (int) (SECOND_MICROS - 1 + (int) (((nanos + 1) / MICRO_NANOS) % SECOND_MICROS));
        }
    }

    // Years in the 1900s are in the second millennium. The third millennium started January 1, 2001.
    public static int getMillennium(long nanos) {
        int year = getYear(nanos);
        int millenniumFirstYear = (((year + 999) / 1000) * 1000) - 999;
        return millenniumFirstYear / 1000 + 1;
    }

    public static long getMillisBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / MILLI_NANOS;
    }

    public static int getMillisOfSecond(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MILLI_NANOS) % SECOND_MILLIS);
        } else {
            return SECOND_MILLIS - 1 + (int) (((nanos + 1) / MILLI_NANOS) % SECOND_MILLIS);
        }
    }

    public static long getMinutesBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / MINUTE_NANOS;
    }

    public static int getMonthOfYear(long nanos) {
        final int y = Nanos.getYear(nanos);
        final boolean leap = Nanos.isLeapYear(y);
        return getMonthOfYear(nanos, y, leap);
    }

    /**
     * Calculates month of year from absolute nanos.
     *
     * @param nanos nanos since 1970
     * @param year  year of month
     * @param leap  true if year was leap
     * @return month of year
     */
    // TODO: reuse complex code
    public static int getMonthOfYear(long nanos, int year, boolean leap) {
        int i = (int) (((nanos - yearNanos(year, leap)) / MILLI_NANOS) >> 10);
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

    public static long getMonthsBetween(long nanosA, long nanosB) {
        if (nanosB < nanosA) {
            return getMonthsBetween(nanosB, nanosA);
        }

        int aYear = getYear(nanosA);
        int bYear = getYear(nanosB);
        boolean aLeap = isLeapYear(aYear);
        boolean bLeap = isLeapYear(bYear);
        int aMonth = getMonthOfYear(nanosA, aYear, aLeap);
        int bMonth = getMonthOfYear(nanosB, bYear, bLeap);

        long aResidual = nanosA - yearNanos(aYear, aLeap) - monthOfYearNanos(aMonth, aLeap);
        long bResidual = nanosB - yearNanos(bYear, bLeap) - monthOfYearNanos(bMonth, bLeap);
        long months = 12L * (bYear - aYear) + (bMonth - aMonth);

        if (aResidual > bResidual) {
            return months - 1;
        } else {
            return months;
        }
    }

    public static long getNanosBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB);
    }

    public static int getNanosOfMicros(long nanos) {
        if (nanos > -1) {
            return (int) (nanos % MICRO_NANOS);
        } else {
            return (int) (MICRO_NANOS - 1 + ((nanos + 1) % MICRO_NANOS));
        }
    }

    public static int getNanosOfSecond(long nanos) {
        if (nanos > -1) {
            return (int) (nanos % SECOND_NANOS);
        } else {
            return (int) (SECOND_NANOS - 1 + ((nanos + 1) % SECOND_NANOS));
        }
    }

    public static long getPeriodBetween(char type, long start, long end) {
        switch (type) {
            case 'n':
                return Nanos.getNanosBetween(start, end);
            case 'u':
                return Nanos.getMicrosBetween(start, end);
            case 'T':
                return Nanos.getMillisBetween(start, end);
            case 's':
                return Nanos.getSecondsBetween(start, end);
            case 'm':
                return Nanos.getMinutesBetween(start, end);
            case 'h':
                return Nanos.getHoursBetween(start, end);
            case 'd':
                return Nanos.getDaysBetween(start, end);
            case 'w':
                return Nanos.getWeeksBetween(start, end);
            case 'M':
                return Nanos.getMonthsBetween(start, end);
            case 'y':
                return Nanos.getYearsBetween(start, end);
            default:
                return Numbers.LONG_NULL;
        }
    }

    // The quarter of the year (1–4) that the date is in
    public static int getQuarter(long nanos) {
        final int month = getMonthOfYear(nanos);
        return ((month - 1) / 3) + 1;
    }

    public static long getSecondsBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / SECOND_NANOS;
    }

    public static char getStrideUnit(CharSequence str) throws SqlException {
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
                return unit;
            default:
                throw SqlException.position(-1).put("Invalid unit: ").put(unit);
        }
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
                    RESOLUTION_NANOS
            );
        }
        return new FixedTimeZoneRule(Numbers.decodeLowInt(l) * MINUTE_NANOS);
    }

    public static int getWallHours(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / HOUR_NANOS) % CommonUtils.DAY_HOURS);
        } else {
            return CommonUtils.DAY_HOURS - 1 + (int) (((nanos + 1) / HOUR_NANOS) % CommonUtils.DAY_HOURS);
        }
    }

    public static int getWallMicros(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos % MILLI_NANOS) / MICRO_NANOS);
        } else {
            return (int) (((((nanos % Nanos.SECOND_NANOS) + Nanos.SECOND_NANOS) % Nanos.SECOND_NANOS) / Nanos.MICRO_NANOS) % Nanos.MICRO_NANOS);
        }
    }

    public static int getWallMillis(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MILLI_NANOS) % SECOND_MILLIS);
        } else {
            return SECOND_MILLIS - 1 + (int) (((nanos + 1) / MILLI_NANOS) % SECOND_MILLIS);
        }
    }

    public static int getWallMinutes(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        } else {
            return CommonUtils.HOUR_MINUTES - 1 + (int) (((nanos + 1) / MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        }
    }

    public static int getWallNanos(long nanoe) {
        if (nanoe > -1) {
            return (int) (nanoe % MICRO_NANOS);
        } else {
            return (int) (MICRO_NANOS - 1 + ((nanoe + 1) % MICRO_NANOS));
        }
    }

    public static int getWallSeconds(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / SECOND_NANOS) % CommonUtils.MINUTE_SECONDS);
        } else {
            return (int) (CommonUtils.MINUTE_SECONDS - 1 + (int) (((nanos + 1) / SECOND_NANOS) % CommonUtils.MINUTE_SECONDS));
        }
    }

    // https://en.wikipedia.org/wiki/ISO_week_date
    public static int getWeek(long nanos) {
        int w = (10 + getDoy(nanos) - getDayOfWeek(nanos)) / 7;
        int y = getYear(nanos);
        if (w < 1) {
            return CommonUtils.getWeeks(y - 1);
        }

        if (w > CommonUtils.getWeeks(y)) {
            return 1;
        }

        return w;
    }

    public static int getWeekOfMonth(long nanos) {
        int year = getYear(nanos);
        boolean leap = isLeapYear(year);
        return getDayOfMonth(nanos, year, getMonthOfYear(nanos, year, leap), leap) / 7 + 1;
    }

    public static int getWeekOfYear(long nanos) {
        return getDayOfYear(nanos) / 7 + 1;
    }

    public static long getWeeksBetween(long nanosA, long nanosB) {
        return Math.abs(nanosA - nanosB) / WEEK_NANOS;
    }

    /**
     * Calculates year number from nanos.
     *
     * @param nanos time nanos.
     * @return year
     */
    public static int getYear(long nanos) {
        // Initial year estimate relative to 1970
        // Use a reasonable approximation of days per year to avoid overflow
        // 365.25 days per year approximation
        int yearsSinceEpoch = (int) (nanos / AVG_YEAR_NANOS);
        int yearEstimate = 1970 + yearsSinceEpoch;

        // Handle negative years appropriately
        if (nanos < 0 && yearEstimate >= 1970) {
            yearEstimate = 1969;
        }

        // Calculate year start
        boolean leap = isLeapYear(yearEstimate);
        long yearStart = yearNanos(yearEstimate, leap);

        // Check if we need to adjust
        long diff = nanos - yearStart;

        if (diff < 0) {
            // We're in the previous year
            yearEstimate--;
        } else {
            // Check if we're in the next year
            long yearLength = leap ? YEAR_NANOS_LEAP : YEAR_NANOS_NONLEAP;
            if (diff >= yearLength) {
                yearEstimate++;
            }
        }

        return yearEstimate;
    }

    public static long getYearsBetween(long nanosA, long nanosB) {
        return getMonthsBetween(nanosA, nanosB) / 12;
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

    public static long monthOfYearNanos(int month, boolean leap) {
        return monthOfYearMicros(month, leap) * MICRO_NANOS;
    }

    public static long nextOrSameDayOfWeek(long nanos, int dow) {
        int thisDow = getDayOfWeek(nanos);
        if (thisDow == dow) {
            return nanos;
        }

        if (thisDow < dow) {
            return nanos + (dow - thisDow) * DAY_NANOS;
        } else {
            return nanos + (7 - (thisDow - dow)) * DAY_NANOS;
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

        return Numbers.encodeLowHighInts(negative ? val : -val, len);
    }

    public static long previousOrSameDayOfWeek(long nanos, int dow) {
        int thisDow = getDayOfWeek(nanos);
        if (thisDow == dow) {
            return nanos;
        }

        if (thisDow < dow) {
            return nanos - (7 + (thisDow - dow)) * DAY_NANOS;
        } else {
            return nanos - (thisDow - dow) * DAY_NANOS;
        }
    }

    /**
     * Convert a timestamp in arbitrary units to nanoseconds.
     *
     * @param value timestamp value
     * @param unit  timestamp unit
     * @return timestamp in nanos
     */
    public static long toNanos(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value;
            case MICROS:
                return value * MICRO_NANOS;
            case MILLIS:
                return value * MILLI_NANOS;
            case SECONDS:
                return value * SECOND_NANOS;
            default:
                Duration duration = unit.getDuration();
                long nanos = duration.getSeconds() * SECOND_NANOS;
                nanos += duration.getNano();
                return nanos * value;
        }
    }

    public static long toNanos(int y, int m, int d, int h, int mi) {
        return toNanos(y, isLeapYear(y), m, d, h, mi);
    }

    public static long toNanos(int y, boolean leap, int m, int d, int h, int mi) {
        return yearNanos(y, leap) + monthOfYearNanos(m, leap) + (d - 1) * DAY_NANOS + h * HOUR_NANOS + mi * MINUTE_NANOS;
    }

    public static long toNanos(
            int y,
            boolean leap,
            int day,
            int month,
            int hour,
            int min,
            int sec,
            int millis,
            int micros,
            int nanos
    ) {
        int maxDay = Math.min(day, CommonUtils.getDaysPerMonth(month, leap)) - 1;
        return yearNanos(y, leap)
                + monthOfYearNanos(month, leap)
                + (long) maxDay * DAY_NANOS
                + (long) hour * HOUR_NANOS
                + (long) min * MINUTE_NANOS
                + (long) sec * SECOND_NANOS
                + (long) millis * MILLI_NANOS
                + (long) micros * MICRO_NANOS
                + nanos;
    }

    public static long toNanos(int y, int m, int d) {
        boolean l = isLeapYear(y);
        return yearNanos(y, l) + monthOfYearNanos(m, l) + (d - 1) * DAY_NANOS;
    }

    public static String toString(long nanos) {
        Utf16Sink sink = Misc.getThreadLocalSink();
        NanosFormatUtils.appendDateTime(sink, nanos);
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
                    RESOLUTION_NANOS
            ).getOffset(utc);
        }
        offset = Numbers.decodeLowInt(l) * MINUTE_NANOS;
        return utc + offset;
    }

    public static String toUSecString(long micros) {
        Utf16Sink sink = Misc.getThreadLocalSink();
        NanosFormatUtils.appendDateTimeUSec(sink, micros);
        return sink.toString();
    }

    public static long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return localTimestamp - zoneRules.getLocalOffset(localTimestamp);
    }

    public static long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return toUTC(localTimestamp, locale, timezone, 0, timezone.length());
    }

    public static long toUTC(
            long tzNanos,
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
                    RESOLUTION_NANOS
            );
            offset = zoneRules.getLocalOffset(tzNanos);
            return tzNanos - offset;
        }
        offset = Numbers.decodeLowInt(l) * MINUTE_NANOS;
        return tzNanos - offset;
    }

    public static long yearNanos(int year, boolean leap) {
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
        long nanos = days * DAY_NANOS;
        if (days < 0 & nanos > 0) {
            return Long.MIN_VALUE;
        }
        return nanos;
    }

    private static long getRemainderNanos(long nanos, long interval) {
        final long rem = nanos % interval;
        return rem < 0 ? interval + rem : rem;
    }

    private static long getTimeNanos(long nanos) {
        return getRemainderNanos(nanos, DAY_NANOS);
    }

    private static long getTimeNanos(long nanos, int strideDays) {
        return getRemainderNanos(nanos, strideDays * DAY_NANOS);
    }

    private static long getTimeNanos(long nanos, int strideDays, long offset) {
        return getRemainderNanos(nanos - offset, strideDays * DAY_NANOS);
    }
}
