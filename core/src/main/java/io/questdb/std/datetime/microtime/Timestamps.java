/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.*;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.CharSink;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public final class Timestamps {

    public static final long DAY_MICROS = 86400000000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long FIRST_CENTURY_MICROS = -62135596800000000L;
    public static final long HOUR_MICROS = 3600000000L;
    public static final long MILLI_MICROS = 1000;
    public static final long MINUTE_MICROS = 60000000;
    public static final long O3_MIN_TS = 0L;
    public static final long SECOND_MICROS = 1000000;
    public static final int SECOND_MILLIS = 1000;
    public static final long STARTUP_TIMESTAMP;
    public static final int STATE_DELIM = 4;
    public static final int STATE_END = 6;
    public static final int STATE_GMT = 2;
    public static final int STATE_HOUR = 3;
    public static final int STATE_INIT = 0;
    public static final int STATE_MINUTE = 5;
    public static final int STATE_SIGN = 7;
    public static final int STATE_UTC = 1;
    public static final long WEEK_MICROS = 604800000000L; // DAY_MICROS * 7
    private static final char AFTER_NINE = '9' + 1;
    private static final long AVG_YEAR_MICROS = (long) (365.2425 * DAY_MICROS);
    private static final long HALF_YEAR_MICROS = AVG_YEAR_MICROS / 2;
    private static final long EPOCH_MICROS = 1970L * AVG_YEAR_MICROS;
    private static final long HALF_EPOCH_MICROS = EPOCH_MICROS / 2;
    private static final char BEFORE_ZERO = '0' - 1;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    private static final int DAY_HOURS = 24;
    private static final int HOUR_MINUTES = 60;
    private static final long LEAP_YEAR_MICROS = 366 * DAY_MICROS;
    private static final long[] MAX_MONTH_OF_YEAR_MICROS = new long[12];
    private static final int MINUTE_SECONDS = 60;
    private static final long[] MIN_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long YEAR_MICROS = 365 * DAY_MICROS;

    private Timestamps() {
    }

    public static long addDays(long micros, int days) {
        return micros + days * DAY_MICROS;
    }

    public static long addHours(long micros, int hours) {
        return micros + hours * HOUR_MICROS;
    }

    public static long addMinutes(long micros, int minutes) {
        return micros + minutes * MINUTE_MICROS;
    }

    public static long addMonths(final long micros, int months) {
        if (months == 0) {
            return micros;
        }
        int y = Timestamps.getYear(micros);
        boolean l = Timestamps.isLeapYear(y);
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
        int maxDay = getDaysPerMonth(_m, isLeapYear(_y));
        if (_d > maxDay) {
            _d = maxDay;
        }
        return toMicros(_y, _m, _d) + getTimeMicros(micros) + (micros < 0 ? 1 : 0);
    }

    public static long addPeriod(long lo, char type, int period) {
        switch (type) {
            case 's':
                return Timestamps.addSeconds(lo, period);
            case 'm':
                return Timestamps.addMinutes(lo, period);
            case 'h':
                return Timestamps.addHours(lo, period);
            case 'd':
                return Timestamps.addDays(lo, period);
            case 'w':
                return Timestamps.addWeeks(lo, period);
            case 'M':
                return Timestamps.addMonths(lo, period);
            case 'y':
                return Timestamps.addYear(lo, period);
            default:
                return Numbers.LONG_NaN;
        }
    }

    public static long addSeconds(long micros, int seconds) {
        return micros + seconds * SECOND_MICROS;
    }

    public static long addWeeks(long micros, int weeks) {
        return micros + weeks * WEEK_MICROS;
    }

    public static long addYear(long micros, int years) {
        if (years == 0) {
            return micros;
        }

        int y = getYear(micros);
        int m;
        boolean leap1 = isLeapYear(y);
        boolean leap2 = isLeapYear(y + years);

        return yearMicros(y + years, leap2)
                + monthOfYearMicros(m = getMonthOfYear(micros, y, leap1), leap2)
                + (getDayOfMonth(micros, y, m, leap1) - 1) * DAY_MICROS
                + getTimeMicros(micros)
                + (micros < 0 ? 1 : 0);

    }

    public static long ceilDD(long micros) {
        int y, m;
        boolean l;
        return yearMicros(y = getYear(micros), l = isLeapYear(y))
                + monthOfYearMicros(m = getMonthOfYear(micros, y, l), l)
                + (getDayOfMonth(micros, y, m, l)) * DAY_MICROS;
    }

    public static long ceilHH(long micros) {
        return floorHH(micros) + HOUR_MICROS;
    }

    public static long ceilMI(long micros) {
        return floorMI(micros) + MINUTE_MICROS;
    }

    public static long ceilMM(long micros) {
        int y, m;
        boolean l;
        return yearMicros(y = getYear(micros), l = isLeapYear(y))
                + monthOfYearMicros(m = getMonthOfYear(micros, y, l), l)
                + (getDaysPerMonth(m, l)) * DAY_MICROS;
    }

    public static long ceilMS(long micros) {
        return floorMS(micros) + MILLI_MICROS;
    }

    public static long ceilSS(long micros) {
        return floorSS(micros) + SECOND_MICROS;
    }

    public static long ceilWW(long micros) {
        return floorWW(micros) + WEEK_MICROS;
    }

    public static long ceilYYYY(long micros) {
        int y;
        boolean l;
        return yearMicros(y = getYear(micros), l = isLeapYear(y))
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
        boolean leapYear = isLeapYear(centuryFirstYear);
        return yearMicros(centuryFirstYear, leapYear);
    }

    public static long floorDD(long micros) {
        return floorDD(micros, 1);
    }

    public static long floorDD(long micros, int stride) {
        long result = micros - getTimeMicros(micros, stride);
        return Math.min(result, micros);
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
        boolean leapYear = isLeapYear(decadeFirstYear);
        return yearMicros(decadeFirstYear, leapYear);
    }

    public static long floorHH(long micros) {
        return floorHH(micros, 1);
    }

    public static long floorHH(long micros, int stride) {
        return micros - micros % (stride * HOUR_MICROS);
    }

    public static long floorMI(long micros) {
        return floorMI(micros, 1);
    }

    public static long floorMI(long micros, int stride) {
        return micros - micros % (stride * MINUTE_MICROS);
    }

    public static long floorMM(long micros) {
        int y;
        boolean l;
        return yearMicros(y = getYear(micros), l = isLeapYear(y)) + monthOfYearMicros(getMonthOfYear(micros, y, l), l);
    }

    public static long floorMM(long micros, int stride) {
        final int origin = getYear(0);
        long m = (getMonthsBetween(0, micros) / stride) * stride;
        int y = (int) (origin + m / 12);
        int mm = (int) (m % 12);
        boolean l = isLeapYear(y);
        return yearMicros(y, l) + (mm > 0 ? monthOfYearMicros(mm, l) : 0);
    }

    public static long floorMS(long micros) {
        return floorMS(micros, 1);
    }

    public static long floorMS(long micros, int stride) {
        return micros - micros % (stride * MILLI_MICROS);
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
        boolean leapYear = isLeapYear(millenniumFirstYear);
        return yearMicros(millenniumFirstYear, leapYear);
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
        boolean leapYear = isLeapYear(year);
        int monthOfYear = getMonthOfYear(micros, year, leapYear);
        int q = (monthOfYear - 1) / 3;
        int month = (3 * q) + 1;
        return yearMicros(year, leapYear) + monthOfYearMicros(month, leapYear);
    }

    public static long floorSS(long micros) {
        return floorSS(micros, 1);
    }

    public static long floorSS(long micros, int stride) {
        return micros - micros % (stride * SECOND_MICROS);
    }

    public static long floorWW(long micros) {
        return floorWW(micros, 1);
    }

    public static long floorWW(long micros, int stride) {
        return (micros - micros % (stride * WEEK_MICROS)) + getIsoWeekMicrosOffset(micros);
    }

    public static long floorYYYY(long micros) {
        int y;
        return yearMicros(y = getYear(micros), isLeapYear(y));
    }

    public static long floorYYYY(long micros, int stride) {
        final int origin = getYear(0);
        int y = origin + ((getYear(micros) - origin) / stride) * stride;
        return yearMicros(y, isLeapYear(y));
    }

    public static int getCentury(long micros) {
        final int year = getYear(micros);
        int century = year / 100;

        if (year > century * 100) {
            century++;
        }

        if (micros >= Timestamps.FIRST_CENTURY_MICROS) {
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
        boolean leap = isLeapYear(year);
        long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static long getDaysBetween(long a, long b) {
        return Math.abs(a - b) / DAY_MICROS;
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

    public static int getDecade(long micros) {
        return getYear(micros) / 10;
    }

    public static int getDow(long micros) {
        return getDayOfWeekSundayFirst(micros) - 1;
    }

    public static int getDoy(long micros) {
        final int year = getYear(micros);
        final boolean leap = isLeapYear(year);
        final long yearStart = yearMicros(year, leap);
        return (int) ((micros - yearStart) / DAY_MICROS) + 1;
    }

    public static int getHourOfDay(long micros) {
        if (micros > -1) {
            return (int) ((micros / HOUR_MICROS) % DAY_HOURS);
        } else {
            return DAY_HOURS - 1 + (int) (((micros + 1) / HOUR_MICROS) % DAY_HOURS);
        }
    }

    public static long getHoursBetween(long a, long b) {
        return Math.abs(a - b) / HOUR_MICROS;
    }

    /**
     * Due to epoch starting on Thursday while ISO week starts on Monday, there is an offset between epoch and ISO week micros when flooring.
     *
     * @param micros epoch microseconds
     * @return 4 days micros offset for Monday through Wednesday and -3 days micros offset for Thursday through Sunday
     */
    public static long getIsoWeekMicrosOffset(long micros) {
        return ((getDayOfWeek(micros) <= 3) ? 4 : -3) * DAY_MICROS;
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

        if (w > getWeeks(y)) {
            return y + 1;
        }

        return y;
    }

    /**
     * Since ISO weeks don't always start on the first day of the year, there is an offset of days from the 1st day of the year.
     *
     * @param year of timestamp
     * @return difference in the days from the start of the year (January 1st) and the first ISO week
     */
    public static int getIsoYearDayOffset(int year) {
        int dayOfTheWeekOfEndOfPreviousYear = getDayOfTheWeekOfEndOfYear(year - 1);
        return ((dayOfTheWeekOfEndOfPreviousYear <= 3) ? 0 : 7) - dayOfTheWeekOfEndOfPreviousYear;
    }

    public static int getMicrosOfMilli(long micros) {
        if (micros > -1) {
            return (int) (micros % MILLI_MICROS);
        } else {
            return (int) (MILLI_MICROS - 1 + ((micros + 1) % MILLI_MICROS));
        }
    }

    public static long getMicrosOfMinute(long micros) {
        if (micros > -1) {
            return micros % MINUTE_MICROS;
        } else {
            return MINUTE_MICROS - 1 + (micros + 1) % MINUTE_MICROS;
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

    public static long getMillisOfMinute(long micros) {
        return getMicrosOfMinute(micros) / 1000;
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
            return (int) ((micros / MINUTE_MICROS) % HOUR_MINUTES);
        } else {
            return HOUR_MINUTES - 1 + (int) (((micros + 1) / MINUTE_MICROS) % HOUR_MINUTES);
        }
    }

    public static long getMinutesBetween(long a, long b) {
        return Math.abs(a - b) / MINUTE_MICROS;
    }

    public static int getMonthOfYear(long micros) {
        final int y = Timestamps.getYear(micros);
        final boolean leap = Timestamps.isLeapYear(y);
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
        boolean aLeap = isLeapYear(aYear);
        boolean bLeap = isLeapYear(bYear);
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

    public static long getPeriodBetween(char type, long start, long end) {
        switch (type) {
            case 's':
                return Timestamps.getSecondsBetween(start, end);
            case 'm':
                return Timestamps.getMinutesBetween(start, end);
            case 'h':
                return Timestamps.getHoursBetween(start, end);
            case 'd':
                return Timestamps.getDaysBetween(start, end);
            case 'w':
                return Timestamps.getWeeksBetween(start, end);
            case 'M':
                return Timestamps.getMonthsBetween(start, end);
            case 'y':
                return Timestamps.getYearsBetween(start, end);
            default:
                return Numbers.LONG_NaN;
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
            return MINUTE_SECONDS - 1 + (int) (((micros + 1) / SECOND_MICROS) % MINUTE_SECONDS);
        }
    }

    public static long getSecondsBetween(long a, long b) {
        return Math.abs(a - b) / SECOND_MICROS;
    }

    // https://en.wikipedia.org/wiki/ISO_week_date
    public static int getWeek(long micros) {
        int w = (10 + getDoy(micros) - getDayOfWeek(micros)) / 7;
        int y = getYear(micros);
        if (w < 1) {
            return getWeeks(y - 1);
        }

        if (w > getWeeks(y)) {
            return 1;
        }

        return w;
    }

    public static int getWeekOfMonth(long micros) {
        int year = getYear(micros);
        boolean leap = isLeapYear(year);
        return getDayOfMonth(micros, year, getMonthOfYear(micros, year, leap), leap) / 7 + 1;
    }

    public static int getWeekOfYear(long micros) {
        return getDayOfYear(micros) / 7 + 1;
    }

    public static int getWeeks(int y) {
        if (getDayOfTheWeekOfEndOfYear(y) == 4 || getDayOfTheWeekOfEndOfYear(y - 1) == 3) {
            return 53;
        }
        return 52;
    }

    public static long getWeeksBetween(long a, long b) {
        return Math.abs(a - b) / WEEK_MICROS;
    }

    /**
     * Calculates year number from micros.
     *
     * @param micros time micros.
     * @return year
     */
    public static int getYear(long micros) {
        long mid = (micros >> 1) + HALF_EPOCH_MICROS;
        if (mid < 0) {
            mid = mid - HALF_YEAR_MICROS + 1;
        }
        int year = (int) (mid / HALF_YEAR_MICROS);

        boolean leap = isLeapYear(year);
        long yearStart = yearMicros(year, leap);
        long diff = micros - yearStart;

        if (diff < 0) {
            year--;
        } else if (diff >= YEAR_MICROS) {
            yearStart += leap ? LEAP_YEAR_MICROS : YEAR_MICROS;
            if (yearStart <= micros) {
                year++;
            }
        }

        return year;
    }

    public static long getYearsBetween(long a, long b) {
        return getMonthsBetween(a, b) / 12;
    }

    /**
     * Calculates if year is leap year using following algorithm:
     * <p>
     * http://en.wikipedia.org/wiki/Leap_year
     *
     * @param year the year
     * @return true if year is leap
     */
    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
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
            throw NumericException.INSTANCE;
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || Numbers.notDigit(sequence.charAt(i))) {
            throw NumericException.INSTANCE;
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
                throw NumericException.INSTANCE;
            }
            val = r;
        }

        final int len = i - p;

        if (len > 9 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.INSTANCE;
        }

        while (i - p < 9) {
            val *= 10;
            i++;
        }

        val /= 1000;

        return Numbers.encodeLowHighInts(negative ? val : -val, len);
    }

    public static long parseOffset(CharSequence in) {
        return parseOffset(in, 0, in.length());
    }

    public static long parseOffset(CharSequence in, int lo, int hi) {
        int p = lo;
        int state = STATE_INIT;
        boolean negative = false;
        int hour = 0;
        int minute = 0;

        try {
            OUT:
            while (p < hi) {
                char c = in.charAt(p);

                switch (state) {
                    case STATE_INIT:
                        switch (c) {
                            case 'U':
                            case 'u':
                                state = STATE_UTC;
                                break;
                            case 'G':
                            case 'g':
                                state = STATE_GMT;
                                break;
                            case 'Z':
                            case 'z':
                                state = STATE_END;
                                break;
                            case '+':
                                state = STATE_HOUR;
                                break;
                            case '-':
                                negative = true;
                                state = STATE_HOUR;
                                break;
                            default:
                                if (isDigit(c)) {
                                    state = STATE_HOUR;
                                    p--;
                                } else {
                                    return Long.MIN_VALUE;
                                }
                                break;
                        }
                        p++;
                        break;
                    case STATE_UTC:
                        if (p > hi - 2 || Chars.noMatch(in, p, p + 2, "tc", 0, 2)) {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_SIGN;
                        p += 2;
                        break;
                    case STATE_GMT:
                        if (p > hi - 2 || Chars.noMatch(in, p, p + 2, "mt", 0, 2)) {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_SIGN;
                        p += 2;
                        break;
                    case STATE_SIGN:
                        switch (c) {
                            case '+':
                                break;
                            case '-':
                                negative = true;
                                break;
                            default:
                                return Long.MIN_VALUE;
                        }
                        p++;
                        state = STATE_HOUR;
                        break;
                    case STATE_HOUR:
                        if (isDigit(c) && p < hi - 1) {
                            hour = Numbers.parseInt(in, p, p + 2);
                        } else {
                            return Long.MIN_VALUE;
                        }
                        state = STATE_DELIM;
                        p += 2;
                        break;
                    case STATE_DELIM:
                        if (c == ':') {
                            state = STATE_MINUTE;
                            p++;
                        } else if (isDigit(c)) {
                            state = STATE_MINUTE;
                        } else {
                            return Long.MIN_VALUE;
                        }
                        break;
                    case STATE_MINUTE:
                        if (isDigit(c) && p < hi - 1) {
                            minute = Numbers.parseInt(in, p, p + 2);
                        } else {
                            return Long.MIN_VALUE;
                        }
                        p += 2;
                        state = STATE_END;
                        break OUT;
                    default:
                        return Long.MIN_VALUE;
                }
            }
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }

        switch (state) {
            case STATE_DELIM:
            case STATE_END:
                if (hour > 23 || minute > 59) {
                    return Long.MIN_VALUE;
                }
                final int min = hour * 60 + minute;
                return Numbers.encodeLowHighInts(negative ? -min : min, p - lo);
            default:
                return Long.MIN_VALUE;
        }
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
        return toMicros(y, isLeapYear(y), m, d, h, mi);
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
        int maxDay = Math.min(day, getDaysPerMonth(month, leap)) - 1;
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
        boolean l = isLeapYear(y);
        return yearMicros(y, l) + monthOfYearMicros(m, l) + (d - 1) * DAY_MICROS;
    }

    public static String toString(long micros) {
        CharSink sink = Misc.getThreadLocalBuilder();
        TimestampFormatUtils.appendDateTime(sink, micros);
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
        long l = parseOffset(timezone, lo, hi);
        if (l == Long.MIN_VALUE) {
            return utc + locale.getZoneRules(
                    Numbers.decodeLowInt(locale.matchZone(timezone, lo, hi)), RESOLUTION_MICROS
            ).getOffset(utc);
        }
        offset = Numbers.decodeLowInt(l) * MINUTE_MICROS;
        return utc + offset;
    }

    public static String toUSecString(long micros) {
        CharSink sink = Misc.getThreadLocalBuilder();
        TimestampFormatUtils.appendDateTimeUSec(sink, micros);
        return sink.toString();
    }

    public static long toUTC(long timestampWithTimezone, DateLocale locale, CharSequence timezone) throws NumericException {
        return toUTC(timestampWithTimezone, locale, timezone, 0, timezone.length());
    }

    public static long toUTC(
            long timestampWithTimezone,
            DateLocale locale,
            CharSequence timezone,
            int lo,
            int hi
    ) throws NumericException {
        long offset;
        long l = parseOffset(timezone, lo, hi);
        if (l == Long.MIN_VALUE) {
            TimeZoneRules zoneRules = locale.getZoneRules(
                    Numbers.decodeLowInt(locale.matchZone(timezone, lo, hi)),
                    RESOLUTION_MICROS
            );
            offset = zoneRules.getOffset(timestampWithTimezone);
            // getOffset really needs UTC date, not local
            offset = zoneRules.getOffset(timestampWithTimezone - offset);
            return timestampWithTimezone - offset;

        }
        offset = Numbers.decodeLowInt(l) * MINUTE_MICROS;
        return timestampWithTimezone - offset;
    }

    /**
     * Calculated start of year in millis. For example of year 2008 this is
     * equivalent to parsing "2008-01-01T00:00:00.000Z", except this method is faster.
     *
     * @param year the year
     * @param leap true if give year is leap year
     * @return millis for start of year.
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

    private static long getTimeMicros(long micros) {
        return micros < 0 ? DAY_MICROS - 1 + (micros % DAY_MICROS) : micros % DAY_MICROS;
    }

    private static long getTimeMicros(long micros, int stride) {
        final long us = stride * DAY_MICROS;
        return micros < 0 ? us - 1 + (micros % us) : micros % us;
    }

    private static boolean isDigit(char c) {
        return c > BEFORE_ZERO && c < AFTER_NINE;
    }

    static {
        STARTUP_TIMESTAMP = Os.currentTimeMicros();
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MICROS;
            MIN_MONTH_OF_YEAR_MICROS[i + 1] = minSum;
            maxSum += getDaysPerMonth(i + 1, true) * DAY_MICROS;
            MAX_MONTH_OF_YEAR_MICROS[i + 1] = maxSum;
        }
    }
}
