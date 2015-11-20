/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.misc;

import com.nfsdb.exceptions.NumericException;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.StringSink;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("CD_CIRCULAR_DEPENDENCY")
final public class Dates {

    public static final long DAY_MILLIS = 86400000L;
    public static final long HOUR_MILLIS = 3600000L;
    public static final long MINUTE_MILLIS = 60000;
    public static final long SECOND_MILLIS = 1000;
    private static final long AVG_YEAR_MILLIS = (long) (365.25 * DAY_MILLIS);
    private static final long YEAR_MILLIS = 365 * DAY_MILLIS;
    private static final long LEAP_YEAR_MILLIS = 366 * DAY_MILLIS;
    private static final long HALF_YEAR_MILLIS = AVG_YEAR_MILLIS / 2;
    private static final long EPOCH_MILLIS = 1970L * AVG_YEAR_MILLIS;
    private static final long HALF_EPOCH_MILLIS = EPOCH_MILLIS / 2;
    private static final int DAY_HOURS = 24;
    private static final int HOUR_MINUTES = 60;
    private static final int MINUTE_SECONDS = 60;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    private static final long[] MIN_MONTH_OF_YEAR_MILLIS = new long[12];
    private static final long[] MAX_MONTH_OF_YEAR_MILLIS = new long[12];

    private Dates() {
    }

    public static long addDays(long millis, int days) {
        return millis + days * DAY_MILLIS;
    }

    public static long addHours(long millis, int hours) {
        return millis + hours * HOUR_MILLIS;
    }

    public static long addMonths(final long millis, int months) {
        if (months == 0) {
            return millis;
        }
        boolean l;
        int y = getYear(millis);
        int m = getMonthOfYear(millis, y, l = isLeapYear(y));
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
        int _d = getDayOfMonth(millis, y, m, l);
        int maxDay = getDaysPerMonth(_m, isLeapYear(_y));
        if (_d > maxDay) {
            _d = maxDay;
        }
        return toMillis(_y, _m, _d) + getTime(millis) + (millis < 0 ? 1 : 0);
    }

    public static long addPeriod(long lo, char type, int period) throws NumericException {
        switch (type) {
            case 's':
                return lo + period * Dates.SECOND_MILLIS;
            case 'm':
                return lo + period * Dates.MINUTE_MILLIS;
            case 'h':
                return Dates.addHours(lo, period);
            case 'd':
                return Dates.addDays(lo, period);
            case 'M':
                return Dates.addMonths(lo, period);
            case 'y':
                return Dates.addYear(lo, period);
            default:
                throw NumericException.INSTANCE;
//                throw new NumberFormatException("Unsupported period: " + type);
        }
    }

    public static long addYear(long millis, int years) {
        if (years == 0) {
            return millis;
        }

        int y, m;
        boolean l;

        return yearMillis((y = getYear(millis)) + years, l = isLeapYear(y + years))
                + monthOfYearMillis(m = getMonthOfYear(millis, y, l), l)
                + (getDayOfMonth(millis, y, m, l) - 1) * DAY_MILLIS
                + getTime(millis)
                + (millis < 0 ? 1 : 0);

    }

    // YYYY-MM-DDThh:mm:ss.mmmmZ
    public static void appendDateTime(CharSink sink, long millis) {
        if (millis == Long.MIN_VALUE) {
            return;
        }
        int y = getYear(millis);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(millis, y, l);
        append0000(sink, y);
        append0(sink.put('-'), m);
        append0(sink.put('-'), getDayOfMonth(millis, y, m, l));
        append0(sink.put('T'), getHourOfDay(millis));
        append0(sink.put(':'), getMinuteOfHour(millis));
        append0(sink.put(':'), getSecondOfMinute(millis));
        append00(sink.put("."), getMillisOfSecond(millis));
        sink.put("Z");
    }

    public static long ceilDD(long millis) {
        int y, m;
        boolean l;
        return yearMillis(y = getYear(millis), l = isLeapYear(y))
                + monthOfYearMillis(m = getMonthOfYear(millis, y, l), l)
                + (getDayOfMonth(millis, y, m, l) - 1) * DAY_MILLIS
                + 23 * HOUR_MILLIS
                + 59 * MINUTE_MILLIS
                + 59 * SECOND_MILLIS
                + 999L
                ;
    }

    public static long ceilMM(long millis) {
        int y, m;
        boolean l;
        return yearMillis(y = getYear(millis), l = isLeapYear(y))
                + monthOfYearMillis(m = getMonthOfYear(millis, y, l), l)
                + (getDaysPerMonth(m, l) - 1) * DAY_MILLIS
                + 23 * HOUR_MILLIS
                + 59 * MINUTE_MILLIS
                + 59 * SECOND_MILLIS
                + 999L
                ;
    }

    public static long ceilYYYY(long millis) {
        int y;
        boolean l;
        return yearMillis(y = getYear(millis), l = isLeapYear(y))
                + monthOfYearMillis(12, l)
                + (DAYS_PER_MONTH[11] - 1) * DAY_MILLIS
                + 23 * HOUR_MILLIS
                + 59 * MINUTE_MILLIS
                + 59 * SECOND_MILLIS
                + 999L;
    }

    public static long floorDD(long millis) {
        return millis - getTime(millis);
    }

    public static long floorHH(long millis) {
        return millis - millis % HOUR_MILLIS;
    }

    public static long floorMI(long millis) {
        return millis - millis % MINUTE_MILLIS;
    }

    public static long floorMM(long millis) {
        int y;
        boolean l;
        return yearMillis(y = getYear(millis), l = isLeapYear(y)) + monthOfYearMillis(getMonthOfYear(millis, y, l), l);
    }

    public static long floorYYYY(long millis) {
        int y;
        return yearMillis(y = getYear(millis), isLeapYear(y));
    }

    // YYYY-MM-DD
    public static void formatDashYYYYMMDD(CharSink sink, long millis) {
        int y = getYear(millis);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
        append0(sink.put('-'), getDayOfMonth(millis, y, m, l));
    }

    // YYYY
    public static void formatYYYY(CharSink sink, long millis) {
        Numbers.append(sink, getYear(millis));
    }

    // YYYY-MM
    public static void formatYYYYMM(CharSink sink, long millis) {
        int y = getYear(millis);
        int m = getMonthOfYear(millis, y, isLeapYear(y));
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
    }

    // YYYYMMDD
    public static void formatYYYYMMDD(CharSink sink, long millis) {
        int y = getYear(millis);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink, m);
        append0(sink, getDayOfMonth(millis, y, m, l));
    }

//    public static int getDayOfWeek(long millis) {
//        // 1970-01-01 is Thursday.
//        long d;
//        if (millis >= 0) {
//            d = millis / DAY_MILLIS;
//        } else {
//            d = (millis - (DAY_MILLIS - 1)) / DAY_MILLIS;
//            if (d < -3) {
//                return 7 + (int) ((d + 4) % 7);
//            }
//        }
//        return 1 + (int) ((d + 3) % 7);
//    }

    public static int getDayOfMonth(long millis, int year, int month, boolean leap) {
        long dateMillis = yearMillis(year, leap);
        dateMillis += monthOfYearMillis(month, leap);
        return (int) ((millis - dateMillis) / DAY_MILLIS) + 1;
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

    public static int getHourOfDay(long millis) {
        if (millis > -1) {
            return (int) ((millis / HOUR_MILLIS) % DAY_HOURS);
        } else {
            return DAY_HOURS - 1 + (int) (((millis + 1) / HOUR_MILLIS) % DAY_HOURS);
        }
    }

    public static int getMillisOfSecond(long millis) {
        if (millis > -1) {
            return (int) (millis % 1000);
        } else {
            return 1000 - 1 + (int) ((millis + 1) % 1000);
        }
    }

    public static int getMinuteOfHour(long millis) {
        if (millis > -1) {
            return (int) ((millis / MINUTE_MILLIS) % HOUR_MINUTES);
        } else {
            return HOUR_MINUTES - 1 + (int) (((millis + 1) / MINUTE_MILLIS) % HOUR_MINUTES);
        }
    }

    /**
     * Calculates month of year from absolute millis.
     *
     * @param millis millis since 1970
     * @param year   year of month
     * @param leap   true if year was leap
     * @return month of year
     */
    public static int getMonthOfYear(long millis, int year, boolean leap) {
        int i = (int) ((millis - yearMillis(year, leap)) >> 10);
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

    public static int getSecondOfMinute(long millis) {
        if (millis > -1) {
            return (int) ((millis / SECOND_MILLIS) % MINUTE_SECONDS);
        } else {
            return MINUTE_SECONDS - 1 + (int) (((millis + 1) / SECOND_MILLIS) % MINUTE_SECONDS);
        }
    }

    public static long getTime(long millis) {
        return millis < 0 ? DAY_MILLIS - 1 + (millis % DAY_MILLIS) : millis % DAY_MILLIS;
    }

    /**
     * Calculates year number from millis.
     *
     * @param millis time millis.
     * @return year
     */
    public static int getYear(long millis) {
        long mid = (millis >> 1) + HALF_EPOCH_MILLIS;
        if (mid < 0) {
            mid = mid - HALF_YEAR_MILLIS + 1;
        }
        int year = (int) (mid / HALF_YEAR_MILLIS);

        boolean leap = isLeapYear(year);
        long yearStart = yearMillis(year, leap);
        long diff = millis - yearStart;

        if (diff < 0) {
            year--;
        } else if (diff >= YEAR_MILLIS) {
            yearStart += leap ? LEAP_YEAR_MILLIS : YEAR_MILLIS;
            if (yearStart <= millis) {
                year++;
            }
        }

        return year;
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

    public static long monthOfYearMillis(int month, boolean leap) {
        return leap ? MAX_MONTH_OF_YEAR_MILLIS[month - 1] : MIN_MONTH_OF_YEAR_MILLIS[month - 1];
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTime(CharSequence seq) throws NumericException {
        return parseDateTime(seq, 0, seq.length());
    }

    @SuppressFBWarnings({"ICAST_INTEGER_MULTIPLY_CAST_TO_LONG", "LEST_LOST_EXCEPTION_STACK_TRACE"})
    public static long parseDateTime(CharSequence seq, int lo, int lim) throws NumericException {
        int p = lo;
        if (p + 4 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Four digit year expected: " + seq);
        }
        int year = Numbers.parseInt(seq, p, p += 4);
        checkChar(seq, p++, lim, '-');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit month expected: " + seq);
        }
        int month = Numbers.parseInt(seq, p, p += 2);
        checkRange(month, 1, 12);
        checkChar(seq, p++, lim, '-');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit day of month expected: " + seq);
        }
        boolean l = isLeapYear(year);
        int day = Numbers.parseInt(seq, p, p += 2);
        checkRange(day, 1, getDaysPerMonth(month, l));
        checkChar(seq, p++, lim, 'T');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of hours (24h scale) expected: " + seq);
        }
        int hour = Numbers.parseInt(seq, p, p += 2);
        checkRange(hour, 0, 23);
        checkChar(seq, p++, lim, ':');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of minutes expected: " + seq);
        }
        int min = Numbers.parseInt(seq, p, p += 2);
        checkRange(min, 0, 59);
        checkChar(seq, p++, lim, ':');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of seconds expected: " + seq);
        }
        int sec = Numbers.parseInt(seq, p, p += 2);
        checkRange(sec, 0, 59);
        int mil = 0;
        if (p < lim && seq.charAt(p) == '.') {

            if (p + 4 > lim) {
                throw NumericException.INSTANCE;
//                    throw new NumberFormatException("Three digit number of millis expected: " + seq);
            }
            mil = Numbers.parseInt(seq, ++p, p += 3);
            checkRange(mil, 0, 999);
        }

        if (p < lim) {
            checkChar(seq, p, lim, 'Z');
        }
        return yearMillis(year, l)
                + monthOfYearMillis(month, l)
                + (day - 1) * DAY_MILLIS
                + hour * HOUR_MILLIS
                + min * MINUTE_MILLIS
                + sec * SECOND_MILLIS
                + mil;

    }

    public static long parseDateTimeFmt1(CharSequence seq) throws NumericException {
        return parseDateTimeFmt1(seq, 0, seq.length());
    }

    // YYYY-MM-DD hh:mm:ss
    @SuppressFBWarnings({"ICAST_INTEGER_MULTIPLY_CAST_TO_LONG", "LEST_LOST_EXCEPTION_STACK_TRACE"})
    public static long parseDateTimeFmt1(CharSequence seq, int lo, int lim) throws NumericException {
        int p = lo;
        if (p + 4 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Four digit year expected: " + seq);
        }
        int year = Numbers.parseInt(seq, p, p += 4);
        checkChar(seq, p++, lim, '-');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit month expected: " + seq);
        }
        int month = Numbers.parseInt(seq, p, p += 2);
        checkRange(month, 1, 12);
        checkChar(seq, p++, lim, '-');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit day of month expected: " + seq);
        }
        boolean l = isLeapYear(year);
        int day = Numbers.parseInt(seq, p, p += 2);
        checkRange(day, 1, getDaysPerMonth(month, l));
        checkChar(seq, p++, lim, ' ');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of hours (24h scale) expected: " + seq);
        }
        int hour = Numbers.parseInt(seq, p, p += 2);
        checkRange(hour, 0, 23);
        checkChar(seq, p++, lim, ':');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of minutes expected: " + seq);
        }
        int min = Numbers.parseInt(seq, p, p += 2);
        checkRange(min, 0, 59);
        checkChar(seq, p++, lim, ':');
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit number of seconds expected: " + seq);
        }
        int sec = Numbers.parseInt(seq, p, p + 2);
        checkRange(sec, 0, 59);

        return yearMillis(year, l)
                + monthOfYearMillis(month, l)
                + (day - 1) * DAY_MILLIS
                + hour * HOUR_MILLIS
                + min * MINUTE_MILLIS
                + sec * SECOND_MILLIS;
    }

    public static long parseDateTimeFmt1Quiet(CharSequence seq) {
        try {
            return parseDateTimeFmt1(seq, 0, seq.length());
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    public static long parseDateTimeFmt2(CharSequence seq) throws NumericException {
        return parseDateTimeFmt2(seq, 0, seq.length());
    }

    // MM/DD/YYYY
    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    public static long parseDateTimeFmt2(CharSequence seq, int lo, int lim) throws NumericException {
        int p = lo;
        if (p + 2 > lim) {
            throw NumericException.INSTANCE;
//                throw new NumberFormatException("Two digit month expected: " + seq);
        }
        int month = Numbers.parseInt(seq, p, p += 2);
        checkRange(month, 1, 12);
        checkChar(seq, p++, lim, '/');

        if (p + 4 > lim) {
//                throw new NumberFormatException("Four digit year expected: " + seq);
            throw NumericException.INSTANCE;
        }
        int year = Numbers.parseInt(seq, p + 3, p + 7);
        boolean l = isLeapYear(year);
        if (p + 2 > lim) {
//                throw new NumberFormatException("Two digit day of month expected: " + seq);
            throw NumericException.INSTANCE;
        }
        int day = Numbers.parseInt(seq, p, p += 2);
        checkRange(day, 1, getDaysPerMonth(month, l));
        checkChar(seq, p, lim, '/');

        return yearMillis(year, l)
                + monthOfYearMillis(month, l)
                + (day - 1) * DAY_MILLIS;
    }

    public static long parseDateTimeFmt2Quiet(CharSequence seq) {
        try {
            return parseDateTimeFmt2(seq, 0, seq.length());
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTimeQuiet(CharSequence seq) {
        try {
            return parseDateTime(seq, 0, seq.length());
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }
    }

    public static Interval parseInterval(CharSequence seq) throws NumericException {
        return parseInterval(seq, 0, seq.length());
    }

    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE"})
    public static Interval parseInterval(CharSequence seq, int p, int lim) throws NumericException {
        int len = lim - p - 1;
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = isLeapYear(year);
        if (p < len) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (p < len) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, getDaysPerMonth(month, l));
                if (p < len) {
                    checkChar(seq, p++, lim, 'T');
                    int hour = Numbers.parseInt(seq, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (p < len) {
                        checkChar(seq, p++, lim, ':');
                        int min = Numbers.parseInt(seq, p, p += 2);
                        checkRange(min, 0, 59);
                        if (p < len) {
                            checkChar(seq, p++, lim, ':');
                            int sec = Numbers.parseInt(seq, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < len) {
//                                    throw new NumberFormatException("not an interval");
                                throw NumericException.INSTANCE;
                            } else {
                                // seconds
                                return new Interval(yearMillis(year, l)
                                        + monthOfYearMillis(month, l)
                                        + (day - 1) * DAY_MILLIS
                                        + hour * HOUR_MILLIS
                                        + min * MINUTE_MILLIS
                                        + sec * SECOND_MILLIS,
                                        yearMillis(year, l)
                                                + monthOfYearMillis(month, l)
                                                + (day - 1) * DAY_MILLIS
                                                + hour * HOUR_MILLIS
                                                + min * MINUTE_MILLIS
                                                + sec * SECOND_MILLIS
                                                + 999
                                );
                            }
                        } else {
                            // minute
                            return new Interval(yearMillis(year, l)
                                    + monthOfYearMillis(month, l)
                                    + (day - 1) * DAY_MILLIS
                                    + hour * HOUR_MILLIS
                                    + min * MINUTE_MILLIS,
                                    yearMillis(year, l)
                                            + monthOfYearMillis(month, l)
                                            + (day - 1) * DAY_MILLIS
                                            + hour * HOUR_MILLIS
                                            + min * MINUTE_MILLIS
                                            + 59 * SECOND_MILLIS
                                            + 999
                            );
                        }
                    } else {
                        // year + month + day + hour
                        return new Interval(yearMillis(year, l)
                                + monthOfYearMillis(month, l)
                                + (day - 1) * DAY_MILLIS
                                + hour * HOUR_MILLIS,
                                yearMillis(year, l)
                                        + monthOfYearMillis(month, l)
                                        + (day - 1) * DAY_MILLIS
                                        + hour * HOUR_MILLIS
                                        + 59 * MINUTE_MILLIS
                                        + 59 * SECOND_MILLIS
                                        + 999
                        );
                    }
                } else {
                    // year + month + day
                    return new Interval(yearMillis(year, l)
                            + monthOfYearMillis(month, l)
                            + (day - 1) * DAY_MILLIS,
                            yearMillis(year, l)
                                    + monthOfYearMillis(month, l)
                                    + +(day - 1) * DAY_MILLIS
                                    + 23 * HOUR_MILLIS
                                    + 59 * MINUTE_MILLIS
                                    + 59 * SECOND_MILLIS
                                    + 999
                    );
                }
            } else {
                // year + month
                return new Interval(yearMillis(year, l) + monthOfYearMillis(month, l),
                        yearMillis(year, l)
                                + monthOfYearMillis(month, l)
                                + (DAYS_PER_MONTH[month - 1] - 1) * DAY_MILLIS
                                + 23 * HOUR_MILLIS
                                + 59 * MINUTE_MILLIS
                                + 59 * SECOND_MILLIS
                                + 999
                );
            }
        } else {
            // year
            return new Interval(yearMillis(year, l) + monthOfYearMillis(1, l),
                    yearMillis(year, l)
                            + monthOfYearMillis(12, l)
                            + (DAYS_PER_MONTH[11] - 1) * DAY_MILLIS
                            + 23 * HOUR_MILLIS
                            + 59 * MINUTE_MILLIS
                            + 59 * SECOND_MILLIS
                            + 999
            );
        }
    }

    public static long toMillis(int y, int m, int d) {
        boolean l = isLeapYear(y);
        return yearMillis(y, l) + monthOfYearMillis(m, l) + (d - 1) * DAY_MILLIS;
    }

    @SuppressFBWarnings({"ICAST_INTEGER_MULTIPLY_CAST_TO_LONG"})
    public static long toMillis(int y, int m, int d, int h, int mi) {
        boolean l = isLeapYear(y);
        return yearMillis(y, l) + monthOfYearMillis(m, l) + (d - 1) * DAY_MILLIS + h * HOUR_MILLIS + mi * MINUTE_MILLIS;
    }

    public static String toString(long millis) {
        StringSink sink = new StringSink();
        Dates.appendDateTime(sink, millis);
        return sink.toString();
    }

    public static long tryParse(CharSequence s) throws NumericException {
        return tryParse(s, 0, s.length());
    }

    public static long tryParse(CharSequence s, int lo, int lim) throws NumericException {
        try {
            return parseDateTime(s, lo, lim);
        } catch (NumericException ignore) {
        }

        try {
            return parseDateTimeFmt1(s, lo, lim);
        } catch (NumericException ignore) {
        }

        return parseDateTimeFmt2(s, lo, lim);
    }

    /**
     * Calculated start of year in millis. For example of year 2008 this is
     * equivalent to parsing "2008-01-01T00:00:00.000Z", except this method is faster.
     *
     * @param year the year
     * @param leap true if give year is leap year
     * @return millis for start of year.
     */
    public static long yearMillis(int year, boolean leap) {
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }

        return (year * 365L + (leapYears - DAYS_0000_TO_1970)) * DAY_MILLIS;
    }

    private static void append0(CharSink sink, int val) {
        if (val < 10) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    private static void append00(CharSink sink, int val) {
        if (val < 10) {
            sink.put('0').put('0');
        } else if (val < 100) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    private static void append0000(CharSink sink, int val) {
        if (val < 10) {
            sink.put('0').put('0').put('0');
        } else if (val < 100) {
            sink.put('0').put('0');
        } else if (val < 1000) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    private static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
//            throw new NumberFormatException("Expected " + c + " at " + p);
            throw NumericException.INSTANCE;
        }
    }

    private static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
//            throw new NumberFormatException(what + " not in range: " + x);
            throw NumericException.INSTANCE;
        }
    }

    static {
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MILLIS;
            MIN_MONTH_OF_YEAR_MILLIS[i + 1] = minSum;
            maxSum += getDaysPerMonth(i + 1, true) * DAY_MILLIS;
            MAX_MONTH_OF_YEAR_MILLIS[i + 1] = maxSum;
        }
    }
}