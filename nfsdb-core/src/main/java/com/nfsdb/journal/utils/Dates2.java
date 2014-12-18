/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.utils;

import com.nfsdb.journal.export.CharSink;

public class Dates2 {

    public static final long DAY_MILLIS = 86400000L;
    public static final long AVG_YEAR_MILLIS = (long) (365.25 * DAY_MILLIS);
    public static final long YEAR_MILLIS = 365 * DAY_MILLIS;
    public static final long LEAP_YEAR_MILLIS = 366 * DAY_MILLIS;
    public static final long HALF_YEAR_MILLIS = AVG_YEAR_MILLIS / 2;
    public static final long EPOCH_MILLIS = 1970L * AVG_YEAR_MILLIS;
    public static final long HALF_EPOCH_MILLIS = EPOCH_MILLIS / 2;
    public static final long HOUR_MILLIS = 3600000L;
    public static final int DAY_HOURS = 24;
    public static final int HOUR_MINUTES = 60;
    public static final int MINUTE_MILLIS = 60000;
    public static final int SECOND_MILLIS = 1000;
    public static final int MINUTE_SECONDS = 60;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    private static final long[] MIN_MONTH_OF_YEAR_MILLIS = new long[12];
    private static final long[] MAX_MONTH_OF_YEAR_MILLIS = new long[12];

    static {
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MILLIS;
            MIN_MONTH_OF_YEAR_MILLIS[i + 1] = minSum;
            maxSum += (i == 1 ? 29 : DAYS_PER_MONTH[i]) * DAY_MILLIS;
            MAX_MONTH_OF_YEAR_MILLIS[i + 1] = maxSum;
        }
    }

    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

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

    public static int getMonthOfYear(long millis, int year, boolean leap) {
        int i = (int) ((millis - yearMillis(year, leap)) >> 10);
        return
                leap
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

    public static long monthOfYearMillis(int month, boolean leap) {
        return leap ? MAX_MONTH_OF_YEAR_MILLIS[month - 1] : MIN_MONTH_OF_YEAR_MILLIS[month - 1];
    }

    public static int getDayOfMonth(long millis, int year, int month, boolean leap) {
        long dateMillis = yearMillis(year, leap);
        dateMillis += monthOfYearMillis(month, leap);
        return (int) ((millis - dateMillis) / DAY_MILLIS) + 1;
    }

    public static int getHourOfDay(long millis) {
        if (millis >= 0) {
            return (int) ((millis / HOUR_MILLIS) % DAY_HOURS);
        } else {
            return DAY_HOURS - 1 + (int) (((millis + 1) / HOUR_MILLIS) % DAY_HOURS);
        }
    }

    public static int getMinuteOfHour(long millis) {
        if (millis >= 0) {
            return (int) ((millis / MINUTE_MILLIS) % HOUR_MINUTES);
        } else {
            return HOUR_MINUTES - 1 + (int) (((millis + 1) / MINUTE_MILLIS) % HOUR_MINUTES);
        }
    }

    public static int getSecondOfMinute(long millis) {
        if (millis >= 0) {
            return (int) ((millis / SECOND_MILLIS) % MINUTE_SECONDS);
        } else {
            return MINUTE_SECONDS - 1 + (int) (((millis + 1) / SECOND_MILLIS) % MINUTE_SECONDS);
        }
    }

    public static int getMillisOfSecond(long millis) {
        if (millis >= 0) {
            return (int) (millis % 1000);
        } else {
            return 1000 - 1 + (int) ((millis + 1) % 1000);
        }
    }

    public static void appendDateISO(CharSink sink, long millis) {
        int y = getYear(millis);
        boolean l = isLeapYear(y);
        int m = getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
        append0(sink.put('-'), getDayOfMonth(millis, y, m, l));
        append0(sink.put('T'), getHourOfDay(millis));
        append0(sink.put(':'), getMinuteOfHour(millis));
        append0(sink.put(':'), getSecondOfMinute(millis));
        sink.put(".");

        int s = getMillisOfSecond(millis);
        if (s < 10) {
            sink.put('0').put('0');
        } else if (s < 100) {
            sink.put('0');
        }
        Numbers.append(sink, s);
        sink.put("Z");
    }

    private static CharSink append0(CharSink sink, int x) {
        if (x < 10) {
            sink.put('0');
        }
        Numbers.append(sink, x);
        return sink;
    }
}