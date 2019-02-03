/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std.microtime;

import com.questdb.std.IntList;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.ObjList;
import com.questdb.std.str.CharSink;

import static com.questdb.std.microtime.DateFormatUtils.*;

public class GenericDateFormat extends AbstractDateFormat {
    private final IntList compiledOps;
    private final ObjList<String> delimiters;

    public GenericDateFormat(IntList compiledOps, ObjList<String> delimiters) {
        this.compiledOps = compiledOps;
        this.delimiters = delimiters;
    }

    @Override
    public void format(long micros, DateLocale locale, CharSequence timeZoneName, CharSink sink) {
        int day = -1;
        int month = -1;
        int year = Integer.MIN_VALUE;
        int hour = -1;
        int minute = -1;
        int second = -1;
        int dayOfWeek = -1;
        boolean leap = false;
        int millis = -1;
        int micros0 = -1;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {
            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case DateFormatCompiler.OP_AM_PM:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    appendAmPm(sink, hour, locale);
                    break;

                // MICROS
                case DateFormatCompiler.OP_MICROS_ONE_DIGIT:
                case DateFormatCompiler.OP_MICROS_GREEDY:
                    if (micros0 == -1) {
                        micros0 = Dates.getMicrosOfSecond(micros);
                    }
                    sink.put(micros0);
                    break;

                case DateFormatCompiler.OP_MICROS_THREE_DIGITS:
                    if (micros0 == -1) {
                        micros0 = Dates.getMicrosOfSecond(micros);
                    }
                    append00(sink, micros0);
                    break;

                // MILLIS
                case DateFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case DateFormatCompiler.OP_MILLIS_GREEDY:
                    if (millis == -1) {
                        millis = Dates.getMillisOfSecond(micros);
                    }
                    sink.put(millis);
                    break;

                case DateFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    if (millis == -1) {
                        millis = Dates.getMillisOfSecond(micros);
                    }
                    append00(sink, millis);
                    break;

                // SECOND
                case DateFormatCompiler.OP_SECOND_ONE_DIGIT:
                case DateFormatCompiler.OP_SECOND_GREEDY:
                    if (second == -1) {
                        second = Dates.getSecondOfMinute(micros);
                    }
                    sink.put(second);
                    break;

                case DateFormatCompiler.OP_SECOND_TWO_DIGITS:
                    if (second == -1) {
                        second = Dates.getSecondOfMinute(micros);
                    }
                    append0(sink, second);
                    break;


                // MINUTE
                case DateFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case DateFormatCompiler.OP_MINUTE_GREEDY:
                    if (minute == -1) {
                        minute = Dates.getMinuteOfHour(micros);
                    }
                    sink.put(minute);
                    break;

                case DateFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    if (minute == -1) {
                        minute = Dates.getMinuteOfHour(micros);
                    }
                    append0(sink, minute);
                    break;


                // HOUR (0-11)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case DateFormatCompiler.OP_HOUR_12_GREEDY:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    appendHour12(sink, hour);
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    appendHour12Padded(sink, hour);
                    break;

                // HOUR (1-12)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case DateFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    appendHour121(sink, hour);
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    appendHour121Padded(sink, hour);
                    break;

                // HOUR (0-23)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case DateFormatCompiler.OP_HOUR_24_GREEDY:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    sink.put(hour);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    append0(sink, hour);
                    break;

                // HOUR (1 - 24)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case DateFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    sink.put(hour + 1);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(micros);
                    }
                    append0(sink, hour + 1);
                    break;

                // DAY
                case DateFormatCompiler.OP_DAY_ONE_DIGIT:
                case DateFormatCompiler.OP_DAY_GREEDY:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Dates.getMonthOfYear(micros, year, leap);
                        }

                        day = Dates.getDayOfMonth(micros, year, month, leap);
                    }
                    sink.put(day);
                    break;
                case DateFormatCompiler.OP_DAY_TWO_DIGITS:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Dates.getMonthOfYear(micros, year, leap);
                        }

                        day = Dates.getDayOfMonth(micros, year, month, leap);
                    }
                    append0(sink, day);
                    break;

                case DateFormatCompiler.OP_DAY_NAME_LONG:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getWeekday(dayOfWeek));
                    break;

                case DateFormatCompiler.OP_DAY_NAME_SHORT:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getShortWeekday(dayOfWeek));
                    break;

                case DateFormatCompiler.OP_DAY_OF_WEEK:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(dayOfWeek);
                    break;

                // MONTH

                case DateFormatCompiler.OP_MONTH_ONE_DIGIT:
                case DateFormatCompiler.OP_MONTH_GREEDY:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(month);
                    break;
                case DateFormatCompiler.OP_MONTH_TWO_DIGITS:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(micros, year, leap);
                    }
                    append0(sink, month);
                    break;

                case DateFormatCompiler.OP_MONTH_SHORT_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getShortMonth(month - 1));
                    break;
                case DateFormatCompiler.OP_MONTH_LONG_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(micros);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getMonth(month - 1));
                    break;

                // YEAR

                case DateFormatCompiler.OP_YEAR_ONE_DIGIT:
                case DateFormatCompiler.OP_YEAR_GREEDY:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(micros);
                        leap = Dates.isLeapYear(year);
                    }
                    sink.put(year);
                    break;
                case DateFormatCompiler.OP_YEAR_TWO_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(micros);
                        leap = Dates.isLeapYear(year);
                    }
                    append0(sink, year % 100);
                    break;
                case DateFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(micros);
                        leap = Dates.isLeapYear(year);
                    }
                    append000(sink, year);
                    break;

                // ERA
                case DateFormatCompiler.OP_ERA:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(micros);
                        leap = Dates.isLeapYear(year);
                    }
                    appendEra(sink, year, locale);
                    break;

                // TIMEZONE
                case DateFormatCompiler.OP_TIME_ZONE_SHORT:
                case DateFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case DateFormatCompiler.OP_TIME_ZONE_LONG:
                case DateFormatCompiler.OP_TIME_ZONE_RFC_822:
                    sink.put(timeZoneName);
                    break;

                // SEPARATORS
                default:
                    sink.put(delimiters.getQuick(-op - 1));
                    break;
            }
        }
    }

    @Override
    public long parse(CharSequence in, int lo, int hi, DateLocale locale) throws NumericException {
        int day = 1;
        int month = 1;
        int year = 1970;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int micros = 0;
        int era = 1;
        int timezone = -1;
        long offset = Long.MIN_VALUE;
        int hourType = HOUR_24;
        int pos = lo;
        long l;
        int len;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {
            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case DateFormatCompiler.OP_AM_PM:
                    l = locale.matchAMPM(in, pos, hi);
                    hourType = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MICROS
                case DateFormatCompiler.OP_MICROS_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    micros = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MICROS_THREE_DIGITS:
                    assertRemaining(pos + 2, hi);
                    micros = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case DateFormatCompiler.OP_MICROS_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MILLIS
                case DateFormatCompiler.OP_MILLIS_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    millis = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    assertRemaining(pos + 2, hi);
                    millis = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case DateFormatCompiler.OP_MILLIS_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    millis = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SECOND
                case DateFormatCompiler.OP_SECOND_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    second = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_SECOND_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    second = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_SECOND_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    second = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MINUTE
                case DateFormatCompiler.OP_MINUTE_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    minute = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    minute = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_MINUTE_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    minute = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // HOUR (0-11)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                // HOUR (1-12)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l) - 1;
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == HOUR_24) {
                        hourType = HOUR_AM;
                    }
                    break;

                // HOUR (0-23)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // HOUR (1 - 24)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l) - 1;
                    pos += Numbers.decodeHighInt(l);
                    break;

                // DAY
                case DateFormatCompiler.OP_DAY_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    day = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_DAY_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    day = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case DateFormatCompiler.OP_DAY_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    day = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case DateFormatCompiler.OP_DAY_NAME_LONG:
                case DateFormatCompiler.OP_DAY_NAME_SHORT:
                    l = locale.matchWeekday(in, pos, hi);
                    // ignore weekday
                    pos += Numbers.decodeHighInt(l);
                    break;

                case DateFormatCompiler.OP_DAY_OF_WEEK:
                    assertRemaining(pos, hi);
                    // ignore weekday
                    Numbers.parseInt(in, pos, ++pos);
                    break;

                // MONTH

                case DateFormatCompiler.OP_MONTH_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    month = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_MONTH_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    month = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case DateFormatCompiler.OP_MONTH_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    month = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case DateFormatCompiler.OP_MONTH_SHORT_NAME:
                case DateFormatCompiler.OP_MONTH_LONG_NAME:
                    l = locale.matchMonth(in, pos, hi);
                    month = Numbers.decodeLowInt(l) + 1;
                    pos += Numbers.decodeHighInt(l);
                    break;

                // YEAR

                case DateFormatCompiler.OP_YEAR_ONE_DIGIT:
                    assertRemaining(pos, hi);
                    year = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_YEAR_TWO_DIGITS:
                    assertRemaining(pos + 1, hi);
                    year = adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    break;
                case DateFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (pos < hi && in.charAt(pos) == '-') {
                        assertRemaining(pos + 4, hi);
                        year = -Numbers.parseInt(in, pos + 1, pos += 5);
                    } else {
                        assertRemaining(pos + 3, hi);
                        year = Numbers.parseInt(in, pos, pos += 4);
                    }
                    break;
                case DateFormatCompiler.OP_YEAR_GREEDY:
                    l = parseYearGreedy(in, pos, hi);
                    year = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // ERA
                case DateFormatCompiler.OP_ERA:
                    l = locale.matchEra(in, pos, hi);
                    era = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // TIMEZONE
                case DateFormatCompiler.OP_TIME_ZONE_SHORT:
                case DateFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case DateFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case DateFormatCompiler.OP_TIME_ZONE_LONG:
                case DateFormatCompiler.OP_TIME_ZONE_RFC_822:

                    l = Dates.parseOffset(in, pos, hi);
                    if (l == Long.MIN_VALUE) {
                        l = locale.matchZone(in, pos, hi);
                        timezone = Numbers.decodeLowInt(l);
                    } else {
                        offset = Numbers.decodeLowInt(l) * Dates.MINUTE_MICROS;
                    }
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SEPARATORS
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    len = delimiter.length();
                    if (len == 1) {
                        assertChar(delimiter.charAt(0), in, pos++, hi);
                    } else {
                        pos = assertString(delimiter, len, in, pos, hi);
                    }
                    break;
            }
        }

        assertNoTail(pos, hi);

        return compute(locale, era, year, month, day, hour, minute, second, millis, micros, timezone, offset, hourType);
    }
}
