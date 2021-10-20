/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std.datetime.millitime;

import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.AbstractDateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.CharSink;

public class GenericDateFormat extends AbstractDateFormat {
    private final IntList compiledOps;
    private final ObjList<String> delimiters;

    public GenericDateFormat(IntList compiledOps, ObjList<String> delimiters) {
        this.compiledOps = compiledOps;
        this.delimiters = delimiters;
    }

    @Override
    public void format(long datetime, DateLocale locale, CharSequence timeZoneName, CharSink sink) {
        int day = -1;
        int month = -1;
        int year = Integer.MIN_VALUE;
        int hour = -1;
        int minute = -1;
        int second = -1;
        int dayOfWeek = -1;
        boolean leap = false;
        int millis = -1;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {
            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case DateFormatCompiler.OP_AM_PM:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.appendAmPm(sink, hour, locale);
                    break;

                // MILLIS
                case DateFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case DateFormatCompiler.OP_MILLIS_GREEDY:
                    if (millis == -1) {
                        millis = Dates.getMillisOfSecond(datetime);
                    }
                    sink.put(millis);
                    break;

                case DateFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    if (millis == -1) {
                        millis = Dates.getMillisOfSecond(datetime);
                    }
                    DateFormatUtils.append00(sink, millis);
                    break;

                // SECOND
                case DateFormatCompiler.OP_SECOND_ONE_DIGIT:
                case DateFormatCompiler.OP_SECOND_GREEDY:
                    if (second == -1) {
                        second = Dates.getSecondOfMinute(datetime);
                    }
                    sink.put(second);
                    break;

                case DateFormatCompiler.OP_SECOND_TWO_DIGITS:
                    if (second == -1) {
                        second = Dates.getSecondOfMinute(datetime);
                    }
                    DateFormatUtils.append0(sink, second);
                    break;


                // MINUTE
                case DateFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case DateFormatCompiler.OP_MINUTE_GREEDY:
                    if (minute == -1) {
                        minute = Dates.getMinuteOfHour(datetime);
                    }
                    sink.put(minute);
                    break;

                case DateFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    if (minute == -1) {
                        minute = Dates.getMinuteOfHour(datetime);
                    }
                    DateFormatUtils.append0(sink, minute);
                    break;


                // HOUR (0-11)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case DateFormatCompiler.OP_HOUR_12_GREEDY:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.appendHour12(sink, hour);
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.appendHour12Padded(sink, hour);
                    break;

                // HOUR (1-12)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case DateFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.appendHour121(sink, hour);
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.appendHour121Padded(sink, hour);
                    break;

                // HOUR (0-23)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case DateFormatCompiler.OP_HOUR_24_GREEDY:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    sink.put(hour);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.append0(sink, hour);
                    break;

                // HOUR (1 - 24)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case DateFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    sink.put(hour + 1);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Dates.getHourOfDay(datetime);
                    }
                    DateFormatUtils.append0(sink, hour + 1);
                    break;

                // DAY
                case DateFormatCompiler.OP_DAY_ONE_DIGIT:
                case DateFormatCompiler.OP_DAY_GREEDY:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Dates.getMonthOfYear(datetime, year, leap);
                        }

                        day = Dates.getDayOfMonth(datetime, year, month, leap);
                    }
                    sink.put(day);
                    break;
                case DateFormatCompiler.OP_DAY_TWO_DIGITS:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Dates.getMonthOfYear(datetime, year, leap);
                        }

                        day = Dates.getDayOfMonth(datetime, year, month, leap);
                    }
                    DateFormatUtils.append0(sink, day);
                    break;

                case DateFormatCompiler.OP_DAY_NAME_LONG:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(datetime);
                    }
                    sink.put(locale.getWeekday(dayOfWeek));
                    break;

                case DateFormatCompiler.OP_DAY_NAME_SHORT:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(datetime);
                    }
                    sink.put(locale.getShortWeekday(dayOfWeek));
                    break;

                case DateFormatCompiler.OP_DAY_OF_WEEK:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Dates.getDayOfWeekSundayFirst(datetime);
                    }
                    sink.put(dayOfWeek);
                    break;

                // MONTH

                case DateFormatCompiler.OP_MONTH_ONE_DIGIT:
                case DateFormatCompiler.OP_MONTH_GREEDY:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(datetime, year, leap);
                    }
                    sink.put(month);
                    break;
                case DateFormatCompiler.OP_MONTH_TWO_DIGITS:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(datetime, year, leap);
                    }
                    DateFormatUtils.append0(sink, month);
                    break;

                case DateFormatCompiler.OP_MONTH_SHORT_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(datetime, year, leap);
                    }
                    sink.put(locale.getShortMonth(month - 1));
                    break;
                case DateFormatCompiler.OP_MONTH_LONG_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Dates.getYear(datetime);
                            leap = Dates.isLeapYear(year);
                        }

                        month = Dates.getMonthOfYear(datetime, year, leap);
                    }
                    sink.put(locale.getMonth(month - 1));
                    break;

                // YEAR

                case DateFormatCompiler.OP_YEAR_ONE_DIGIT:
                case DateFormatCompiler.OP_YEAR_GREEDY:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(datetime);
                        leap = Dates.isLeapYear(year);
                    }
                    sink.put(year);
                    break;
                case DateFormatCompiler.OP_YEAR_TWO_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(datetime);
                        leap = Dates.isLeapYear(year);
                    }
                    DateFormatUtils.append0(sink, year % 100);
                    break;
                case DateFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(datetime);
                        leap = Dates.isLeapYear(year);
                    }
                    DateFormatUtils.append000(sink, year);
                    break;

                // ERA
                case DateFormatCompiler.OP_ERA:
                    if (year == Integer.MIN_VALUE) {
                        year = Dates.getYear(datetime);
                        leap = Dates.isLeapYear(year);
                    }
                    DateFormatUtils.appendEra(sink, year, locale);
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
        int era = 1;
        int timezone = -1;
        long offset = Long.MIN_VALUE;
        int hourType = DateFormatUtils.HOUR_24;
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

                // MILLIS
                case DateFormatCompiler.OP_MILLIS_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    millis = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 2, hi);
                    millis = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case DateFormatCompiler.OP_MILLIS_GREEDY:
                    l = Numbers.parseInt000Greedy(in, pos, hi);
                    millis = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SECOND
                case DateFormatCompiler.OP_SECOND_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    second = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_SECOND_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    second = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_SECOND_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    second = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MINUTE
                case DateFormatCompiler.OP_MINUTE_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    minute = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    minute = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_MINUTE_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    minute = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // HOUR (0-11)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                // HOUR (1-12)
                case DateFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    DateFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                case DateFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l) - 1;
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == DateFormatUtils.HOUR_24) {
                        hourType = DateFormatUtils.HOUR_AM;
                    }
                    break;

                // HOUR (0-23)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // HOUR (1 - 24)
                case DateFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    DateFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2) - 1;
                    break;

                case DateFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l) - 1;
                    pos += Numbers.decodeHighInt(l);
                    break;

                // DAY
                case DateFormatCompiler.OP_DAY_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    day = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_DAY_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
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
                    DateFormatUtils.assertRemaining(pos, hi);
                    // ignore weekday
                    Numbers.parseInt(in, pos, ++pos);
                    break;

                // MONTH

                case DateFormatCompiler.OP_MONTH_ONE_DIGIT:
                    DateFormatUtils.assertRemaining(pos, hi);
                    month = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_MONTH_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
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
                    DateFormatUtils.assertRemaining(pos, hi);
                    year = Numbers.parseInt(in, pos, ++pos);
                    break;
                case DateFormatCompiler.OP_YEAR_TWO_DIGITS:
                    DateFormatUtils.assertRemaining(pos + 1, hi);
                    year = DateFormatUtils.adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    break;
                case DateFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (pos < hi && in.charAt(pos) == '-') {
                        DateFormatUtils.assertRemaining(pos + 4, hi);
                        year = -Numbers.parseInt(in, pos + 1, pos += 5);
                    } else {
                        DateFormatUtils.assertRemaining(pos + 3, hi);
                        year = Numbers.parseInt(in, pos, pos += 4);
                    }
                    break;
                case DateFormatCompiler.OP_YEAR_GREEDY:
                    l = DateFormatUtils.parseYearGreedy(in, pos, hi);
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
                        offset = Numbers.decodeLowInt(l) * Dates.MINUTE_MILLIS;
                    }
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SEPARATORS
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    len = delimiter.length();
                    if (len == 1) {
                        DateFormatUtils.assertChar(delimiter.charAt(0), in, pos++, hi);
                    } else {
                        pos = DateFormatUtils.assertString(delimiter, len, in, pos, hi);
                    }
                    break;
            }
        }

        DateFormatUtils.assertNoTail(pos, hi);

        return DateFormatUtils.compute(locale, era, year, month, day, hour, minute, second, millis, timezone, offset, hourType);
    }
}
