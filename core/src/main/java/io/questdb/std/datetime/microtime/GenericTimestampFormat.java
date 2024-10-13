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

import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.AbstractDateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GenericTimestampFormat extends AbstractDateFormat {
    private final IntList compiledOps;
    private final ObjList<String> delimiters;

    public GenericTimestampFormat(IntList compiledOps, ObjList<String> delimiters) {
        this.compiledOps = compiledOps;
        this.delimiters = delimiters;
    }

    @Override
    public void format(long micros, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
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
                case TimestampFormatCompiler.OP_AM_PM:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendAmPm(sink, hour, locale);
                    break;

                // MICROS
                case TimestampFormatCompiler.OP_MICROS_ONE_DIGIT:
                case TimestampFormatCompiler.OP_MICROS_GREEDY3:
                    if (micros0 == -1) {
                        micros0 = Timestamps.getMicrosOfMilli(micros);
                    }
                    sink.put(micros0);
                    break;

                case TimestampFormatCompiler.OP_MICROS_GREEDY6:
                case TimestampFormatCompiler.OP_NANOS_GREEDY9:
                    if (micros0 == -1) {
                        micros0 = Timestamps.getMicrosOfSecond(micros);
                    }
                    TimestampFormatUtils.append00000(sink, micros0);
                    break;

                case TimestampFormatCompiler.OP_MICROS_THREE_DIGITS:
                    if (micros0 == -1) {
                        micros0 = Timestamps.getMicrosOfMilli(micros);
                    }
                    TimestampFormatUtils.append00(sink, micros0);
                    break;

                // MILLIS
                case TimestampFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case TimestampFormatCompiler.OP_MILLIS_GREEDY:
                    if (millis == -1) {
                        millis = Timestamps.getMillisOfSecond(micros);
                    }
                    sink.put(millis);
                    break;

                case TimestampFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    if (millis == -1) {
                        millis = Timestamps.getMillisOfSecond(micros);
                    }
                    TimestampFormatUtils.append00(sink, millis);
                    break;

                // NANOS
                case TimestampFormatCompiler.OP_NANOS_ONE_DIGIT:
                case TimestampFormatCompiler.OP_NANOS_GREEDY:
                    sink.put(0);
                    break;

                case TimestampFormatCompiler.OP_NANOS_THREE_DIGITS:
                    TimestampFormatUtils.append00(sink, 0);
                    break;

                // SECOND
                case TimestampFormatCompiler.OP_SECOND_ONE_DIGIT:
                case TimestampFormatCompiler.OP_SECOND_GREEDY:
                    if (second == -1) {
                        second = Timestamps.getSecondOfMinute(micros);
                    }
                    sink.put(second);
                    break;

                case TimestampFormatCompiler.OP_SECOND_TWO_DIGITS:
                    if (second == -1) {
                        second = Timestamps.getSecondOfMinute(micros);
                    }
                    TimestampFormatUtils.append0(sink, second);
                    break;

                // MINUTE
                case TimestampFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case TimestampFormatCompiler.OP_MINUTE_GREEDY:
                    if (minute == -1) {
                        minute = Timestamps.getMinuteOfHour(micros);
                    }
                    sink.put(minute);
                    break;

                case TimestampFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    if (minute == -1) {
                        minute = Timestamps.getMinuteOfHour(micros);
                    }
                    TimestampFormatUtils.append0(sink, minute);
                    break;


                // HOUR (0-11)
                case TimestampFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case TimestampFormatCompiler.OP_HOUR_12_GREEDY:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour12(sink, hour);
                    break;

                case TimestampFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour12Padded(sink, hour);
                    break;

                // HOUR (1-12)
                case TimestampFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case TimestampFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour121(sink, hour);
                    break;

                case TimestampFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour121Padded(sink, hour);
                    break;

                // HOUR (0-23)
                case TimestampFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case TimestampFormatCompiler.OP_HOUR_24_GREEDY:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    sink.put(hour);
                    break;

                case TimestampFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.append0(sink, hour);
                    break;

                // HOUR (1 - 24)
                case TimestampFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case TimestampFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour241(sink, hour);
                    break;

                case TimestampFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Timestamps.getHourOfDay(micros);
                    }
                    TimestampFormatUtils.appendHour241Padded(sink, hour);
                    break;

                // DAY
                case TimestampFormatCompiler.OP_DAY_ONE_DIGIT:
                case TimestampFormatCompiler.OP_DAY_GREEDY:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Timestamps.getMonthOfYear(micros, year, leap);
                        }

                        day = Timestamps.getDayOfMonth(micros, year, month, leap);
                    }
                    sink.put(day);
                    break;
                case TimestampFormatCompiler.OP_DAY_TWO_DIGITS:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Timestamps.getMonthOfYear(micros, year, leap);
                        }

                        day = Timestamps.getDayOfMonth(micros, year, month, leap);
                    }
                    TimestampFormatUtils.append0(sink, day);
                    break;

                case TimestampFormatCompiler.OP_DAY_NAME_LONG:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Timestamps.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getWeekday(dayOfWeek));
                    break;

                case TimestampFormatCompiler.OP_DAY_NAME_SHORT:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Timestamps.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getShortWeekday(dayOfWeek));
                    break;

                case TimestampFormatCompiler.OP_DAY_OF_WEEK:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Timestamps.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(dayOfWeek);
                    break;
                case TimestampFormatCompiler.OP_DAY_OF_YEAR:
                    sink.put(Timestamps.getDayOfYear(micros));
                    break;
                case TimestampFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    TimestampFormatUtils.append0(sink, Timestamps.getWeek(micros));
                    break;
                case TimestampFormatCompiler.OP_WEEK_OF_YEAR:
                    sink.put(Timestamps.getWeekOfYear(micros));
                    break;

                // MONTH
                case TimestampFormatCompiler.OP_MONTH_ONE_DIGIT:
                case TimestampFormatCompiler.OP_MONTH_GREEDY:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        month = Timestamps.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(month);
                    break;
                case TimestampFormatCompiler.OP_MONTH_TWO_DIGITS:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        month = Timestamps.getMonthOfYear(micros, year, leap);
                    }
                    TimestampFormatUtils.append0(sink, month);
                    break;

                case TimestampFormatCompiler.OP_MONTH_SHORT_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        month = Timestamps.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getShortMonth(month - 1));
                    break;
                case TimestampFormatCompiler.OP_MONTH_LONG_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Timestamps.getYear(micros);
                            leap = Timestamps.isLeapYear(year);
                        }

                        month = Timestamps.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getMonth(month - 1));
                    break;

                // YEAR
                case TimestampFormatCompiler.OP_YEAR_ONE_DIGIT:
                case TimestampFormatCompiler.OP_YEAR_GREEDY:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getYear(micros);
                        leap = Timestamps.isLeapYear(year);
                    }
                    TimestampFormatUtils.appendYear(sink, year);
                    break;
                case TimestampFormatCompiler.OP_YEAR_TWO_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getYear(micros);
                        leap = Timestamps.isLeapYear(year);
                    }
                    TimestampFormatUtils.appendYear0(sink, year % 100);
                    break;
                case TimestampFormatCompiler.OP_YEAR_THREE_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getYear(micros);
                        leap = Timestamps.isLeapYear(year);
                    }
                    TimestampFormatUtils.appendYear00(sink, year % 1000);
                    break;
                case TimestampFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getYear(micros);
                        leap = Timestamps.isLeapYear(year);
                    }
                    TimestampFormatUtils.appendYear000(sink, year);
                    break;
                case TimestampFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getIsoYear(micros);
                    }
                    TimestampFormatUtils.appendYear000(sink, year);
                    break;

                // ERA
                case TimestampFormatCompiler.OP_ERA:
                    if (year == Integer.MIN_VALUE) {
                        year = Timestamps.getYear(micros);
                        leap = Timestamps.isLeapYear(year);
                    }
                    TimestampFormatUtils.appendEra(sink, year, locale);
                    break;

                // TIMEZONE
                case TimestampFormatCompiler.OP_TIME_ZONE_SHORT:
                case TimestampFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case TimestampFormatCompiler.OP_TIME_ZONE_LONG:
                case TimestampFormatCompiler.OP_TIME_ZONE_RFC_822:
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
    public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
        int day = 1;
        int month = 1;
        int year = 1970;
        int hour = 0;
        int minute = 0;
        int second = 0;
        int millis = 0;
        int micros = 0;
        int week = -1;
        int era = 1;
        int timezone = -1;
        long offset = Long.MIN_VALUE;
        int hourType = TimestampFormatUtils.HOUR_24;
        int pos = lo;
        long l;
        int len;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {
            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case TimestampFormatCompiler.OP_AM_PM:
                    l = locale.matchAMPM(in, pos, hi);
                    hourType = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MICROS
                case TimestampFormatCompiler.OP_MICROS_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    micros = Numbers.parseInt(in, pos, ++pos);
                    break;

                case TimestampFormatCompiler.OP_MICROS_THREE_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 2, hi);
                    micros = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case TimestampFormatCompiler.OP_MICROS_GREEDY3:
                    l = Numbers.parseInt000Greedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case TimestampFormatCompiler.OP_MICROS_GREEDY6:
                    l = Numbers.parseLong000000Greedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case TimestampFormatCompiler.OP_NANOS_GREEDY9:
                    l = Timestamps.parseNanosAsMicrosGreedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // NANOS - ignoring parsed values
                case TimestampFormatCompiler.OP_NANOS_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos++, hi);
                    break;

                case TimestampFormatCompiler.OP_NANOS_THREE_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 2, hi);
                    pos += 3;
                    break;

                case TimestampFormatCompiler.OP_NANOS_GREEDY:
                    pos += Numbers.decodeHighInt(Numbers.parseIntSafely(in, pos, hi));
                    break;

                // MILLIS
                case TimestampFormatCompiler.OP_MILLIS_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    millis = Numbers.parseInt(in, pos, ++pos);
                    break;

                case TimestampFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 2, hi);
                    millis = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case TimestampFormatCompiler.OP_MILLIS_GREEDY:
                    l = Numbers.parseInt000Greedy(in, pos, hi);
                    millis = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SECOND
                case TimestampFormatCompiler.OP_SECOND_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    second = Numbers.parseInt(in, pos, ++pos);
                    break;

                case TimestampFormatCompiler.OP_SECOND_TWO_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    second = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case TimestampFormatCompiler.OP_SECOND_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    second = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MINUTE
                case TimestampFormatCompiler.OP_MINUTE_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    minute = Numbers.parseInt(in, pos, ++pos);
                    break;

                case TimestampFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    minute = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case TimestampFormatCompiler.OP_MINUTE_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    minute = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // HOUR - 12-hour clock convention
                // Note: both the 0-11 system and the 1-12 system are parsed exactly the same way.
                // In the 1-12 system, hour '12' is the same as hour '0' in the 0-11 system.
                // All other values are the same.
                // In fact, the 1-12 system should be called the 12-11 system, since it maps 12 to 0.
                // Comparison table:
                // 0-11 | 12-11
                // 0    | 12
                // 1    | 1
                // 2    | 2
                // [...]
                //
                // 11   | 11
                // This means that in both the 0-11 and 1-12 systems, we can use the same parsing logic and later treat 12 as if it were 0.
                case TimestampFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case TimestampFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    if (hourType == TimestampFormatUtils.HOUR_24) {
                        hourType = TimestampFormatUtils.HOUR_AM;
                    }
                    break;

                case TimestampFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                case TimestampFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    if (hourType == TimestampFormatUtils.HOUR_24) {
                        hourType = TimestampFormatUtils.HOUR_AM;
                    }
                    break;

                case TimestampFormatCompiler.OP_HOUR_12_GREEDY:
                case TimestampFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == TimestampFormatUtils.HOUR_24) {
                        hourType = TimestampFormatUtils.HOUR_AM;
                    }
                    break;

                // HOUR
                case TimestampFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case TimestampFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    break;
                case TimestampFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                case TimestampFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case TimestampFormatCompiler.OP_HOUR_24_GREEDY:
                case TimestampFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // DAY
                case TimestampFormatCompiler.OP_DAY_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    day = Numbers.parseInt(in, pos, ++pos);
                    break;
                case TimestampFormatCompiler.OP_DAY_TWO_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    day = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case TimestampFormatCompiler.OP_DAY_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    day = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case TimestampFormatCompiler.OP_DAY_NAME_LONG:
                case TimestampFormatCompiler.OP_DAY_NAME_SHORT:
                    l = locale.matchWeekday(in, pos, hi);
                    // ignore weekday
                    pos += Numbers.decodeHighInt(l);
                    break;
                case TimestampFormatCompiler.OP_DAY_OF_YEAR:
                case TimestampFormatCompiler.OP_WEEK_OF_YEAR:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    pos += Numbers.decodeHighInt(l);
                    break;
                case TimestampFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    week = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case TimestampFormatCompiler.OP_DAY_OF_WEEK:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    // ignore weekday
                    Numbers.parseInt(in, pos, ++pos);
                    break;

                // MONTH

                case TimestampFormatCompiler.OP_MONTH_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    month = Numbers.parseInt(in, pos, ++pos);
                    break;
                case TimestampFormatCompiler.OP_MONTH_TWO_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    month = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case TimestampFormatCompiler.OP_MONTH_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    month = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case TimestampFormatCompiler.OP_MONTH_SHORT_NAME:
                case TimestampFormatCompiler.OP_MONTH_LONG_NAME:
                    l = locale.matchMonth(in, pos, hi);
                    month = Numbers.decodeLowInt(l) + 1;
                    pos += Numbers.decodeHighInt(l);
                    break;

                // YEAR

                case TimestampFormatCompiler.OP_YEAR_ONE_DIGIT:
                    TimestampFormatUtils.assertRemaining(pos, hi);
                    year = Numbers.parseInt(in, pos, ++pos);
                    break;
                case TimestampFormatCompiler.OP_YEAR_TWO_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 1, hi);
                    year = TimestampFormatUtils.adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    break;
                case TimestampFormatCompiler.OP_YEAR_THREE_DIGITS:
                    TimestampFormatUtils.assertRemaining(pos + 2, hi);
                    year = Numbers.parseInt(in, pos, pos += 3);
                    break;
                case TimestampFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                case TimestampFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (pos < hi && in.charAt(pos) == '-') {
                        TimestampFormatUtils.assertRemaining(pos + 4, hi);
                        year = -Numbers.parseInt(in, pos + 1, pos += 5);
                    } else {
                        TimestampFormatUtils.assertRemaining(pos + 3, hi);
                        year = Numbers.parseInt(in, pos, pos += 4);
                    }
                    break;
                case TimestampFormatCompiler.OP_YEAR_GREEDY:
                    l = TimestampFormatUtils.parseYearGreedy(in, pos, hi);
                    year = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // ERA
                case TimestampFormatCompiler.OP_ERA:
                    l = locale.matchEra(in, pos, hi);
                    era = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // TIMEZONE
                case TimestampFormatCompiler.OP_TIME_ZONE_SHORT:
                case TimestampFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case TimestampFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case TimestampFormatCompiler.OP_TIME_ZONE_LONG:
                case TimestampFormatCompiler.OP_TIME_ZONE_RFC_822:

                    l = Timestamps.parseOffset(in, pos, hi);
                    if (l == Long.MIN_VALUE) {
                        l = locale.matchZone(in, pos, hi);
                        timezone = Numbers.decodeLowInt(l);
                    } else {
                        offset = Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS;
                    }
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SEPARATORS
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    len = delimiter.length();
                    if (len == 1) {
                        TimestampFormatUtils.assertChar(delimiter.charAt(0), in, pos++, hi);
                    } else {
                        pos = TimestampFormatUtils.assertString(delimiter, len, in, pos, hi);
                    }
                    break;
            }
        }

        TimestampFormatUtils.assertNoTail(pos, hi);

        return TimestampFormatUtils.compute(locale, era, year, month, week, day, hour, minute, second, millis, micros, timezone, offset, hourType);
    }
}
