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

import io.questdb.cairo.ColumnType;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.AbstractDateFormat;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GenericMicrosFormat extends AbstractDateFormat {
    private final IntList compiledOps;
    private final ObjList<String> delimiters;

    public GenericMicrosFormat(IntList compiledOps, ObjList<String> delimiters) {
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
                case MicrosFormatCompiler.OP_AM_PM:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendAmPm(sink, hour, locale);
                    break;

                // MICROS
                case MicrosFormatCompiler.OP_MICROS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MICROS_GREEDY3:
                    if (micros0 == -1) {
                        micros0 = Micros.getMicrosOfMilli(micros);
                    }
                    sink.put(micros0);
                    break;

                case MicrosFormatCompiler.OP_MICROS_GREEDY6:
                case MicrosFormatCompiler.OP_NANOS_GREEDY9:
                    if (micros0 == -1) {
                        micros0 = Micros.getMicrosOfSecond(micros);
                    }
                    MicrosFormatUtils.append00000(sink, micros0);
                    break;

                case MicrosFormatCompiler.OP_MICROS_THREE_DIGITS:
                    if (micros0 == -1) {
                        micros0 = Micros.getMicrosOfMilli(micros);
                    }
                    MicrosFormatUtils.append00(sink, micros0);
                    break;

                // MILLIS
                case MicrosFormatCompiler.OP_MILLIS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MILLIS_GREEDY:
                    if (millis == -1) {
                        millis = Micros.getMillisOfSecond(micros);
                    }
                    sink.put(millis);
                    break;

                case MicrosFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    if (millis == -1) {
                        millis = Micros.getMillisOfSecond(micros);
                    }
                    MicrosFormatUtils.append00(sink, millis);
                    break;

                // NANOS
                case MicrosFormatCompiler.OP_NANOS_ONE_DIGIT:
                case MicrosFormatCompiler.OP_NANOS_GREEDY:
                    sink.put(0);
                    break;

                case MicrosFormatCompiler.OP_NANOS_THREE_DIGITS:
                    MicrosFormatUtils.append00(sink, 0);
                    break;

                // SECOND
                case MicrosFormatCompiler.OP_SECOND_ONE_DIGIT:
                case MicrosFormatCompiler.OP_SECOND_GREEDY:
                    if (second == -1) {
                        second = Micros.getSecondOfMinute(micros);
                    }
                    sink.put(second);
                    break;

                case MicrosFormatCompiler.OP_SECOND_TWO_DIGITS:
                    if (second == -1) {
                        second = Micros.getSecondOfMinute(micros);
                    }
                    MicrosFormatUtils.append0(sink, second);
                    break;

                // MINUTE
                case MicrosFormatCompiler.OP_MINUTE_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MINUTE_GREEDY:
                    if (minute == -1) {
                        minute = Micros.getMinuteOfHour(micros);
                    }
                    sink.put(minute);
                    break;

                case MicrosFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    if (minute == -1) {
                        minute = Micros.getMinuteOfHour(micros);
                    }
                    MicrosFormatUtils.append0(sink, minute);
                    break;


                // HOUR (0-11)
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour12(sink, hour);
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour12Padded(sink, hour);
                    break;

                // HOUR (1-12)
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour121(sink, hour);
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour121Padded(sink, hour);
                    break;

                // HOUR (0-23)
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    sink.put(hour);
                    break;

                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.append0(sink, hour);
                    break;

                // HOUR (1 - 24)
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour241(sink, hour);
                    break;

                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    if (hour == -1) {
                        hour = Micros.getHourOfDay(micros);
                    }
                    MicrosFormatUtils.appendHour241Padded(sink, hour);
                    break;

                // DAY
                case MicrosFormatCompiler.OP_DAY_ONE_DIGIT:
                case MicrosFormatCompiler.OP_DAY_GREEDY:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Micros.getMonthOfYear(micros, year, leap);
                        }

                        day = Micros.getDayOfMonth(micros, year, month, leap);
                    }
                    sink.put(day);
                    break;
                case MicrosFormatCompiler.OP_DAY_TWO_DIGITS:
                    if (day == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        if (month == -1) {
                            month = Micros.getMonthOfYear(micros, year, leap);
                        }

                        day = Micros.getDayOfMonth(micros, year, month, leap);
                    }
                    MicrosFormatUtils.append0(sink, day);
                    break;

                case MicrosFormatCompiler.OP_DAY_NAME_LONG:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Micros.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getWeekday(dayOfWeek));
                    break;

                case MicrosFormatCompiler.OP_DAY_NAME_SHORT:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Micros.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(locale.getShortWeekday(dayOfWeek));
                    break;

                case MicrosFormatCompiler.OP_DAY_OF_WEEK:
                    if (dayOfWeek == -1) {
                        dayOfWeek = Micros.getDayOfWeekSundayFirst(micros);
                    }
                    sink.put(dayOfWeek);
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_YEAR:
                    sink.put(Micros.getDayOfYear(micros));
                    break;
                case MicrosFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    MicrosFormatUtils.append0(sink, Micros.getWeek(micros));
                    break;
                case MicrosFormatCompiler.OP_WEEK_OF_YEAR:
                    sink.put(Micros.getWeekOfYear(micros));
                    break;

                // MONTH
                case MicrosFormatCompiler.OP_MONTH_ONE_DIGIT:
                case MicrosFormatCompiler.OP_MONTH_GREEDY:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        month = Micros.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(month);
                    break;
                case MicrosFormatCompiler.OP_MONTH_TWO_DIGITS:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        month = Micros.getMonthOfYear(micros, year, leap);
                    }
                    MicrosFormatUtils.append0(sink, month);
                    break;

                case MicrosFormatCompiler.OP_MONTH_SHORT_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        month = Micros.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getShortMonth(month - 1));
                    break;
                case MicrosFormatCompiler.OP_MONTH_LONG_NAME:
                    if (month == -1) {
                        if (year == Integer.MIN_VALUE) {
                            year = Micros.getYear(micros);
                            leap = CommonUtils.isLeapYear(year);
                        }

                        month = Micros.getMonthOfYear(micros, year, leap);
                    }
                    sink.put(locale.getMonth(month - 1));
                    break;

                // YEAR
                case MicrosFormatCompiler.OP_YEAR_ONE_DIGIT:
                case MicrosFormatCompiler.OP_YEAR_GREEDY:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getYear(micros);
                        leap = CommonUtils.isLeapYear(year);
                    }
                    MicrosFormatUtils.appendYear(sink, year);
                    break;
                case MicrosFormatCompiler.OP_YEAR_TWO_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getYear(micros);
                        leap = CommonUtils.isLeapYear(year);
                    }
                    MicrosFormatUtils.appendYear0(sink, year % 100);
                    break;
                case MicrosFormatCompiler.OP_YEAR_THREE_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getYear(micros);
                        leap = CommonUtils.isLeapYear(year);
                    }
                    MicrosFormatUtils.appendYear00(sink, year % 1000);
                    break;
                case MicrosFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getYear(micros);
                        leap = CommonUtils.isLeapYear(year);
                    }
                    MicrosFormatUtils.appendYear000(sink, year);
                    break;
                case MicrosFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getIsoYear(micros);
                    }
                    MicrosFormatUtils.appendYear000(sink, year);
                    break;

                // ERA
                case MicrosFormatCompiler.OP_ERA:
                    if (year == Integer.MIN_VALUE) {
                        year = Micros.getYear(micros);
                        leap = CommonUtils.isLeapYear(year);
                    }
                    MicrosFormatUtils.appendEra(sink, year, locale);
                    break;

                // TIMEZONE
                case MicrosFormatCompiler.OP_TIME_ZONE_SHORT:
                case MicrosFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case MicrosFormatCompiler.OP_TIME_ZONE_LONG:
                case MicrosFormatCompiler.OP_TIME_ZONE_RFC_822:
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
    public int getColumnType() {
        return ColumnType.TIMESTAMP_MICRO;
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
        long offsetMinutes = Long.MIN_VALUE;
        int hourType = CommonUtils.HOUR_24;
        int pos = lo;
        long l;
        int len;

        for (int i = 0, n = compiledOps.size(); i < n; i++) {
            int op = compiledOps.getQuick(i);
            switch (op) {

                // AM/PM
                case MicrosFormatCompiler.OP_AM_PM:
                    l = locale.matchAMPM(in, pos, hi);
                    hourType = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MICROS
                case MicrosFormatCompiler.OP_MICROS_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    micros = Numbers.parseInt(in, pos, ++pos);
                    break;

                case MicrosFormatCompiler.OP_MICROS_THREE_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 2, hi);
                    micros = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case MicrosFormatCompiler.OP_MICROS_GREEDY3:
                    l = Numbers.parseInt000Greedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case MicrosFormatCompiler.OP_MICROS_GREEDY6:
                    l = Numbers.parseLong000000Greedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case MicrosFormatCompiler.OP_NANOS_GREEDY9:
                    l = Micros.parseNanosAsMicrosGreedy(in, pos, hi);
                    micros = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // NANOS - ignoring parsed values
                case MicrosFormatCompiler.OP_NANOS_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos++, hi);
                    break;

                case MicrosFormatCompiler.OP_NANOS_THREE_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 2, hi);
                    pos += 3;
                    break;

                case MicrosFormatCompiler.OP_NANOS_GREEDY:
                    pos += Numbers.decodeHighInt(Numbers.parseIntSafely(in, pos, hi));
                    break;

                // MILLIS
                case MicrosFormatCompiler.OP_MILLIS_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    millis = Numbers.parseInt(in, pos, ++pos);
                    break;

                case MicrosFormatCompiler.OP_MILLIS_THREE_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 2, hi);
                    millis = Numbers.parseInt(in, pos, pos += 3);
                    break;

                case MicrosFormatCompiler.OP_MILLIS_GREEDY:
                    l = Numbers.parseInt000Greedy(in, pos, hi);
                    millis = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SECOND
                case MicrosFormatCompiler.OP_SECOND_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    second = Numbers.parseInt(in, pos, ++pos);
                    break;

                case MicrosFormatCompiler.OP_SECOND_TWO_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    second = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case MicrosFormatCompiler.OP_SECOND_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    second = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // MINUTE
                case MicrosFormatCompiler.OP_MINUTE_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    minute = Numbers.parseInt(in, pos, ++pos);
                    break;

                case MicrosFormatCompiler.OP_MINUTE_TWO_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    minute = Numbers.parseInt(in, pos, pos += 2);
                    break;

                case MicrosFormatCompiler.OP_MINUTE_GREEDY:
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
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_12_ONE_DIGIT_ONE_BASED:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    if (hourType == CommonUtils.HOUR_24) {
                        hourType = CommonUtils.HOUR_AM;
                    }
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS:
                case MicrosFormatCompiler.OP_HOUR_12_TWO_DIGITS_ONE_BASED:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    if (hourType == CommonUtils.HOUR_24) {
                        hourType = CommonUtils.HOUR_AM;
                    }
                    break;

                case MicrosFormatCompiler.OP_HOUR_12_GREEDY:
                case MicrosFormatCompiler.OP_HOUR_12_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    if (hourType == CommonUtils.HOUR_24) {
                        hourType = CommonUtils.HOUR_AM;
                    }
                    break;

                // HOUR
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT:
                case MicrosFormatCompiler.OP_HOUR_24_ONE_DIGIT_ONE_BASED:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    hour = Numbers.parseInt(in, pos, ++pos);
                    break;
                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS:
                case MicrosFormatCompiler.OP_HOUR_24_TWO_DIGITS_ONE_BASED:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    hour = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY:
                case MicrosFormatCompiler.OP_HOUR_24_GREEDY_ONE_BASED:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    hour = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // DAY
                case MicrosFormatCompiler.OP_DAY_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    day = Numbers.parseInt(in, pos, ++pos);
                    break;
                case MicrosFormatCompiler.OP_DAY_TWO_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    day = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case MicrosFormatCompiler.OP_DAY_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    day = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case MicrosFormatCompiler.OP_DAY_NAME_LONG:
                case MicrosFormatCompiler.OP_DAY_NAME_SHORT:
                    l = locale.matchWeekday(in, pos, hi);
                    // ignore weekday
                    pos += Numbers.decodeHighInt(l);
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_YEAR:
                case MicrosFormatCompiler.OP_WEEK_OF_YEAR:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    pos += Numbers.decodeHighInt(l);
                    break;
                case MicrosFormatCompiler.OP_ISO_WEEK_OF_YEAR:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    week = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case MicrosFormatCompiler.OP_DAY_OF_WEEK:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    // ignore weekday
                    Numbers.parseInt(in, pos, ++pos);
                    break;

                // MONTH

                case MicrosFormatCompiler.OP_MONTH_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    month = Numbers.parseInt(in, pos, ++pos);
                    break;
                case MicrosFormatCompiler.OP_MONTH_TWO_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    month = Numbers.parseInt(in, pos, pos += 2);
                    break;
                case MicrosFormatCompiler.OP_MONTH_GREEDY:
                    l = Numbers.parseIntSafely(in, pos, hi);
                    month = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                case MicrosFormatCompiler.OP_MONTH_SHORT_NAME:
                case MicrosFormatCompiler.OP_MONTH_LONG_NAME:
                    l = locale.matchMonth(in, pos, hi);
                    month = Numbers.decodeLowInt(l) + 1;
                    pos += Numbers.decodeHighInt(l);
                    break;

                // YEAR

                case MicrosFormatCompiler.OP_YEAR_ONE_DIGIT:
                    MicrosFormatUtils.assertRemaining(pos, hi);
                    year = Numbers.parseInt(in, pos, ++pos);
                    break;
                case MicrosFormatCompiler.OP_YEAR_TWO_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 1, hi);
                    year = MicrosFormatUtils.adjustYear(Numbers.parseInt(in, pos, pos += 2));
                    break;
                case MicrosFormatCompiler.OP_YEAR_THREE_DIGITS:
                    MicrosFormatUtils.assertRemaining(pos + 2, hi);
                    year = Numbers.parseInt(in, pos, pos += 3);
                    break;
                case MicrosFormatCompiler.OP_YEAR_ISO_FOUR_DIGITS:
                case MicrosFormatCompiler.OP_YEAR_FOUR_DIGITS:
                    if (pos < hi && in.charAt(pos) == '-') {
                        MicrosFormatUtils.assertRemaining(pos + 4, hi);
                        year = -Numbers.parseInt(in, pos + 1, pos += 5);
                    } else {
                        MicrosFormatUtils.assertRemaining(pos + 3, hi);
                        year = Numbers.parseInt(in, pos, pos += 4);
                    }
                    break;
                case MicrosFormatCompiler.OP_YEAR_GREEDY:
                    l = MicrosFormatUtils.parseYearGreedy(in, pos, hi);
                    year = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // ERA
                case MicrosFormatCompiler.OP_ERA:
                    l = locale.matchEra(in, pos, hi);
                    era = Numbers.decodeLowInt(l);
                    pos += Numbers.decodeHighInt(l);
                    break;

                // TIMEZONE
                case MicrosFormatCompiler.OP_TIME_ZONE_SHORT:
                case MicrosFormatCompiler.OP_TIME_ZONE_GMT_BASED:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_1:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_2:
                case MicrosFormatCompiler.OP_TIME_ZONE_ISO_8601_3:
                case MicrosFormatCompiler.OP_TIME_ZONE_LONG:
                case MicrosFormatCompiler.OP_TIME_ZONE_RFC_822:

                    l = Dates.parseOffset(in, pos, hi);
                    if (l == Long.MIN_VALUE) {
                        l = locale.matchZone(in, pos, hi);
                        timezone = Numbers.decodeLowInt(l);
                    } else {
                        offsetMinutes = Numbers.decodeLowInt(l);
                    }
                    pos += Numbers.decodeHighInt(l);
                    break;

                // SEPARATORS
                default:
                    String delimiter = delimiters.getQuick(-op - 1);
                    len = delimiter.length();
                    if (len == 1) {
                        MicrosFormatUtils.assertChar(delimiter.charAt(0), in, pos++, hi);
                    } else {
                        pos = MicrosFormatUtils.assertString(delimiter, len, in, pos, hi);
                    }
                    break;
            }
        }

        MicrosFormatUtils.assertNoTail(pos, hi);

        return MicrosFormatUtils.compute(
                locale,
                era,
                year,
                month,
                week,
                day,
                hour,
                minute,
                second,
                millis,
                micros,
                timezone,
                offsetMinutes,
                hourType
        );
    }
}
