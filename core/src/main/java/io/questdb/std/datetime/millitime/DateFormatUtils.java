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

package io.questdb.std.datetime.millitime;

import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.str.CharSinkBase;
import org.jetbrains.annotations.NotNull;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MILLIS;

public class DateFormatUtils {
    public static final DateLocale EN_LOCALE = DateLocaleFactory.INSTANCE.getLocale("en");
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_PM = 1;
    public static final DateFormat PG_DATE_FORMAT;
    public static final DateFormat PG_DATE_MILLI_TIME_Z_FORMAT;
    public static final DateFormat PG_DATE_MILLI_TIME_Z_PRINT_FORMAT;
    public static final DateFormat PG_DATE_Z_FORMAT;
    public static final DateFormat UTC_FORMAT;
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    private static final DateFormat HTTP_FORMAT;
    private static final int DATE_FORMATS_SIZE;
    private static final DateFormat[] DATE_FORMATS;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static long newYear;

    public static int adjustYear(int year) {
        return thisCenturyLow + year;
    }

    public static int adjustYearMillennium(int year) {
        return thisCenturyLow - thisCenturyLow % 1000 + year;
    }

    public static void append0(@NotNull CharSinkBase<?> sink, int val) {
        if (Math.abs(val) < 10) {
            sink.putAscii('0');
        }
        sink.put(val);
    }

    public static void append00(@NotNull CharSinkBase<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0');
        }
        sink.put(val);
    }

    public static void appendAmPm(@NotNull CharSinkBase<?> sink, int hour, @NotNull DateLocale locale) {
        if (hour < 12) {
            sink.put(locale.getAMPM(0));
        } else {
            sink.put(locale.getAMPM(1));
        }
    }

    // YYYY-MM-DDThh:mm:ss.mmmmZ
    public static void appendDateTime(@NotNull CharSinkBase<?> sink, long millis) {
        if (millis == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(millis, EN_LOCALE, "Z", sink);
    }

    public static void appendEra(@NotNull CharSinkBase<?> sink, int year, @NotNull DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    public static void appendHour12(@NotNull CharSinkBase<?> sink, int hour) {
        if (hour < 12) {
            Numbers.append(sink, hour);
        } else {
            Numbers.append(sink, hour - 12);
        }
    }

    public static void appendHour121(@NotNull CharSinkBase<?> sink, int hour) {
        if (hour < 12) {
            Numbers.append(sink, hour + 1);
        } else {
            Numbers.append(sink, hour - 11);
        }
    }

    public static void appendHour121Padded(@NotNull CharSinkBase<?> sink, int hour) {
        if (hour < 12) {
            append0(sink, hour + 1);
        } else {
            append0(sink, hour - 11);
        }
    }

    public static void appendHour12Padded(@NotNull CharSinkBase<?> sink, int hour) {
        if (hour < 12) {
            append0(sink, hour);
        } else {
            append0(sink, hour - 12);
        }
    }

    public static void assertChar(char c, @NotNull CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.INSTANCE;
        }
    }

    public static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.INSTANCE;
        }
    }

    public static void assertRemaining(int pos, int hi) throws NumericException {
        if (pos < hi) {
            return;
        }
        throw NumericException.INSTANCE;
    }

    public static int assertString(@NotNull CharSequence delimiter, int len, @NotNull CharSequence in, int pos, int hi) throws NumericException {
        if (delimiter.charAt(0) == '\'' && delimiter.charAt(len - 1) == '\'') {
            assertRemaining(pos + len - 3, hi);
            if (!Chars.equals(delimiter, 1, len - 1, in, pos, pos + len - 2)) {
                throw NumericException.INSTANCE;
            }
            return pos + len - 2;
        } else {
            assertRemaining(pos + len - 1, hi);
            if (!Chars.equals(delimiter, in, pos, pos + len)) {
                throw NumericException.INSTANCE;
            }
            return pos + len;
        }
    }

    public static long compute(
            @NotNull DateLocale locale,
            int era,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int timezone,
            long offset,
            int hourType
    ) throws NumericException {
        if (era == 0) {
            year = -(year - 1);
        }

        boolean leap = Dates.isLeapYear(year);

        // wrong month
        if (month < 1 || month > 12) {
            throw NumericException.INSTANCE;
        }

        switch (hourType) {
            case HOUR_PM:
                hour += 12;
            case HOUR_24:
                // wrong hour
                if (hour < 0 || hour > 23) {
                    throw NumericException.INSTANCE;
                }
                break;
            default:
                // wrong 12-hour clock hour
                if (hour < 0 || hour > 11) {
                    throw NumericException.INSTANCE;
                }
        }

        // wrong day of month
        if (day < 1 || day > Dates.getDaysPerMonth(month, leap)) {
            throw NumericException.INSTANCE;
        }

        if (minute < 0 || minute > 59) {
            throw NumericException.INSTANCE;
        }

        if (second < 0 || second > 59) {
            throw NumericException.INSTANCE;
        }

        long datetime = Dates.yearMillis(year, leap)
                + Dates.monthOfYearMillis(month, leap)
                + (day - 1) * Dates.DAY_MILLIS
                + hour * Dates.HOUR_MILLIS
                + minute * Dates.MINUTE_MILLIS
                + second * Dates.SECOND_MILLIS
                + millis;

        if (timezone > -1) {
            datetime -= locale.getZoneRules(timezone, RESOLUTION_MILLIS).getOffset(datetime, year, leap);
        } else if (offset > Long.MIN_VALUE) {
            datetime -= offset;
        }

        return datetime;
    }

    // YYYY-MM-DD
    public static void formatDashYYYYMMDD(@NotNull CharSinkBase<?> sink, long millis) {
        int y = Dates.getYear(millis);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink.putAscii('-'), m);
        append0(sink.putAscii('-'), Dates.getDayOfMonth(millis, y, m, l));
    }

    public static void formatHTTP(@NotNull CharSinkBase<?> sink, long millis) {
        HTTP_FORMAT.format(millis, EN_LOCALE, "GMT", sink);
    }

    // YYYY-MM
    public static void formatYYYYMM(@NotNull CharSinkBase<?> sink, long millis) {
        int y = Dates.getYear(millis);
        int m = Dates.getMonthOfYear(millis, y, Dates.isLeapYear(y));
        Numbers.append(sink, y);
        append0(sink.putAscii('-'), m);
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseUTCDate(@NotNull CharSequence value) throws NumericException {
        return UTC_FORMAT.parse(value, 0, value.length(), EN_LOCALE);
    }

    /**
     * Parse date and return number of <b>milliseconds</b> since epoch.
     * <p>
     * The method tries to parse date using a number of formats which are supported by QuestDB
     * when inserting String into DATE column.
     *
     * @param value date string
     * @return number of milliseconds since epoch
     * @throws NumericException if date cannot be parsed
     */
    public static long parseDate(CharSequence value) throws NumericException {
        if (value == null) {
            return Numbers.LONG_NaN;
        }

        final int hi = value.length();
        for (int i = 0; i < DATE_FORMATS_SIZE; i++) {
            try {
                return DATE_FORMATS[i].parse(value, 0, hi, DateFormatUtils.EN_LOCALE);
            } catch (NumericException ignore) {
            }
        }
        return Numbers.parseLong(value, 0, hi);
    }

    public static long parseYearGreedy(@NotNull CharSequence in, int pos, int hi) throws NumericException {
        long l = Numbers.parseIntSafely(in, pos, hi);
        int len = Numbers.decodeHighInt(l);
        int year;
        if (len == 2) {
            year = adjustYear(Numbers.decodeLowInt(l));
        } else {
            year = Numbers.decodeLowInt(l);
        }

        return Numbers.encodeLowHighInts(year, len);
    }

    public static void updateReferenceYear(long millis) {
        referenceYear = millis;

        int referenceYear = Dates.getYear(millis);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        newYear = Dates.endOfYear(referenceYear);
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
        final DateFormatCompiler compiler = new DateFormatCompiler();
        UTC_FORMAT = compiler.compile(UTC_PATTERN);
        HTTP_FORMAT = compiler.compile("E, d MMM yyyy HH:mm:ss Z");
        PG_DATE_FORMAT = compiler.compile("y-MM-dd");
        PG_DATE_Z_FORMAT = compiler.compile("y-MM-dd z");
        PG_DATE_MILLI_TIME_Z_FORMAT = compiler.compile("y-MM-dd HH:mm:ss.Sz");
        PG_DATE_MILLI_TIME_Z_PRINT_FORMAT = compiler.compile("y-MM-dd HH:mm:ss.SSSz");

        final DateFormat pgDateTimeFormat = compiler.compile("y-MM-dd HH:mm:ssz");
        DATE_FORMATS = new DateFormat[]{
                pgDateTimeFormat,
                PG_DATE_FORMAT,
                PG_DATE_Z_FORMAT,
                PG_DATE_MILLI_TIME_Z_FORMAT,
                UTC_FORMAT
        };
        DATE_FORMATS_SIZE = DATE_FORMATS.length;
    }
}
