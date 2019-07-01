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

import com.questdb.std.Chars;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.Os;
import com.questdb.std.str.CharSink;

public class DateFormatUtils {
    public static final int HOUR_24 = 2;
    public static final int HOUR_PM = 1;
    public static final int HOUR_AM = 0;
    public static final DateFormat UTC_FORMAT;
    public static final DateFormat USEC_UTC_FORMAT;
    public static final DateFormat PG_TIMESTAMP_FORMAT;
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
    private static final DateFormat HTTP_FORMAT;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    static int prevCenturyLow;
    private static long newYear;

    static {
        updateReferenceYear(Os.currentTimeMicros());
        DateFormatCompiler compiler = new DateFormatCompiler();
        UTC_FORMAT = compiler.compile(UTC_PATTERN);
        HTTP_FORMAT = compiler.compile("E, d MMM yyyy HH:mm:ss Z");
        USEC_UTC_FORMAT = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUz");
        PG_TIMESTAMP_FORMAT = compiler.compile("yyyy-MM-dd HH:mm:ss.SSSUUU");
    }

    public static void append0(CharSink sink, int val) {
        if (Math.abs(val) < 10) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    public static void append00(CharSink sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.put('0').put('0');
        } else if (v < 100) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    public static void append000(CharSink sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.put('0').put('0').put('0');
        } else if (v < 100) {
            sink.put('0').put('0');
        } else if (v < 1000) {
            sink.put('0');
        }
        Numbers.append(sink, val);
    }

    // YYYY-MM-DDThh:mm:ss.mmmZ
    public static void appendDateTime(CharSink sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(micros, defaultLocale, "Z", sink);
    }

    // YYYY-MM-DDThh:mm:ss.mmmuuuZ
    public static void appendDateTimeUSec(CharSink sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        USEC_UTC_FORMAT.format(micros, defaultLocale, "Z", sink);
    }

    // YYYY-MM-DD
    public static void formatDashYYYYMMDD(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
        append0(sink.put('-'), Dates.getDayOfMonth(millis, y, m, l));
    }

    public static void formatHTTP(CharSink sink, long millis) {
        HTTP_FORMAT.format(millis, defaultLocale, "GMT", sink);
    }

    // YYYY-MM
    public static void formatYYYYMM(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        int m = Dates.getMonthOfYear(millis, y, Dates.isLeapYear(y));
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
    }

    // YYYYMMDD
    public static void formatYYYYMMDD(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink, m);
        append0(sink, Dates.getDayOfMonth(millis, y, m, l));
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTime(CharSequence seq) throws NumericException {
        return parseDateTime(seq, 0, seq.length());
    }

    // YYYY-MM-DDThh:mm:ss.mmm
    public static long parseDateTimeQuiet(CharSequence seq) {
        try {
            return parseDateTime(seq, 0, seq.length());
        } catch (NumericException e) {
            return Long.MIN_VALUE;
        }
    }

    // YYYY-MM-DDThh:mm:ss.mmmnnn
    public static long parseTimestamp(CharSequence seq) throws NumericException {
        return parseTimestamp(seq, 0, seq.length());
    }

    public static long tryParse(CharSequence s) throws NumericException {
        return tryParse(s, 0, s.length());
    }

    public static long tryParse(CharSequence s, int lo, int lim) throws NumericException {
        return parseDateTime(s, lo, lim);
    }

    public static void updateReferenceYear(long micros) {
        referenceYear = micros;

        int referenceYear = Dates.getYear(micros);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        prevCenturyLow = thisCenturyLow - 100;
        newYear = Dates.endOfYear(referenceYear);
    }

    static void appendAmPm(CharSink sink, int hour, DateLocale locale) {
        if (hour < 12) {
            sink.put(locale.getAMPM(0));
        } else {
            sink.put(locale.getAMPM(1));
        }
    }

    static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.INSTANCE;
        }
    }

    static int assertString(CharSequence delimiter, int len, CharSequence in, int pos, int hi) throws NumericException {
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

    static void assertRemaining(int pos, int hi) throws NumericException {
        if (pos < hi) {
            return;
        }
        throw NumericException.INSTANCE;
    }

    static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.INSTANCE;
        }
    }

    static long compute(
            DateLocale locale,
            int era,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int micros,
            int timezone,
            long offset,
            int hourType) throws NumericException {

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

        long datetime = Dates.yearMicros(year, leap)
                + Dates.monthOfYearMicros(month, leap)
                + (day - 1) * Dates.DAY_MICROS
                + hour * Dates.HOUR_MICROS
                + minute * Dates.MINUTE_MICROS
                + second * Dates.SECOND_MICROS
                + millis * Dates.MILLI_MICROS
                + micros;

        if (timezone > -1) {
            datetime -= locale.getZoneRules(timezone).getOffset(datetime, year, leap);
        } else if (offset > Long.MIN_VALUE) {
            datetime -= offset;
        }

        return datetime;
    }

    static long parseYearGreedy(CharSequence in, int pos, int hi) throws NumericException {
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

    static int adjustYear(int year) {
        return (year < thisCenturyLimit ? thisCenturyLow : prevCenturyLow) + year;
    }

    static void appendHour12(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour);
        } else {
            sink.put(hour - 12);
        }
    }

    static void appendHour12Padded(CharSink sink, int hour) {
        if (hour < 12) {
            append0(sink, hour);
        } else {
            append0(sink, hour - 12);
        }
    }

    static void appendHour121Padded(CharSink sink, int hour) {
        if (hour < 12) {
            append0(sink, hour + 1);
        } else {
            append0(sink, hour - 11);
        }
    }

    static void appendHour121(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour + 1);
        } else {
            sink.put(hour - 11);
        }
    }

    static void appendEra(CharSink sink, int year, DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    private static long parseDateTime(CharSequence seq, int lo, int lim) throws NumericException {
        return UTC_FORMAT.parse(seq, lo, lim, defaultLocale);
    }

    private static long parseTimestamp(CharSequence seq, int lo, int lim) throws NumericException {
        return USEC_UTC_FORMAT.parse(seq, lo, lim, defaultLocale);
    }
}
