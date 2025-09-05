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

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class MicrosFormatUtils {
    public static final DateFormat DAY_FORMAT;
    public static final DateFormat GREEDY_MILLIS1_UTC_FORMAT;
    public static final DateFormat GREEDY_MILLIS2_UTC_FORMAT;
    public static final DateFormat HOUR_FORMAT;
    public static final DateFormat MONTH_FORMAT;
    public static final DateFormat NANOS_UTC_FORMAT;
    public static final DateFormat PG_TIMESTAMP_FORMAT;
    public static final DateFormat PG_TIMESTAMP_MILLI_TIME_Z_FORMAT;
    public static final DateFormat PG_TIMESTAMP_TIME_Z_FORMAT;
    public static final DateFormat SEC_UTC_FORMAT;
    public static final DateFormat USEC_UTC_FORMAT;
    public static final DateFormat UTC_FORMAT;
    public static final DateFormat WEEK_FORMAT;
    public static final DateFormat YEAR_FORMAT;
    private static final DateFormat[] FORMATS;
    private static final DateFormat[] HTTP_FORMATS;
    static int prevCenturyLow;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static long newYear;

    public static int adjustYear(int year) {
        return thisCenturyLow + year;
    }

    public static void append0(@NotNull CharSink<?> sink, int val) {
        DateFormatUtils.append0(sink, val);
    }

    public static void append00(@NotNull CharSink<?> sink, int val) {
        DateFormatUtils.append00(sink, val);
    }

    public static void append00000(@NotNull CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0').putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0').putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 1000) {
            sink.putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 10000) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 100000) {
            sink.putAscii('0');
        }
        sink.put(val);
    }

    public static void appendAmPm(@NotNull CharSink<?> sink, int hour, @NotNull DateLocale locale) {
        if (hour < 12) {
            sink.putAscii(locale.getAMPM(0));
        } else {
            sink.putAscii(locale.getAMPM(1));
        }
    }

    // YYYY-MM-DDThh:mm:ss.mmmZ
    public static void appendDateTime(@NotNull CharSink<?> sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(micros, EN_LOCALE, "Z", sink);
    }

    // YYYY-MM-DDThh:mm:ss.mmmuuuZ
    public static void appendDateTimeUSec(@NotNull CharSink<?> sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        USEC_UTC_FORMAT.format(micros, EN_LOCALE, "Z", sink);
    }

    public static void appendEra(@NotNull CharSink<?> sink, int year, @NotNull DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    public static void appendHour12(@NotNull CharSink<?> sink, int hour) {
        Numbers.append(sink, hour % 12);
    }

    public static void appendHour121(@NotNull CharSink<?> sink, int hour) {
        DateFormatUtils.appendHour121(sink, hour);
    }

    public static void appendHour121Padded(@NotNull CharSink<?> sink, int hour) {
        int h12 = (hour + 11) % 12 + 1;
        append0(sink, h12);
    }

    public static void appendHour12Padded(@NotNull CharSink<?> sink, int hour) {
        append0(sink, hour % 12);
    }

    public static void appendHour241(@NotNull CharSink<?> sink, int hour) {
        DateFormatUtils.appendHour241(sink, hour);
    }

    public static void appendHour241Padded(@NotNull CharSink<?> sink, int hour) {
        int h24 = (hour + 23) % 24 + 1;
        append0(sink, h24);
    }

    public static void appendYear(@NotNull CharSink<?> sink, int val) {
        Numbers.append(sink, val != 0 ? val : 1);
    }

    public static void appendYear0(@NotNull CharSink<?> sink, int val) {
        if (Math.abs(val) < 10) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void appendYear00(@NotNull CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void appendYear000(@NotNull CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii('0').putAscii('0').putAscii('0');
        } else if (v < 100) {
            sink.putAscii('0').putAscii('0');
        } else if (v < 1000) {
            sink.putAscii('0');
        }
        appendYear(sink, val);
    }

    public static void assertChar(char c, @NotNull CharSequence in, int pos, int hi) throws NumericException {
        assertRemaining(pos, hi);
        if (in.charAt(pos) != c) {
            throw NumericException.instance();
        }
    }

    public static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.instance();
        }
    }

    public static void assertRemaining(int pos, int hi) throws NumericException {
        DateFormatUtils.assertRemaining(pos, hi);
    }

    public static int assertString(@NotNull CharSequence delimiter, int len, @NotNull CharSequence in, int pos, int hi) throws NumericException {
        return DateFormatUtils.assertString(delimiter, len, in, pos, hi);
    }

    public static long compute(
            @NotNull DateLocale locale,
            int era,
            int year,
            int month,
            int week,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int micros,
            int timezone,
            long offsetMinutes,
            int hourType
    ) throws NumericException {
        if (era == 0) {
            year = -(year - 1);
        }

        boolean leap = CommonUtils.isLeapYear(year);

        // wrong month
        if (month < 1 || month > 12) {
            throw NumericException.instance();
        }

        if (hourType == CommonUtils.HOUR_24) {
            // wrong 24-hour clock hour
            if (hour < 0 || hour > 24) {
                throw NumericException.instance();
            }
            hour %= 24;
        } else {
            // wrong 12-hour clock hour
            if (hour < 0 || hour > 12) {
                throw NumericException.instance();
            }
            hour %= 12;
            if (hourType == CommonUtils.HOUR_PM) {
                hour += 12;
            }
        }

        // wrong day of month
        if (day < 1 || day > CommonUtils.getDaysPerMonth(month, leap)) {
            throw NumericException.instance();
        }

        if (minute < 0 || minute > 59) {
            throw NumericException.instance();
        }

        if (second < 0 || second > 59) {
            throw NumericException.instance();
        }

        if ((week <= 0 && week != -1) || week > CommonUtils.getWeeks(year)) {
            throw NumericException.instance();
        }

        // calculate year, month, and day of ISO week
        if (week != -1) {
            long firstDayOfIsoWeekMicros = Micros.yearMicros(year, CommonUtils.isLeapYear(year)) +
                    (week - 1) * Micros.WEEK_MICROS +
                    CommonUtils.getIsoYearDayOffset(year) * Micros.DAY_MICROS;
            month = Micros.getMonthOfYear(firstDayOfIsoWeekMicros);
            year += (week == 1 && CommonUtils.getIsoYearDayOffset(year) < 0) ? -1 : 0;
            day = Micros.getDayOfMonth(firstDayOfIsoWeekMicros, year, month, CommonUtils.isLeapYear(year));
        }

        long outMicros = Micros.yearMicros(year, leap)
                + Micros.monthOfYearMicros(month, leap)
                + (long) (day - 1) * Micros.DAY_MICROS
                + (long) hour * Micros.HOUR_MICROS
                + (long) minute * Micros.MINUTE_MICROS
                + (long) second * Micros.SECOND_MICROS
                + (long) millis * Micros.MILLI_MICROS
                + micros;

        if (timezone > -1) {
            outMicros -= locale.getZoneRules(timezone, RESOLUTION_MICROS).getOffset(outMicros, year);
        } else if (offsetMinutes > Long.MIN_VALUE) {
            outMicros -= offsetMinutes * Micros.MINUTE_MICROS;
        }

        return outMicros;
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    // may be used to initialize calendar indexes ahead of using them
    @TestOnly
    public static void init() {
    }

    @TestOnly
    public static long parseDateTime(@NotNull CharSequence seq) throws NumericException {
        return NANOS_UTC_FORMAT.parse(seq, 0, seq.length(), EN_LOCALE);
    }

    public static long parseHTTP(@NotNull CharSequence in) throws NumericException {
        for (int i = 0, n = HTTP_FORMATS.length; i < n; i++) {
            try {
                return HTTP_FORMATS[i].parse(in, EN_LOCALE);
            } catch (NumericException ignore) {
                // try next
            }
        }
        throw NumericException.instance();
    }

    // YYYY-MM-DDThh:mm:ss.mmmZ
    public static long parseTimestamp(@NotNull CharSequence seq) throws NumericException {
        return parseTimestamp(seq, 0, seq.length());
    }

    public static long parseTimestamp(@NotNull CharSequence value, int lo, int hi) throws NumericException {
        for (int i = 0, n = FORMATS.length; i < n; i++) {
            try {
                return FORMATS[i].parse(value, lo, hi, EN_LOCALE);
            } catch (NumericException ignore) {
                // try next
            }
        }
        throw NumericException.instance();
    }

    // YYYY-MM-DDThh:mm:ss.mmmnnn
    public static long parseUTCTimestamp(@NotNull CharSequence seq) throws NumericException {
        return USEC_UTC_FORMAT.parse(seq, 0, seq.length(), EN_LOCALE);
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

    public static long tryParse(@NotNull CharSequence s, int lo, int lim) throws NumericException {
        return parseTimestamp(s, lo, lim);
    }

    public static void updateReferenceYear(long micros) {
        referenceYear = micros;

        int referenceYear = Micros.getYear(micros);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        prevCenturyLow = thisCenturyLow - 100;
        newYear = Micros.endOfYear(referenceYear);
    }

    static {
        updateReferenceYear(Os.currentTimeMicros());

        final MicrosFormatCompiler compiler = new MicrosFormatCompiler();
        PG_TIMESTAMP_FORMAT = compiler.compile("y-MM-dd HH:mm:ss.SSSUUU");
        PG_TIMESTAMP_TIME_Z_FORMAT = compiler.compile("y-MM-dd HH:mm:ssz");
        NANOS_UTC_FORMAT = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz");

        final String[] httpPatterns = new String[]{ // priority sorted
                "E, d MMM yyyy HH:mm:ss Z",     // HTTP standard
                "E, d-MMM-yyyy HH:mm:ss Z"      // Microsoft EntraID
        };
        HTTP_FORMATS = new DateFormat[httpPatterns.length];
        for (int i = 0; i < httpPatterns.length; i++) {
            HTTP_FORMATS[i] = compiler.compile(httpPatterns[i]);
        }

        final String[] patterns = new String[]{ // priority sorted
                CommonUtils.PG_TIMESTAMP_MILLI_TIME_Z_PATTERN, // y-MM-dd HH:mm:ss.SSSz
                CommonUtils.GREEDY_MILLIS1_UTC_PATTERN,        // yyyy-MM-ddTHH:mm:ss.Sz
                CommonUtils.USEC_UTC_PATTERN,                  // yyyy-MM-ddTHH:mm:ss.SSSUUUz
                CommonUtils.SEC_UTC_PATTERN,                   // yyyy-MM-ddTHH:mm:ssz
                CommonUtils.GREEDY_MILLIS2_UTC_PATTERN,        // yyyy-MM-ddTHH:mm:ss.SSz
                CommonUtils.UTC_PATTERN,                       // yyyy-MM-ddTHH:mm:ss.SSSz
                CommonUtils.HOUR_PATTERN,                      // yyyy-MM-ddTHH
                CommonUtils.DAY_PATTERN,                       // yyyy-MM-dd
                CommonUtils.WEEK_PATTERN,                      // YYYY-Www
                CommonUtils.MONTH_PATTERN,                     // yyyy-MM
                CommonUtils.YEAR_PATTERN                       // yyyy
        };
        FORMATS = new DateFormat[patterns.length];
        CharSequenceObjHashMap<DateFormat> dateFormats = new CharSequenceObjHashMap<>();
        for (int i = 0; i < patterns.length; i++) {
            String pattern = patterns[i];
            DateFormat format = compiler.compile(pattern);
            dateFormats.put(pattern, format);
            FORMATS[i] = format;
        }
        PG_TIMESTAMP_MILLI_TIME_Z_FORMAT = dateFormats.get(CommonUtils.PG_TIMESTAMP_MILLI_TIME_Z_PATTERN);
        GREEDY_MILLIS1_UTC_FORMAT = dateFormats.get(CommonUtils.GREEDY_MILLIS1_UTC_PATTERN);
        USEC_UTC_FORMAT = dateFormats.get(CommonUtils.USEC_UTC_PATTERN);
        SEC_UTC_FORMAT = dateFormats.get(CommonUtils.SEC_UTC_PATTERN);
        GREEDY_MILLIS2_UTC_FORMAT = dateFormats.get(CommonUtils.GREEDY_MILLIS2_UTC_PATTERN);
        UTC_FORMAT = dateFormats.get(CommonUtils.UTC_PATTERN);
        HOUR_FORMAT = dateFormats.get(CommonUtils.HOUR_PATTERN);
        DAY_FORMAT = dateFormats.get(CommonUtils.DAY_PATTERN);
        WEEK_FORMAT = dateFormats.get(CommonUtils.WEEK_PATTERN);
        MONTH_FORMAT = dateFormats.get(CommonUtils.MONTH_PATTERN);
        YEAR_FORMAT = dateFormats.get(CommonUtils.YEAR_PATTERN);
    }
}
