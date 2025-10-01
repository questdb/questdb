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

package io.questdb.std.datetime.nanotime;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.datetime.CommonUtils.*;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_NANOS;

public class NanosFormatUtils {
    public static final DateFormat DAY_FORMAT;
    public static final DateFormat HOUR_FORMAT;
    public static final DateFormat MONTH_FORMAT;
    public static final DateFormat NSEC_UTC_FORMAT;
    public static final DateFormat PG_TIMESTAMP_FORMAT;
    public static final DateFormat USEC_UTC_FORMAT;
    public static final DateFormat UTC_FORMAT;
    public static final DateFormat WEEK_FORMAT;
    public static final DateFormat YEAR_FORMAT;
    private static final DateFormat[] FORMATS;
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

    public static void append00000000(@NotNull CharSink<?> sink, int val) {
        int v = Math.abs(val);
        if (v < 10) {
            sink.putAscii("00000000");
        } else if (v < 100) {
            sink.putAscii("0000000");
        } else if (v < 1000) {
            sink.putAscii("000000");
        } else if (v < 10000) {
            sink.putAscii("00000");
        } else if (v < 100000) {
            sink.putAscii("0000");
        } else if (v < 1000000) {
            sink.putAscii("000");
        } else if (v < 10000000) {
            sink.putAscii("00");
        } else if (v < 100000000) {
            sink.putAscii("0");
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
    public static void appendDateTime(@NotNull CharSink<?> sink, long nanos) {
        if (nanos == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(nanos, DateLocaleFactory.EN_LOCALE, "Z", sink);
    }

    public static void appendDateTimeNSec(@NotNull CharSink<?> sink, long nanos) {
        if (nanos == Long.MIN_VALUE) {
            return;
        }
        NSEC_UTC_FORMAT.format(nanos, DateLocaleFactory.EN_LOCALE, "Z", sink);
    }

    public static void appendDateTimeUSec(@NotNull CharSink<?> sink, long nanos) {
        if (nanos == Long.MIN_VALUE) {
            return;
        }
        USEC_UTC_FORMAT.format(nanos, DateLocaleFactory.EN_LOCALE, "Z", sink);
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
            throw NumericException.INSTANCE;
        }
    }

    public static void assertNoTail(int pos, int hi) throws NumericException {
        if (pos < hi) {
            throw NumericException.INSTANCE;
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
            int nanos,
            int timezone,
            long offsetMinutes,
            int hourType
    ) throws NumericException {
        if (era == 0) {
            // era out of range
            throw NumericException.INSTANCE;
        }

        if (year > 1677 && year < 2262) {
            boolean leap = CommonUtils.isLeapYear(year);

            // wrong month
            if (month < 1 || month > 12) {
                throw NumericException.INSTANCE;
            }

            if (hourType == CommonUtils.HOUR_24) {
                // wrong 24-hour clock hour
                if (hour < 0 || hour > 24) {
                    throw NumericException.INSTANCE;
                }
                hour %= 24;
            } else {
                // wrong 12-hour clock hour
                if (hour < 0 || hour > 12) {
                    throw NumericException.INSTANCE;
                }
                hour %= 12;
                if (hourType == CommonUtils.HOUR_PM) {
                    hour += 12;
                }
            }

            // wrong day of month
            if (day < 1 || day > CommonUtils.getDaysPerMonth(month, leap)) {
                throw NumericException.INSTANCE;
            }

            if (minute < 0 || minute > 59) {
                throw NumericException.INSTANCE;
            }

            if (second < 0 || second > 59) {
                throw NumericException.INSTANCE;
            }

            if ((week <= 0 && week != -1) || week > CommonUtils.getWeeks(year)) {
                throw NumericException.INSTANCE;
            }

            // calculate year, month, and day of ISO week
            if (week != -1) {
                long firstDayOfIsoWeekNanos = Nanos.yearNanos(year, CommonUtils.isLeapYear(year)) +
                        (week - 1) * Nanos.WEEK_NANOS +
                        getIsoYearDayOffset(year) * Nanos.DAY_NANOS;
                month = Nanos.getMonthOfYear(firstDayOfIsoWeekNanos);
                year += (week == 1 && getIsoYearDayOffset(year) < 0) ? -1 : 0;
                day = Nanos.getDayOfMonth(firstDayOfIsoWeekNanos, year, month, CommonUtils.isLeapYear(year));
            }

            long outNanos = Nanos.yearNanos(year, leap)
                    + Nanos.monthOfYearNanos(month, leap)
                    + (long) (day - 1) * Nanos.DAY_NANOS
                    + (long) hour * Nanos.HOUR_NANOS
                    + (long) minute * Nanos.MINUTE_NANOS
                    + (long) second * Nanos.SECOND_NANOS
                    + (long) millis * Nanos.MILLI_NANOS
                    + (long) micros * Nanos.MICRO_NANOS
                    + nanos;

            if (timezone > -1) {
                outNanos -= locale.getZoneRules(timezone, RESOLUTION_NANOS).getOffset(outNanos, year);
            } else if (offsetMinutes > Long.MIN_VALUE) {
                outNanos -= offsetMinutes * Nanos.MINUTE_NANOS;
            }

            return outNanos;
        }

        throw NumericException.INSTANCE;
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    // may be used to initialize calendar indexes ahead of using them
    @TestOnly
    public static void init() {
    }

    @TestOnly
    public static long parseNSecUTC(@NotNull CharSequence seq) throws NumericException {
        return NSEC_UTC_FORMAT.parse(seq, 0, seq.length(), DateLocaleFactory.EN_LOCALE);
    }

    // YYYY-MM-DDThh:mm:ss.mmmZ
    public static long parseNanos(@NotNull CharSequence seq) throws NumericException {
        return parseNanos(seq, 0, seq.length());
    }

    public static long parseNanos(@NotNull CharSequence value, int lo, int hi) throws NumericException {
        for (int i = 0, n = FORMATS.length; i < n; i++) {
            try {
                return FORMATS[i].parse(value, lo, hi, DateLocaleFactory.EN_LOCALE);
            } catch (NumericException ignore) {
                // try next
            }
        }
        throw NumericException.INSTANCE;
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
        return parseNanos(s, lo, lim);
    }

    public static void updateReferenceYear(long micros) {
        referenceYear = micros;

        int referenceYear = Nanos.getYear(micros);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        prevCenturyLow = thisCenturyLow - 100;
        newYear = Nanos.endOfYear(referenceYear);
    }

    static {
        updateReferenceYear(Os.currentTimeMicros());
        final NanosFormatCompiler compiler = new NanosFormatCompiler();
        // PostgreSQL timestamp format with microsecond precision (not nanosecond).
        // This format intentionally truncates nanoseconds to microsecond resolution because:
        // 1. PostgreSQL's timestamp type only supports microsecond precision (6 decimal places)
        // 2. Some PostgreSQL clients (e.g., psycopg2) fail when receiving timestamps with
        //    nanosecond precision (9 decimal places)
        // 3. PostgreSQL's binary wire protocol also uses microsecond precision, making this
        //    consistent across both text and binary formats
        PG_TIMESTAMP_FORMAT = compiler.compile("y-MM-dd HH:mm:ss.SSSUUU");
        final String[] patterns = new String[]{ // priority sorted
                NSEC_UTC_PATTERN,
                PG_TIMESTAMP_MILLI_TIME_Z_PATTERN, // y-MM-dd HH:mm:ss.SSSz
                GREEDY_MILLIS1_UTC_PATTERN,        // yyyy-MM-ddTHH:mm:ss.Sz
                USEC_UTC_PATTERN,                  // yyyy-MM-ddTHH:mm:ss.SSSUUUz
                SEC_UTC_PATTERN,                   // yyyy-MM-ddTHH:mm:ssz
                GREEDY_MILLIS2_UTC_PATTERN,        // yyyy-MM-ddTHH:mm:ss.SSz
                UTC_PATTERN,                       // yyyy-MM-ddTHH:mm:ss.SSSz
                HOUR_PATTERN,                      // yyyy-MM-ddTHH
                DAY_PATTERN,                       // yyyy-MM-dd
                WEEK_PATTERN,                      // YYYY-Www
                MONTH_PATTERN,                     // yyyy-MM
                YEAR_PATTERN,                      // yyyy
        };
        FORMATS = new DateFormat[patterns.length];
        CharSequenceObjHashMap<DateFormat> dateFormats = new CharSequenceObjHashMap<>();
        for (int i = 0; i < patterns.length; i++) {
            String pattern = patterns[i];
            DateFormat format = compiler.compile(pattern);
            dateFormats.put(pattern, format);
            FORMATS[i] = format;
        }

        UTC_FORMAT = dateFormats.get(UTC_PATTERN);
        USEC_UTC_FORMAT = dateFormats.get(USEC_UTC_PATTERN);
        NSEC_UTC_FORMAT = dateFormats.get(NSEC_UTC_PATTERN);

        HOUR_FORMAT = dateFormats.get(CommonUtils.HOUR_PATTERN);
        DAY_FORMAT = dateFormats.get(CommonUtils.DAY_PATTERN);
        WEEK_FORMAT = dateFormats.get(CommonUtils.WEEK_PATTERN);
        MONTH_FORMAT = dateFormats.get(CommonUtils.MONTH_PATTERN);
        YEAR_FORMAT = dateFormats.get(CommonUtils.YEAR_PATTERN);
    }
}
