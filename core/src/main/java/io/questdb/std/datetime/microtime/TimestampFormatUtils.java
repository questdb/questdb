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

package io.questdb.std.datetime.microtime;

import io.questdb.std.*;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.str.CharSink;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;

public class TimestampFormatUtils {
    public static final int HOUR_24 = 2;
    public static final int HOUR_PM = 1;
    public static final int HOUR_AM = 0;
    public static final DateFormat UTC_FORMAT;
    public static final DateFormat SEC_UTC_FORMAT;
    public static final DateFormat GREEDY_MILLIS1_UTC_FORMAT;
    public static final DateFormat GREEDY_MILLIS2_UTC_FORMAT;
    public static final DateFormat USEC_UTC_FORMAT;
    public static final DateFormat PG_TIMESTAMP_FORMAT;
    public static final DateFormat PG_TIMESTAMPZ_FORMAT;
    public static final DateFormat PG_TIMESTAMP_MILLI_TIME_Z_FORMAT;
    public static final DateFormat PG_TIMESTAMP_TIME_Z_FORMAT;
    public static final DateFormat NANOS_UTC_FORMAT;
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final DateLocale enLocale = DateLocaleFactory.INSTANCE.getLocale("en");
    public static final int TIMESTAMP_FORMAT_MIN_LENGTH;
    private static final String PG_TIMESTAMP_MILLI_TIME_Z_PATTERN = "y-MM-dd HH:mm:ss.SSSz";
    private static final String GREEDY_MILLIS1_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.Sz";
    private static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    private static final String SEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ssz";
    private static final String GREEDY_MILLIS2_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSz";
    private static final DateFormat HTTP_FORMAT;
    private static final DateFormat[] FORMATS;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    static int prevCenturyLow;
    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static long newYear;

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
        UTC_FORMAT.format(micros, null, "Z", sink);
    }

    // YYYY-MM-DDThh:mm:ss.mmmuuuZ
    public static void appendDateTimeUSec(CharSink sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        USEC_UTC_FORMAT.format(micros, null, "Z", sink);
    }

    // YYYY-MM-DD
    public static void formatDashYYYYMMDD(CharSink sink, long millis) {
        int y = Timestamps.getYear(millis);
        boolean l = Timestamps.isLeapYear(y);
        int m = Timestamps.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
        append0(sink.put('-'), Timestamps.getDayOfMonth(millis, y, m, l));
    }

    public static void formatHTTP(CharSink sink, long millis) {
        HTTP_FORMAT.format(millis, enLocale, "GMT", sink);
    }

    // YYYY-MM
    public static void formatYYYYMM(CharSink sink, long millis) {
        int y = Timestamps.getYear(millis);
        int m = Timestamps.getMonthOfYear(millis, y, Timestamps.isLeapYear(y));
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
    }

    // YYYYMMDD
    public static void formatYYYYMMDD(CharSink sink, long millis) {
        int y = Timestamps.getYear(millis);
        boolean l = Timestamps.isLeapYear(y);
        int m = Timestamps.getMonthOfYear(millis, y, l);
        Numbers.append(sink, y);
        append0(sink, m);
        append0(sink, Timestamps.getDayOfMonth(millis, y, m, l));
    }

    public static long getReferenceYear() {
        return referenceYear;
    }

    // YYYY-MM-DDThh:mm:ss.mmmZ
    public static long parseTimestamp(CharSequence seq) throws NumericException {
        return parseTimestamp(seq, 0, seq.length());
    }

    // YYYY-MM-DDThh:mm:ss.mmmnnn
    public static long parseUTCTimestamp(CharSequence seq) throws NumericException {
        return USEC_UTC_FORMAT.parse(seq, 0, seq.length(), enLocale);
    }

    public static long parseDateTime(CharSequence seq) throws NumericException {
        return NANOS_UTC_FORMAT.parse(seq, 0, seq.length(), enLocale);
    }

    public static long tryParse(CharSequence s, int lo, int lim) throws NumericException {
        return parseTimestamp(s, lo, lim);
    }

    public static void updateReferenceYear(long micros) {
        referenceYear = micros;

        int referenceYear = Timestamps.getYear(micros);
        int centuryOffset = referenceYear % 100;
        thisCenturyLimit = centuryOffset + 20;
        if (thisCenturyLimit > 100) {
            thisCenturyLimit = thisCenturyLimit % 100;
            thisCenturyLow = referenceYear - centuryOffset + 100;
        } else {
            thisCenturyLow = referenceYear - centuryOffset;
        }
        prevCenturyLow = thisCenturyLow - 100;
        newYear = Timestamps.endOfYear(referenceYear);
    }

    public static int adjustYear(int year) {
        return (year < thisCenturyLimit ? thisCenturyLow : prevCenturyLow) + year;
    }

    public static void appendAmPm(CharSink sink, int hour, DateLocale locale) {
        if (hour < 12) {
            sink.put(locale.getAMPM(0));
        } else {
            sink.put(locale.getAMPM(1));
        }
    }

    public static void appendEra(CharSink sink, int year, DateLocale locale) {
        if (year < 0) {
            sink.put(locale.getEra(0));
        } else {
            sink.put(locale.getEra(1));
        }
    }

    public static void appendHour12(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour);
        } else {
            sink.put(hour - 12);
        }
    }

    public static void appendHour121(CharSink sink, int hour) {
        if (hour < 12) {
            sink.put(hour + 1);
        } else {
            sink.put(hour - 11);
        }
    }

    public static void appendHour121Padded(CharSink sink, int hour) {
        if (hour < 12) {
            append0(sink, hour + 1);
        } else {
            append0(sink, hour - 11);
        }
    }

    public static void appendHour12Padded(CharSink sink, int hour) {
        if (hour < 12) {
            append0(sink, hour);
        } else {
            append0(sink, hour - 12);
        }
    }

    public static void assertChar(char c, CharSequence in, int pos, int hi) throws NumericException {
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

    public static int assertString(CharSequence delimiter, int len, CharSequence in, int pos, int hi) throws NumericException {
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
            int hourType
    ) throws NumericException {

        if (era == 0) {
            year = -(year - 1);
        }

        boolean leap = Timestamps.isLeapYear(year);

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
        if (day < 1 || day > Timestamps.getDaysPerMonth(month, leap)) {
            throw NumericException.INSTANCE;
        }

        if (minute < 0 || minute > 59) {
            throw NumericException.INSTANCE;
        }

        if (second < 0 || second > 59) {
            throw NumericException.INSTANCE;
        }

        long datetime = Timestamps.yearMicros(year, leap)
                + Timestamps.monthOfYearMicros(month, leap)
                + (day - 1) * Timestamps.DAY_MICROS
                + hour * Timestamps.HOUR_MICROS
                + minute * Timestamps.MINUTE_MICROS
                + second * Timestamps.SECOND_MICROS
                + (long) millis * Timestamps.MILLI_MICROS
                + micros;

        if (timezone > -1) {
            datetime -= locale.getZoneRules(timezone, RESOLUTION_MICROS).getOffset(datetime, year, leap);
        } else if (offset > Long.MIN_VALUE) {
            datetime -= offset;
        }

        return datetime;
    }

    public static long parseYearGreedy(CharSequence in, int pos, int hi) throws NumericException {
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

    private static long parseTimestamp(CharSequence value, int lo, int hi) throws NumericException {
        for (int i = 0, n = FORMATS.length; i < n; i++) {
            try {
                return FORMATS[i].parse(value, lo, hi, enLocale);
            } catch (NumericException ignore) {
            }
        }
        throw NumericException.INSTANCE;
    }

    static {
        updateReferenceYear(Os.currentTimeMicros());
        TimestampFormatCompiler compiler = new TimestampFormatCompiler();
        HTTP_FORMAT = compiler.compile("E, d MMM yyyy HH:mm:ss Z");
        PG_TIMESTAMP_FORMAT = compiler.compile("y-MM-dd HH:mm:ss.SSSUUU");
        PG_TIMESTAMPZ_FORMAT = compiler.compile("y-MM-ddTHH:mm:ss.SSSUUU");
        PG_TIMESTAMP_TIME_Z_FORMAT = compiler.compile("y-MM-dd HH:mm:ssz");
        NANOS_UTC_FORMAT = compiler.compile("yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz");

        String[] patterns = new String[]{
                PG_TIMESTAMP_MILLI_TIME_Z_PATTERN,
                GREEDY_MILLIS1_UTC_PATTERN,
                USEC_UTC_PATTERN,
                SEC_UTC_PATTERN,
                GREEDY_MILLIS2_UTC_PATTERN,
                UTC_PATTERN
        };
        FORMATS = new DateFormat[patterns.length];
        CharSequenceObjHashMap<DateFormat> dateFormats = new CharSequenceObjHashMap<>();
        int patternMinLength = Integer.MAX_VALUE;
        for (int i = 0; i < patterns.length; i++) {
            String pattern = patterns[i];
            DateFormat format = compiler.compile(pattern);
            dateFormats.put(pattern, format);
            FORMATS[i] = format;
            patternMinLength = Math.min(patternMinLength, pattern.length());
        }
        TIMESTAMP_FORMAT_MIN_LENGTH = patternMinLength;
        PG_TIMESTAMP_MILLI_TIME_Z_FORMAT = dateFormats.get(PG_TIMESTAMP_MILLI_TIME_Z_PATTERN);
        GREEDY_MILLIS1_UTC_FORMAT = dateFormats.get(GREEDY_MILLIS1_UTC_PATTERN);
        USEC_UTC_FORMAT = dateFormats.get(USEC_UTC_PATTERN);
        SEC_UTC_FORMAT = dateFormats.get(SEC_UTC_PATTERN);
        GREEDY_MILLIS2_UTC_FORMAT = dateFormats.get(GREEDY_MILLIS2_UTC_PATTERN);
        UTC_FORMAT = dateFormats.get(UTC_PATTERN);
    }
}
