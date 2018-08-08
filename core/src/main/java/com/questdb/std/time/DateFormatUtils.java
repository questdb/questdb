/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.std.time;

import com.questdb.std.Chars;
import com.questdb.std.LongList;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.str.CharSink;
import com.questdb.store.Interval;

public class DateFormatUtils {
    public static final int HOUR_24 = 2;
    public static final int HOUR_PM = 1;
    public static final int HOUR_AM = 0;
    public static final DateFormat UTC_FORMAT;
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final DateLocale defaultLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
    private static final DateFormat FMT4;
    private static final DateFormat HTTP_FORMAT;
    static long referenceYear;
    static int thisCenturyLimit;
    static int thisCenturyLow;
    static int prevCenturyLow;
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

    // YYYY-MM-DDThh:mm:ss.mmmmZ
    public static void appendDateTime(CharSink sink, long millis) {
        if (millis == Long.MIN_VALUE) {
            return;
        }
        UTC_FORMAT.format(millis, defaultLocale, "Z", sink);
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

    public static void formatMMMDYYYY(CharSink sink, long millis) {
        FMT4.format(millis, defaultLocale, "Z", sink);
    }

    // YYYY
    public static void formatYYYY(CharSink sink, long millis) {
        Numbers.append(sink, Dates.getYear(millis));
    }

    // YYYY-MM
    public static void formatYYYYMM(CharSink sink, long millis) {
        int y = Dates.getYear(millis);
        int m = Dates.getMonthOfYear(millis, y, Dates.isLeapYear(y));
        Numbers.append(sink, y);
        append0(sink.put('-'), m);
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

    public static Interval parseInterval(CharSequence seq) throws NumericException {
        int lim = seq.length();
        int p = 0;
        if (lim < 4) {
            throw NumericException.INSTANCE;
        }
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = Dates.isLeapYear(year);
        if (checkLen(p, lim)) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen(p, lim)) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, Dates.getDaysPerMonth(month, l));
                if (checkLen(p, lim)) {
                    checkChar(seq, p++, lim, 'T');
                    int hour = Numbers.parseInt(seq, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (checkLen(p, lim)) {
                        checkChar(seq, p++, lim, ':');
                        int min = Numbers.parseInt(seq, p, p += 2);
                        checkRange(min, 0, 59);
                        if (checkLen(p, lim)) {
                            checkChar(seq, p++, lim, ':');
                            int sec = Numbers.parseInt(seq, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < lim) {
                                throw NumericException.INSTANCE;
                            } else {
                                // seconds
                                return new Interval(Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS,
                                        Dates.yearMillis(year, l)
                                                + Dates.monthOfYearMillis(month, l)
                                                + (day - 1) * Dates.DAY_MILLIS
                                                + hour * Dates.HOUR_MILLIS
                                                + min * Dates.MINUTE_MILLIS
                                                + sec * Dates.SECOND_MILLIS
                                                + 999
                                );
                            }
                        } else {
                            // minute
                            return new Interval(Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + hour * Dates.HOUR_MILLIS
                                    + min * Dates.MINUTE_MILLIS,
                                    Dates.yearMillis(year, l)
                                            + Dates.monthOfYearMillis(month, l)
                                            + (day - 1) * Dates.DAY_MILLIS
                                            + hour * Dates.HOUR_MILLIS
                                            + min * Dates.MINUTE_MILLIS
                                            + 59 * Dates.SECOND_MILLIS
                                            + 999
                            );
                        }
                    } else {
                        // year + month + day + hour
                        return new Interval(Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (day - 1) * Dates.DAY_MILLIS
                                + hour * Dates.HOUR_MILLIS,
                                Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + 59 * Dates.MINUTE_MILLIS
                                        + 59 * Dates.SECOND_MILLIS
                                        + 999
                        );
                    }
                } else {
                    // year + month + day
                    return new Interval(Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(month, l)
                            + (day - 1) * Dates.DAY_MILLIS,
                            Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + +(day - 1) * Dates.DAY_MILLIS
                                    + 23 * Dates.HOUR_MILLIS
                                    + 59 * Dates.MINUTE_MILLIS
                                    + 59 * Dates.SECOND_MILLIS
                                    + 999
                    );
                }
            } else {
                // year + month
                return new Interval(Dates.yearMillis(year, l) + Dates.monthOfYearMillis(month, l),
                        Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (Dates.getDaysPerMonth(month, l) - 1) * Dates.DAY_MILLIS
                                + 23 * Dates.HOUR_MILLIS
                                + 59 * Dates.MINUTE_MILLIS
                                + 59 * Dates.SECOND_MILLIS
                                + 999
                );
            }
        } else {
            // year
            return new Interval(Dates.yearMillis(year, l) + Dates.monthOfYearMillis(1, l),
                    Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(12, l)
                            + (Dates.getDaysPerMonth(12, l) - 1) * Dates.DAY_MILLIS
                            + 23 * Dates.HOUR_MILLIS
                            + 59 * Dates.MINUTE_MILLIS
                            + 59 * Dates.SECOND_MILLIS
                            + 999
            );
        }
    }

    public static void parseInterval(CharSequence seq, final int pos, int lim, LongList out) throws NumericException {
        int len = lim - pos;
        int p = pos;
        if (len < 4) {
            throw NumericException.INSTANCE;
        }
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = Dates.isLeapYear(year);
        if (checkLen(p, len)) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen(p, len)) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, Dates.getDaysPerMonth(month, l));
                if (checkLen(p, len)) {
                    checkChar(seq, p++, lim, 'T');
                    int hour = Numbers.parseInt(seq, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (checkLen(p, len)) {
                        checkChar(seq, p++, lim, ':');
                        int min = Numbers.parseInt(seq, p, p += 2);
                        checkRange(min, 0, 59);
                        if (checkLen(p, len)) {
                            checkChar(seq, p++, lim, ':');
                            int sec = Numbers.parseInt(seq, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < len) {
                                throw NumericException.INSTANCE;
                            } else {
                                // seconds
                                out.add(Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS);
                                out.add(Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS
                                        + 999);
                            }
                        } else {
                            // minute
                            out.add(Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + hour * Dates.HOUR_MILLIS
                                    + min * Dates.MINUTE_MILLIS);
                            out.add(Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + hour * Dates.HOUR_MILLIS
                                    + min * Dates.MINUTE_MILLIS
                                    + 59 * Dates.SECOND_MILLIS
                                    + 999);
                        }
                    } else {
                        // year + month + day + hour
                        out.add(Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (day - 1) * Dates.DAY_MILLIS
                                + hour * Dates.HOUR_MILLIS);
                        out.add(Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (day - 1) * Dates.DAY_MILLIS
                                + hour * Dates.HOUR_MILLIS
                                + 59 * Dates.MINUTE_MILLIS
                                + 59 * Dates.SECOND_MILLIS
                                + 999);
                    }
                } else {
                    // year + month + day
                    out.add(Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(month, l)
                            + (day - 1) * Dates.DAY_MILLIS);
                    out.add(Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(month, l)
                            + +(day - 1) * Dates.DAY_MILLIS
                            + 23 * Dates.HOUR_MILLIS
                            + 59 * Dates.MINUTE_MILLIS
                            + 59 * Dates.SECOND_MILLIS
                            + 999);
                }
            } else {
                // year + month
                out.add(Dates.yearMillis(year, l) + Dates.monthOfYearMillis(month, l));
                out.add(Dates.yearMillis(year, l)
                        + Dates.monthOfYearMillis(month, l)
                        + (Dates.getDaysPerMonth(month, l) - 1) * Dates.DAY_MILLIS
                        + 23 * Dates.HOUR_MILLIS
                        + 59 * Dates.MINUTE_MILLIS
                        + 59 * Dates.SECOND_MILLIS
                        + 999);
            }
        } else {
            // year
            out.add(Dates.yearMillis(year, l) + Dates.monthOfYearMillis(1, l));
            out.add(Dates.yearMillis(year, l)
                    + Dates.monthOfYearMillis(12, l)
                    + (Dates.getDaysPerMonth(12, l) - 1) * Dates.DAY_MILLIS
                    + 23 * Dates.HOUR_MILLIS
                    + 59 * Dates.MINUTE_MILLIS
                    + 59 * Dates.SECOND_MILLIS
                    + 999);
        }
    }

    public static long tryParse(CharSequence s) throws NumericException {
        return tryParse(s, 0, s.length());
    }

    public static long tryParse(CharSequence s, int lo, int lim) throws NumericException {
        return parseDateTime(s, lo, lim);
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

        long datetime = Dates.yearMillis(year, leap)
                + Dates.monthOfYearMillis(month, leap)
                + (day - 1) * Dates.DAY_MILLIS
                + hour * Dates.HOUR_MILLIS
                + minute * Dates.MINUTE_MILLIS
                + second * Dates.SECOND_MILLIS
                + millis;

        if (timezone > -1) {
            datetime -= locale.getZoneRules(timezone).getOffset(datetime, year, leap);
        } else if (offset > Long.MIN_VALUE) {
            datetime -= offset;
        }

        return datetime;
    }

    static long parseYearGreedy(CharSequence in, int pos, int hi) throws NumericException {
        long l = Numbers.parseIntSafely(in, pos, hi);
        int len = Numbers.decodeLen(l);
        int year;
        if (len == 2) {
            year = adjustYear(Numbers.decodeInt(l));
        } else {
            year = Numbers.decodeInt(l);
        }

        return Numbers.encodeIntAndLen(year, len);
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

    private static boolean checkLen(int p, int lim) throws NumericException {
        if (lim - p > 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    private static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    private static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.INSTANCE;
        }
    }

    static {
        updateReferenceYear(System.currentTimeMillis());
        DateFormatCompiler compiler = new DateFormatCompiler();
        UTC_FORMAT = compiler.compile(UTC_PATTERN);
        HTTP_FORMAT = compiler.compile("E, d MMM yyyy HH:mm:ss Z");
        FMT4 = compiler.compile("MMM d yyyy");
    }
}
