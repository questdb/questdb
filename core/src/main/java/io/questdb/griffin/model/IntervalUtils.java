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

package io.questdb.griffin.model;

import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

public final class IntervalUtils {

    public static long getIntervalHi(LongList intervals, int pos) {
        return intervals.getQuick((pos << 1) + 1);
    }

    public static long getIntervalLo(LongList intervals, int pos) {
        return intervals.getQuick(pos << 1);
    }

    public static long parseCCPartialDate(CharSequence seq) throws NumericException {
        return parseCCPartialDate(seq, 0, seq.length());
    }

    public static long parseCCPartialDate(CharSequence seq, final int pos, int lim) throws NumericException {
        long ts;
        if (lim - pos < 4) {
            throw NumericException.INSTANCE;
        }
        int p = pos;
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen(p, lim)) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen(p, lim)) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
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
                            if (lim - p > 3) {
                                checkChar(seq, p++, lim, '.');
                                int ms = Numbers.parseInt(seq, p, p += 3);
                                if (lim - p > 2) {
                                    // micros
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + ms * Timestamps.MILLI_MICROS
                                            + Numbers.parseInt(seq, p, p + 3) + 1;
                                } else {
                                    // millis
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + (ms + 1) * Timestamps.MILLI_MICROS;
                                }
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + (sec + 1) * Timestamps.SECOND_MICROS;
                            }
                        } else {
                            // minute
                            ts = Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS
                                    + hour * Timestamps.HOUR_MICROS
                                    + (min + 1) * Timestamps.MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Timestamps.yearMicros(year, l)
                                + Timestamps.monthOfYearMicros(month, l)
                                + (day - 1) * Timestamps.DAY_MICROS
                                + (hour + 1) * Timestamps.HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = Timestamps.addDays(Timestamps.yearMicros(year, l)
                            + Timestamps.monthOfYearMicros(month, l)
                            + (day - 1) * Timestamps.DAY_MICROS, 1);
                }
            } else {
                // year + month
                ts = Timestamps.addMonths(Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l), 1);
            }
        } else {
            // year
            ts = Timestamps.yearMicros(year + 1, Timestamps.isLeapYear(year + 1));
        }
        return ts;
    }

    public static long parseFloorPartialDate(CharSequence seq) throws NumericException {
        return parseFloorPartialDate(seq, 0, seq.length());
    }

    public static long parseFloorPartialDate(CharSequence seq, final int pos, int lim) throws NumericException {
        long ts;
        if (lim - pos < 4) {
            throw NumericException.INSTANCE;
        }
        int p = pos;
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen(p, lim)) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen(p, lim)) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
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
                            if (lim - p > 3) {
                                checkChar(seq, p++, lim, '.');
                                int ms = Numbers.parseInt(seq, p, p += 3);
                                if (lim - p > 2) {
                                    // micros
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + ms * Timestamps.MILLI_MICROS
                                            + Numbers.parseInt(seq, p, p + 3);
                                } else {
                                    // millis
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + ms * Timestamps.MILLI_MICROS;
                                }
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + sec * Timestamps.SECOND_MICROS;
                            }
                        } else {
                            // minute
                            ts = Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS
                                    + hour * Timestamps.HOUR_MICROS
                                    + min * Timestamps.MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Timestamps.yearMicros(year, l)
                                + Timestamps.monthOfYearMicros(month, l)
                                + (day - 1) * Timestamps.DAY_MICROS
                                + hour * Timestamps.HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = Timestamps.yearMicros(year, l)
                            + Timestamps.monthOfYearMicros(month, l)
                            + (day - 1) * Timestamps.DAY_MICROS;
                }
            } else {
                // year + month
                ts = (Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(1, l));
        }
        return ts;
    }

    /**
     * Intersects two lists of intervals and returns result list. Both lists are expected
     * to be chronologically ordered and result list will be ordered as well.
     *
     * @param a   list of intervals
     * @param b   list of intervals
     * @param out intersection target
     */
    static void intersect(LongList a, LongList b, LongList out) {
        final int sizeA = a.size() / 2;
        final int sizeB = b.size() / 2;
        int intervalA = 0;
        int intervalB = 0;

        while (intervalA != sizeA && intervalB != sizeB) {

            long aLo = getIntervalLo(a, intervalA);
            long aHi = getIntervalHi(a, intervalA);

            long bLo = getIntervalLo(b, intervalB);
            long bHi = getIntervalHi(b, intervalB);

            // a fully above b
            if (aHi < bLo) {
                // a loses
                intervalA++;
            } else if (getIntervalLo(a, intervalA) > getIntervalHi(b, intervalB)) {
                // a fully below b
                // b loses
                intervalB++;
            } else {

                append(out, Math.max(aLo, bLo), Math.min(aHi, bHi));

                if (aHi < bHi) {
                    // b hanging lower than a
                    // a loses
                    intervalA++;
                } else {
                    // otherwise a lower than b
                    // a loses
                    intervalB++;
                }
            }
        }
    }


    public static void apply(LongList temp, long lo, long hi, int period, char periodType, int count) {
        append(temp, lo, hi);
        if (count > 1) {
            switch (periodType) {
                case 'y':
                    addYearIntervals(period, count, temp);
                    break;
                case 'M':
                    addMonthInterval(period, count, temp);
                    break;
                case 'h':
                    addMillisInterval(period * Timestamps.HOUR_MICROS, count, temp);
                    break;
                case 'm':
                    addMillisInterval(period * Timestamps.MINUTE_MICROS, count, temp);
                    break;
                case 's':
                    addMillisInterval(period * Timestamps.SECOND_MICROS, count, temp);
                    break;
                case 'd':
                    addMillisInterval(period * Timestamps.DAY_MICROS, count, temp);
                    break;
            }
        }
    }

    static void append(LongList list, long lo, long hi) {
        int n = list.size();
        if (n > 0) {
            long prevHi = list.getQuick(n - 1) + 1;
            if (prevHi >= lo) {
                list.setQuick(n - 1, hi);
                return;
            }
        }

        list.add(lo);
        list.add(hi);
    }

    private static void parseRange(CharSequence seq, int lo, int p, int lim, int position, Interval out) throws SqlException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw SqlException.$(position, "Range not a number");
        }
        try {
            parseInterval(seq, lo, p, out);
            out.of(out.lo, Timestamps.addPeriod(out.hi, type, period));
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMillis = TimestampFormatUtils.tryParse(seq, lo, p);
            out.of(loMillis, Timestamps.addPeriod(loMillis, type, period));
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
    }

    private static void addMillisInterval(long period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);

        for (int i = 0, n = count - 1; i < n; i++) {
            lo += period;
            hi += period;
            append(out, lo, hi);
        }
    }

    private static void addMonthInterval(int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);

        for (int i = 0, n = count - 1; i < n; i++) {
            lo = Timestamps.addMonths(lo, period);
            hi = Timestamps.addMonths(hi, period);
            append(out, lo, hi);
        }
    }

    private static void addYearIntervals(int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);

        for (int i = 0, n = count - 1; i < n; i++) {
            lo = Timestamps.addYear(lo, period);
            hi = Timestamps.addYear(hi, period);
            append(out, lo, hi);
        }
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

    /**
     * Inverts intervals. This method also produces inclusive edges that differ from source ones by 1 milli.
     *
     * @param intervals collection of intervals
     */
    static void invert(LongList intervals) {
        long last = Long.MIN_VALUE;
        int n = intervals.size();
        for (int i = 0; i < n; i += 2) {
            final long lo = intervals.getQuick(i);
            final long hi = intervals.getQuick(i + 1);
            intervals.setQuick(i, last);
            intervals.setQuick(i + 1, lo - 1);
            last = hi + 1;
        }
        intervals.extendAndSet(n + 1, Long.MAX_VALUE);
        intervals.extendAndSet(n, last);
    }

    static void parseIntervalEx(CharSequence seq, int lo, int lim, int position, Interval temp, LongList out) throws SqlException {
        parseIntervalEx(seq, lo, lim, position, temp);
        apply(out, temp.lo, temp.hi, temp.period, temp.periodType, temp.count);
    }

    static void parseIntervalEx(CharSequence seq, int lo, int lim, int position, Interval out) throws SqlException {
        int[] pos = new int[3];
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw SqlException.$(position, "Invalid interval format");
                }
                pos[++p] = i;
            }
        }

        switch (p) {
            case -1:
                // no semicolons, just date part, which can be interval in itself
                try {
                    parseInterval(seq, lo, lim, out);
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long millis = TimestampFormatUtils.tryParse(seq, lo, lim);
                    out.of(millis, millis);
                    break;
                } catch (NumericException e) {
                    throw SqlException.$(position, "Not a date");
                }
            case 0:
                // single semicolon, expect period format after date
                parseRange(seq, lo, pos[0], lim, position, out);
                break;
            case 2:
                // 2018-01-10T10:30:00.000Z;30m;2d;2
                // means 10:30-11:00 every second day starting 2018-01-10
                int period;
                try {
                    period = Numbers.parseInt(seq, pos[1] + 1, pos[2] - 1);
                } catch (NumericException e) {
                    throw SqlException.$(position, "Period not a number");
                }
                int count;
                try {
                    count = Numbers.parseInt(seq, pos[2] + 1, lim);
                } catch (NumericException e) {
                    throw SqlException.$(position, "Count not a number");
                }

                parseRange(seq, lo, pos[0], pos[1], position, out);
                char type = seq.charAt(pos[2] - 1);
                out.of(out.lo, out.hi, period, type, count);
                switch (type) {
                    case 'y':
                    case 'M':
                    case 'h':
                    case 'm':
                    case 's':
                    case 'd':
                        break;
                    default:
                        throw SqlException.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw SqlException.$(position, "Invalid interval format");
        }
    }

    static void parseInterval(CharSequence seq, int pos, int lim, Interval out) throws NumericException {
        if (lim - pos < 4) {
            throw NumericException.INSTANCE;
        }
        int p = pos;
        int year = Numbers.parseInt(seq, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen(p, lim)) {
            checkChar(seq, p++, lim, '-');
            int month = Numbers.parseInt(seq, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen(p, lim)) {
                checkChar(seq, p++, lim, '-');
                int day = Numbers.parseInt(seq, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
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
                                out.of(Timestamps.yearMicros(year, l)
                                                + Timestamps.monthOfYearMicros(month, l)
                                                + (day - 1) * Timestamps.DAY_MICROS
                                                + hour * Timestamps.HOUR_MICROS
                                                + min * Timestamps.MINUTE_MICROS
                                                + sec * Timestamps.SECOND_MICROS
                                        , Timestamps.yearMicros(year, l)
                                                + Timestamps.monthOfYearMicros(month, l)
                                                + (day - 1) * Timestamps.DAY_MICROS
                                                + hour * Timestamps.HOUR_MICROS
                                                + min * Timestamps.MINUTE_MICROS
                                                + sec * Timestamps.SECOND_MICROS
                                                + 999999);
                            }
                        } else {
                            // minute
                            out.of(Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS,
                                    Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + 59 * Timestamps.SECOND_MICROS
                                            + 999999);
                        }
                    } else {
                        // year + month + day + hour
                        out.of(Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS,
                                Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + 59 * Timestamps.MINUTE_MICROS
                                        + 59 * Timestamps.SECOND_MICROS
                                        + 999999);
                    }
                } else {
                    // year + month + day
                    out.of(Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS,
                            Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + +(day - 1) * Timestamps.DAY_MICROS
                                    + 23 * Timestamps.HOUR_MICROS
                                    + 59 * Timestamps.MINUTE_MICROS
                                    + 59 * Timestamps.SECOND_MICROS
                                    + 999999);
                }
            } else {
                // year + month
                out.of(Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l),
                        Timestamps.yearMicros(year, l)
                                + Timestamps.monthOfYearMicros(month, l)
                                + (Timestamps.getDaysPerMonth(month, l) - 1) * Timestamps.DAY_MICROS
                                + 23 * Timestamps.HOUR_MICROS
                                + 59 * Timestamps.MINUTE_MICROS
                                + 59 * Timestamps.SECOND_MICROS
                                + 999999);
            }
        } else {
            // year
            out.of(Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(1, l),
                    Timestamps.yearMicros(year, l)
                            + Timestamps.monthOfYearMicros(12, l)
                            + (Timestamps.getDaysPerMonth(12, l) - 1) * Timestamps.DAY_MICROS
                            + 23 * Timestamps.HOUR_MICROS
                            + 59 * Timestamps.MINUTE_MICROS
                            + 59 * Timestamps.SECOND_MICROS
                            + 999999);
        }
    }

    @FunctionalInterface
    public interface IntervalSetAdd {
        void add(long lo, long hi);
    }
}
