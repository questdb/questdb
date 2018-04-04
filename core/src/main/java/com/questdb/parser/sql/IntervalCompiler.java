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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.std.LongList;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;

public class IntervalCompiler {

    public static String asIntervalStr(LongList l) {
        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0, n = l.size(); i < n; i += 2) {
            if (i > 0) {
                b.append(',');
            }
            b.append("Interval{");
            b.append("lo=");
            b.append(Dates.toString(l.getQuick(i)));
            b.append(", ");
            b.append("hi=");
            b.append(Dates.toString(l.getQuick(i + 1)));
            b.append('}');
        }
        b.append(']');
        return b.toString();
    }

    public static long getIntervalHi(LongList intervals, int pos) {
        return intervals.getQuick((pos << 1) + 1);
    }

    public static long getIntervalLo(LongList intervals, int pos) {
        return intervals.getQuick(pos << 1);
    }

    /**
     * Intersects two lists of intervals and returns result list. Both lists are expected
     * to be chronologically ordered and result list will be ordered as well.
     *
     * @param a   list of intervals
     * @param b   list of intervals
     * @param out intersection target
     */
    public static void intersect(LongList a, LongList b, LongList out) {

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

                append(out, aLo > bLo ? aLo : bLo, aHi < bHi ? aHi : bHi);

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

    public static void parseIntervalEx(CharSequence seq, int lo, int lim, int position, LongList out) throws ParserException {
        int pos[] = new int[3];
        int p = -1;
        for (int i = lo; i < lim; i++) {
            if (seq.charAt(i) == ';') {
                if (p > 1) {
                    throw QueryError.$(position, "Invalid interval format");
                }
                pos[++p] = i;
            }
        }

        switch (p) {
            case -1:
                // no semicolons, just date part, which can be interval in itself
                try {
                    DateFormatUtils.parseInterval(seq, lo, lim, out);
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long millis = DateFormatUtils.tryParse(seq, lo, lim);
                    append(out, millis, millis);
                    break;
                } catch (NumericException e) {
                    throw QueryError.$(position, "Not a date");
                }
            case 0:
                // single semicolon, expect period format after date
                parseRange(seq, lo, pos[0], lim, position, out);
                break;
            case 2:
                int period;
                try {
                    period = Numbers.parseInt(seq, pos[1] + 1, pos[2] - 1);
                } catch (NumericException e) {
                    throw QueryError.$(position, "Period not a number");
                }
                int count;
                try {
                    count = Numbers.parseInt(seq, pos[2] + 1, seq.length());
                } catch (NumericException e) {
                    throw QueryError.$(position, "Count not a number");
                }

                parseRange(seq, lo, pos[0], pos[1], position, out);
                char type = seq.charAt(pos[2] - 1);
                switch (type) {
                    case 'y':
                        addYearIntervals(period, count, out);
                        break;
                    case 'M':
                        addMonthInterval(period, count, out);
                        break;
                    case 'h':
                        addMillisInterval(period * Dates.HOUR_MILLIS, count, out);
                        break;
                    case 'm':
                        addMillisInterval(period * Dates.MINUTE_MILLIS, count, out);
                        break;
                    case 's':
                        addMillisInterval(period * Dates.SECOND_MILLIS, count, out);
                        break;
                    case 'd':
                        addMillisInterval(period * Dates.DAY_MILLIS, count, out);
                        break;
                    default:
                        throw QueryError.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw QueryError.$(position, "Invalid interval format");
        }
    }

    /**
     * Performs set subtraction on two lists of intervals. Subtracts b from a, e.g. result = a - b.
     * Both sets are expected to be ordered chronologically.
     *
     * @param a   list of intervals
     * @param b   list of intervals
     * @param out result of subtraction
     */
    public static void subtract(LongList a, LongList b, LongList out) {

        final int sizeA = a.size() / 2;
        final int sizeB = b.size() / 2;
        int intervalA = 0;
        int intervalB = 0;
        boolean fetchA = true;

        if (intervalA < sizeA) {

            long aLo = getIntervalLo(a, intervalA);
            long aHi = getIntervalHi(a, intervalA);

            while (intervalA < sizeA) {


                if (fetchA) {
                    aLo = getIntervalLo(a, intervalA);
                    aHi = getIntervalHi(a, intervalA);
                    fetchA = true;
                }

                if (intervalB == sizeB) {
                    append(out, aLo, aHi);
                    intervalA++;
                    continue;
                }

                long bLo = getIntervalLo(b, intervalB);
                long bHi = getIntervalHi(b, intervalB);

                // a fully above b
                if (aHi < bLo) {
                    // a loses
                    append(out, aLo, aHi);
                    intervalA++;
                } else if (aLo > bHi) {
                    // a fully below b
                    // b loses
                    intervalB++;
                } else {

                    if (aLo < bLo) {
                        // top part of a is above b
                        append(out, aLo, bLo - 1);
                    }

                    if (aHi > bHi) {
                        aLo = bHi + 1;
                        fetchA = false;
                        intervalB++;
                    } else {
                        intervalA++;
                    }
                }
            }
        }
    }

    private static void append(LongList list, long lo, long hi) {
        int n = list.size();
        if (n > 0) {
            long prevHi = list.getQuick(n - 1) + 1;
//            assert prevHi < lo;
            if (prevHi >= lo) {
                list.setQuick(n - 1, hi);
                return;
            }
        }

        list.add(lo);
        list.add(hi);
    }

    private static void parseRange(CharSequence seq, int lo, int p, int lim, int position, LongList out) throws ParserException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw QueryError.$(position, "Range not a number");
        }
        try {
            DateFormatUtils.parseInterval(seq, lo, p, out);
            int n = out.size();
            out.setQuick(n - 1, Dates.addPeriod(out.getQuick(n - 1), type, period));
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMillis = DateFormatUtils.tryParse(seq, lo, p);
            append(out, loMillis, Dates.addPeriod(loMillis, type, period));
        } catch (NumericException e) {
            throw QueryError.$(position, "Neither interval nor date");
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
            lo = Dates.addMonths(lo, period);
            hi = Dates.addMonths(hi, period);
            append(out, lo, hi);
        }
    }

    private static void addYearIntervals(int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);

        for (int i = 0, n = count - 1; i < n; i++) {
            lo = Dates.addYear(lo, period);
            hi = Dates.addYear(hi, period);
            append(out, lo, hi);
        }
    }
}