/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql.parser;

import com.questdb.ex.NumericException;
import com.questdb.ex.ParserException;
import com.questdb.misc.Dates;
import com.questdb.misc.Interval;
import com.questdb.misc.Numbers;
import com.questdb.std.ObjList;

public class IntervalCompiler {

    /**
     * Intersects two lists of intervals and returns result list. Both lists are expected
     * to be chronologically ordered and result list will be ordered as well.
     *
     * @param a list of intervals
     * @param b list of intervals
     * @return intersection
     */
    public static ObjList<Interval> intersect(ObjList<Interval> a, ObjList<Interval> b) {
        ObjList<Interval> out = new ObjList<>();

        int indexA = 0;
        int indexB = 0;
        final int sizeA = a.size();
        final int sizeB = b.size();
        Interval intervalA = null;
        Interval intervalB = null;

        while (true) {
            if (intervalA == null && indexA < sizeA) {
                intervalA = a.getQuick(indexA++);
            }

            if (intervalB == null && indexB < sizeB) {
                intervalB = b.getQuick(indexB++);
            }

            if (intervalA == null || intervalB == null) {
                break;
            }

            // a fully above b
            if (intervalA.getHi() < intervalB.getLo()) {
                // a loses
                intervalA = null;
            } else if (intervalA.getLo() > intervalB.getHi()) {
                // a fully below b
                // b loses
                intervalB = null;
            } else {
                append(out, new Interval(
                        Math.max(intervalA.getLo(), intervalB.getLo()),
                        Math.min(intervalA.getHi(), intervalB.getHi())
                ));

                if (intervalA.getHi() < intervalB.getHi()) {
                    // b hanging lower than a
                    // a loses
                    intervalA = null;
                } else {
                    // otherwise a lower than b
                    // a loses
                    intervalB = null;
                }
            }
        }

        return out;
    }

    public static ObjList<Interval> parseIntervalEx(CharSequence seq, int lo, int lim, int position) throws ParserException {
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

        final ObjList<Interval> out = new ObjList<>();

        switch (p) {
            case -1:
                // no semicolons, just date part, which can be interval in itself
                try {
                    out.add(Dates.parseInterval(seq, lo, lim));
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long millis = Dates.tryParse(seq, lo, lim);
                    out.add(new Interval(millis, millis));
                    break;
                } catch (NumericException e) {
                    throw QueryError.$(position, "Not a date");
                }
            case 0:
                // single semicolon, expect period format after date
                out.add(parseRange(seq, lo, pos[0], lim, position));
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

                char type = seq.charAt(pos[2] - 1);
                switch (type) {
                    case 'y':
                        addYearIntervals(parseRange(seq, lo, pos[0], pos[1], position), period, count, out);
                        break;
                    case 'M':
                        addMonthInterval(parseRange(seq, lo, pos[0], pos[1], position), period, count, out);
                        break;
                    case 'h':
                        addMillisInterval(parseRange(seq, lo, pos[0], pos[1], position), period * Dates.HOUR_MILLIS, count, out);
                        break;
                    case 'm':
                        addMillisInterval(parseRange(seq, lo, pos[0], pos[1], position), period * Dates.MINUTE_MILLIS, count, out);
                        break;
                    case 's':
                        addMillisInterval(parseRange(seq, lo, pos[0], pos[1], position), period * Dates.SECOND_MILLIS, count, out);
                        break;
                    case 'd':
                        addMillisInterval(parseRange(seq, lo, pos[0], pos[1], position), period * Dates.DAY_MILLIS, count, out);
                        break;
                    default:
                        throw QueryError.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw QueryError.$(position, "Invalid interval format");
        }

        return out;
    }

    /**
     * Performs set subtraction on two lists of intervals. Subtracts b from a, e.g. result = a - b.
     * Both sets are expected to be ordered chronologically.
     *
     * @param a list of intervals
     * @param b list of intervals
     * @return result of subtraction
     */
    public static ObjList<Interval> subtract(ObjList<Interval> a, ObjList<Interval> b) {
        ObjList<Interval> out = new ObjList<>();

        int indexA = 0;
        int indexB = 0;
        final int sizeA = a.size();
        final int sizeB = b.size();
        Interval intervalA = null;
        Interval intervalB = null;

        while (true) {
            if (intervalA == null && indexA < sizeA) {
                intervalA = a.getQuick(indexA++);
            }

            if (intervalB == null && indexB < sizeB) {
                intervalB = b.getQuick(indexB++);
            }

            if (intervalA != null && intervalB == null) {
                append(out, intervalA);
                intervalA = null;
                continue;
            }

            if (intervalA == null) {
                break;
            }

            // a fully above b
            if (intervalA.getHi() < intervalB.getLo()) {
                // a loses
                append(out, intervalA);
                intervalA = null;
            } else if (intervalA.getLo() > intervalB.getHi()) {
                // a fully below b
                // b loses
                intervalB = null;
            } else {

                if (intervalA.getLo() < intervalB.getLo()) {
                    // top part of a is above b
                    append(out, new Interval(intervalA.getLo(), intervalB.getLo()));
                }

                if (intervalA.getHi() > intervalB.getHi()) {
                    intervalA = new Interval(intervalB.getHi(), intervalA.getHi());
                    intervalB = null;
                } else {
                    intervalA = null;
                }
            }
        }
        return out;
    }

    /**
     * Creates a union (set operation) of two lists of intervals. Both lists are
     * expected to be ordered chronologically.
     *
     * @param a list of intervals
     * @param b list of intervals
     * @return union of intervals
     */
    public static ObjList<Interval> union(ObjList<Interval> a, ObjList<Interval> b) {
        ObjList<Interval> out = new ObjList<>();

        int indexA = 0;
        int indexB = 0;
        final int sizeA = a.size();
        final int sizeB = b.size();
        Interval intervalA = null;
        Interval intervalB = null;

        while (true) {
            if (intervalA == null && indexA < sizeA) {
                intervalA = a.getQuick(indexA++);
            }

            if (intervalB == null && indexB < sizeB) {
                intervalB = b.getQuick(indexB++);
            }


            if (intervalA == null && intervalB != null) {
                append(out, intervalB);
                intervalB = null;
                continue;
            }

            if (intervalA != null && intervalB == null) {
                append(out, intervalA);
                intervalA = null;
                continue;
            }

            if (intervalA == null) {
                break;
            }

            // a fully above b
            if (intervalA.getHi() < intervalB.getLo()) {
                append(out, intervalA);
                intervalA = null;
            } else if (intervalA.getLo() > intervalB.getHi()) {
                // a fully below b
                append(out, intervalB);
                intervalB = null;
            } else {

                Interval next = new Interval(
                        Math.min(intervalA.getLo(), intervalB.getLo()),
                        Math.max(intervalA.getHi(), intervalB.getHi())
                );

                if (intervalA.getHi() < intervalB.getHi()) {
                    // b hanging lower than a
                    intervalB = next;
                    intervalA = null;
                } else {
                    // otherwise a lower than b
                    intervalA = next;
                    intervalB = null;
                }
            }
        }
        return out;
    }

    private static void append(ObjList<Interval> list, Interval interval) {
        int n = list.size();
        if (n > 0) {
            Interval prev = list.getQuick(n - 1);
            if (prev.getHi() + 1 == interval.getLo()) {
                list.setQuick(n - 1, new Interval(prev.getLo(), interval.getHi()));
                return;
            }
        }

        list.add(interval);
    }

    private static Interval parseRange(CharSequence seq, int lo, int p, int lim, int position) throws ParserException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw QueryError.$(position, "Range not a number");
        }
        try {
            Interval interval = Dates.parseInterval(seq, lo, p);
            return new Interval(interval.getLo(), Dates.addPeriod(interval.getHi(), type, period));
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMillis = Dates.tryParse(seq, lo, p - 1);
            long hiMillis = Dates.addPeriod(loMillis, type, period);
            return new Interval(loMillis, hiMillis);
        } catch (NumericException e) {
            throw QueryError.$(position, "Neither interval nor date");
        }
    }

    private static void addMillisInterval(Interval interval, long period, int count, ObjList<Interval> out) {
        out.add(interval);
        for (int i = 0, n = count - 1; i < n; i++) {
            interval = new Interval(
                    interval.getLo() + period,
                    interval.getHi() + period
            );
            out.add(interval);
        }
    }

    private static void addMonthInterval(Interval interval, int period, int count, ObjList<Interval> out) {
        out.add(interval);
        for (int i = 0, n = count - 1; i < n; i++) {
            interval = new Interval(
                    Dates.addMonths(interval.getLo(), period),
                    Dates.addMonths(interval.getHi(), period)
            );
            out.add(interval);
        }
    }

    private static void addYearIntervals(Interval interval, int period, int count, ObjList<Interval> out) {
        out.add(interval);
        for (int i = 0, n = count - 1; i < n; i++) {
            interval = new Interval(
                    Dates.addYear(interval.getLo(), period),
                    Dates.addYear(interval.getHi(), period)
            );
            out.add(interval);
        }
    }

    public static class Result {
        ObjList<Interval> intervals = null;
        boolean contaminated = false;
    }
}