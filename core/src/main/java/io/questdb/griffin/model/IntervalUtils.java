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
    public static final int LO_INDEX = 0;
    public static final int HI_INDEX = 1;
    public static final int OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX = 2;
    public static final int PERIOD_COUNT_INDEX = 3;
    public static final int STATIC_LONGS_PER_DYNAMIC_INTERVAL = 4;

    public static void addHiLoInterval(
            long lo,
            long hi,
            int period,
            char periodType,
            int periodCount,
            short operation,
            LongList out) {
        addHiLoInterval(lo, hi, period, periodType, periodCount, (short) 0, (short) 0, operation, out);
    }

    public static void addHiLoInterval(
            long lo,
            long hi,
            int period,
            char periodType,
            int periodCount,
            short adjustment,
            short dynamicIndicator,
            short operation,
            LongList out) {
        out.add(lo);
        out.add(hi);
        out.add(Numbers.encodeLowHighInts(
                Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                Numbers.encodeLowHighShorts(adjustment, dynamicIndicator)));
        out.add(Numbers.encodeLowHighInts(period, periodCount));
    }

    public static void addHiLoInterval(long lo,
                                       long hi,
                                       short adjustment,
                                       short dynamicIndicator,
                                       short operation,
                                       LongList out) {
        addHiLoInterval(lo, hi, 0, (char) 0, 1, adjustment, dynamicIndicator, operation, out);
    }

    public static void addHiLoInterval(long lo, long hi, short operation, LongList out) {
        addHiLoInterval(lo, hi, 0, (char) 0, 1, operation, out);
    }

    public static void apply(LongList temp, long lo, long hi, int period, char periodType, int count) {
        temp.add(lo);
        temp.add(hi);
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

    public static void applyLastEncodedIntervalEx(LongList intervals) {
        int index = intervals.size() - 4;
        long lo = getEncodedPeriodLo(intervals, index);
        long hi = getEncodedPeriodHi(intervals, index);
        int period = getEncodedPeriod(intervals, index);
        char periodType = getEncodedPeriodType(intervals, index);
        int count = getEncodedPeriodCount(intervals, index);

        intervals.truncateTo(index);
        if (periodType == 0) {
            intervals.extendAndSet(index + 1, hi);
            intervals.setQuick(index, lo);
            return;
        }
        apply(intervals, lo, hi, period, periodType, count);
    }

    public static short getEncodedAdjustment(LongList intervals, int index) {
        return Numbers.decodeLowShort(Numbers.decodeHighInt(
                intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)));
    }

    public static short getEncodedDynamicIndicator(LongList intervals, int index) {
        return Numbers.decodeHighShort(Numbers.decodeHighInt(
                intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)));
    }

    public static short getEncodedOperation(LongList intervals, int index) {
        return Numbers.decodeLowShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
    }

    public static int getEncodedPeriod(LongList intervals, int index) {
        return Numbers.decodeLowInt(
                intervals.getQuick(index + PERIOD_COUNT_INDEX)
        );
    }

    public static int getEncodedPeriodCount(LongList intervals, int index) {
        return Numbers.decodeHighInt(
                intervals.getQuick(index + PERIOD_COUNT_INDEX));
    }

    public static long getEncodedPeriodHi(LongList out, int index) {
        return out.getQuick(index + HI_INDEX);
    }

    public static long getEncodedPeriodLo(LongList out, int index) {
        return out.getQuick(index + LO_INDEX);
    }

    public static char getEncodedPeriodType(LongList intervals, int index) {
        int pts = Numbers.decodeHighShort(
                Numbers.decodeLowInt(
                        intervals.getQuick(index + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX)
                )
        );
        return (char) (pts - (int) Short.MIN_VALUE);
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

    public static long parseCCPartialDate(CharSequence seq) throws NumericException {
        return parseCCPartialDate(seq, 0, seq.length());
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

    public static long parseFloorPartialDate(CharSequence seq) throws NumericException {
        return parseFloorPartialDate(seq, 0, seq.length());
    }

    public static void subtract(LongList intervals, int divider) {
        IntervalUtils.invert(intervals, divider);
        IntervalUtils.intersectInplace(intervals, divider);
    }

    /**
     * Intersects two lists of intervals compacted in one list in place.
     * Intervals to be chronologically ordered and result list will be ordered as well.
     * <p>
     * Treat a as 2 lists,
     * a: first from 0 to divider
     * b: from divider to the end of list
     *
     * @param concatenatedIntervals 2 lists of intervals concatenated in 1
     */
    static void intersectInplace(LongList concatenatedIntervals, int dividerIndex) {
        final int sizeA = dividerIndex / 2;
        final int sizeB = sizeA + (concatenatedIntervals.size() - dividerIndex) / 2;
        int aLower = 0;

        int intervalB = sizeA;
        int writePoint = 0;

        int aUpperSize = sizeB;
        int aUpper = sizeB;

        while ((aLower < sizeA || aUpper < aUpperSize) && intervalB < sizeB) {

            int intervalA = aUpper < aUpperSize ? aUpper : aLower;
            long aLo = concatenatedIntervals.getQuick(intervalA * 2);
            long aHi = concatenatedIntervals.getQuick(intervalA * 2 + 1);

            long bLo = concatenatedIntervals.getQuick(intervalB * 2);
            long bHi = concatenatedIntervals.getQuick(intervalB * 2 + 1);

            if (aHi < bLo) {
                // a fully above b
                // a loses
                if (aUpper < aUpperSize) {
                    aUpper++;
                } else {
                    aLower++;
                }
            } else if (aLo > bHi) {
                // a fully below b
                // b loses
                intervalB++;
            } else {
                if (aHi < bHi) {
                    // b hanging lower than a
                    // a loses
                    if (aUpper < aUpperSize) {
                        aUpper++;
                    } else {
                        aLower++;
                    }
                } else {
                    // otherwise a lower than b
                    // a loses
                    intervalB++;
                }

                assert writePoint <= aLower || writePoint >= sizeA;
                if (writePoint == aLower && aLower < sizeA) {
                    // We cannot keep A position, it will be overwritten, hence intervalB++; is not possible
                    // Copy a point to A area instead
                    concatenatedIntervals.add(concatenatedIntervals.getQuick(writePoint * 2));
                    concatenatedIntervals.add(concatenatedIntervals.getQuick(writePoint * 2 + 1));
                    aUpperSize = concatenatedIntervals.size() / 2;
                    aLower++;
                }

                writePoint = append(concatenatedIntervals, writePoint, Math.max(aLo, bLo), Math.min(aHi, bHi));
            }
        }

        concatenatedIntervals.truncateTo(2 * writePoint);
    }

    static int append(LongList list, int writePoint, long lo, long hi) {
        if (writePoint > 0) {
            long prevHi = list.getQuick(2 * writePoint - 1) + 1;
            if (prevHi >= lo) {
                list.setQuick(2 * writePoint - 1, hi);
                return writePoint;
            }
        }

        list.extendAndSet(2 * writePoint + 1, hi);
        list.setQuick(2 * writePoint, lo);
        return writePoint + 1;
    }

    private static void parseRange(CharSequence seq, int lo, int p, int lim, int position, short operation, LongList out) throws SqlException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw SqlException.$(position, "Range not a number");
        }
        try {
            int index = out.size();
            parseInterval(seq, lo, p, operation, out);
            long low = getEncodedPeriodLo(out, index);
            long hi = getEncodedPeriodHi(out, index);
            replaceHiLoInterval(low, Timestamps.addPeriod(hi, type, period), operation, out);
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMillis = TimestampFormatUtils.tryParse(seq, lo, p);
            addHiLoInterval(loMillis, Timestamps.addPeriod(loMillis, type, period), operation, out);
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
    }

    private static void addMillisInterval(long period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int writePoint = k / 2;

        for (int i = 0, n = count - 1; i < n; i++) {
            lo += period;
            hi += period;
            writePoint = append(out, writePoint, lo, hi);
        }
    }

    private static void addMonthInterval(int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int writePoint = k / 2;

        for (int i = 0, n = count - 1; i < n; i++) {
            lo = Timestamps.addMonths(lo, period);
            hi = Timestamps.addMonths(hi, period);
            writePoint = append(out, writePoint, lo, hi);
        }
    }

    private static void addYearIntervals(int period, int count, LongList out) {
        int k = out.size();
        long lo = out.getQuick(k - 2);
        long hi = out.getQuick(k - 1);
        int writePoint = k / 2;

        for (int i = 0, n = count - 1; i < n; i++) {
            lo = Timestamps.addYear(lo, period);
            hi = Timestamps.addYear(hi, period);
            writePoint = append(out, writePoint, lo, hi);
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
        invert(intervals, 0);
    }

    /**
     * Inverts intervals. This method also produces inclusive edges that differ from source ones by 1 milli.
     *
     * @param intervals collection of intervals
     */
    static void invert(LongList intervals, int startIndex) {
        long last = Long.MIN_VALUE;
        int n = intervals.size();
        int writeIndex = startIndex;
        for (int i = startIndex; i < n; i += 2) {
            final long lo = intervals.getQuick(i);
            final long hi = intervals.getQuick(i + 1);
            if (lo > last) {
                intervals.setQuick(writeIndex, last);
                intervals.setQuick(writeIndex + 1, lo - 1);
                writeIndex += 2;
            }
            last = hi + 1;
        }

        // If last hi was Long.MAX_VALUE then last will be Long.MIN_VALUE after +1 overflow
        if (last != Long.MIN_VALUE) {
            intervals.extendAndSet(writeIndex + 1, Long.MAX_VALUE);
            intervals.setQuick(writeIndex, last);
            writeIndex += 2;
        }

        intervals.truncateTo(writeIndex);
    }

    public static boolean isInIntervals(LongList intervals, long timestamp) {
        int left = 0;
        int right = intervals.size() / 2 - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            long lo = getEncodedPeriodLo(intervals, mid * 2);
            long hi = getEncodedPeriodHi(intervals, mid * 2);
            if (lo > timestamp) {
                right = mid - 1;
            } else if (hi < timestamp) {
                left = mid + 1;
            } else {
                return true;
            }
        }
        return false;
    }

    public static void parseIntervalEx(CharSequence seq, int lo, int lim, int position, LongList out, short operation) throws SqlException {
        int writeIndex = out.size();
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
                    parseInterval(seq, lo, lim, operation, out);
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long millis = TimestampFormatUtils.tryParse(seq, lo, lim);
                    addHiLoInterval(millis, millis, operation, out);
                    break;
                } catch (NumericException e) {
                    throw SqlException.$(position, "Not a date");
                }
            case 0:
                // single semicolon, expect period format after date
                parseRange(seq, lo, pos[0], lim, position, operation, out);
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

                parseRange(seq, lo, pos[0], pos[1], position, operation, out);
                char type = seq.charAt(pos[2] - 1);
                long low = getEncodedPeriodLo(out, writeIndex);
                long hi = getEncodedPeriodHi(out, writeIndex);

                replaceHiLoInterval(low, hi, period, type, count, operation, out);
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

    private static void replaceHiLoInterval(long lo, long hi, int period, char periodType, int periodCount, short operation, LongList out) {
        int lastIndex = out.size() - 4;
        out.setQuick(lastIndex, lo);
        out.setQuick(lastIndex + HI_INDEX, hi);
        out.setQuick(lastIndex + OPERATION_PERIOD_TYPE_ADJUSTMENT_INDEX, Numbers.encodeLowHighInts(
                Numbers.encodeLowHighShorts(operation, (short) ((int) periodType + Short.MIN_VALUE)),
                0));
        out.setQuick(lastIndex + PERIOD_COUNT_INDEX, Numbers.encodeLowHighInts(period, periodCount));
    }

    private static void replaceHiLoInterval(long lo, long hi, short operation, LongList out) {
        replaceHiLoInterval(lo, hi, 0, (char) 0, 1, operation, out);
    }

    public static void parseInterval(CharSequence seq, int pos, int lim, short operation, LongList out) throws NumericException {
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
                                addHiLoInterval(Timestamps.yearMicros(year, l)
                                                + Timestamps.monthOfYearMicros(month, l)
                                                + (day - 1) * Timestamps.DAY_MICROS
                                                + hour * Timestamps.HOUR_MICROS
                                                + min * Timestamps.MINUTE_MICROS
                                                + sec * Timestamps.SECOND_MICROS,
                                        Timestamps.yearMicros(year, l)
                                                + Timestamps.monthOfYearMicros(month, l)
                                                + (day - 1) * Timestamps.DAY_MICROS
                                                + hour * Timestamps.HOUR_MICROS
                                                + min * Timestamps.MINUTE_MICROS
                                                + sec * Timestamps.SECOND_MICROS
                                                + 999999,
                                        operation,
                                        out);
                            }
                        } else {
                            // minute
                            addHiLoInterval(Timestamps.yearMicros(year, l)
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
                                            + 999999,
                                    operation,
                                    out);
                        }
                    } else {
                        // year + month + day + hour
                        addHiLoInterval(Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS,
                                Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + 59 * Timestamps.MINUTE_MICROS
                                        + 59 * Timestamps.SECOND_MICROS
                                        + 999999,
                                operation,
                                out);
                    }
                } else {
                    // year + month + day
                    addHiLoInterval(Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS,
                            Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + +(day - 1) * Timestamps.DAY_MICROS
                                    + 23 * Timestamps.HOUR_MICROS
                                    + 59 * Timestamps.MINUTE_MICROS
                                    + 59 * Timestamps.SECOND_MICROS
                                    + 999999,
                            operation,
                            out);
                }
            } else {
                // year + month
                addHiLoInterval(Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l),
                        Timestamps.yearMicros(year, l)
                                + Timestamps.monthOfYearMicros(month, l)
                                + (Timestamps.getDaysPerMonth(month, l) - 1) * Timestamps.DAY_MICROS
                                + 23 * Timestamps.HOUR_MICROS
                                + 59 * Timestamps.MINUTE_MICROS
                                + 59 * Timestamps.SECOND_MICROS
                                + 999999,
                        operation,
                        out);
            }
        } else {
            // year
            addHiLoInterval(Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(1, l),
                    Timestamps.yearMicros(year, l)
                            + Timestamps.monthOfYearMicros(12, l)
                            + (Timestamps.getDaysPerMonth(12, l) - 1) * Timestamps.DAY_MICROS
                            + 23 * Timestamps.HOUR_MICROS
                            + 59 * Timestamps.MINUTE_MICROS
                            + 59 * Timestamps.SECOND_MICROS
                            + 999999,
                    operation,
                    out);
        }
    }
}
