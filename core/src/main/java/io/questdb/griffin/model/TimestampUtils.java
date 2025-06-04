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

package io.questdb.griffin.model;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.Nullable;

public class TimestampUtils {
    public static void parseAndApplyIntervalEx(int timestampType, @Nullable CharSequence seq, LongList out, int position) throws SqlException {
        if (seq != null) {
            parseIntervalEx(timestampType, seq, 0, seq.length(), position, out, IntervalOperation.INTERSECT);
        } else {
            IntervalUtils.encodeInterval(Numbers.LONG_NULL, Numbers.LONG_NULL, IntervalOperation.INTERSECT, out);
        }
        IntervalUtils.applyLastEncodedIntervalEx(ColumnType.TIMESTAMP, out);
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
                    checkSpecialChar(seq, p++, lim);
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
                            if (lim - p > 3 && seq.charAt(p) == '.') {
                                p++;
                                int ms = Numbers.parseInt(seq, p, p += 3);
                                if (lim - p > 2 && Character.isDigit(seq.charAt(p))) {
                                    // micros
                                    int micr = Numbers.parseInt(seq, p, p += 3);
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + ms * Timestamps.MILLI_MICROS
                                            + micr
                                            + checkTimezoneTail(seq, p, lim)
                                            + 1;
                                } else {
                                    // millis
                                    ts = Timestamps.yearMicros(year, l)
                                            + Timestamps.monthOfYearMicros(month, l)
                                            + (day - 1) * Timestamps.DAY_MICROS
                                            + hour * Timestamps.HOUR_MICROS
                                            + min * Timestamps.MINUTE_MICROS
                                            + sec * Timestamps.SECOND_MICROS
                                            + (ms + 1) * Timestamps.MILLI_MICROS
                                            + checkTimezoneTail(seq, p, lim);
                                }
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + (sec + 1) * Timestamps.SECOND_MICROS
                                        + checkTimezoneTail(seq, p, lim);
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

    public static long parseFloorPartialTimestamp(CharSequence seq, final int pos, int lim) throws NumericException {
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
                    checkSpecialChar(seq, p++, lim);
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
                            if (p < lim && seq.charAt(p) == '.') {
                                p++;
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(lim, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = seq.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                micr *= tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(lim, p + 3); p < nlim; p++) {
                                    char c = seq.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                }

                                // micros
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + sec * Timestamps.SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(seq, p, lim);
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + sec * Timestamps.SECOND_MICROS
                                        + checkTimezoneTail(seq, p, lim);
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

    public static void parseIntervalEx(int timestampType, CharSequence seq, int lo, int lim, int position, LongList out, short operation) throws SqlException {
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
                // no semicolons, just date part, which can be the interval in itself
                try {
                    ColumnType.getTimestampDriver(timestampType).parseInterval(seq, lo, lim, operation, out);
                    break;
                } catch (NumericException ignore) {
                    // this must be a date then?
                }

                try {
                    long timestamp = ColumnType.getTimestampDriver(timestampType).parseFloor(seq, lo, lim);
                    IntervalUtils.encodeInterval(timestamp, timestamp, operation, out);
                    break;
                } catch (NumericException e) {
                    try {
                        final long timestamp = Numbers.parseLong(seq);
                        IntervalUtils.encodeInterval(timestamp, timestamp, operation, out);
                        break;
                    } catch (NumericException e2) {
                        throw SqlException.$(position, "Invalid date");
                    }
                }
            case 0:
                // single semicolon, expect a period format after date
                parseRange(timestampType, seq, lo, pos[0], lim, position, operation, out);
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

                parseRange(timestampType, seq, lo, pos[0], pos[1], position, operation, out);
                char type = seq.charAt(pos[2] - 1);
                long low = IntervalUtils.decodeIntervalLo(out, writeIndex);
                long hi = IntervalUtils.decodeIntervalHi(out, writeIndex);

                IntervalUtils.replaceHiLoInterval(low, hi, period, type, count, operation, out);
                switch (type) {
                    case PeriodType.YEAR:
                    case PeriodType.MONTH:
                    case PeriodType.HOUR:
                    case PeriodType.MINUTE:
                    case PeriodType.SECOND:
                    case PeriodType.DAY:
                        break;
                    default:
                        throw SqlException.$(position, "Unknown period: " + type + " at " + (p - 1));
                }
                break;
            default:
                throw SqlException.$(position, "Invalid interval format");
        }
    }

    private static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.INSTANCE;
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

    private static boolean checkLenStrict(int p, int lim) throws NumericException {
        if (lim - p == 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    private static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.INSTANCE;
        }
    }

    private static void checkSpecialChar(CharSequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.charAt(p) != 'T' && s.charAt(p) != ' ')) {
            throw NumericException.INSTANCE;
        }
    }

    private static long checkTimezoneTail(CharSequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = parseSign(seq.charAt(p++));
            int hour = Numbers.parseInt(seq, p, p += 2);
            checkRange(hour, 0, 23);

            if (lim - p == 3) {
                // Optional : separator between hours and mins in timezone
                checkChar(seq, p++, lim, ':');
            }

            if (checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                checkRange(min, 0, 59);
                return tzSign * (hour * Timestamps.HOUR_MICROS + min * Timestamps.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Timestamps.HOUR_MICROS);
            }
        }
        throw NumericException.INSTANCE;
    }

    private static void parseRange(int timestampType, CharSequence seq, int lo, int p, int lim, int position, short operation, LongList out) throws SqlException {
        char type = seq.charAt(lim - 1);
        int period;
        try {
            period = Numbers.parseInt(seq, p + 1, lim - 1);
        } catch (NumericException e) {
            throw SqlException.$(position, "Range not a number");
        }
        try {
            int index = out.size();
            ColumnType.getTimestampDriver(timestampType).parseInterval(seq, lo, p, operation, out);
            long low = IntervalUtils.decodeIntervalLo(out, index);
            long hi = IntervalUtils.decodeIntervalHi(out, index);
            hi = Timestamps.addPeriod(hi, type, period);
            if (hi < low) {
                throw SqlException.invalidDate(position);
            }
            IntervalUtils.replaceHiLoInterval(low, hi, operation, out);
            return;
        } catch (NumericException ignore) {
            // try date instead
        }
        try {
            long loMicros = TimestampFormatUtils.tryParse(seq, lo, p);
            long hiMicros = Timestamps.addPeriod(loMicros, type, period);
            if (hiMicros < loMicros) {
                throw SqlException.invalidDate(position);
            }
            IntervalUtils.encodeInterval(loMicros, hiMicros, operation, out);
        } catch (NumericException e) {
            throw SqlException.invalidDate(position);
        }
    }

    private static int parseSign(char c) throws NumericException {
        int tzSign;
        switch (c) {
            case '+':
                tzSign = -1;
                break;
            case '-':
                tzSign = 1;
                break;
            default:
                throw NumericException.INSTANCE;
        }
        return tzSign;
    }

    private static int tenPow(int i) throws NumericException {
        switch (i) {
            case 0:
                return 1;
            case 1:
                return 10;
            case 2:
                return 100;
            case 3:
                return 1000;
            case 4:
                return 10000;
            case 5:
                return 100000;
            default:
                throw NumericException.INSTANCE;
        }
    }
}
