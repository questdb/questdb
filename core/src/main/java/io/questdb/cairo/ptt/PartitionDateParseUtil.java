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

package io.questdb.cairo.ptt;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Timestamps;

public class PartitionDateParseUtil {

    public static long parseDayTime(CharSequence seq, int lim, int pos, long ts, int dayRange, int dayDigits) throws NumericException {
        checkChar(seq, pos++, lim, '-');
        int day = Numbers.parseInt(seq, pos, pos += dayDigits);
        checkRange(day, 1, dayRange);
        if (checkLen(pos, lim)) {
            checkChar(seq, pos++, lim, 'T');
            int hour = Numbers.parseInt(seq, pos, pos += 2);
            checkRange(hour, 0, 23);
            if (checkLen(pos, lim)) {
                int min = Numbers.parseInt(seq, pos, pos += 2);
                checkRange(min, 0, 59);
                if (checkLen(pos, lim)) {
                    int sec = Numbers.parseInt(seq, pos, pos += 2);
                    checkRange(sec, 0, 59);
                    if (pos < lim && seq.charAt(pos) == '-') {
                        pos++;
                        // varlen milli and micros
                        int micrLim = pos + 6;
                        int mlim = Math.min(lim, micrLim);
                        int micr = 0;
                        for (; pos < mlim; pos++) {
                            char c = seq.charAt(pos);
                            if (c < '0' || c > '9') {
                                throw NumericException.INSTANCE;
                            }
                            micr *= 10;
                            micr += c - '0';
                        }
                        micr *= tenPow(micrLim - pos);

                        // micros
                        ts += (day - 1) * Timestamps.DAY_MICROS
                                + hour * Timestamps.HOUR_MICROS
                                + min * Timestamps.MINUTE_MICROS
                                + sec * Timestamps.SECOND_MICROS
                                + micr;
                    } else {
                        if (pos == lim) {
                            // seconds
                            ts += (day - 1) * Timestamps.DAY_MICROS
                                    + hour * Timestamps.HOUR_MICROS
                                    + min * Timestamps.MINUTE_MICROS
                                    + sec * Timestamps.SECOND_MICROS;
                        } else {
                            throw NumericException.INSTANCE;
                        }
                    }
                } else {
                    // minute
                    ts += (day - 1) * Timestamps.DAY_MICROS
                            + hour * Timestamps.HOUR_MICROS
                            + min * Timestamps.MINUTE_MICROS;

                }
            } else {
                // year + month + day + hour
                ts += (day - 1) * Timestamps.DAY_MICROS
                        + hour * Timestamps.HOUR_MICROS;
            }
        } else {
            // year + month + day
            ts += (day - 1) * Timestamps.DAY_MICROS;
        }
        return ts;
    }

    public static long parseFloorPartialTimestamp(CharSequence seq, final int pos, int lim) throws NumericException {
        long ts;
        if (lim - pos < 4 || lim - pos > 25) {
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
                int dayRange = Timestamps.getDaysPerMonth(month, l);
                ts = Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l);
                ts = parseDayTime(seq, lim, p, ts, dayRange, 2);
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

    private static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    private static boolean checkLen(int p, int lim) throws NumericException {
        if (lim - p >= 2) {
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
