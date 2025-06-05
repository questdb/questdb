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

package io.questdb.cairo;

import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.CommonFormatUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.PartitionBy.*;
import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;
import static io.questdb.griffin.SqlUtil.castPGDates;
import static io.questdb.std.datetime.CommonFormatUtils.EN_LOCALE;
import static io.questdb.std.datetime.microtime.TimestampFormatUtils.*;

public class MicrosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new MicrosTimestampDriver();

    private static final PartitionAddMethod ADD_DD = Timestamps::addDays;
    private static final PartitionAddMethod ADD_HH = Timestamps::addHours;
    private static final PartitionAddMethod ADD_MM = Timestamps::addMonths;
    private static final PartitionAddMethod ADD_WW = Timestamps::addWeeks;
    private static final PartitionAddMethod ADD_YYYY = Timestamps::addYears;
    private static final PartitionCeilMethod CEIL_DD = Timestamps::ceilDD;
    private static final PartitionCeilMethod CEIL_HH = Timestamps::ceilHH;
    private static final PartitionCeilMethod CEIL_MM = Timestamps::ceilMM;
    private static final PartitionCeilMethod CEIL_WW = Timestamps::ceilWW;
    private static final PartitionCeilMethod CEIL_YYYY = Timestamps::ceilYYYY;
    private static final DateFormat DEFAULT_FORMAT = new DateFormat() {
        @Override
        public void format(long datetime, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            sink.putAscii(DEFAULT_PARTITION_NAME);
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) {
            return 0;
        }
    };
    private static final PartitionFloorMethod FLOOR_DD = Timestamps::floorDD;
    private static final PartitionFloorMethod FLOOR_HH = Timestamps::floorHH;
    private static final PartitionFloorMethod FLOOR_MM = Timestamps::floorMM;
    private static final PartitionFloorMethod FLOOR_WW = Timestamps::floorWW;
    private static final PartitionFloorMethod FLOOR_YYYY = Timestamps::floorYYYY;
    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(FLOOR_DD, DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(FLOOR_HH, HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(FLOOR_MM, MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(FLOOR_YYYY, YEAR_FORMAT);

    public static CairoException expectedPartitionDirNameFormatCairoException(CharSequence partitionName, int lo, int hi, int partitionBy) {
        final CairoException ee = CairoException.critical(0).put('\'');
        switch (partitionBy) {
            case DAY:
                ee.put(CommonFormatUtils.DAY_PATTERN);
                break;
            case WEEK:
                ee.put(CommonFormatUtils.WEEK_PATTERN).put("' or '").put(CommonFormatUtils.DAY_PATTERN);
                break;
            case MONTH:
                ee.put(CommonFormatUtils.MONTH_PATTERN);
                break;
            case YEAR:
                ee.put(CommonFormatUtils.YEAR_PATTERN);
                break;
            case HOUR:
                ee.put(CommonFormatUtils.HOUR_PATTERN);
                break;
        }
        ee.put("' expected, found [ts=").put(partitionName.subSequence(lo, hi)).put(']');
        return ee;
    }

    public static long floor(CharSequence value) throws NumericException {
        return INSTANCE.parseFloorLiteral(value);
    }

    public static long parseDayTime(CharSequence seq, int lim, int pos, long ts, int dayRange, int dayDigits) throws NumericException {
        checkChar(seq, pos++, lim, '-');
        int day = Numbers.parseInt(seq, pos, pos += dayDigits);
        checkRange(day, 1, dayRange);
        if (checkLen3(pos, lim)) {
            checkChar(seq, pos++, lim, 'T');
            int hour = Numbers.parseInt(seq, pos, pos += 2);
            checkRange(hour, 0, 23);
            if (checkLen2(pos, lim)) {
                int min = Numbers.parseInt(seq, pos, pos += 2);
                checkRange(min, 0, 59);
                if (checkLen2(pos, lim)) {
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

    @Override
    public long addMonths(long timestamp, int months) {
        return Timestamps.addMonths(timestamp, months);
    }

    @Override
    public long addPeriod(long lo, char type, int period) {
        return Timestamps.addPeriod(lo, type, period);
    }

    @Override
    public long addYears(long timestamp, int years) {
        return Timestamps.addYears(timestamp, years);
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        TimestampFormatUtils.appendDateTimeUSec(sink, timestamp);
    }

    @Override
    public void appendMem(CharSequence value, MemoryA mem) {
        try {
            mem.putLong(parseFloorLiteral(value));
        } catch (NumericException e) {
            mem.putLong(Numbers.LONG_NULL);
        }
    }

    @Override
    public void appendPGWireText(CharSink<?> sink, long timestamp) {
        TimestampFormatUtils.PG_TIMESTAMP_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public long castStr(CharSequence value, int tupleIndex, short fromType, short toType) {
        try {
            return parseFloorLiteral(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(tupleIndex, value, fromType, toType);
        }
    }

    @Override
    public boolean convertToVar(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            TimestampFormatUtils.appendDateTimeUSec(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public long fromDays(int days) {
        return days * Timestamps.DAY_MICROS;
    }

    @Override
    public long fromHours(int hours) {
        return hours * Timestamps.HOUR_MICROS;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Timestamps.MINUTE_MICROS;
    }

    @Override
    public long fromSeconds(int seconds) {
        return seconds * Timestamps.SECOND_MICROS;
    }

    @Override
    public PartitionAddMethod getPartitionAddMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return ADD_DD;
            case MONTH:
                return ADD_MM;
            case YEAR:
                return ADD_YYYY;
            case HOUR:
                return ADD_HH;
            case WEEK:
                return ADD_WW;
            default:
                return null;
        }
    }

    @Override
    public PartitionCeilMethod getPartitionCeilMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return CEIL_DD;
            case MONTH:
                return CEIL_MM;
            case YEAR:
                return CEIL_YYYY;
            case HOUR:
                return CEIL_HH;
            case WEEK:
                return CEIL_WW;
            default:
                return null;
        }
    }

    @Override
    public DateFormat getPartitionDirFormatMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return PARTITION_DAY_FORMAT;
            case MONTH:
                return PARTITION_MONTH_FORMAT;
            case YEAR:
                return PARTITION_YEAR_FORMAT;
            case HOUR:
                return PARTITION_HOUR_FORMAT;
            case WEEK:
                return PARTITION_WEEK_FORMAT;
            case NONE:
                return DEFAULT_FORMAT;
            default:
                throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
        }
    }

    @Override
    public PartitionFloorMethod getPartitionFloorMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return FLOOR_DD;
            case WEEK:
                return FLOOR_WW;
            case MONTH:
                return FLOOR_MM;
            case YEAR:
                return FLOOR_YYYY;
            case HOUR:
                return FLOOR_HH;
            default:
                return null;
        }
    }

    @Override
    public long implicitCast(CharSequence value, int typeFrom) {
        assert typeFrom == ColumnType.STRING || typeFrom == ColumnType.SYMBOL;
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }

            // Parse as ISO with variable length.
            try {
                return parseFloorLiteral(value);
            } catch (NumericException ignore) {
            }

            return castPGDates(value, typeFrom);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public long implicitCastVarchar(Utf8Sequence value) {
        if (value != null) {
            try {
                return Numbers.parseLong(value);
            } catch (NumericException ignore) {
            }

            // Parse as ISO with variable length.
            try {
                return parseFloorLiteral(value);
            } catch (NumericException ignore) {
            }

            // all formats are ascii
            if (value.isAscii()) {
                return castPGDates(value.asAsciiCharSequence(), ColumnType.VARCHAR);
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, ColumnType.TIMESTAMP);
        }
        return Numbers.LONG_NULL;
    }

    @Override
    public long monthsBetween(long hi, long lo) {
        return Timestamps.getMonthsBetween(hi, lo);
    }

    @Override
    public long parseAnyFormat(CharSequence token, int start, int len) throws NumericException {
        return TimestampFormatUtils.tryParse(token, start, len);
    }

    @Override
    public long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.INSTANCE;
        }
        int p = lo;
        int year = Numbers.parseInt(str, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen3(p, hi)) {
            checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen3(p, hi)) {
                checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
                if (checkLen3(p, hi)) {
                    checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (checkLen3(p, hi)) {
                        checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        checkRange(min, 0, 59);
                        if (checkLen3(p, hi)) {
                            checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < hi && str.byteAt(p) == '.') {

                                p++;
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(hi, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                micr *= tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = (char) str.byteAt(p);
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
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + sec * Timestamps.SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
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

    @Override
    public long parseFloor(CharSequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.INSTANCE;
        }
        int p = lo;
        int year = Numbers.parseInt(str, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen3(p, hi)) {
            checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen3(p, hi)) {
                checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
                if (checkLen3(p, hi)) {
                    checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (checkLen3(p, hi)) {
                        checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        checkRange(min, 0, 59);
                        if (checkLen3(p, hi)) {
                            checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < hi && str.charAt(p) == '.') {
                                p++;
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(hi, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                micr *= tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = str.charAt(p);
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
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Timestamps.yearMicros(year, l)
                                        + Timestamps.monthOfYearMicros(month, l)
                                        + (day - 1) * Timestamps.DAY_MICROS
                                        + hour * Timestamps.HOUR_MICROS
                                        + min * Timestamps.MINUTE_MICROS
                                        + sec * Timestamps.SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
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

    public void parseInterval(CharSequence input, int pos, int lim, short operation, LongList out) throws NumericException {
        if (lim - pos < 4) {
            throw NumericException.INSTANCE;
        }
        int p = pos;
        int year = Numbers.parseInt(input, p, p += 4);
        boolean l = Timestamps.isLeapYear(year);
        if (checkLen3(p, lim)) {
            checkChar(input, p++, lim, '-');
            int month = Numbers.parseInt(input, p, p += 2);
            checkRange(month, 1, 12);
            if (checkLen3(p, lim)) {
                checkChar(input, p++, lim, '-');
                int day = Numbers.parseInt(input, p, p += 2);
                checkRange(day, 1, Timestamps.getDaysPerMonth(month, l));
                if (checkLen3(p, lim)) {
                    checkChar(input, p++, lim, 'T');
                    int hour = Numbers.parseInt(input, p, p += 2);
                    checkRange(hour, 0, 23);
                    if (checkLen3(p, lim)) {
                        checkChar(input, p++, lim, ':');
                        int min = Numbers.parseInt(input, p, p += 2);
                        checkRange(min, 0, 59);
                        if (checkLen3(p, lim)) {
                            checkChar(input, p++, lim, ':');
                            int sec = Numbers.parseInt(input, p, p += 2);
                            checkRange(sec, 0, 59);
                            if (p < lim) {
                                throw NumericException.INSTANCE;
                            } else {
                                // seconds
                                IntervalUtils.encodeInterval(Timestamps.yearMicros(year, l)
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
                            IntervalUtils.encodeInterval(
                                    Timestamps.yearMicros(year, l)
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
                                    out
                            );
                        }
                    } else {
                        // year + month + day + hour
                        IntervalUtils.encodeInterval(
                                Timestamps.yearMicros(year, l)
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
                                out
                        );
                    }
                } else {
                    // year + month + day
                    IntervalUtils.encodeInterval(
                            Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS,
                            Timestamps.yearMicros(year, l)
                                    + Timestamps.monthOfYearMicros(month, l)
                                    + (day - 1) * Timestamps.DAY_MICROS
                                    + 23 * Timestamps.HOUR_MICROS
                                    + 59 * Timestamps.MINUTE_MICROS
                                    + 59 * Timestamps.SECOND_MICROS
                                    + 999999,
                            operation,
                            out
                    );
                }
            } else {
                // year + month
                IntervalUtils.encodeInterval(
                        Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l),
                        Timestamps.yearMicros(year, l)
                                + Timestamps.monthOfYearMicros(month, l)
                                + (Timestamps.getDaysPerMonth(month, l) - 1) * Timestamps.DAY_MICROS
                                + 23 * Timestamps.HOUR_MICROS
                                + 59 * Timestamps.MINUTE_MICROS
                                + 59 * Timestamps.SECOND_MICROS
                                + 999999,
                        operation,
                        out
                );
            }
        } else {
            // year
            IntervalUtils.encodeInterval(
                    Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(1, l),
                    Timestamps.yearMicros(year, l)
                            + Timestamps.monthOfYearMicros(12, l)
                            + (Timestamps.getDaysPerMonth(12, l) - 1) * Timestamps.DAY_MICROS
                            + 23 * Timestamps.HOUR_MICROS
                            + 59 * Timestamps.MINUTE_MICROS
                            + 59 * Timestamps.SECOND_MICROS
                            + 999999,
                    operation,
                    out
            );
        }
    }

    @Override
    public long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi) {
        CharSequence fmtStr;
        try {
            DateFormat fmtMethod;
            switch (partitionBy) {
                case DAY:
                    fmtMethod = PARTITION_DAY_FORMAT;
                    fmtStr = CommonFormatUtils.DAY_PATTERN;
                    break;
                case MONTH:
                    fmtMethod = PARTITION_MONTH_FORMAT;
                    fmtStr = CommonFormatUtils.MONTH_PATTERN;
                    break;
                case YEAR:
                    fmtMethod = PARTITION_YEAR_FORMAT;
                    fmtStr = CommonFormatUtils.YEAR_PATTERN;
                    break;
                case HOUR:
                    fmtMethod = PARTITION_HOUR_FORMAT;
                    fmtStr = CommonFormatUtils.HOUR_PATTERN;
                    break;
                case WEEK:
                    fmtMethod = PARTITION_WEEK_FORMAT;
                    fmtStr = CommonFormatUtils.WEEK_PATTERN;
                    break;
                case NONE:
                    fmtMethod = DEFAULT_FORMAT;
                    fmtStr = partitionName;
                    break;
                default:
                    throw new UnsupportedOperationException("partition by " + partitionBy + " does not have date format");
            }
            int limit = fmtStr.length();
            if (hi < 0) {
                // Automatic partition name trimming.
                hi = lo + Math.min(limit, partitionName.length());
            }
            if (hi - lo < limit) {
                throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
            }
            return fmtMethod.parse(partitionName, lo, hi, EN_LOCALE);
        } catch (NumericException e) {
            if (partitionBy == PartitionBy.WEEK) {
                // maybe the user used a timestamp, or a date, string.
                int localLimit = CommonFormatUtils.DAY_PATTERN.length();
                try {
                    // trim to the lowest precision needed and get the timestamp
                    // convert timestamp to first day of the week
                    return Timestamps.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, EN_LOCALE));
                } catch (NumericException ignore) {
                    throw expectedPartitionDirNameFormatCairoException(partitionName, 0, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
        }
    }

    @Override
    public long toNanosScale() {
        return Timestamps.MICRO_NANOS;
    }

    @Override
    public void validateBounds(long timestamp) {
        if (Long.compareUnsigned(timestamp, Timestamps.YEAR_10000) >= 0) {
            validateBounds0(timestamp);
        }
    }

    private static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    private static void checkChar(Utf8Sequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.byteAt(p) != c) {
            throw NumericException.INSTANCE;
        }
    }

    private static boolean checkLen2(int p, int lim) throws NumericException {
        if (lim - p >= 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.INSTANCE;
    }

    private static boolean checkLen3(int p, int lim) throws NumericException {
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

    private static void checkSpecialChar(Utf8Sequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.byteAt(p) != 'T' && s.byteAt(p) != ' ')) {
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

    private static long checkTimezoneTail(Utf8Sequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = parseSign((char) seq.byteAt(p++));
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

    private static void validateBounds0(long timestamp) {
        if (timestamp == Long.MIN_VALUE) {
            throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
        }
        if (timestamp < TableWriter.TIMESTAMP_EPOCH) {
            throw CairoException.nonCritical().put("designated timestamp before 1970-01-01 is not allowed");
        }
        if (timestamp >= Timestamps.YEAR_10000) {
            throw CairoException.nonCritical().put("designated timestamp beyond 9999-12-31 is not allowed");
        }
    }

    public static class IsoDatePartitionFormat implements DateFormat {
        private final DateFormat baseFormat;
        private final PartitionFloorMethod floorMethod;

        public IsoDatePartitionFormat(PartitionFloorMethod floorMethod, DateFormat baseFormat) {
            this.floorMethod = floorMethod;
            this.baseFormat = baseFormat;
        }

        @Override
        public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            long overspill = timestamp - floorMethod.floor(timestamp);

            if (overspill > 0) {
                DAY_FORMAT.format(timestamp, locale, timeZoneName, sink);
                long time = timestamp - (timestamp / Timestamps.DAY_MICROS) * Timestamps.DAY_MICROS;

                if (time > 0) {
                    int hour = (int) (time / Timestamps.HOUR_MICROS);
                    int minute = (int) ((time % Timestamps.HOUR_MICROS) / Timestamps.MINUTE_MICROS);
                    int second = (int) ((time % Timestamps.MINUTE_MICROS) / Timestamps.SECOND_MICROS);
                    int milliMicros = (int) (time % Timestamps.SECOND_MICROS);

                    sink.putAscii('T');
                    append0(sink, hour);

                    if (minute > 0 || second > 0 || milliMicros > 0) {
                        append0(sink, minute);
                        append0(sink, second);

                        if (milliMicros > 0) {
                            sink.putAscii('-');
                            append00000(sink, milliMicros);
                        }
                    }
                }
            } else {
                baseFormat.format(timestamp, locale, timeZoneName, sink);
            }
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
            long ts;
            if (hi - lo < 4 || hi - lo > 25) {
                throw NumericException.INSTANCE;
            }
            int p = lo;
            int year = Numbers.parseInt(in, p, p += 4);
            boolean l = Timestamps.isLeapYear(year);
            if (checkLen2(p, hi)) {
                checkChar(in, p++, hi, '-');
                int month = Numbers.parseInt(in, p, p += 2);
                checkRange(month, 1, 12);
                if (checkLen2(p, hi)) {
                    int dayRange = Timestamps.getDaysPerMonth(month, l);
                    ts = Timestamps.yearMicros(year, l) + Timestamps.monthOfYearMicros(month, l);
                    ts = parseDayTime(in, hi, p, ts, dayRange, 2);
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
    }

    public static class IsoWeekPartitionFormat implements DateFormat {

        @Override
        public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            long weekTime = timestamp - Timestamps.floorWW(timestamp);
            WEEK_FORMAT.format(timestamp, locale, timeZoneName, sink);

            if (weekTime > 0) {
                int dayOfWeek = (int) (weekTime / Timestamps.DAY_MICROS) + 1;
                int hour = (int) ((weekTime % Timestamps.DAY_MICROS) / Timestamps.HOUR_MICROS);
                int minute = (int) ((weekTime % Timestamps.HOUR_MICROS) / Timestamps.MINUTE_MICROS);
                int second = (int) ((weekTime % Timestamps.MINUTE_MICROS) / Timestamps.SECOND_MICROS);
                int milliMicros = (int) (weekTime % Timestamps.SECOND_MICROS);

                sink.putAscii('-');
                sink.put(dayOfWeek);

                if (hour > 0 || minute > 0 || second > 0 || milliMicros > 0) {
                    sink.putAscii('T');
                    append0(sink, hour);

                    if (minute > 0 || second > 0 || milliMicros > 0) {
                        append0(sink, minute);
                        append0(sink, second);

                        if (milliMicros > 0) {
                            sink.putAscii('-');
                            append00000(sink, milliMicros);
                        }
                    }
                }
            }
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
            long baseTs = WEEK_FORMAT.parse(in, lo, lo + 8, locale);
            lo += 8;
            if (lo < hi) {
                return parseDayTime(in, hi, lo, baseTs, 7, 1);
            }
            return baseTs;
        }
    }
}
