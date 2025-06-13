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

import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.PartitionBy.*;
import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.microtime.TimestampFormatUtils.*;

public class NanoTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new NanoTimestampDriver();
    private static final PartitionAddMethod ADD_DD = Nanos::addDays;
    private static final PartitionAddMethod ADD_HH = Nanos::addHours;
    private static final PartitionAddMethod ADD_MM = Nanos::addMonths;
    private static final PartitionAddMethod ADD_WW = Nanos::addWeeks;
    private static final PartitionAddMethod ADD_YYYY = Nanos::addYears;
    private static final PartitionCeilMethod CEIL_DD = Nanos::ceilDD;
    private static final PartitionCeilMethod CEIL_HH = Nanos::ceilHH;
    private static final PartitionCeilMethod CEIL_MM = Nanos::ceilMM;
    private static final PartitionCeilMethod CEIL_WW = Nanos::ceilWW;
    private static final PartitionCeilMethod CEIL_YYYY = Nanos::ceilYYYY;
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
    private static final PartitionFloorMethod FLOOR_DD = Nanos::floorDD;
    private static final PartitionFloorMethod FLOOR_HH = Nanos::floorHH;
    private static final PartitionFloorMethod FLOOR_MM = Nanos::floorMM;
    private static final PartitionFloorMethod FLOOR_WW = Nanos::floorWW;
    private static final PartitionFloorMethod FLOOR_YYYY = Nanos::floorYYYY;
    private static final String MAX_NANO_TIMESTAMP = "2262-04-11 23:47:16.854775807";
    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(FLOOR_DD, DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(FLOOR_HH, HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(FLOOR_MM, MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(FLOOR_YYYY, YEAR_FORMAT);

    public static CairoException expectedPartitionDirNameFormatCairoException(CharSequence partitionName, int lo, int hi, int partitionBy) {
        final CairoException ee = CairoException.critical(0).put('\'');
        switch (partitionBy) {
            case DAY:
                ee.put(CommonUtils.DAY_PATTERN);
                break;
            case WEEK:
                ee.put(CommonUtils.WEEK_PATTERN).put("' or '").put(CommonUtils.DAY_PATTERN);
                break;
            case MONTH:
                ee.put(CommonUtils.MONTH_PATTERN);
                break;
            case YEAR:
                ee.put(CommonUtils.YEAR_PATTERN);
                break;
            case HOUR:
                ee.put(CommonUtils.HOUR_PATTERN);
                break;
        }
        ee.put("' expected, found [ts=").put(partitionName.subSequence(lo, hi)).put(']');
        return ee;
    }

    public static long floor(CharSequence value) throws NumericException {
        return INSTANCE.parseFloorLiteral(value);
    }

    public static long parseDayTime(CharSequence seq, int lim, int pos, long ts, int dayRange, int dayDigits) throws NumericException {
        CommonUtils.checkChar(seq, pos++, lim, '-');
        int day = Numbers.parseInt(seq, pos, pos += dayDigits);
        CommonUtils.checkRange(day, 1, dayRange);
        if (CommonUtils.checkLen3(pos, lim)) {
            CommonUtils.checkChar(seq, pos++, lim, 'T');
            int hour = Numbers.parseInt(seq, pos, pos += 2);
            CommonUtils.checkRange(hour, 0, 23);
            if (CommonUtils.checkLen2(pos, lim)) {
                int min = Numbers.parseInt(seq, pos, pos += 2);
                CommonUtils.checkRange(min, 0, 59);
                if (CommonUtils.checkLen2(pos, lim)) {
                    int sec = Numbers.parseInt(seq, pos, pos += 2);
                    CommonUtils.checkRange(sec, 0, 59);
                    if (pos < lim && seq.charAt(pos) == '-') {
                        pos++;
                        // var len milli, micros and nanos
                        int nanoLim = pos + 9;
                        int nlim = Math.min(lim, nanoLim);
                        int nanos = 0;
                        for (; pos < nlim; pos++) {
                            char c = seq.charAt(pos);
                            if (c < '0' || c > '9') {
                                throw NumericException.INSTANCE;
                            }
                            nanos *= 10;
                            nanos += c - '0';
                        }
                        nanos *= CommonUtils.tenPow(nanoLim - pos);

                        ts += (day - 1) * Nanos.DAY_NANOS
                                + hour * Nanos.HOUR_NANOS
                                + min * Nanos.MINUTE_NANOS
                                + sec * Nanos.SECOND_NANOS
                                + nanos;
                    } else {
                        if (pos == lim) {
                            // seconds
                            ts += (day - 1) * Nanos.DAY_NANOS
                                    + hour * Nanos.HOUR_NANOS
                                    + min * Nanos.MINUTE_NANOS
                                    + sec * Nanos.SECOND_NANOS;
                        } else {
                            throw NumericException.INSTANCE;
                        }
                    }
                } else {
                    // minute
                    ts += (day - 1) * Nanos.DAY_NANOS
                            + hour * Nanos.HOUR_NANOS
                            + min * Nanos.MINUTE_NANOS;

                }
            } else {
                // year + month + day + hour
                ts += (day - 1) * Nanos.DAY_NANOS
                        + hour * Nanos.HOUR_NANOS;
            }
        } else {
            // year + month + day
            ts += (day - 1) * Nanos.DAY_NANOS;
        }
        return ts;
    }

    @Override
    public long addMonths(long timestamp, int months) {
        return Nanos.addMonths(timestamp, months);
    }

    @Override
    public long addPeriod(long lo, char type, int period) {
        return Nanos.addPeriod(lo, type, period);
    }

    @Override
    public long addYears(long timestamp, int years) {
        return Nanos.addYears(timestamp, years);
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        NanosFormatUtils.appendDateTimeNSec(sink, timestamp);
    }

    @Override
    public void appendPGWireText(CharSink<?> sink, long timestamp) {
        NanosFormatUtils.PG_TIMESTAMP_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public long castAsDate(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / 1000_000L;
    }

    @Override
    public long castDateAs(long date) {
        if (date == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        try {
            return Math.multiplyExact(date, 1000_000L);
        } catch (ArithmeticException e) {
            throw ImplicitCastException.inconvertibleValue(date, ColumnType.DATE, ColumnType.TIMESTAMP_NANO);
        }
    }

    @Override
    public boolean convertToVar(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            NanosFormatUtils.appendDateTimeNSec(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public long from(long timestamp, int timestampType) {
        if (timestampType != ColumnType.TIMESTAMP_MICRO) {
            return timestamp;
        }
        return CommonUtils.nanosToMicros(timestamp);
    }

    @Override
    public long fromDays(int days) {
        return days * Nanos.DAY_NANOS;
    }

    @Override
    public long fromHours(int hours) {
        return hours * Nanos.HOUR_NANOS;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Nanos.MINUTE_NANOS;
    }

    @Override
    public long fromSeconds(int seconds) {
        return seconds * Nanos.SECOND_NANOS;
    }

    @Override
    public int getColumnType() {
        return ColumnType.TIMESTAMP_NANO;
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
    public CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType) {
        if (srcTimestampType == ColumnType.TIMESTAMP_MICRO) {
            return CommonUtils::microsToNanos;
        }
        return null;
    }

    @Override
    public long monthsBetween(long hi, long lo) {
        return Nanos.getMonthsBetween(hi, lo);
    }

    @Override
    public long parseAnyFormat(CharSequence token, int start, int len) throws NumericException {
        return NanosFormatUtils.tryParse(token, start, len);
    }

    @Override
    public long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.INSTANCE;
        }
        int p = lo;
        int year = Numbers.parseInt(str, p, p += 4);
        boolean l = CommonUtils.isLeapYear(year);
        if (CommonUtils.checkLen3(p, hi)) {
            CommonUtils.checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            CommonUtils.checkRange(month, 1, 12);
            if (CommonUtils.checkLen3(p, hi)) {
                CommonUtils.checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                CommonUtils.checkRange(day, 1, CommonUtils.getDaysPerMonth(month, l));
                if (CommonUtils.checkLen3(p, hi)) {
                    CommonUtils.checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    CommonUtils.checkRange(hour, 0, 23);
                    if (CommonUtils.checkLen3(p, hi)) {
                        CommonUtils.checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        CommonUtils.checkRange(min, 0, 59);
                        if (CommonUtils.checkLen3(p, hi)) {
                            CommonUtils.checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            CommonUtils.checkRange(sec, 0, 59);
                            if (p < hi && str.byteAt(p) == '.') {

                                p++;
                                // var len milli, micros and seconds
                                int nanoLim = p + 9;
                                int nlim = Math.min(hi, nanoLim);
                                int nano = 0;
                                for (; p < nlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    nano *= 10;
                                    nano += c - '0';
                                }
                                nano *= CommonUtils.tenPow(nlim - p);
                                // micros
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + nano
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Nanos.yearNanos(year, l)
                                    + Nanos.monthOfYearNanos(month, l)
                                    + (day - 1) * Nanos.DAY_NANOS
                                    + hour * Nanos.HOUR_NANOS
                                    + min * Nanos.MINUTE_NANOS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Nanos.yearNanos(year, l)
                                + Nanos.monthOfYearNanos(month, l)
                                + (day - 1) * Nanos.DAY_NANOS
                                + hour * Nanos.HOUR_NANOS;

                    }
                } else {
                    // year + month + day
                    ts = Nanos.yearNanos(year, l)
                            + Nanos.monthOfYearNanos(month, l)
                            + (day - 1) * Nanos.DAY_NANOS;
                }
            } else {
                // year + month
                ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l));
            }
        } else {
            // year
            ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(1, l));
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
        boolean l = CommonUtils.isLeapYear(year);
        if (CommonUtils.checkLen3(p, hi)) {
            CommonUtils.checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            CommonUtils.checkRange(month, 1, 12);
            if (CommonUtils.checkLen3(p, hi)) {
                CommonUtils.checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                CommonUtils.checkRange(day, 1, CommonUtils.getDaysPerMonth(month, l));
                if (CommonUtils.checkLen3(p, hi)) {
                    CommonUtils.checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    CommonUtils.checkRange(hour, 0, 23);
                    if (CommonUtils.checkLen3(p, hi)) {
                        CommonUtils.checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        CommonUtils.checkRange(min, 0, 59);
                        if (CommonUtils.checkLen3(p, hi)) {
                            CommonUtils.checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            CommonUtils.checkRange(sec, 0, 59);
                            if (p < hi && str.charAt(p) == '.') {
                                p++;
                                // varlen milli, micros and nanos
                                int nanoLim = p + 9;
                                int nlim = Math.min(hi, nanoLim);
                                int nano = 0;
                                for (; p < nlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    nano *= 10;
                                    nano += c - '0';
                                }
                                nano *= CommonUtils.tenPow(nanoLim - p);

                                // nanos
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + nano
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Nanos.yearNanos(year, l)
                                    + Nanos.monthOfYearNanos(month, l)
                                    + (day - 1) * Nanos.DAY_NANOS
                                    + hour * Nanos.HOUR_NANOS
                                    + min * Nanos.MINUTE_NANOS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Nanos.yearNanos(year, l)
                                + Nanos.monthOfYearNanos(month, l)
                                + (day - 1) * Nanos.DAY_NANOS
                                + hour * Nanos.HOUR_NANOS;

                    }
                } else {
                    // year + month + day
                    ts = Nanos.yearNanos(year, l)
                            + Nanos.monthOfYearNanos(month, l)
                            + (day - 1) * Nanos.DAY_NANOS;
                }
            } else {
                // year + month
                ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l));
            }
        } else {
            // year
            ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(1, l));
        }
        if (ts < 0L) {
            throw CairoException.nonCritical().put("timestamp_ns before 1970-01-01 and beyond ").put(MAX_NANO_TIMESTAMP).put(" is not allowed");
        }
        return ts;
    }

    public void parseInterval(CharSequence input, int pos, int lim, short operation, LongList out) throws NumericException {
        if (lim - pos < 4) {
            throw NumericException.INSTANCE;
        }
        int p = pos;
        int year = Numbers.parseInt(input, p, p += 4);
        boolean l = CommonUtils.isLeapYear(year);
        if (CommonUtils.checkLen3(p, lim)) {
            CommonUtils.checkChar(input, p++, lim, '-');
            int month = Numbers.parseInt(input, p, p += 2);
            CommonUtils.checkRange(month, 1, 12);
            if (CommonUtils.checkLen3(p, lim)) {
                CommonUtils.checkChar(input, p++, lim, '-');
                int day = Numbers.parseInt(input, p, p += 2);
                CommonUtils.checkRange(day, 1, CommonUtils.getDaysPerMonth(month, l));
                if (CommonUtils.checkLen3(p, lim)) {
                    CommonUtils.checkChar(input, p++, lim, 'T');
                    int hour = Numbers.parseInt(input, p, p += 2);
                    CommonUtils.checkRange(hour, 0, 23);
                    if (CommonUtils.checkLen3(p, lim)) {
                        CommonUtils.checkChar(input, p++, lim, ':');
                        int min = Numbers.parseInt(input, p, p += 2);
                        CommonUtils.checkRange(min, 0, 59);
                        if (CommonUtils.checkLen3(p, lim)) {
                            CommonUtils.checkChar(input, p++, lim, ':');
                            int sec = Numbers.parseInt(input, p, p += 2);
                            CommonUtils.checkRange(sec, 0, 59);
                            if (p < lim) {
                                throw NumericException.INSTANCE;
                            } else {
                                // seconds
                                IntervalUtils.encodeInterval(Nanos.yearNanos(year, l)
                                                + Nanos.monthOfYearNanos(month, l)
                                                + (day - 1) * Nanos.DAY_NANOS
                                                + hour * Nanos.HOUR_NANOS
                                                + min * Nanos.MINUTE_NANOS
                                                + sec * Nanos.SECOND_NANOS,
                                        Nanos.yearNanos(year, l)
                                                + Nanos.monthOfYearNanos(month, l)
                                                + (day - 1) * Nanos.DAY_NANOS
                                                + hour * Nanos.HOUR_NANOS
                                                + min * Nanos.MINUTE_NANOS
                                                + sec * Nanos.SECOND_NANOS
                                                + 999999,
                                        operation,
                                        out);
                            }
                        } else {
                            // minute
                            IntervalUtils.encodeInterval(
                                    Nanos.yearNanos(year, l)
                                            + Nanos.monthOfYearNanos(month, l)
                                            + (day - 1) * Nanos.DAY_NANOS
                                            + hour * Nanos.HOUR_NANOS
                                            + min * Nanos.MINUTE_NANOS,
                                    Nanos.yearNanos(year, l)
                                            + Nanos.monthOfYearNanos(month, l)
                                            + (day - 1) * Nanos.DAY_NANOS
                                            + hour * Nanos.HOUR_NANOS
                                            + min * Nanos.MINUTE_NANOS
                                            + 59 * Nanos.SECOND_NANOS
                                            + 999999,
                                    operation,
                                    out
                            );
                        }
                    } else {
                        // year + month + day + hour
                        IntervalUtils.encodeInterval(
                                Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS,
                                Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + 59 * Nanos.MINUTE_NANOS
                                        + 59 * Nanos.SECOND_NANOS
                                        + 999999,
                                operation,
                                out
                        );
                    }
                } else {
                    // year + month + day
                    IntervalUtils.encodeInterval(
                            Nanos.yearNanos(year, l)
                                    + Nanos.monthOfYearNanos(month, l)
                                    + (day - 1) * Nanos.DAY_NANOS,
                            Nanos.yearNanos(year, l)
                                    + Nanos.monthOfYearNanos(month, l)
                                    + (day - 1) * Nanos.DAY_NANOS
                                    + 23 * Nanos.HOUR_NANOS
                                    + 59 * Nanos.MINUTE_NANOS
                                    + 59 * Nanos.SECOND_NANOS
                                    + 999999,
                            operation,
                            out
                    );
                }
            } else {
                // year + month
                IntervalUtils.encodeInterval(
                        Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l),
                        Nanos.yearNanos(year, l)
                                + Nanos.monthOfYearNanos(month, l)
                                + (CommonUtils.getDaysPerMonth(month, l) - 1) * Nanos.DAY_NANOS
                                + 23 * Nanos.HOUR_NANOS
                                + 59 * Nanos.MINUTE_NANOS
                                + 59 * Nanos.SECOND_NANOS
                                + 999999,
                        operation,
                        out
                );
            }
        } else {
            // year
            IntervalUtils.encodeInterval(
                    Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(1, l),
                    Nanos.yearNanos(year, l)
                            + Nanos.monthOfYearNanos(12, l)
                            + (CommonUtils.getDaysPerMonth(12, l) - 1) * Nanos.DAY_NANOS
                            + 23 * Nanos.HOUR_NANOS
                            + 59 * Nanos.MINUTE_NANOS
                            + 59 * Nanos.SECOND_NANOS
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
                    fmtStr = CommonUtils.DAY_PATTERN;
                    break;
                case MONTH:
                    fmtMethod = PARTITION_MONTH_FORMAT;
                    fmtStr = CommonUtils.MONTH_PATTERN;
                    break;
                case YEAR:
                    fmtMethod = PARTITION_YEAR_FORMAT;
                    fmtStr = CommonUtils.YEAR_PATTERN;
                    break;
                case HOUR:
                    fmtMethod = PARTITION_HOUR_FORMAT;
                    fmtStr = CommonUtils.HOUR_PATTERN;
                    break;
                case WEEK:
                    fmtMethod = PARTITION_WEEK_FORMAT;
                    fmtStr = CommonUtils.WEEK_PATTERN;
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
                int localLimit = CommonUtils.DAY_PATTERN.length();
                try {
                    // trim to the lowest precision needed and get the timestamp
                    // convert timestamp to first day of the week
                    return Nanos.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, EN_LOCALE));
                } catch (NumericException ignore) {
                    throw expectedPartitionDirNameFormatCairoException(partitionName, 0, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
        }
    }

    @Override
    public long toMicros(long nanos) {
        return CommonUtils.nanosToMicros(nanos);
    }

    @Override
    public long toNanos(long timestamp) {
        return timestamp;
    }

    @Override
    public long toNanosScale() {
        return 1;
    }

    @Override
    public void validateBounds(long timestamp) {
        if (timestamp < 0) {
            validateBounds0(timestamp);
        }
    }


    private static long checkTimezoneTail(CharSequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            CommonUtils.checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = CommonUtils.parseSign(seq.charAt(p++));
            int hour = Numbers.parseInt(seq, p, p += 2);
            CommonUtils.checkRange(hour, 0, 23);

            if (lim - p == 3) {
                // Optional : separator between hours and mins in timezone
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Nanos.HOUR_NANOS + min * Nanos.MINUTE_NANOS);
            } else {
                return tzSign * (hour * Nanos.HOUR_NANOS);
            }
        }
        throw NumericException.INSTANCE;
    }

    private static long checkTimezoneTail(Utf8Sequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            CommonUtils.checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = CommonUtils.parseSign((char) seq.byteAt(p++));
            int hour = Numbers.parseInt(seq, p, p += 2);
            CommonUtils.checkRange(hour, 0, 23);

            if (lim - p == 3) {
                // Optional : separator between hours and mins in timezone
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Nanos.HOUR_NANOS + min * Nanos.MINUTE_NANOS);
            } else {
                return tzSign * (hour * Nanos.HOUR_NANOS);
            }
        }
        throw NumericException.INSTANCE;
    }

    private static void validateBounds0(long timestamp) {
        if (timestamp == Long.MIN_VALUE) {
            throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
        }
        if (timestamp < TableWriter.TIMESTAMP_EPOCH) {
            throw CairoException.nonCritical().put("designated timestamp_ns before 1970-01-01 and beyond ").put(MAX_NANO_TIMESTAMP).put(" is not allowed");
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
                long time = timestamp - (timestamp / Nanos.DAY_NANOS) * Nanos.DAY_NANOS;

                if (time > 0) {
                    int hour = (int) (time / Nanos.HOUR_NANOS);
                    int minute = (int) ((time % Nanos.HOUR_NANOS) / Nanos.MINUTE_NANOS);
                    int second = (int) ((time % Nanos.MINUTE_NANOS) / Nanos.SECOND_NANOS);
                    int milliNanos = (int) (time % Nanos.SECOND_NANOS);

                    sink.putAscii('T');
                    append0(sink, hour);

                    if (minute > 0 || second > 0 || milliNanos > 0) {
                        append0(sink, minute);
                        append0(sink, second);

                        if (milliNanos > 0) {
                            sink.putAscii('-');
                            append00000000(sink, milliNanos);
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
            boolean l = CommonUtils.isLeapYear(year);
            if (CommonUtils.checkLen2(p, hi)) {
                CommonUtils.checkChar(in, p++, hi, '-');
                int month = Numbers.parseInt(in, p, p += 2);
                CommonUtils.checkRange(month, 1, 12);
                if (CommonUtils.checkLen2(p, hi)) {
                    int dayRange = CommonUtils.getDaysPerMonth(month, l);
                    ts = Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l);
                    ts = parseDayTime(in, hi, p, ts, dayRange, 2);
                } else {
                    // year + month
                    ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l));
                }
            } else {
                // year
                ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(1, l));
            }
            if (ts < 0L) {
                throw CairoException.nonCritical().put("timestamp_ns before 1970-01-01 and beyond ").put(MAX_NANO_TIMESTAMP).put(" is not allowed");
            }
            return ts;
        }
    }

    public static class IsoWeekPartitionFormat implements DateFormat {

        @Override
        public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            long weekTime = timestamp - Nanos.floorWW(timestamp);
            WEEK_FORMAT.format(timestamp, locale, timeZoneName, sink);

            if (weekTime > 0) {
                int dayOfWeek = (int) (weekTime / Nanos.DAY_NANOS) + 1;
                int hour = (int) ((weekTime % Nanos.DAY_NANOS) / Nanos.HOUR_NANOS);
                int minute = (int) ((weekTime % Nanos.HOUR_NANOS) / Nanos.MINUTE_NANOS);
                int second = (int) ((weekTime % Nanos.MINUTE_NANOS) / Nanos.SECOND_NANOS);
                int milliNanos = (int) (weekTime % Nanos.SECOND_NANOS);

                sink.putAscii('-');
                sink.put(dayOfWeek);

                if (hour > 0 || minute > 0 || second > 0 || milliNanos > 0) {
                    sink.putAscii('T');
                    append0(sink, hour);

                    if (minute > 0 || second > 0 || milliNanos > 0) {
                        append0(sink, minute);
                        append0(sink, second);

                        if (milliNanos > 0) {
                            sink.putAscii('-');
                            append00000000(sink, milliNanos);
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
