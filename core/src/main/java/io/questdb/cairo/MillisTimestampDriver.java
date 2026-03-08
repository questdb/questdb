/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.constants.ConstantFunction;
import io.questdb.griffin.engine.functions.constants.DateConstant;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.millitime.DateFormatFactory;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.datetime.millitime.MillisecondClockImpl;
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.cairo.PartitionBy.*;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MILLIS;

/**
 * MillsTimestampDriver is the implementation for {@link ColumnType#DATE} type.
 * Since Date type does not support being used as a designated timestamp,
 * some related methods are not implemented and will throw UnsupportedOperationException.
 */
public class MillisTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new MillisTimestampDriver();
    private final Clock clock = MillisecondClockImpl.INSTANCE;
    private final ColumnTypeConverter.Fixed2VarConverter converterDate2Str = this::append;
    private final ColumnTypeConverter.Var2FixedConverter<CharSequence> converterStr2Date = this::appendToMem;

    private MillisTimestampDriver() {
    }

    public static long floor(CharSequence value) throws NumericException {
        return INSTANCE.parseFloorLiteral(value);
    }

    @Override
    public long add(long timestamp, char type, int stride) {
        return switch (type) {
            case 'n' -> Dates.addNanos(timestamp, stride);
            case 'u', 'U' -> Dates.addMicros(timestamp, stride);
            case 'T' -> Dates.addMillis(timestamp, stride);
            case 's' -> Dates.addSeconds(timestamp, stride);
            case 'm' -> Dates.addMinutes(timestamp, stride);
            case 'H', 'h' -> Dates.addHours(timestamp, stride);
            case 'd' -> Dates.addDays(timestamp, stride);
            case 'w' -> Dates.addWeeks(timestamp, stride);
            case 'M' -> Dates.addMonths(timestamp, stride);
            case 'y' -> Dates.addYears(timestamp, stride);
            default -> throw new UnsupportedOperationException("Unsupported time unit: " + type);
        };
    }

    @Override
    public long addDays(long timestamp, int days) {
        return Dates.addDays(timestamp, days);
    }

    @Override
    public long addMonths(long timestamp, int months) {
        return Dates.addMonths(timestamp, months);
    }

    @Override
    public long addWeeks(long timestamp, int weeks) {
        return Dates.addWeeks(timestamp, weeks);
    }

    @Override
    public long addYears(long timestamp, int years) {
        return Dates.addYears(timestamp, years);
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        DateFormatUtils.appendDateTime(sink, timestamp);
    }

    @Override
    public boolean append(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            DateFormatUtils.appendDateTime(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public void appendToPGWireText(CharSink<?> sink, long timestamp) {
        DateFormatUtils.PG_DATE_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public void appendTypeToPlan(PlanSink sink) {
        sink.val("Date");
    }

    @Override
    public long approxPartitionDuration(int partitionBy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ceilYYYY(long timestamp) {
        return Dates.ceilYYYY(timestamp);
    }

    @Override
    public long endOfDay(long start) {
        return start + Dates.DAY_MILLIS - 1;
    }

    @Override
    public Interval fixInterval(Interval interval, int intervalType) {
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO) {
            long lo = interval.getLo() / Nanos.MILLI_NANOS;
            long hi = interval.getHi() / Nanos.MILLI_NANOS;
            interval.of(lo, hi);
        } else if (intervalType == ColumnType.INTERVAL_TIMESTAMP_MICRO) {
            long lo = interval.getLo() / Micros.MILLI_MICROS;
            long hi = interval.getHi() / Micros.MILLI_MICROS;
            interval.of(lo, hi);
        }
        return interval;
    }

    @Override
    public long floorYYYY(long timestamp) {
        return Dates.floorYYYY(timestamp);
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / Nanos.MILLI_NANOS;
            case MICROS:
                return value / Micros.MILLI_MICROS;
            case MILLIS:
                return value;
            case SECONDS:
                return Math.multiplyExact(value, Dates.SECOND_MILLIS);
            default:
                Duration duration = unit.getDuration();
                long millis = Math.multiplyExact(duration.getSeconds(), Dates.SECOND_MILLIS);
                millis = Math.addExact(millis, duration.getNano() / Nanos.MILLI_NANOS);
                return Math.multiplyExact(millis, value);
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Dates.SECOND_MILLIS), instant.getNano() / Nanos.MILLI_NANOS);
    }

    @Override
    public long from(long timestamp, int columnType) {
        if (ColumnType.isTimestampNano(columnType)) {
            return timestamp / Nanos.MILLI_NANOS;
        } else if (ColumnType.isTimestampMicro(columnType)) {
            return timestamp / Micros.MILLI_MICROS;
        }
        return timestamp;
    }

    @Override
    public long from(long ts, byte unit) {
        if (ts == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }

        return switch (unit) {
            case CommonUtils.TIMESTAMP_UNIT_NANOS -> ts / Nanos.MILLI_NANOS;
            case CommonUtils.TIMESTAMP_UNIT_MICROS -> ts / Micros.MILLI_MICROS;
            case CommonUtils.TIMESTAMP_UNIT_MILLIS -> ts;
            case CommonUtils.TIMESTAMP_UNIT_SECONDS -> Math.multiplyExact(ts, Dates.SECOND_MILLIS);
            case CommonUtils.TIMESTAMP_UNIT_MINUTES -> Math.multiplyExact(ts, Dates.MINUTE_MILLIS);
            case CommonUtils.TIMESTAMP_UNIT_HOURS -> Math.multiplyExact(ts, Dates.HOUR_MILLIS);
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    public long fromDate(long date) {
        return date;
    }

    @Override
    public long fromDays(int days) {
        return days * Dates.DAY_MILLIS;
    }

    @Override
    public long fromHours(int hours) {
        return hours * Dates.HOUR_MILLIS;
    }

    @Override
    public long fromMicros(long micros) {
        return micros / Micros.MILLI_MICROS;
    }

    @Override
    public long fromMillis(long millis) {
        return millis;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Dates.MINUTE_MILLIS;
    }

    @Override
    public long fromNanos(long nanos) {
        return nanos == Numbers.LONG_NULL ? nanos : nanos / Nanos.MILLI_NANOS;
    }

    @Override
    public long fromSeconds(long seconds) {
        return seconds * Dates.SECOND_MILLIS;
    }

    @Override
    public long fromWeeks(int weeks) {
        return weeks * Dates.WEEK_MILLIS;
    }

    @Override
    public TimestampAddMethod getAddMethod(char c) {
        return switch (c) {
            case 'n' -> Dates::addNanos;
            case 'u', 'U' -> Dates::addMicros;
            case 'T' -> Dates::addMillis;
            case 's' -> Dates::addSeconds;
            case 'm' -> Dates::addMinutes;
            case 'H', 'h' -> Dates::addHours;
            case 'd' -> Dates::addDays;
            case 'w' -> Dates::addWeeks;
            case 'M' -> Dates::addMonths;
            case 'y' -> Dates::addYears;
            default -> null;
        };
    }

    @Override
    public int getCentury(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getCentury(timestamp);
    }

    @Override
    public ColumnTypeConverter.Var2FixedConverter<CharSequence> getConverterStr2Timestamp() {
        return converterStr2Date;
    }

    @Override
    public ColumnTypeConverter.Fixed2VarConverter getConverterTimestamp2Str() {
        return converterDate2Str;
    }

    @Override
    public int getDayOfMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Dates.getYear(timestamp);
        boolean leap = CommonUtils.isLeapYear(year);
        int month = Dates.getMonthOfYear(timestamp, year, leap);
        return Dates.getDayOfMonth(timestamp, year, month, leap);
    }

    @Override
    public int getDayOfWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getDayOfWeek(timestamp);
    }

    @Override
    public int getDayOfWeekSundayFirst(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getDayOfWeekSundayFirst(timestamp);
    }

    @Override
    public int getDaysPerMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Dates.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        int month = Dates.getMonthOfYear(timestamp, year, isLeap);
        return CommonUtils.getDaysPerMonth(month, isLeap);
    }

    @Override
    public int getDecade(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getDecade(timestamp);
    }

    @Override
    public int getDow(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getDow(timestamp);
    }

    @Override
    public int getDoy(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getDoy(timestamp);
    }

    @Override
    public int getGKKHourInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHourOfDay(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getHourOfDay(timestamp);
    }

    @Override
    public IntervalConstant getIntervalConstantNull() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIsoYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getIsoYear(timestamp);
    }

    @Override
    public int getMicrosOfMilli(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return 0;
    }

    @Override
    public int getMicrosOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return (int) ((timestamp % Dates.SECOND_MILLIS) * Micros.MILLI_MICROS);
    }

    @Override
    public int getMillennium(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getMillennium(timestamp);
    }

    @Override
    public int getMillisOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return (int) (timestamp % Dates.SECOND_MILLIS);
    }

    @Override
    public int getMinuteOfHour(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getMinuteOfHour(timestamp);
    }

    @Override
    public int getMonthOfYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Dates.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        return Dates.getMonthOfYear(timestamp, year, isLeap);
    }

    @Override
    public int getNanosOfMicros(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return 0;
    }

    @Override
    public int getNanosOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return (int) ((timestamp % Dates.SECOND_MILLIS) * Nanos.MILLI_NANOS);
    }

    @Override
    public PartitionAddMethod getPartitionAddMethod(int partitionBy) {
        return switch (partitionBy) {
            case DAY -> Dates::addDays;
            case MONTH -> Dates::addMonths;
            case YEAR -> Dates::addYears;
            case HOUR -> Dates::addHours;
            case WEEK -> Dates::addWeeks;
            default -> null;
        };
    }

    @Override
    public TimestampCeilMethod getPartitionCeilMethod(int partitionBy) {
        return switch (partitionBy) {
            case DAY -> MillisTimestampDriver::partitionCeilDD;
            case MONTH -> MillisTimestampDriver::partitionCeilMM;
            case YEAR -> MillisTimestampDriver::partitionCeilYYYY;
            case HOUR -> MillisTimestampDriver::partitionCeilHH;
            case WEEK -> MillisTimestampDriver::partitionCeilWW;
            default -> null;
        };
    }

    @Override
    public DateFormat getPartitionDirFormatMethod(int partitionBy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampFloorMethod getPartitionFloorMethod(int partitionBy) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPeriodBetween(char unit, long start, long end, int leftType, int rightType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getQuarter(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSecondOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getSecondOfMinute(timestamp);
    }

    @Override
    public int getTZRuleResolution() {
        return RESOLUTION_MILLIS;
    }

    @Override
    public long getTicks() {
        return clock.getTicks();
    }

    @Override
    public TimestampCeilMethod getTimestampCeilMethod(char unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConstantFunction getTimestampConstantNull() {
        return DateConstant.NULL;
    }

    @Override
    public TimestampDateFormatFactory getTimestampDateFormatFactory() {
        return DateFormatFactory.INSTANCE;
    }

    @Override
    public TimestampDiffMethod getTimestampDiffMethod(char type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampFloorMethod getTimestampFloorMethod(String unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampFloorWithOffsetMethod getTimestampFloorWithOffsetMethod(char unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampFloorWithStrideMethod getTimestampFloorWithStrideMethod(String unit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampSampler getTimestampSampler(long interval, char timeUnit, int position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getTimestampType() {
        return ColumnType.DATE;
    }

    @Override
    public CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getWeek(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Dates.getYear(timestamp);
    }

    @Override
    public boolean inInterval(long value, int intervalType, Interval interval) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long monthsBetween(long hi, long lo) {
        return Dates.getMonthsBetween(hi, lo);
    }

    @Override
    public long parseAnyFormat(CharSequence token, int start, int len) throws NumericException {
        return DateFormatUtils.parseDate(token, start, len);
    }

    @Override
    public long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException {
        long date;
        if (hi - lo < 4) {
            throw NumericException.instance();
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
                                int milliLim = p + 3;
                                int mlim = Math.min(hi, milliLim);
                                int millis = 0;
                                for (; p < mlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        break;
                                    }
                                    millis *= 10;
                                    millis += c - '0';
                                }
                                millis *= CommonUtils.tenPow(milliLim - p);

                                // truncate remaining if any
                                for (int nlim = Math.min(hi, p + 6); p < nlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        break;
                                    }
                                }

                                date = Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS
                                        + millis
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                date = Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            date = Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + hour * Dates.HOUR_MILLIS
                                    + min * Dates.MINUTE_MILLIS;
                        }
                    } else {
                        date = Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (day - 1) * Dates.DAY_MILLIS
                                + hour * Dates.HOUR_MILLIS;
                    }
                } else {
                    date = Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(month, l)
                            + (day - 1) * Dates.DAY_MILLIS;
                }
            } else {
                date = (Dates.yearMillis(year, l) + Dates.monthOfYearMillis(month, l));
            }
        } else {
            date = (Dates.yearMillis(year, l) + Dates.monthOfYearMillis(1, l));
        }
        return date;
    }

    @Override
    public long parseFloor(CharSequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.instance();
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
                                int milliLim = p + 3;
                                int mlim = Math.min(hi, milliLim);
                                int millis = 0;
                                for (; p < mlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        break;
                                    }
                                    millis *= 10;
                                    millis += c - '0';
                                }
                                millis *= CommonUtils.tenPow(milliLim - p);

                                // truncate remaining if any
                                for (int nlim = Math.min(hi, p + 6); p < nlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        break;
                                    }
                                }

                                ts = Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS
                                        + millis
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                ts = Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            ts = Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + hour * Dates.HOUR_MILLIS
                                    + min * Dates.MINUTE_MILLIS;
                        }
                    } else {
                        ts = Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (day - 1) * Dates.DAY_MILLIS
                                + hour * Dates.HOUR_MILLIS;
                    }
                } else {
                    ts = Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(month, l)
                            + (day - 1) * Dates.DAY_MILLIS;
                }
            } else {
                ts = (Dates.yearMillis(year, l) + Dates.monthOfYearMillis(month, l));
            }
        } else {
            ts = (Dates.yearMillis(year, l) + Dates.monthOfYearMillis(1, l));
        }
        return ts;
    }

    public void parseInterval(CharSequence input, int pos, int lim, short operation, LongList out) throws NumericException {
        if (lim - pos < 4) {
            throw NumericException.instance();
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
                            if (p < lim && input.charAt(p) == '.') {
                                p++;
                                int milliLim = p + 3;
                                int mlim = Math.min(lim, milliLim);
                                int millis = 0;
                                for (; p < mlim; p++) {
                                    char c = input.charAt(p);
                                    if (c < '0' || c > '9') {
                                        throw NumericException.instance();
                                    }
                                    millis *= 10;
                                    millis += c - '0';
                                }
                                int remainingDigits = milliLim - p;
                                millis *= CommonUtils.tenPow(remainingDigits);

                                if (p < lim) {
                                    throw NumericException.instance();
                                }

                                long baseTime = Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + min * Dates.MINUTE_MILLIS
                                        + sec * Dates.SECOND_MILLIS;
                                int rangeMillis = CommonUtils.tenPow(remainingDigits) - 1;
                                IntervalUtils.encodeInterval(baseTime + millis,
                                        baseTime + millis + rangeMillis,
                                        operation,
                                        out);
                            } else if (p == lim) {
                                IntervalUtils.encodeInterval(Dates.yearMillis(year, l)
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
                                                + 999,
                                        operation,
                                        out);
                            } else {
                                throw NumericException.instance();
                            }
                        } else {
                            IntervalUtils.encodeInterval(
                                    Dates.yearMillis(year, l)
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
                                            + 999,
                                    operation,
                                    out
                            );
                        }
                    } else {
                        IntervalUtils.encodeInterval(
                                Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS,
                                Dates.yearMillis(year, l)
                                        + Dates.monthOfYearMillis(month, l)
                                        + (day - 1) * Dates.DAY_MILLIS
                                        + hour * Dates.HOUR_MILLIS
                                        + 59 * Dates.MINUTE_MILLIS
                                        + 59 * Dates.SECOND_MILLIS
                                        + 999,
                                operation,
                                out
                        );
                    }
                } else {
                    IntervalUtils.encodeInterval(
                            Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS,
                            Dates.yearMillis(year, l)
                                    + Dates.monthOfYearMillis(month, l)
                                    + (day - 1) * Dates.DAY_MILLIS
                                    + 23 * Dates.HOUR_MILLIS
                                    + 59 * Dates.MINUTE_MILLIS
                                    + 59 * Dates.SECOND_MILLIS
                                    + 999,
                            operation,
                            out
                    );
                }
            } else {
                IntervalUtils.encodeInterval(
                        Dates.yearMillis(year, l) + Dates.monthOfYearMillis(month, l),
                        Dates.yearMillis(year, l)
                                + Dates.monthOfYearMillis(month, l)
                                + (CommonUtils.getDaysPerMonth(month, l) - 1) * Dates.DAY_MILLIS
                                + 23 * Dates.HOUR_MILLIS
                                + 59 * Dates.MINUTE_MILLIS
                                + 59 * Dates.SECOND_MILLIS
                                + 999,
                        operation,
                        out
                );
            }
        } else {
            IntervalUtils.encodeInterval(
                    Dates.yearMillis(year, l) + Dates.monthOfYearMillis(1, l),
                    Dates.yearMillis(year, l)
                            + Dates.monthOfYearMillis(12, l)
                            + (CommonUtils.getDaysPerMonth(12, l) - 1) * Dates.DAY_MILLIS
                            + 23 * Dates.HOUR_MILLIS
                            + 59 * Dates.MINUTE_MILLIS
                            + 59 * Dates.SECOND_MILLIS
                            + 999,
                    operation,
                    out
            );
        }
    }

    @Override
    public long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long startOfDay(long now, int shiftDays) {
        return Dates.floorDD(Dates.addDays(now, shiftDays));
    }

    @Override
    public long toDate(long timestamp) {
        return timestamp;
    }

    @Override
    public long toHours(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Dates.HOUR_MILLIS;
    }

    @Override
    public String toMSecString(long timestamp) {
        return Dates.toString(timestamp);
    }

    @Override
    public long toMicros(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? timestamp : timestamp * Micros.MILLI_MICROS;
    }

    @Override
    public long toNanos(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? timestamp : timestamp * Nanos.MILLI_NANOS;
    }

    @Override
    public long toNanosScale() {
        return Nanos.MILLI_NANOS;
    }

    @Override
    public long toSeconds(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Dates.SECOND_MILLIS;
    }

    @Override
    public long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Dates.toTimezone(utcTimestamp, locale, timezone);
    }

    @Override
    public String toUSecString(long millis) {
        return Dates.toUSecString(millis);
    }

    @Override
    public long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return Dates.toUTC(localTimestamp, zoneRules);
    }

    @Override
    public long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Dates.toUTC(localTimestamp, locale, timezone);
    }

    @Override
    public void validateBounds(long timestamp) {
        throw new UnsupportedOperationException();
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
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Dates.HOUR_MILLIS + min * Dates.MINUTE_MILLIS);
            } else {
                return tzSign * (hour * Dates.HOUR_MILLIS);
            }
        }
        throw NumericException.instance();
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
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Dates.HOUR_MILLIS + min * Dates.MINUTE_MILLIS);
            } else {
                return tzSign * (hour * Dates.HOUR_MILLIS);
            }
        }
        throw NumericException.instance();
    }

    private static long partitionCeilDD(long millis) {
        return Dates.ceilDD(Math.max(millis, 0));
    }

    private static long partitionCeilHH(long millis) {
        return Dates.ceilHH(Math.max(millis, 0));
    }

    private static long partitionCeilMM(long millis) {
        return Dates.ceilMM(Math.max(millis, 0));
    }

    private static long partitionCeilWW(long millis) {
        return Dates.ceilWW(Math.max(millis, 0));
    }

    private static long partitionCeilYYYY(long millis) {
        return Dates.ceilYYYY(Math.max(millis, 0));
    }
}