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

import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.MonthTimestampNanosSampler;
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.WeekTimestampNanosSampler;
import io.questdb.griffin.engine.groupby.YearTimestampNanosSampler;
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
import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.datetime.nanotime.NanosFormatFactory;
import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.datetime.nanotime.NanosecondClockImpl;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.cairo.PartitionBy.*;
import static io.questdb.cairo.TableUtils.DEFAULT_PARTITION_NAME;
import static io.questdb.std.datetime.DateLocaleFactory.EN_LOCALE;
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_NANOS;
import static io.questdb.std.datetime.nanotime.NanosFormatUtils.*;

public class NanosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new NanosTimestampDriver();
    public static final int MAX_NANO_YEAR = 2261;
    private static final DateFormat DEFAULT_FORMAT = new DateFormat() {
        @Override
        public void format(long datetime, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            sink.putAscii(DEFAULT_PARTITION_NAME);
        }

        @Override
        public int getColumnType() {
            return ColumnType.TIMESTAMP_NANO;
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
    private static final String MAX_NANO_TIMESTAMP_STR = "2261-12-31 23:59:59.999999999";
    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(NanosTimestampDriver::partitionFloorDD, NanosFormatUtils.DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(NanosTimestampDriver::partitionFloorHH, NanosFormatUtils.HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(NanosTimestampDriver::partitionFloorMM, NanosFormatUtils.MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(NanosTimestampDriver::partitionFloorYYYY, NanosFormatUtils.YEAR_FORMAT);

    private final ColumnTypeConverter.Var2FixedConverter<CharSequence> converterStr2Timestamp = this::appendToMem;
    private final ColumnTypeConverter.Fixed2VarConverter converterTimestamp2Str = this::append;
    private Clock clock = NanosecondClockImpl.INSTANCE;

    private NanosTimestampDriver() {
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
                                throw NumericException.instance();
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
                            throw NumericException.instance();
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
    public long add(long timestamp, char type, int stride) {
        return Nanos.addPeriod(timestamp, type, stride);
    }

    @Override
    public long addDays(long timestamp, int days) {
        return Nanos.addDays(timestamp, days);
    }

    @Override
    public long addMonths(long timestamp, int months) {
        return Nanos.addMonths(timestamp, months);
    }

    @Override
    public long addWeeks(long timestamp, int weeks) {
        return Nanos.addWeeks(timestamp, weeks);
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
    public boolean append(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            NanosFormatUtils.appendDateTimeNSec(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public void appendToPGWireText(CharSink<?> sink, long timestamp) {
        NanosFormatUtils.PG_TIMESTAMP_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public void appendTypeToPlan(PlanSink sink) {
        sink.val("timestamp_ns");
    }

    @Override
    public long approxPartitionDuration(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.HOUR:
                return Nanos.HOUR_NANOS;
            case PartitionBy.DAY:
                return Nanos.DAY_NANOS;
            case PartitionBy.WEEK:
                return Nanos.WEEK_NANOS;
            case PartitionBy.MONTH:
                return Nanos.MONTH_NANOS_APPROX;
            case PartitionBy.YEAR:
                return Nanos.YEAR_NANOS_NONLEAP;
            default:
                throw new UnsupportedOperationException("unexpected partition by: " + partitionBy);
        }
    }

    @Override
    public long ceilYYYY(long timestamp) {
        return Nanos.ceilYYYY(timestamp);
    }

    @Override
    public long endOfDay(long start) {
        return start + Nanos.DAY_NANOS - 1;
    }

    @Override
    public Interval fixInterval(Interval interval, int intervalType) {
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_MICRO) {
            long lo = interval.getLo() * Nanos.MICRO_NANOS;
            long hi = interval.getHi() * Nanos.MICRO_NANOS;
            interval.of(lo, hi);
        }
        return interval;
    }

    @Override
    public long floorYYYY(long timestamp) {
        return Nanos.floorYYYY(timestamp);
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value;
            case MICROS:
                return Math.multiplyExact(value, Nanos.MICRO_NANOS);
            case MILLIS:
                return Math.multiplyExact(value, Nanos.MILLI_NANOS);
            case SECONDS:
                return Math.multiplyExact(value, Nanos.SECOND_NANOS);
            default:
                Duration duration = unit.getDuration();
                long totalSeconds = Math.multiplyExact(duration.getSeconds(), value);
                long totalNanos = Math.multiplyExact(duration.getNano(), value);
                return Math.addExact(
                        Math.multiplyExact(totalSeconds, Nanos.SECOND_NANOS),
                        totalNanos
                );
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Nanos.SECOND_NANOS), instant.getNano());
    }

    @Override
    public long from(long timestamp, int columnType) {
        if (ColumnType.isTimestampMicro(columnType)) {
            return CommonUtils.microsToNanos(timestamp);
        }
        return timestamp;
    }

    @Override
    public long fromDate(long date) {
        return date == Numbers.LONG_NULL ? Numbers.LONG_NULL : date * Nanos.MILLI_NANOS;
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
    public long fromMicros(long micros) {
        return micros == Numbers.LONG_NULL ? Numbers.LONG_NULL : micros * Nanos.MICRO_NANOS;
    }

    @Override
    public long fromMillis(long millis) {
        return millis * Nanos.MILLI_NANOS;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Nanos.MINUTE_NANOS;
    }

    @Override
    public long fromNanos(long nanos) {
        return nanos;
    }

    @Override
    public long fromSeconds(long seconds) {
        return seconds * Nanos.SECOND_NANOS;
    }

    @Override
    public long fromWeeks(int weeks) {
        return weeks * Nanos.WEEK_NANOS;
    }

    @Override
    public TimestampAddMethod getAddMethod(char c) {
        switch (c) {
            case 'n':
                return Nanos::addNanos;
            case 'u':
            case 'U':
                return Nanos::addMicros;
            case 'T':
                return Nanos::addMillis;
            case 's':
                return Nanos::addSeconds;
            case 'm':
                return Nanos::addMinutes;
            case 'H':
            case 'h': // compatibility with sample by syntax
                return Nanos::addHours;
            case 'd':
                return Nanos::addDays;
            case 'w':
                return Nanos::addWeeks;
            case 'M':
                return Nanos::addMonths;
            case 'y':
                return Nanos::addYears;
            default:
                return null;
        }
    }

    @Override
    public int getCentury(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getCentury(timestamp);
    }

    @Override
    public ColumnTypeConverter.Var2FixedConverter<CharSequence> getConverterStr2Timestamp() {
        return converterStr2Timestamp;
    }

    @Override
    public ColumnTypeConverter.Fixed2VarConverter getConverterTimestamp2Str() {
        return converterTimestamp2Str;
    }

    @Override
    public int getDayOfMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Nanos.getYear(timestamp);
        boolean leap = CommonUtils.isLeapYear(year);
        int month = Nanos.getMonthOfYear(timestamp, year, leap);
        return Nanos.getDayOfMonth(timestamp, year, month, leap);
    }

    @Override
    public int getDayOfWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getDayOfWeek(timestamp);
    }

    @Override
    public int getDayOfWeekSundayFirst(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getDayOfWeekSundayFirst(timestamp);
    }

    @Override
    public int getDaysPerMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Nanos.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        int month = Nanos.getMonthOfYear(timestamp, year, isLeap);
        return CommonUtils.getDaysPerMonth(month, isLeap);
    }

    @Override
    public int getDecade(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getDecade(timestamp);
    }

    @Override
    public int getDow(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getDow(timestamp);
    }

    @Override
    public int getDoy(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getDoy(timestamp);
    }

    @Override
    public int getGKKHourInt() {
        return SqlCodeGenerator.GKK_NANO_HOUR_INT;
    }

    @Override
    public int getHourOfDay(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        if (timestamp > -1) {
            return (int) ((timestamp / Nanos.HOUR_NANOS) % CommonUtils.DAY_HOURS);
        } else {
            return CommonUtils.DAY_HOURS - 1 + (int) (((timestamp + 1) / Nanos.HOUR_NANOS) % CommonUtils.DAY_HOURS);
        }
    }

    @Override
    public IntervalConstant getIntervalConstantNull() {
        return IntervalConstant.TIMESTAMP_NANO_NULL;
    }

    @Override
    public int getIsoYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getIsoYear(timestamp);
    }

    @Override
    public int getMicrosOfMilli(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getMicrosOfMilli(timestamp);
    }

    @Override
    public int getMicrosOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getMicrosOfSecond(timestamp);
    }

    @Override
    public int getMillennium(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getMillennium(timestamp);
    }

    @Override
    public int getMillisOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getMillisOfSecond(timestamp);
    }

    @Override
    public int getMinuteOfHour(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        if (timestamp > -1) {
            return (int) ((timestamp / Nanos.MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        } else {
            return CommonUtils.HOUR_MINUTES - 1 + (int) (((timestamp + 1) / Nanos.MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        }
    }

    @Override
    public int getMonthOfYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Nanos.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        return Nanos.getMonthOfYear(timestamp, year, isLeap);
    }

    @Override
    public int getNanosOfMicros(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getNanosOfMicros(timestamp);
    }

    @Override
    public PartitionAddMethod getPartitionAddMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Nanos::addDays;
            case MONTH:
                return Nanos::addMonths;
            case YEAR:
                return Nanos::addYears;
            case HOUR:
                return Nanos::addHours;
            case WEEK:
                return Nanos::addWeeks;
            default:
                return null;
        }
    }

    @Override
    public TimestampCeilMethod getPartitionCeilMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return NanosTimestampDriver::partitionCeilDD;
            case MONTH:
                return NanosTimestampDriver::partitionCeilMM;
            case YEAR:
                return NanosTimestampDriver::partitionCeilYYYY;
            case HOUR:
                return NanosTimestampDriver::partitionCeilHH;
            case WEEK:
                return NanosTimestampDriver::partitionCeilWW;
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
    public TimestampFloorMethod getPartitionFloorMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return NanosTimestampDriver::partitionFloorDD;
            case WEEK:
                return NanosTimestampDriver::partitionFloorWW;
            case MONTH:
                return NanosTimestampDriver::partitionFloorMM;
            case YEAR:
                return NanosTimestampDriver::partitionFloorYYYY;
            case HOUR:
                return NanosTimestampDriver::partitionFloorHH;
            default:
                return null;
        }
    }

    @Override
    public long getPeriodBetween(char unit, long start, long end, int startType, int endType) {
        if (start == Numbers.LONG_NULL || end == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        start = from(start, startType);
        end = from(end, endType);
        return Nanos.getPeriodBetween(unit, start, end);
    }

    @Override
    public int getQuarter(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getQuarter(timestamp);
    }

    @Override
    public int getSecondOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        if (timestamp > -1) {
            return (int) ((timestamp / Nanos.SECOND_NANOS) % 60);
        } else {
            return 59 + (int) (((timestamp + 1) / Nanos.SECOND_NANOS) % 60);
        }
    }

    @Override
    public int getTZRuleResolution() {
        return RESOLUTION_NANOS;
    }

    @Override
    public long getTicks() {
        return clock.getTicks();
    }

    @Override
    public TimestampCeilMethod getTimestampCeilMethod(char unit) {
        switch (unit) {
            case 'd':
                return Nanos::ceilDD;
            case 'M':
                return Nanos::ceilMM;
            case 'y':
                return Nanos::ceilYYYY;
            case 'w':
                return Nanos::ceilWW;
            case 'h':
                return Nanos::ceilHH;
            case 'm':
                return Nanos::ceilMI;
            case 's':
                return Nanos::ceilSS;
            case 'T':
                return Nanos::ceilMS;
            case 'U':
                return Nanos::ceilMC;
            case 'n':
                return Nanos::ceilNS;
            default:
                return null;
        }
    }

    @Override
    public TimestampConstant getTimestampConstantNull() {
        return TimestampConstant.TIMESTAMP_NANO_NULL;
    }

    @Override
    public TimestampDateFormatFactory getTimestampDateFormatFactory() {
        return NanosFormatFactory.INSTANCE;
    }

    @Override
    public TimestampDiffMethod getTimestampDiffMethod(char type) {
        switch (type) {
            case 'n':
                return Nanos::getNanosBetween;
            case 'u':
                return Nanos::getMicrosBetween;
            case 'T':
                return Nanos::getMillisBetween;
            case 's':
                return Nanos::getSecondsBetween;
            case 'm':
                return Nanos::getMinutesBetween;
            case 'h':
                return Nanos::getHoursBetween;
            case 'd':
                return Nanos::getDaysBetween;
            case 'w':
                return Nanos::getWeeksBetween;
            case 'M':
                return Nanos::getMonthsBetween;
            case 'y':
                return Nanos::getYearsBetween;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorMethod getTimestampFloorMethod(String unit) {
        switch (unit) {
            case "century":
                return Nanos::floorCentury;
            case "day":
                return Nanos::floorDD;
            case "week":
                return Nanos::floorDOW;
            case "decade":
                return Nanos::floorDecade;
            case "hour":
                return Nanos::floorHH;
            case "microsecond":
                return Nanos::floorMC;
            case "minute":
                return Nanos::floorMI;
            case "month":
                return Nanos::floorMM;
            case "millisecond":
                return Nanos::floorMS;
            case "nanosecond":
                return Nanos::floorNS;
            case "millennium":
                return Nanos::floorMillennium;
            case "quarter":
                return Nanos::floorQuarter;
            case "second":
                return Nanos::floorSS;
            case "year":
                return Nanos::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithOffsetMethod getTimestampFloorWithOffsetMethod(char unit) {
        switch (unit) {
            case 'M':
                return Nanos::floorMM;
            case 'y':
                return Nanos::floorYYYY;
            case 'w':
                return Nanos::floorWW;
            case 'd':
                return Nanos::floorDD;
            case 'h':
                return Nanos::floorHH;
            case 'm':
                return Nanos::floorMI;
            case 's':
                return Nanos::floorSS;
            case 'T':
                return Nanos::floorMS;
            case 'U':
                return Nanos::floorMC;
            case 'n':
                return Nanos::floorNS;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithStrideMethod getTimestampFloorWithStrideMethod(String unit) {
        switch (unit) {
            case "day":
                return Nanos::floorDD;
            case "hour":
                return Nanos::floorHH;
            case "microsecond":
                return Nanos::floorMC;
            case "minute":
                return Nanos::floorMI;
            case "month":
                return Nanos::floorMM;
            case "millisecond":
                return Nanos::floorMS;
            case "nanosecond":
                return Nanos::floorNS;
            case "second":
                return Nanos::floorSS;
            case "week":
                return Nanos::floorWW;
            case "year":
                return Nanos::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampSampler getTimestampSampler(long interval, char timeUnit, int position) throws SqlException {
        switch (timeUnit) {
            case 'n':
                // nanos
                return new SimpleTimestampSampler(interval, ColumnType.TIMESTAMP_NANO);
            case 'U':
                // micros
                return new SimpleTimestampSampler(interval * Nanos.MICRO_NANOS, ColumnType.TIMESTAMP_NANO);
            case 'T':
                // millis
                return new SimpleTimestampSampler(Nanos.MILLI_NANOS * interval, ColumnType.TIMESTAMP_NANO);
            case 's':
                // seconds
                return new SimpleTimestampSampler(Nanos.SECOND_NANOS * interval, ColumnType.TIMESTAMP_NANO);
            case 'm':
                // minutes
                return new SimpleTimestampSampler(Nanos.MINUTE_NANOS * interval, ColumnType.TIMESTAMP_NANO);
            case 'h':
                // hours
                return new SimpleTimestampSampler(Nanos.HOUR_NANOS * interval, ColumnType.TIMESTAMP_NANO);
            case 'd':
                // days
                return new SimpleTimestampSampler(Nanos.DAY_NANOS * interval, ColumnType.TIMESTAMP_NANO);
            case 'w':
                // weeks
                return new WeekTimestampNanosSampler((int) interval);
            case 'M':
                // months
                return new MonthTimestampNanosSampler((int) interval);
            case 'y':
                // years
                return new YearTimestampNanosSampler((int) interval);
            default:
                throw SqlException.$(position, "unsupported interval qualifier");
        }
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_NANO;
    }

    @Override
    public CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType) {
        if (ColumnType.isTimestampMicro(srcTimestampType)) {
            return CommonUtils::microsToNanos;
        }
        return null;
    }

    @Override
    public TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone) {
        try {
            return Nanos.getTimezoneRules(locale, timezone);
        } catch (NumericException e) {
            throw CairoException.critical(0).put("invalid timezone: ").put(timezone);
        }
    }

    @Override
    public int getWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getWeek(timestamp);
    }

    @Override
    public int getYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Nanos.getYear(timestamp);
    }

    @Override
    public boolean inInterval(long value, int intervalType, Interval interval) {
        assert intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO || intervalType == ColumnType.INTERVAL_TIMESTAMP_MICRO;
        long lo = interval.getLo();
        long hi = interval.getHi();
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_MICRO) {
            lo = CommonUtils.microsToNanos(lo);
            hi = CommonUtils.microsToNanos(hi);
        }
        return value >= lo && value <= hi;
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
                                nano *= CommonUtils.tenPow(nanoLim - p);
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
                                // varlen milli, micros and nanos
                                int nanoLim = p + 9;
                                if (nanoLim < lim) {
                                    throw NumericException.instance();
                                }

                                int nanos = 0;
                                for (; p < lim; p++) {
                                    char c = input.charAt(p);
                                    if (c < '0' || c > '9') {
                                        throw NumericException.instance();
                                    }
                                    nanos *= 10;
                                    nanos += c - '0';
                                }
                                int remainingDigits = nanoLim - p;
                                nanos *= CommonUtils.tenPow(remainingDigits);
                                long baseTime = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS;
                                int rangeNanos = CommonUtils.tenPow(remainingDigits) - 1;
                                IntervalUtils.encodeInterval(baseTime + nanos,
                                        baseTime + nanos + rangeNanos,
                                        operation,
                                        out);
                            } else if (p == lim) {
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
                                                + 999999999,
                                        operation,
                                        out);
                            } else {
                                throw NumericException.instance();
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
                                            + 999999999,
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
                                        + 999999999,
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
                                    + 999999999,
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
                                + 999999999,
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
                            + 999999999,
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
                throw TimestampDriver.expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
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
                    throw TimestampDriver.expectedPartitionDirNameFormatCairoException(partitionName, 0, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw TimestampDriver.expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
        }
    }

    @TestOnly
    public void setTicker(Clock clock) {
        this.clock = clock;
    }

    @Override
    public long startOfDay(long now, int shiftDays) {
        return Nanos.floorDD(Nanos.addDays(now, shiftDays));
    }

    @Override
    public long toDate(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Nanos.MILLI_NANOS;
    }

    @Override
    public long toHours(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Nanos.HOUR_NANOS;
    }

    @Override
    public String toMSecString(long timestamp) {
        return Nanos.toString(timestamp);
    }

    @Override
    public long toMicros(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Nanos.MICRO_NANOS;
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
    public long toSeconds(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Nanos.SECOND_NANOS;
    }

    @Override
    public long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Nanos.toTimezone(utcTimestamp, locale, timezone);
    }

    @Override
    public String toUSecString(long nanos) {
        return Nanos.toUSecString(nanos);
    }

    @Override
    public long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return Nanos.toUTC(localTimestamp, zoneRules);
    }

    @Override
    public long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Nanos.toUTC(localTimestamp, locale, timezone);
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
        throw NumericException.instance();
    }

    private static long partitionCeilDD(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.ceilDD(Math.max(nanos, 0));
    }

    private static long partitionCeilHH(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.ceilHH(Math.max(nanos, 0));
    }

    private static long partitionCeilMM(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.ceilMM(Math.max(nanos, 0));
    }

    private static long partitionCeilWW(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.ceilWW(Math.max(nanos, 0));
    }

    private static long partitionCeilYYYY(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.ceilYYYY(Math.max(nanos, 0));
    }

    private static long partitionFloorDD(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.floorDD(Math.max(nanos, 0));
    }

    private static long partitionFloorHH(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.floorHH(Math.max(nanos, 0));
    }

    private static long partitionFloorMM(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.floorMM(Math.max(nanos, 0));
    }

    private static long partitionFloorWW(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.floorWW(Math.max(nanos, 0));
    }

    private static long partitionFloorYYYY(long nanos) {
        // Designated timestamp can't be negative.
        return Nanos.floorYYYY(Math.max(nanos, 0));
    }

    private static void validateBounds0(long timestamp) {
        if (timestamp == Long.MIN_VALUE) {
            throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
        }
        if (timestamp < TableWriter.TIMESTAMP_EPOCH || timestamp > CommonUtils.TIMESTAMP_UNIT_NANOS) {
            throw CairoException.nonCritical().put("designated timestamp_ns before 1970-01-01 and beyond ").put(MAX_NANO_TIMESTAMP_STR).put(" is not allowed");
        }
    }

    public static class IsoDatePartitionFormat implements DateFormat {
        private final DateFormat baseFormat;
        private final TimestampFloorMethod floorMethod;

        public IsoDatePartitionFormat(TimestampFloorMethod floorMethod, DateFormat baseFormat) {
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
        public int getColumnType() {
            return ColumnType.TIMESTAMP_NANO;
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
            long ts;
            if (hi - lo < 4 || hi - lo > 28) {
                throw NumericException.instance();
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
            return ts;
        }
    }

    public static class IsoWeekPartitionFormat implements DateFormat {

        @Override
        public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            long weekTime = timestamp - NanosTimestampDriver.partitionFloorWW(timestamp);
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
        public int getColumnType() {
            return ColumnType.TIMESTAMP_NANO;
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
