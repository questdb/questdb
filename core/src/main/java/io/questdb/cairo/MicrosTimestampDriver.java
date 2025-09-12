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
import io.questdb.griffin.engine.groupby.MonthTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.SimpleTimestampSampler;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.griffin.engine.groupby.WeekTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.YearTimestampMicrosSampler;
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
import io.questdb.std.datetime.microtime.MicrosFormatFactory;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
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
import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.MicrosFormatUtils.*;

public class MicrosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new MicrosTimestampDriver();
    private static final DateFormat DEFAULT_FORMAT = new DateFormat() {
        @Override
        public void format(long datetime, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            sink.putAscii(DEFAULT_PARTITION_NAME);
        }

        @Override
        public int getColumnType() {
            return ColumnType.TIMESTAMP_MICRO;
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

    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(MicrosTimestampDriver::partitionFloorDD, DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(MicrosTimestampDriver::partitionFloorHH, HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(MicrosTimestampDriver::partitionFloorMM, MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(MicrosTimestampDriver::partitionFloorYYYY, YEAR_FORMAT);

    private final ColumnTypeConverter.Var2FixedConverter<CharSequence> converterStr2Timestamp = this::appendToMem;
    private final ColumnTypeConverter.Fixed2VarConverter converterTimestamp2Str = this::append;
    private Clock clock = MicrosecondClockImpl.INSTANCE;

    private MicrosTimestampDriver() {
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
                        // varlen milli and micros
                        int micrLim = pos + 6;
                        int mlim = Math.min(lim, micrLim);
                        int micr = 0;
                        for (; pos < mlim; pos++) {
                            char c = seq.charAt(pos);
                            if (c < '0' || c > '9') {
                                throw NumericException.instance();
                            }
                            micr *= 10;
                            micr += c - '0';
                        }
                        micr *= CommonUtils.tenPow(micrLim - pos);

                        // micros
                        ts += (day - 1) * Micros.DAY_MICROS
                                + hour * Micros.HOUR_MICROS
                                + min * Micros.MINUTE_MICROS
                                + sec * Micros.SECOND_MICROS
                                + micr;
                    } else {
                        if (pos == lim) {
                            // seconds
                            ts += (day - 1) * Micros.DAY_MICROS
                                    + hour * Micros.HOUR_MICROS
                                    + min * Micros.MINUTE_MICROS
                                    + sec * Micros.SECOND_MICROS;
                        } else {
                            throw NumericException.instance();
                        }
                    }
                } else {
                    // minute
                    ts += (day - 1) * Micros.DAY_MICROS
                            + hour * Micros.HOUR_MICROS
                            + min * Micros.MINUTE_MICROS;

                }
            } else {
                // year + month + day + hour
                ts += (day - 1) * Micros.DAY_MICROS
                        + hour * Micros.HOUR_MICROS;
            }
        } else {
            // year + month + day
            ts += (day - 1) * Micros.DAY_MICROS;
        }
        return ts;
    }

    @Override
    public long add(long timestamp, char type, int stride) {
        return Micros.addPeriod(timestamp, type, stride);
    }

    @Override
    public long addDays(long timestamp, int days) {
        return Micros.addDays(timestamp, days);
    }

    @Override
    public long addMonths(long timestamp, int months) {
        return Micros.addMonths(timestamp, months);
    }

    @Override
    public long addWeeks(long timestamp, int weeks) {
        return Micros.addWeeks(timestamp, weeks);
    }

    @Override
    public long addYears(long timestamp, int years) {
        return Micros.addYears(timestamp, years);
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        MicrosFormatUtils.appendDateTimeUSec(sink, timestamp);
    }

    @Override
    public boolean append(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            MicrosFormatUtils.appendDateTimeUSec(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public void appendToPGWireText(CharSink<?> sink, long timestamp) {
        MicrosFormatUtils.PG_TIMESTAMP_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public void appendTypeToPlan(PlanSink sink) {
        sink.val("timestamp");
    }

    @Override
    public long approxPartitionDuration(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.HOUR:
                return Micros.HOUR_MICROS;
            case PartitionBy.DAY:
                return Micros.DAY_MICROS;
            case PartitionBy.WEEK:
                return Micros.WEEK_MICROS;
            case PartitionBy.MONTH:
                return Micros.MONTH_MICROS_APPROX;
            case PartitionBy.YEAR:
                return Micros.YEAR_MICROS_NONLEAP;
            default:
                throw new UnsupportedOperationException("unexpected partition by: " + partitionBy);
        }
    }

    @Override
    public long ceilYYYY(long timestamp) {
        return Micros.ceilYYYY(timestamp);
    }

    @Override
    public long endOfDay(long start) {
        return start + Micros.DAY_MICROS - 1;
    }

    @Override
    public Interval fixInterval(Interval interval, int intervalType) {
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO) {
            long lo = interval.getLo() / Micros.MICRO_NANOS;
            long hi = interval.getHi() / Micros.MICRO_NANOS;
            interval.of(lo, hi);
        }
        return interval;
    }

    @Override
    public long floorYYYY(long timestamp) {
        return Micros.floorYYYY(timestamp);
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / Micros.MICRO_NANOS;
            case MICROS:
                return value;
            case MILLIS:
                return Math.multiplyExact(value, Micros.MILLI_MICROS);
            case SECONDS:
                return Math.multiplyExact(value, Micros.SECOND_MICROS);
            default:
                Duration duration = unit.getDuration();
                long micros = Math.multiplyExact(duration.getSeconds(), Micros.SECOND_MICROS);
                micros = Math.addExact(micros, duration.getNano() / Micros.MICRO_NANOS);
                return Math.multiplyExact(micros, value);
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Micros.SECOND_MICROS), instant.getNano() / Micros.MICRO_NANOS);
    }

    @Override
    public long from(long timestamp, int columnType) {
        if (ColumnType.isTimestampNano(columnType)) {
            return CommonUtils.nanosToMicros(timestamp);
        }
        return timestamp;
    }

    @Override
    public long fromDate(long date) {
        return date == Numbers.LONG_NULL ? Numbers.LONG_NULL : date * Micros.MILLI_MICROS;
    }

    @Override
    public long fromDays(int days) {
        return days * Micros.DAY_MICROS;
    }

    @Override
    public long fromHours(int hours) {
        return hours * Micros.HOUR_MICROS;
    }

    @Override
    public long fromMicros(long micros) {
        return micros;
    }

    @Override
    public long fromMillis(long millis) {
        return millis * Micros.MILLI_MICROS;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Micros.MINUTE_MICROS;
    }

    @Override
    public long fromNanos(long nanos) {
        return nanos == Numbers.LONG_NULL ? nanos : nanos / Micros.MICRO_NANOS;
    }

    @Override
    public long fromSeconds(long seconds) {
        return seconds * Micros.SECOND_MICROS;
    }

    @Override
    public long fromWeeks(int weeks) {
        return weeks * Micros.WEEK_MICROS;
    }

    @Override
    public TimestampAddMethod getAddMethod(char c) {
        switch (c) {
            case 'n':
                return Micros::addNanos;
            case 'u':
            case 'U':
                return Micros::addMicros;
            case 'T':
                return Micros::addMillis;
            case 's':
                return Micros::addSeconds;
            case 'm':
                return Micros::addMinutes;
            case 'H':
            case 'h': // compatibility with sample by syntax
                return Micros::addHours;
            case 'd':
                return Micros::addDays;
            case 'w':
                return Micros::addWeeks;
            case 'M':
                return Micros::addMonths;
            case 'y':
                return Micros::addYears;
            default:
                return null;
        }
    }

    @Override
    public int getCentury(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getCentury(timestamp);
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
        int year = Micros.getYear(timestamp);
        boolean leap = CommonUtils.isLeapYear(year);
        int month = Micros.getMonthOfYear(timestamp, year, leap);
        return Micros.getDayOfMonth(timestamp, year, month, leap);
    }

    @Override
    public int getDayOfWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getDayOfWeek(timestamp);
    }

    @Override
    public int getDayOfWeekSundayFirst(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getDayOfWeekSundayFirst(timestamp);
    }

    @Override
    public int getDaysPerMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Micros.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        int month = Micros.getMonthOfYear(timestamp, year, isLeap);
        return CommonUtils.getDaysPerMonth(month, isLeap);
    }

    @Override
    public int getDecade(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getDecade(timestamp);
    }

    @Override
    public int getDow(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getDow(timestamp);
    }

    @Override
    public int getDoy(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getDoy(timestamp);
    }

    @Override
    public int getGKKHourInt() {
        return SqlCodeGenerator.GKK_MICRO_HOUR_INT;
    }

    @Override
    public int getHourOfDay(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getHourOfDay(timestamp);
    }

    @Override
    public IntervalConstant getIntervalConstantNull() {
        return IntervalConstant.TIMESTAMP_MICRO_NULL;
    }

    @Override
    public int getIsoYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getIsoYear(timestamp);
    }

    @Override
    public int getMicrosOfMilli(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getMicrosOfMilli(timestamp);
    }

    @Override
    public int getMicrosOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getMicrosOfSecond(timestamp);
    }

    @Override
    public int getMillennium(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getMillennium(timestamp);
    }

    @Override
    public int getMillisOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getMillisOfSecond(timestamp);
    }

    @Override
    public int getMinuteOfHour(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getMinuteOfHour(timestamp);
    }

    @Override
    public int getMonthOfYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Micros.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        return Micros.getMonthOfYear(timestamp, year, isLeap);
    }

    @Override
    public int getNanosOfMicros(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return 0;
    }

    @Override
    public PartitionAddMethod getPartitionAddMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Micros::addDays;
            case MONTH:
                return Micros::addMonths;
            case YEAR:
                return Micros::addYears;
            case HOUR:
                return Micros::addHours;
            case WEEK:
                return Micros::addWeeks;
            default:
                return null;
        }
    }

    @Override
    public TimestampCeilMethod getPartitionCeilMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return MicrosTimestampDriver::partitionCeilDD;
            case MONTH:
                return MicrosTimestampDriver::partitionCeilMM;
            case YEAR:
                return MicrosTimestampDriver::partitionCeilYYYY;
            case HOUR:
                return MicrosTimestampDriver::partitionCeilHH;
            case WEEK:
                return MicrosTimestampDriver::partitionCeilWW;
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
                return MicrosTimestampDriver::partitionFloorDD;
            case WEEK:
                return MicrosTimestampDriver::partitionFloorWW;
            case MONTH:
                return MicrosTimestampDriver::partitionFloorMM;
            case YEAR:
                return MicrosTimestampDriver::partitionFloorYYYY;
            case HOUR:
                return MicrosTimestampDriver::partitionFloorHH;
            default:
                return null;
        }
    }

    @Override
    public long getPeriodBetween(char unit, long start, long end, int leftType, int rightType) {
        if (start == Numbers.LONG_NULL || end == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        start = from(start, leftType);
        end = from(end, rightType);
        return Micros.getPeriodBetween(unit, start, end);
    }

    @Override
    public int getQuarter(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getQuarter(timestamp);
    }

    @Override
    public int getSecondOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getSecondOfMinute(timestamp);
    }

    @Override
    public int getTZRuleResolution() {
        return RESOLUTION_MICROS;
    }

    @Override
    public long getTicks() {
        return clock.getTicks();
    }

    @Override
    public TimestampCeilMethod getTimestampCeilMethod(char unit) {
        switch (unit) {
            case 'd':
                return Micros::ceilDD;
            case 'M':
                return Micros::ceilMM;
            case 'y':
                return Micros::ceilYYYY;
            case 'w':
                return Micros::ceilWW;
            case 'h':
                return Micros::ceilHH;
            case 'm':
                return Micros::ceilMI;
            case 's':
                return Micros::ceilSS;
            case 'T':
                return Micros::ceilMS;
            case 'U':
                return Micros::ceilMC;
            case 'n':
                return Micros::ceilNS;
            default:
                return null;
        }
    }

    @Override
    public TimestampConstant getTimestampConstantNull() {
        return TimestampConstant.TIMESTAMP_MICRO_NULL;
    }

    @Override
    public TimestampDateFormatFactory getTimestampDateFormatFactory() {
        return MicrosFormatFactory.INSTANCE;
    }

    @Override
    public TimestampDiffMethod getTimestampDiffMethod(char type) {
        switch (type) {
            case 'n':
                return Micros::getNanosBetween;
            case 'u':
                return Micros::getMicrosBetween;
            case 'T':
                return Micros::getMillisBetween;
            case 's':
                return Micros::getSecondsBetween;
            case 'm':
                return Micros::getMinutesBetween;
            case 'h':
                return Micros::getHoursBetween;
            case 'd':
                return Micros::getDaysBetween;
            case 'w':
                return Micros::getWeeksBetween;
            case 'M':
                return Micros::getMonthsBetween;
            case 'y':
                return Micros::getYearsBetween;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorMethod getTimestampFloorMethod(String unit) {
        switch (unit) {
            case "century":
                return Micros::floorCentury;
            case "day":
                return Micros::floorDD;
            case "week":
                return Micros::floorDOW;
            case "decade":
                return Micros::floorDecade;
            case "hour":
                return Micros::floorHH;
            case "microsecond":
                return Micros::floorMC;
            case "minute":
                return Micros::floorMI;
            case "month":
                return Micros::floorMM;
            case "millisecond":
                return Micros::floorMS;
            case "nanosecond":
                return Micros::floorNS;
            case "millennium":
                return Micros::floorMillennium;
            case "quarter":
                return Micros::floorQuarter;
            case "second":
                return Micros::floorSS;
            case "year":
                return Micros::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithOffsetMethod getTimestampFloorWithOffsetMethod(char unit) {
        switch (unit) {
            case 'M':
                return Micros::floorMM;
            case 'y':
                return Micros::floorYYYY;
            case 'w':
                return Micros::floorWW;
            case 'd':
                return Micros::floorDD;
            case 'h':
                return Micros::floorHH;
            case 'm':
                return Micros::floorMI;
            case 's':
                return Micros::floorSS;
            case 'T':
                return Micros::floorMS;
            case 'U':
                return Micros::floorMC;
            case 'n':
                return Micros::floorNS;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithStrideMethod getTimestampFloorWithStrideMethod(String unit) {
        switch (unit) {
            case "day":
                return Micros::floorDD;
            case "hour":
                return Micros::floorHH;
            case "microsecond":
                return Micros::floorMC;
            case "minute":
                return Micros::floorMI;
            case "month":
                return Micros::floorMM;
            case "millisecond":
                return Micros::floorMS;
            case "nanosecond":
                return Micros::floorNS;
            case "second":
                return Micros::floorSS;
            case "week":
                return Micros::floorWW;
            case "year":
                return Micros::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampSampler getTimestampSampler(long interval, char timeUnit, int position) throws SqlException {
        switch (timeUnit) {
            case 'n':
                // nanos
                return new SimpleTimestampSampler(Math.max(interval / Micros.MICRO_NANOS, 1), ColumnType.TIMESTAMP_MICRO);
            case 'U':
                // micros
                return new SimpleTimestampSampler(interval, ColumnType.TIMESTAMP_MICRO);
            case 'T':
                // millis
                return new SimpleTimestampSampler(Micros.MILLI_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 's':
                // seconds
                return new SimpleTimestampSampler(Micros.SECOND_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'm':
                // minutes
                return new SimpleTimestampSampler(Micros.MINUTE_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'h':
                // hours
                return new SimpleTimestampSampler(Micros.HOUR_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'd':
                // days
                return new SimpleTimestampSampler(Micros.DAY_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'w':
                // weeks
                return new WeekTimestampMicrosSampler((int) interval);
            case 'M':
                // months
                return new MonthTimestampMicrosSampler((int) interval);
            case 'y':
                // years
                return new YearTimestampMicrosSampler((int) interval);
            default:
                throw SqlException.$(position, "unsupported interval qualifier");
        }
    }

    @Override
    public int getTimestampType() {
        return ColumnType.TIMESTAMP_MICRO;
    }

    @Override
    public CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType) {
        if (ColumnType.isTimestampNano(srcTimestampType)) {
            return CommonUtils::nanosToMicros;
        }
        return null;
    }

    @Override
    public TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone) {
        try {
            return Micros.getTimezoneRules(locale, timezone);
        } catch (NumericException e) {
            throw CairoException.critical(0).put("invalid timezone: ").put(timezone);
        }
    }

    @Override
    public int getWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getWeek(timestamp);
    }

    @Override
    public int getYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Micros.getYear(timestamp);
    }

    @Override
    public boolean inInterval(long value, int intervalType, Interval interval) {
        assert intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO || intervalType == ColumnType.INTERVAL_TIMESTAMP_MICRO;
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO) {
            value = CommonUtils.microsToNanos(value);
        }
        return value >= interval.getLo() && value <= interval.getHi();
    }

    @Override
    public long monthsBetween(long hi, long lo) {
        return Micros.getMonthsBetween(hi, lo);
    }

    @Override
    public long parseAnyFormat(CharSequence token, int start, int len) throws NumericException {
        return MicrosFormatUtils.tryParse(token, start, len);
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
                                micr *= CommonUtils.tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                }

                                // micros
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Micros.yearMicros(year, l)
                                    + Micros.monthOfYearMicros(month, l)
                                    + (day - 1) * Micros.DAY_MICROS
                                    + hour * Micros.HOUR_MICROS
                                    + min * Micros.MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Micros.yearMicros(year, l)
                                + Micros.monthOfYearMicros(month, l)
                                + (day - 1) * Micros.DAY_MICROS
                                + hour * Micros.HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = Micros.yearMicros(year, l)
                            + Micros.monthOfYearMicros(month, l)
                            + (day - 1) * Micros.DAY_MICROS;
                }
            } else {
                // year + month
                ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(1, l));
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
                                micr *= CommonUtils.tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                }

                                // micros
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Micros.yearMicros(year, l)
                                    + Micros.monthOfYearMicros(month, l)
                                    + (day - 1) * Micros.DAY_MICROS
                                    + hour * Micros.HOUR_MICROS
                                    + min * Micros.MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Micros.yearMicros(year, l)
                                + Micros.monthOfYearMicros(month, l)
                                + (day - 1) * Micros.DAY_MICROS
                                + hour * Micros.HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = Micros.yearMicros(year, l)
                            + Micros.monthOfYearMicros(month, l)
                            + (day - 1) * Micros.DAY_MICROS;
                }
            } else {
                // year + month
                ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(1, l));
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
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(lim, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = input.charAt(p);
                                    if (c < '0' || c > '9') {
                                        throw NumericException.instance();
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                int remainingDigits = micrLim - p;
                                micr *= CommonUtils.tenPow(remainingDigits);

                                if (p + 3 < lim) {
                                    throw NumericException.instance();
                                }
                                // ignore nanos for MicroTimestamp
                                for (; p < lim; p++) {
                                    char c = input.charAt(p);
                                    if (c < '0' || c > '9') {
                                        throw NumericException.instance();
                                    }
                                }

                                long baseTime = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS;
                                int rangeMicros = CommonUtils.tenPow(remainingDigits) - 1;
                                IntervalUtils.encodeInterval(baseTime + micr,
                                        baseTime + micr + rangeMicros,
                                        operation,
                                        out);
                            } else if (p == lim) {
                                // seconds
                                IntervalUtils.encodeInterval(Micros.yearMicros(year, l)
                                                + Micros.monthOfYearMicros(month, l)
                                                + (day - 1) * Micros.DAY_MICROS
                                                + hour * Micros.HOUR_MICROS
                                                + min * Micros.MINUTE_MICROS
                                                + sec * Micros.SECOND_MICROS,
                                        Micros.yearMicros(year, l)
                                                + Micros.monthOfYearMicros(month, l)
                                                + (day - 1) * Micros.DAY_MICROS
                                                + hour * Micros.HOUR_MICROS
                                                + min * Micros.MINUTE_MICROS
                                                + sec * Micros.SECOND_MICROS
                                                + 999999,
                                        operation,
                                        out);
                            } else {
                                throw NumericException.instance();
                            }
                        } else {
                            // minute
                            IntervalUtils.encodeInterval(
                                    Micros.yearMicros(year, l)
                                            + Micros.monthOfYearMicros(month, l)
                                            + (day - 1) * Micros.DAY_MICROS
                                            + hour * Micros.HOUR_MICROS
                                            + min * Micros.MINUTE_MICROS,
                                    Micros.yearMicros(year, l)
                                            + Micros.monthOfYearMicros(month, l)
                                            + (day - 1) * Micros.DAY_MICROS
                                            + hour * Micros.HOUR_MICROS
                                            + min * Micros.MINUTE_MICROS
                                            + 59 * Micros.SECOND_MICROS
                                            + 999999,
                                    operation,
                                    out
                            );
                        }
                    } else {
                        // year + month + day + hour
                        IntervalUtils.encodeInterval(
                                Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS,
                                Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + 59 * Micros.MINUTE_MICROS
                                        + 59 * Micros.SECOND_MICROS
                                        + 999999,
                                operation,
                                out
                        );
                    }
                } else {
                    // year + month + day
                    IntervalUtils.encodeInterval(
                            Micros.yearMicros(year, l)
                                    + Micros.monthOfYearMicros(month, l)
                                    + (day - 1) * Micros.DAY_MICROS,
                            Micros.yearMicros(year, l)
                                    + Micros.monthOfYearMicros(month, l)
                                    + (day - 1) * Micros.DAY_MICROS
                                    + 23 * Micros.HOUR_MICROS
                                    + 59 * Micros.MINUTE_MICROS
                                    + 59 * Micros.SECOND_MICROS
                                    + 999999,
                            operation,
                            out
                    );
                }
            } else {
                // year + month
                IntervalUtils.encodeInterval(
                        Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l),
                        Micros.yearMicros(year, l)
                                + Micros.monthOfYearMicros(month, l)
                                + (CommonUtils.getDaysPerMonth(month, l) - 1) * Micros.DAY_MICROS
                                + 23 * Micros.HOUR_MICROS
                                + 59 * Micros.MINUTE_MICROS
                                + 59 * Micros.SECOND_MICROS
                                + 999999,
                        operation,
                        out
                );
            }
        } else {
            // year
            IntervalUtils.encodeInterval(
                    Micros.yearMicros(year, l) + Micros.monthOfYearMicros(1, l),
                    Micros.yearMicros(year, l)
                            + Micros.monthOfYearMicros(12, l)
                            + (CommonUtils.getDaysPerMonth(12, l) - 1) * Micros.DAY_MICROS
                            + 23 * Micros.HOUR_MICROS
                            + 59 * Micros.MINUTE_MICROS
                            + 59 * Micros.SECOND_MICROS
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
                    return Micros.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, EN_LOCALE));
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
        return Micros.floorDD(Micros.addDays(now, shiftDays));
    }

    @Override
    public long toDate(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Micros.MILLI_MICROS;
    }

    @Override
    public long toHours(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Micros.HOUR_MICROS;
    }

    @Override
    public String toMSecString(long timestamp) {
        return Micros.toString(timestamp);
    }

    @Override
    public long toMicros(long timestamp) {
        return timestamp;
    }

    @Override
    public long toNanos(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? timestamp : timestamp * Micros.MICRO_NANOS;
    }

    @Override
    public long toNanosScale() {
        return Micros.MICRO_NANOS;
    }

    @Override
    public long toSeconds(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Micros.SECOND_MICROS;
    }

    @Override
    public long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Micros.toTimezone(utcTimestamp, locale, timezone);
    }

    @Override
    public String toUSecString(long micros) {
        return Micros.toUSecString(micros);
    }

    @Override
    public long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return Micros.toUTC(localTimestamp, zoneRules);
    }

    @Override
    public long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Micros.toUTC(localTimestamp, locale, timezone);
    }

    @Override
    public void validateBounds(long timestamp) {
        if (Long.compareUnsigned(timestamp, Micros.YEAR_10000) >= 0) {
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
                return tzSign * (hour * Micros.HOUR_MICROS + min * Micros.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Micros.HOUR_MICROS);
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
                return tzSign * (hour * Micros.HOUR_MICROS + min * Micros.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Micros.HOUR_MICROS);
            }
        }
        throw NumericException.instance();
    }

    private static long partitionCeilDD(long micros) {
        // Designated timestamp can't be negative.
        return Micros.ceilDD(Math.max(micros, 0));
    }

    private static long partitionCeilHH(long micros) {
        // Designated timestamp can't be negative.
        return Micros.ceilHH(Math.max(micros, 0));
    }

    private static long partitionCeilMM(long micros) {
        // Designated timestamp can't be negative.
        return Micros.ceilMM(Math.max(micros, 0));
    }

    private static long partitionCeilWW(long micros) {
        // Designated timestamp can't be negative.
        return Micros.ceilWW(Math.max(micros, 0));
    }

    private static long partitionCeilYYYY(long micros) {
        // Designated timestamp can't be negative.
        return Micros.ceilYYYY(Math.max(micros, 0));
    }

    private static long partitionFloorDD(long micros) {
        // Designated timestamp can't be negative.
        return Micros.floorDD(Math.max(micros, 0));
    }

    private static long partitionFloorHH(long micros) {
        // Designated timestamp can't be negative.
        return Micros.floorHH(Math.max(micros, 0));
    }

    private static long partitionFloorMM(long micros) {
        // Designated timestamp can't be negative.
        return Micros.floorMM(Math.max(micros, 0));
    }

    private static long partitionFloorWW(long micros) {
        // Designated timestamp can't be negative.
        return Micros.floorWW(Math.max(micros, 0));
    }

    private static long partitionFloorYYYY(long micros) {
        // Designated timestamp can't be negative.
        return Micros.floorYYYY(Math.max(micros, 0));
    }

    private static void validateBounds0(long timestamp) {
        if (timestamp == Long.MIN_VALUE) {
            throw CairoException.nonCritical().put("designated timestamp column cannot be NULL");
        }
        if (timestamp < TableWriter.TIMESTAMP_EPOCH) {
            throw CairoException.nonCritical().put("designated timestamp before 1970-01-01 is not allowed");
        }
        if (timestamp >= Micros.YEAR_10000) {
            throw CairoException.nonCritical().put("designated timestamp beyond 9999-12-31 is not allowed");
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
                long time = timestamp - (timestamp / Micros.DAY_MICROS) * Micros.DAY_MICROS;

                if (time > 0) {
                    int hour = (int) (time / Micros.HOUR_MICROS);
                    int minute = (int) ((time % Micros.HOUR_MICROS) / Micros.MINUTE_MICROS);
                    int second = (int) ((time % Micros.MINUTE_MICROS) / Micros.SECOND_MICROS);
                    int milliMicros = (int) (time % Micros.SECOND_MICROS);

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
        public int getColumnType() {
            return ColumnType.TIMESTAMP_MICRO;
        }

        @Override
        public long parse(@NotNull CharSequence in, @NotNull DateLocale locale) throws NumericException {
            return parse(in, 0, in.length(), locale);
        }

        @Override
        public long parse(@NotNull CharSequence in, int lo, int hi, @NotNull DateLocale locale) throws NumericException {
            long ts;
            if (hi - lo < 4 || hi - lo > 25) {
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
                    ts = Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l);
                    ts = parseDayTime(in, hi, p, ts, dayRange, 2);
                } else {
                    // year + month
                    ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l));
                }
            } else {
                // year
                ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(1, l));
            }
            return ts;
        }
    }

    public static class IsoWeekPartitionFormat implements DateFormat {

        @Override
        public void format(long timestamp, @NotNull DateLocale locale, @Nullable CharSequence timeZoneName, @NotNull CharSink<?> sink) {
            long weekTime = timestamp - MicrosTimestampDriver.partitionFloorWW(timestamp);
            WEEK_FORMAT.format(timestamp, locale, timeZoneName, sink);

            if (weekTime > 0) {
                int dayOfWeek = (int) (weekTime / Micros.DAY_MICROS) + 1;
                int hour = (int) ((weekTime % Micros.DAY_MICROS) / Micros.HOUR_MICROS);
                int minute = (int) ((weekTime % Micros.HOUR_MICROS) / Micros.MINUTE_MICROS);
                int second = (int) ((weekTime % Micros.MINUTE_MICROS) / Micros.SECOND_MICROS);
                int milliMicros = (int) (weekTime % Micros.SECOND_MICROS);

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
        public int getColumnType() {
            return ColumnType.TIMESTAMP_MICRO;
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
