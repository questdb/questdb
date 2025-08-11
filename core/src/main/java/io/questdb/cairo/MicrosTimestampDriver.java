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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.BaseTimestampSampler;
import io.questdb.griffin.engine.groupby.MonthTimestampMicrosSampler;
import io.questdb.griffin.engine.groupby.TimestampSampler;
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
import io.questdb.std.datetime.microtime.MicrosFormatFactory;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
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
import static io.questdb.std.datetime.microtime.TimestampFormatUtils.*;

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

    private static final DateFormat PARTITION_DAY_FORMAT = new IsoDatePartitionFormat(Timestamps::floorDD, DAY_FORMAT);
    private static final DateFormat PARTITION_HOUR_FORMAT = new IsoDatePartitionFormat(Timestamps::floorHH, HOUR_FORMAT);
    private static final DateFormat PARTITION_MONTH_FORMAT = new IsoDatePartitionFormat(Timestamps::floorMM, MONTH_FORMAT);
    private static final DateFormat PARTITION_WEEK_FORMAT = new IsoWeekPartitionFormat();
    private static final DateFormat PARTITION_YEAR_FORMAT = new IsoDatePartitionFormat(Timestamps::floorYYYY, YEAR_FORMAT);
    private Clock clock = MicrosecondClockImpl.INSTANCE;

    private MicrosTimestampDriver() {
    }

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
                        micr *= CommonUtils.tenPow(micrLim - pos);

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
    public long addDays(long timestamp, int days) {
        return Timestamps.addDays(timestamp, days);
    }

    @Override
    public long addHours(long timestamp, int hours) {
        return Timestamps.addHours(timestamp, hours);
    }

    @Override
    public long addMicros(long timestamp, int micros) {
        return Timestamps.addMicros(timestamp, micros);
    }

    @Override
    public long addMillis(long timestamp, int millis) {
        return Timestamps.addMillis(timestamp, millis);
    }

    @Override
    public long addMinutes(long timestamp, int minutes) {
        return Timestamps.addMinutes(timestamp, minutes);
    }

    @Override
    public long addMonths(long timestamp, int months) {
        return Timestamps.addMonths(timestamp, months);
    }

    @Override
    public long addNanos(long timestamp, int nanos) {
        return Timestamps.addNanos(timestamp, nanos);
    }

    @Override
    public long addPeriod(long lo, char type, int period) {
        return Timestamps.addPeriod(lo, type, period);
    }

    @Override
    public long addSeconds(long timestamp, int seconds) {
        return Timestamps.addSeconds(timestamp, seconds);
    }

    @Override
    public long addWeeks(long timestamp, int weeks) {
        return Timestamps.addWeeks(timestamp, weeks);
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
    public void appendPGWireText(CharSink<?> sink, long timestamp) {
        TimestampFormatUtils.PG_TIMESTAMP_FORMAT.format(timestamp, EN_LOCALE, null, sink);
    }

    @Override
    public PlanSink appendTypeToPlan(PlanSink sink) {
        return sink.val("timestamp");
    }

    @Override
    public long approxPartitionTimestamps(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.HOUR:
                return Timestamps.HOUR_MICROS;
            case PartitionBy.DAY:
                return Timestamps.DAY_MICROS;
            case PartitionBy.WEEK:
                return Timestamps.WEEK_MICROS;
            case PartitionBy.MONTH:
                return Timestamps.MONTH_MICROS_APPROX;
            case PartitionBy.YEAR:
                return Timestamps.YEAR_MICROS_NONLEAP;
            default:
                throw new UnsupportedOperationException("unexpected partition by: " + partitionBy);
        }
    }

    @Override
    public long ceilYYYY(long timestamp) {
        return Timestamps.ceilYYYY(timestamp);
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
    public long dayEnd(long start) {
        return start + Timestamps.DAY_MICROS - 1;
    }

    @Override
    public long dayStart(long now, int shiftDays) {
        return Timestamps.floorDD(Timestamps.addDays(now, shiftDays));
    }

    @Override
    public Interval fixInterval(Interval interval, int intervalType) {
        if (intervalType == ColumnType.INTERVAL_TIMESTAMP_NANO) {
            long lo = interval.getLo() / 1000L;
            long hi = interval.getHi() / 1000L;
            interval.of(lo, hi);
        }
        return interval;
    }

    @Override
    public long floorYYYY(long timestamp) {
        return Timestamps.floorYYYY(timestamp);
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / 1_000;
            case MICROS:
                return value;
            case MILLIS:
                return Math.multiplyExact(value, 1_000);
            case SECONDS:
                return Math.multiplyExact(value, 1_000_000);
            default:
                Duration duration = unit.getDuration();
                long micros = Math.multiplyExact(duration.getSeconds(), 1_000_000L);
                micros = Math.addExact(micros, duration.getNano() / 1_000);
                return Math.multiplyExact(micros, value);
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Timestamps.SECOND_MICROS), instant.getNano() / 1000);
    }

    @Override
    public long from(long timestamp, int timestampType) {
        if (timestampType == ColumnType.TIMESTAMP_NANO) {
            return CommonUtils.nanosToMicros(timestamp);
        }
        return timestamp;
    }

    @Override
    public long fromDate(long date) {
        return date == Numbers.LONG_NULL ? Numbers.LONG_NULL : date * Timestamps.MILLI_MICROS;
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
    public long fromMicros(long micros) {
        return micros;
    }

    @Override
    public long fromMillis(long millis) {
        return millis * Timestamps.MILLI_MICROS;
    }

    @Override
    public long fromMinutes(int minutes) {
        return minutes * Timestamps.MINUTE_MICROS;
    }

    @Override
    public long fromNanos(long nanos) {
        return nanos == Numbers.LONG_NULL ? nanos : nanos / Timestamps.MICRO_NANOS;
    }

    @Override
    public long fromSeconds(long seconds) {
        return seconds * Timestamps.SECOND_MICROS;
    }

    @Override
    public long fromWeeks(int weeks) {
        return weeks * Timestamps.WEEK_MICROS;
    }

    @Override
    public TimestampAddMethod getAddMethod(char c) {
        switch (c) {
            case 'n':
                return Timestamps::addNanos;
            case 'u':
            case 'U':
                return Timestamps::addMicros;
            case 'T':
                return Timestamps::addMillis;
            case 's':
                return Timestamps::addSeconds;
            case 'm':
                return Timestamps::addMinutes;
            case 'H':
            case 'h': // compatibility with sample by syntax
                return Timestamps::addHours;
            case 'd':
                return Timestamps::addDays;
            case 'w':
                return Timestamps::addWeeks;
            case 'M':
                return Timestamps::addMonths;
            case 'y':
                return Timestamps::addYears;
            default:
                return null;
        }
    }

    @Override
    public int getCentury(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getCentury(timestamp);
    }

    @Override
    public int getColumnType() {
        return ColumnType.TIMESTAMP_MICRO;
    }

    @Override
    public int getDayOfMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Timestamps.getYear(timestamp);
        boolean leap = CommonUtils.isLeapYear(year);
        int month = Timestamps.getMonthOfYear(timestamp, year, leap);
        return Timestamps.getDayOfMonth(timestamp, year, month, leap);
    }

    @Override
    public int getDayOfWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getDayOfWeek(timestamp);
    }

    @Override
    public int getDayOfWeekSundayFirst(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getDayOfWeekSundayFirst(timestamp);
    }

    @Override
    public int getDaysPerMonth(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Timestamps.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        int month = Timestamps.getMonthOfYear(timestamp, year, isLeap);
        return CommonUtils.getDaysPerMonth(month, isLeap);
    }

    @Override
    public int getDecade(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getDecade(timestamp);
    }

    @Override
    public int getDow(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getDow(timestamp);
    }

    @Override
    public int getDoy(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getDoy(timestamp);
    }

    @Override
    public int getHourOfDay(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getHourOfDay(timestamp);
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
        return Timestamps.getIsoYear(timestamp);
    }

    @Override
    public int getMicrosOfMilli(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getMicrosOfMilli(timestamp);
    }

    @Override
    public long getMicrosOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        return Timestamps.getMicrosOfMinute(timestamp);
    }

    @Override
    public int getMillennium(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getMillennium(timestamp);
    }

    @Override
    public long getMillisOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        return Timestamps.getMillisOfMinute(timestamp);
    }

    @Override
    public int getMillisOfSecond(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getMillisOfSecond(timestamp);
    }

    @Override
    public int getMinuteOfHour(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getMinuteOfHour(timestamp);
    }

    @Override
    public int getMonthOfYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        int year = Timestamps.getYear(timestamp);
        boolean isLeap = CommonUtils.isLeapYear(year);
        return Timestamps.getMonthOfYear(timestamp, year, isLeap);
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
                return Timestamps::addDays;
            case MONTH:
                return Timestamps::addMonths;
            case YEAR:
                return Timestamps::addYears;
            case HOUR:
                return Timestamps::addHours;
            case WEEK:
                return Timestamps::addWeeks;
            default:
                return null;
        }
    }

    @Override
    public TimestampCeilMethod getPartitionCeilMethod(int partitionBy) {
        switch (partitionBy) {
            case DAY:
                return Timestamps::ceilDD;
            case MONTH:
                return Timestamps::ceilMM;
            case YEAR:
                return Timestamps::ceilYYYY;
            case HOUR:
                return Timestamps::ceilHH;
            case WEEK:
                return Timestamps::ceilWW;
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
                return Timestamps::floorDD;
            case WEEK:
                return Timestamps::floorWW;
            case MONTH:
                return Timestamps::floorMM;
            case YEAR:
                return Timestamps::floorYYYY;
            case HOUR:
                return Timestamps::floorHH;
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
        return Timestamps.getPeriodBetween(unit, start, end);
    }

    @Override
    public int getQuarter(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getQuarter(timestamp);
    }

    @Override
    public int getSecondOfMinute(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getSecondOfMinute(timestamp);
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
    public TimestampCeilMethod getTimestampCeilMethod(char c) {
        switch (c) {
            case 'd':
                return Timestamps::ceilDD;
            case 'M':
                return Timestamps::ceilMM;
            case 'y':
                return Timestamps::ceilYYYY;
            case 'w':
                return Timestamps::ceilWW;
            case 'h':
                return Timestamps::ceilHH;
            case 'm':
                return Timestamps::ceilMI;
            case 's':
                return Timestamps::ceilSS;
            case 'T':
                return Timestamps::ceilMS;
            case 'U':
                return Timestamps::ceilMR;
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
                return Timestamps::getNanosBetween;
            case 'u':
                return Timestamps::getMicrosBetween;
            case 'T':
                return Timestamps::getMillisBetween;
            case 's':
                return Timestamps::getSecondsBetween;
            case 'm':
                return Timestamps::getMinutesBetween;
            case 'h':
                return Timestamps::getHoursBetween;
            case 'd':
                return Timestamps::getDaysBetween;
            case 'w':
                return Timestamps::getWeeksBetween;
            case 'M':
                return Timestamps::getMonthsBetween;
            case 'y':
                return Timestamps::getYearsBetween;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorMethod getTimestampFloorMethod(String c) {
        switch (c) {
            case "century":
                return Timestamps::floorCentury;
            case "day":
                return Timestamps::floorDD;
            case "week":
                return Timestamps::floorDOW;
            case "decade":
                return Timestamps::floorDecade;
            case "hour":
                return Timestamps::floorHH;
            case "microsecond":
                return Timestamps::floorMC;
            case "minute":
                return Timestamps::floorMI;
            case "month":
                return Timestamps::floorMM;
            case "millisecond":
                return Timestamps::floorMS;
            case "nanosecond":
                return Timestamps::floorNS;
            case "millennium":
                return Timestamps::floorMillennium;
            case "quarter":
                return Timestamps::floorQuarter;
            case "second":
                return Timestamps::floorSS;
            case "year":
                return Timestamps::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithOffsetMethod getTimestampFloorWithOffsetMethod(char c) {
        switch (c) {
            case 'M':
                return Timestamps::floorMM;
            case 'y':
                return Timestamps::floorYYYY;
            case 'w':
                return Timestamps::floorWW;
            case 'd':
                return Timestamps::floorDD;
            case 'h':
                return Timestamps::floorHH;
            case 'm':
                return Timestamps::floorMI;
            case 's':
                return Timestamps::floorSS;
            case 'T':
                return Timestamps::floorMS;
            case 'U':
                return Timestamps::floorMC;
            case 'n':
                return Timestamps::floorNS;
            default:
                return null;
        }
    }

    @Override
    public TimestampFloorWithStrideMethod getTimestampFloorWithStrideMethod(String c) {
        switch (c) {
            case "day":
                return Timestamps::floorDD;
            case "hour":
                return Timestamps::floorHH;
            case "microsecond":
                return Timestamps::floorMC;
            case "minute":
                return Timestamps::floorMI;
            case "month":
                return Timestamps::floorMM;
            case "millisecond":
                return Timestamps::floorMS;
            case "nanosecond":
                return Timestamps::floorNS;
            case "second":
                return Timestamps::floorSS;
            case "week":
                return Timestamps::floorWW;
            case "year":
                return Timestamps::floorYYYY;
            default:
                return null;
        }
    }

    @Override
    public TimestampSampler getTimestampSampler(long interval, char timeUnit, int position) throws SqlException {
        switch (timeUnit) {
            case 'U':
                // micros
                return new BaseTimestampSampler(interval, ColumnType.TIMESTAMP_MICRO);
            case 'T':
                // millis
                return new BaseTimestampSampler(Timestamps.MILLI_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 's':
                // seconds
                return new BaseTimestampSampler(Timestamps.SECOND_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'm':
                // minutes
                return new BaseTimestampSampler(Timestamps.MINUTE_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'h':
                // hours
                return new BaseTimestampSampler(Timestamps.HOUR_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'd':
                // days
                return new BaseTimestampSampler(Timestamps.DAY_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'w':
                // weeks
                return new BaseTimestampSampler(Timestamps.WEEK_MICROS * interval, ColumnType.TIMESTAMP_MICRO);
            case 'M':
                // months
                return new MonthTimestampMicrosSampler((int) interval);
            case 'y':
                return new YearTimestampMicrosSampler((int) interval);
            default:
                throw SqlException.$(position, "unsupported interval qualifier");
        }
    }

    @Override
    public CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType) {
        if (srcTimestampType == ColumnType.TIMESTAMP_NANO) {
            return CommonUtils::nanosToMicros;
        }
        return null;
    }

    @Override
    public TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone) {
        try {
            return Timestamps.getTimezoneRules(locale, timezone);
        } catch (NumericException e) {
            throw CairoException.critical(0).put("invalid timezone: ").put(timezone);
        }
    }

    @Override
    public int getWeek(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getWeek(timestamp);
    }

    @Override
    public int getYear(long timestamp) {
        if (timestamp == Numbers.LONG_NULL) {
            return Numbers.INT_NULL;
        }
        return Timestamps.getYear(timestamp);
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
                                + (CommonUtils.getDaysPerMonth(month, l) - 1) * Timestamps.DAY_MICROS
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
                            + (CommonUtils.getDaysPerMonth(12, l) - 1) * Timestamps.DAY_MICROS
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
                    return Timestamps.floorDOW(DAY_FORMAT.parse(partitionName, 0, localLimit, EN_LOCALE));
                } catch (NumericException ignore) {
                    throw expectedPartitionDirNameFormatCairoException(partitionName, 0, Math.min(partitionName.length(), localLimit), partitionBy);
                }
            }
            throw expectedPartitionDirNameFormatCairoException(partitionName, lo, hi, partitionBy);
        }
    }

    @TestOnly
    public void setTicker(Clock clock) {
        this.clock = clock;
    }

    @Override
    public long toDate(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / 1000L;
    }

    @Override
    public long toHours(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Timestamps.HOUR_MICROS;
    }

    @Override
    public String toMSecString(long timestamp) {
        return Timestamps.toString(timestamp);
    }

    @Override
    public long toMicros(long timestamp) {
        return timestamp;
    }

    @Override
    public long toNanos(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? timestamp : timestamp * Timestamps.MICRO_NANOS;
    }

    @Override
    public long toNanosScale() {
        return Timestamps.MICRO_NANOS;
    }

    @Override
    public long toSeconds(long timestamp) {
        return timestamp == Numbers.LONG_NULL ? Numbers.LONG_NULL : timestamp / Timestamps.SECOND_MICROS;
    }

    @Override
    public long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Timestamps.toTimezone(utcTimestamp, locale, timezone);
    }

    @Override
    public String toUSecString(long micros) {
        return Timestamps.toUSecString(micros);
    }

    @Override
    public long toUTC(long localTimestamp, TimeZoneRules zoneRules) {
        return Timestamps.toUTC(localTimestamp, zoneRules);
    }

    @Override
    public long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException {
        return Timestamps.toUTC(localTimestamp, locale, timezone);
    }

    @Override
    public void validateBounds(long timestamp) {
        if (Long.compareUnsigned(timestamp, Timestamps.YEAR_10000) >= 0) {
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
                return tzSign * (hour * Timestamps.HOUR_MICROS + min * Timestamps.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Timestamps.HOUR_MICROS);
            }
        }
        throw NumericException.INSTANCE;
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
