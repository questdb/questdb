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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.constants.IntervalConstant;
import io.questdb.griffin.engine.functions.constants.TimestampConstant;
import io.questdb.griffin.engine.groupby.TimestampSampler;
import io.questdb.std.Interval;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRuleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.cairo.PartitionBy.*;
import static io.questdb.griffin.SqlUtil.castPGDates;

public interface TimestampDriver {
    static CairoException expectedPartitionDirNameFormatCairoException(CharSequence partitionName, int lo, int hi, int partitionBy) {
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

    /**
     * Adds a time period to a timestamp based on the specified type and stride.
     *
     * @param timestamp the base timestamp value
     * @param type      the time unit type ('s'=seconds, 'm'=minutes, 'h'=hours, 'd'=days, etc.)
     * @param stride    the number of units to add
     * @return the new timestamp after adding the specified period
     */
    long add(long timestamp, char type, int stride);

    long addDays(long timestamp, int days);

    long addMonths(long timestamp, int months);

    long addWeeks(long timestamp, int weeks);

    long addYears(long timestamp, int years);

    void append(CharSink<?> sink, long timestamp);

    /**
     * Reads a timestamp from a fixed memory address and appends its string representation to the sink.
     *
     * @param fixedAddr  the fixed memory address containing the timestamp data
     * @param stringSink the character sink to append the formatted timestamp to
     * @return true if the operation was successful, false otherwise
     */
    boolean append(long fixedAddr, CharSink<?> stringSink);

    default void appendToMem(CharSequence value, MemoryA mem) {
        try {
            mem.putLong(parseFloorLiteral(value));
        } catch (NumericException e) {
            mem.putLong(Numbers.LONG_NULL);
        }
    }

    void appendToPGWireText(CharSink<?> sink, long timestamp);

    void appendTypeToPlan(PlanSink sink);

    // returns approximate partition duration in driver unit (nanos/micros)
    long approxPartitionDuration(int partitionBy);

    long ceilYYYY(long timestamp);

    long endOfDay(long start);

    /**
     * Adjusts an interval to be consistency with the driver's timestampType.
     *
     * @param interval     the interval to fix/adjust
     * @param intervalType the type of interval being processed
     * @return the adjusted interval
     */
    Interval fixInterval(Interval interval, int intervalType);

    long floorYYYY(long timestamp);

    /**
     * Converts a value from the specified {@link ChronoUnit} to the timestamp value.
     *
     * @param value the time value to convert
     * @param unit  the {@link ChronoUnit} of the input value
     * @return the timestamp value
     */
    long from(long value, ChronoUnit unit);

    /**
     * Converts a {@link Instant} to the timestamp value.
     *
     * @param instant the {@link Instant} to convert
     * @return the timestamp value
     */
    long from(Instant instant);

    /**
     * Converts a timestamp from one type to the driver's timestamp value.
     *
     * @param timestamp     the source timestamp value
     * @param timestampType the type of the source timestamp
     * @return the timestamp value
     */
    long from(long timestamp, int timestampType);

    /**
     * Converts a time value to the driver's timestamp value based on the unit character.
     *
     * @param value the time value to convert
     * @param unit  the time unit character ('n'=nanos, 'u'/'U'=micros, 'T'=millis, 's'=seconds, 'm'=minutes, 'h'/'H'=hours, 'd'=days, 'w'=weeks)
     * @return the timestamp in the driver's native value, or 0 if unit is not recognized
     */
    default long from(long value, char unit) {
        switch (unit) {
            case 'n':
                return fromNanos(value);
            case 'u':
            case 'U':
                return fromMicros(value);
            case 'T':
                return fromMillis(value);
            case 's':
                return fromSeconds(value);
            case 'm':
                return fromMinutes((int) value);
            case 'H':
            case 'h':
                return fromHours((int) value);
            case 'd':
                return fromDays((int) value);
            case 'w':
                return fromWeeks((int) value);
        }
        return 0;
    }

    /**
     * Converts a timestamp to the driver's format based on the unit byte constant.
     *
     * @param ts   the timestamp value to convert
     * @param unit the time unit byte constant from CommonUtils.TIMESTAMP_UNIT_*
     * @return the timestamp value
     */
    default long from(long ts, byte unit) {
        switch (unit) {
            case CommonUtils.TIMESTAMP_UNIT_NANOS:
                return fromNanos(ts);
            case CommonUtils.TIMESTAMP_UNIT_MICROS:
                return fromMicros(ts);
            case CommonUtils.TIMESTAMP_UNIT_MILLIS:
                return fromMillis(ts);
            case CommonUtils.TIMESTAMP_UNIT_SECONDS:
                return fromSeconds(ts);
            case CommonUtils.TIMESTAMP_UNIT_MINUTES:
                return fromMinutes((int) ts);
            case CommonUtils.TIMESTAMP_UNIT_HOURS:
                return fromHours((int) ts);
            default:
                throw new UnsupportedOperationException();
        }
    }

    long fromDate(long timestamp);

    long fromDays(int days);

    long fromHours(int hours);

    long fromMicros(long micros);

    long fromMillis(long millis);

    long fromMinutes(int minutes);

    long fromNanos(long nanos);

    long fromSeconds(long seconds);

    default long fromStr(CharSequence value, int tupleIndex, int fromType, int toType) {
        try {
            return parseFloorLiteral(value);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(tupleIndex, value, fromType, toType);
        }
    }

    // used by the row copier
    @SuppressWarnings("unused")
    long fromWeeks(int weeks);

    TimestampAddMethod getAddMethod(char c);

    int getCentury(long timestamp);

    ColumnTypeConverter.Var2FixedConverter<CharSequence> getConverterStr2Timestamp();

    ColumnTypeConverter.Fixed2VarConverter getConverterTimestamp2Str();

    /**
     * Extracts the day of month from a timestamp value.
     * This method handles the driver-specific timestamp format and precision.
     *
     * @param timestamp the timestamp value
     * @return the day of month (1-31), or Numbers.INT_NULL if timestamp is null
     */
    int getDayOfMonth(long timestamp);

    /**
     * Extracts the day of week from a timestamp value.
     * This method handles the driver-specific timestamp format and precision.
     *
     * @param timestamp the timestamp value
     * @return the day of week (1=Monday, 7=Sunday), or Numbers.INT_NULL if timestamp is null
     */
    int getDayOfWeek(long timestamp);

    /**
     * Extracts the day of week from a timestamp value with Sunday as the first day.
     * This method handles the driver-specific timestamp format and precision.
     *
     * @param timestamp the timestamp value
     * @return the day of week (1=Sunday, 7=Saturday), or Numbers.INT_NULL if timestamp is null
     */
    int getDayOfWeekSundayFirst(long timestamp);

    /**
     * Gets the number of days in the month for a timestamp value.
     * This method handles the driver-specific timestamp format and precision.
     *
     * @param timestamp the timestamp value
     * @return the number of days in the month (28-31), or Numbers.INT_NULL if timestamp is null
     */
    int getDaysPerMonth(long timestamp);

    /**
     * Gets the decade from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the decade, or Numbers.INT_NULL if timestamp is null
     */
    int getDecade(long timestamp);

    /**
     * Gets the day of week (Sunday-first) from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the day of week (0=Sunday, 6=Saturday), or Numbers.INT_NULL if timestamp is null
     */
    int getDow(long timestamp);

    /**
     * Gets the day of year from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the day of year (1-366), or Numbers.INT_NULL if timestamp is null
     */
    int getDoy(long timestamp);

    int getGKKHourInt();

    /**
     * Gets the hour of day from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the hour of day (0-23), or Numbers.INT_NULL if timestamp is null
     */
    int getHourOfDay(long timestamp);

    IntervalConstant getIntervalConstantNull();

    /**
     * Gets the ISO year from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the ISO year, or Numbers.INT_NULL if timestamp is null
     */
    int getIsoYear(long timestamp);

    /**
     * Gets the microseconds within the millisecond from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the microseconds within the millisecond (0-999), or Numbers.INT_NULL if timestamp is null
     */
    int getMicrosOfMilli(long timestamp);

    /**
     * Gets the microseconds within the second from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the microseconds within the second, or Numbers.INT_NULL if timestamp is null
     */
    int getMicrosOfSecond(long timestamp);

    /**
     * Gets the millennium from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the millennium, or Numbers.INT_NULL if timestamp is null
     */
    int getMillennium(long timestamp);

    /**
     * Gets the milliseconds within the second from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the milliseconds within the second (0-999), or Numbers.INT_NULL if timestamp is null
     */
    int getMillisOfSecond(long timestamp);

    /**
     * Gets the minute of hour from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the minute of hour (0-59), or Numbers.INT_NULL if timestamp is null
     */
    int getMinuteOfHour(long timestamp);

    /**
     * Gets the month of year from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the month of year (1-12), or Numbers.INT_NULL if timestamp is null
     */
    int getMonthOfYear(long timestamp);

    /**
     * Gets the nanoseconds within the microsecond from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the nanoseconds within the millisecond (0-999), or Numbers.INT_NULL if timestamp is null
     */
    int getNanosOfMicros(long timestamp);

    PartitionAddMethod getPartitionAddMethod(int partitionBy);

    TimestampCeilMethod getPartitionCeilMethod(int partitionBy);

    DateFormat getPartitionDirFormatMethod(int partitionBy);

    TimestampFloorMethod getPartitionFloorMethod(int partitionBy);

    /**
     * Calculates the period between two timestamps in the specified time unit.
     *
     * @param unit      the time unit
     * @param start     the start timestamp value
     * @param end       the end timestamp value
     * @param leftType  left column type
     * @param rightType right column type
     * @return the period between the timestamps in the specified unit
     */
    long getPeriodBetween(char unit, long start, long end, int leftType, int rightType);

    /**
     * Gets the quarter from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the quarter (1-4), or Numbers.INT_NULL if timestamp is null
     */
    int getQuarter(long timestamp);

    /**
     * Gets the second of minute from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the second of minute (0-59), or Numbers.INT_NULL if timestamp is null
     */
    int getSecondOfMinute(long timestamp);

    /**
     * Gets the time zone rule resolution {@link TimeZoneRuleFactory#RESOLUTION_MICROS} or {@link TimeZoneRuleFactory#RESOLUTION_NANOS}
     * for this timestamp driver.
     *
     * @return the time zone rule resolution
     */
    int getTZRuleResolution();

    /**
     * Gets the current timestamp ticks.
     *
     * @return the current timestamp value
     */
    long getTicks();

    TimestampCeilMethod getTimestampCeilMethod(char unit);

    TimestampConstant getTimestampConstantNull();

    TimestampDateFormatFactory getTimestampDateFormatFactory();

    TimestampDiffMethod getTimestampDiffMethod(char type);

    TimestampFloorMethod getTimestampFloorMethod(String unit);

    TimestampFloorWithOffsetMethod getTimestampFloorWithOffsetMethod(char unit);

    TimestampFloorWithStrideMethod getTimestampFloorWithStrideMethod(String unit);

    /**
     * Creates a timestamp sampler instance for the given interval and time unit.
     *
     * @param interval the interval value
     * @param timeUnit the time unit qualifier ('n', 'U', 'T', 's', 'm', 'h', 'd', 'w', 'M', 'y')
     * @return a timestamp sampler instance
     */
    TimestampSampler getTimestampSampler(long interval, char timeUnit, int position) throws SqlException;

    /**
     * Gets the timestamp column type identifier for this driver.
     *
     * @return the timestamp type constant (e.g., {@link ColumnType#TIMESTAMP_MICRO} or {@link ColumnType#TIMESTAMP_NANO} )
     */
    int getTimestampType();

    /**
     * Creates converted from a given type to the type supported by the driver implementation.
     *
     * @param srcTimestampType type of the timestamp to convert from.
     * @return either converter or null. When the value is null, the long should be taken verbatim. The driver decided
     * that it is either not able to convert or the type is the same as of this driver's.
     */
    CommonUtils.TimestampUnitConverter getTimestampUnitConverter(int srcTimestampType);

    TimeZoneRules getTimezoneRules(@NotNull DateLocale locale, @NotNull CharSequence timezone);

    /**
     * Gets the week of year from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the week of year, or Numbers.INT_NULL if timestamp is null
     */
    int getWeek(long timestamp);

    /**
     * Gets the year from a timestamp value.
     *
     * @param timestamp the timestamp value
     * @return the year, or Numbers.INT_NULL if timestamp is null
     */
    int getYear(long timestamp);

    /**
     * Performs implicit casting from a character sequence to timestamp.
     * Attempts multiple parsing strategies in order: numeric parsing, ISO format parsing, and PostgreSQL date formats.
     *
     * @param value    the character sequence to cast
     * @param fromType the source column type (must be STRING or SYMBOL)
     * @return the parsed timestamp value, or Numbers.LONG_NULL if parsing fails or value is null
     */
    default long implicitCast(CharSequence value, int fromType) {
        assert fromType == ColumnType.STRING || fromType == ColumnType.SYMBOL;
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

            return castPGDates(value, fromType, this);
        }
        return Numbers.LONG_NULL;
    }

    default long implicitCast(CharSequence value) {
        return implicitCast(value, ColumnType.STRING);
    }

    default long implicitCastVarchar(Utf8Sequence value) {
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
                return castPGDates(value.asAsciiCharSequence(), ColumnType.VARCHAR, this);
            }
            throw ImplicitCastException.inconvertibleValue(value, ColumnType.VARCHAR, getTimestampType());
        }
        return Numbers.LONG_NULL;
    }

    boolean inInterval(long value, int intervalType, Interval interval);

    long monthsBetween(long hi, long lo);

    long parseAnyFormat(CharSequence token, int start, int len) throws NumericException;

    long parseFloor(CharSequence str, int lo, int hi) throws NumericException;

    long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException;

    default long parseFloorLiteral(@Nullable CharSequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.length()) : Numbers.LONG_NULL;
    }

    default long parseFloorLiteral(@Nullable Utf8Sequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.size()) : Numbers.LONG_NULL;
    }

    void parseInterval(CharSequence input, int pos, int lim, short operation, LongList out) throws NumericException;

    long parsePartitionDirName(@NotNull CharSequence partitionName, int partitionBy, int lo, int hi);

    default long parseQuotedLiteral(@NotNull CharSequence quotedTimestampStr) throws NumericException {
        return parseFloor(quotedTimestampStr, 1, quotedTimestampStr.length() - 1);
    }

    long startOfDay(long now, int shiftDays);

    long toDate(long timestamp);

    long toHours(long timestamp);

    String toMSecString(long timestamp);

    long toMicros(long timestamp);

    long toNanos(long timestamp);

    long toNanosScale();

    long toSeconds(long timestamp);

    long toTimezone(long utcTimestamp, DateLocale locale, CharSequence timezone) throws NumericException;

    String toUSecString(long timestamp);

    long toUTC(long localTimestamp, TimeZoneRules zoneRules);

    long toUTC(long localTimestamp, DateLocale locale, CharSequence timezone) throws NumericException;

    void validateBounds(long timestamp);

    @FunctionalInterface
    interface PartitionAddMethod {
        long calculate(long timestamp, int increment);
    }

    @FunctionalInterface
    interface TimestampAddMethod {
        long add(long a, int b);
    }

    @FunctionalInterface
    interface TimestampCeilMethod {
        // returns exclusive ceiling for the give timestamp
        long ceil(long timestamp);
    }

    @FunctionalInterface
    interface TimestampDiffMethod {
        long diff(long a, long b);
    }

    @FunctionalInterface
    interface TimestampFloorMethod {
        long floor(long timestamp);
    }

    @FunctionalInterface
    interface TimestampFloorWithOffsetMethod {
        long floor(long micros, int stride, long offset);
    }

    @FunctionalInterface
    interface TimestampFloorWithStrideMethod {
        long floor(long micros, int stride);
    }
}
