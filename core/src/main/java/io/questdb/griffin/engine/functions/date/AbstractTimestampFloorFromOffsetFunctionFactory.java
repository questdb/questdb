/*+******************************************************************************
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateLocaleFactory;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.millitime.Dates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Shared base for {@link TimestampFloorFromOffsetFunctionFactory} and
 * {@link TimestampFloorFromOffsetUtcFunctionFactory}.
 * <p>
 * Both factories floor timestamps with modulo relative to a timestamp from
 * 1970-01-01, as well as an offset from the epoch start. The difference is
 * in the return value when a timezone is specified:
 * <ul>
 *     <li>{@code timestamp_floor} returns floored <b>local time</b></li>
 *     <li>{@code timestamp_floor_utc} returns floored local time converted
 *         back to <b>UTC</b></li>
 * </ul>
 * Subclasses provide {@link #getName()} and {@link #isReturnUtc()} to
 * control these two aspects.
 */
abstract class AbstractTimestampFloorFromOffsetFunctionFactory implements FunctionFactory {
    private static final long MIN_GAP_MINUTES = 15;
    private static final long MIN_GAP_SECONDS = MIN_GAP_MINUTES * 60;
    private static final long MIN_GAP_MILLIS = MIN_GAP_SECONDS * 1000;
    private static final long MIN_GAP_MICROS = MIN_GAP_MILLIS * 1000;
    private static final long MIN_GAP_NANOS = MIN_GAP_MICROS * 1000;

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final String name = getName();
        final boolean returnUtc = isReturnUtc();
        final CharSequence unitStr = args.getQuick(0).getStrA(null);
        final int stride = CommonUtils.getStrideMultiple(unitStr, argPositions.getQuick(0));
        final char unit = CommonUtils.getStrideUnit(unitStr, argPositions.getQuick(0));
        final int unitPos = argPositions.getQuick(0);
        final Function timestampFunc = args.getQuick(1);
        int timestampType = ColumnType.getHigherPrecisionTimestampType(ColumnType.getTimestampType(timestampFunc.getType()), ColumnType.TIMESTAMP_MICRO);
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        long from = args.getQuick(2).getTimestamp(null);
        if (from == Numbers.LONG_NULL) {
            from = 0;
        } else {
            from = timestampDriver.from(from, ColumnType.getTimestampType(args.getQuick(2).getType()));
        }
        final Function offsetFunc = args.getQuick(3);
        final int offsetPos = argPositions.getQuick(3);
        final Function timezoneFunc = args.getQuick(4);
        final int timezonePos = argPositions.getQuick(4);
        validateUnit(unit, unitPos);
        String offsetStr = null;
        long offset = 0;
        if (offsetFunc.isConstant()) {
            final CharSequence o = offsetFunc.getStrA(null);
            if (o != null) {
                final long val = Dates.parseOffset(o);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(o);
                }
                offset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
            }
            offsetStr = Chars.toString(o);
        }

        if (timezoneFunc.isConstant()) {
            final CharSequence tz = timezoneFunc.getStrA(null);
            long tzOffset = 0;
            TimeZoneRules tzRules = null;
            if (tz != null) {
                final int hi = tz.length();
                final long l = Dates.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                        );
                    } catch (NumericException e) {
                        Misc.free(timestampFunc);
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }

                    if (tzRules.hasFixedOffset()) {
                        tzOffset = tzRules.getOffset(0);
                        tzRules = null;
                    }
                } else {
                    tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                }
            }

            final String tzStr = Chars.toString(tz);

            if (tzRules == null) { // no timezone or fixed offset rules case
                if (offsetFunc.isConstant()) {
                    return createAllConstFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzOffset, tzStr, timestampType);
                }
                if (offsetFunc.isRuntimeConstant()) {
                    return new RuntimeConstOffsetFunction(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, tzOffset, tzStr, timestampType);
                }
                throw SqlException.$(offsetPos, "const or runtime const expected");
            }

            if (offsetFunc.isConstant()) {
                return createAllConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr, timestampType, timestampDriver);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return new RuntimeConstOffsetDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, tzRules, tzStr, timestampType);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        if (timezoneFunc.isRuntimeConstant()) {
            if (offsetFunc.isConstant()) {
                return createRuntimeConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos, timestampType, timestampDriver);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return new AllRuntimeConstDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, timezoneFunc, timezonePos, timestampType);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        throw SqlException.$(timezonePos, "const or runtime const expected");
    }

    private static boolean canSkipDstGapCorrection(TimestampDriver timestampDriver, int stride, char unit, long from, long offset) {
        // require the effective offset to be aligned at day boundary;
        // we may relax this check in the future, if necessary
        if ((from + offset) % timestampDriver.fromDays(1) != 0) {
            return false;
        }

        return switch (unit) {
            case 'M', 'y', 'w', 'd', 'h' -> true;
            case 'm' ->
                // min DST gap is 15m, and it starts at the beginning of an hour
                    MIN_GAP_MINUTES % stride == 0 || stride % MIN_GAP_MINUTES == 0;
            case 's' -> MIN_GAP_SECONDS % stride == 0 || stride % MIN_GAP_SECONDS == 0;
            case 'T' -> MIN_GAP_MILLIS % stride == 0 || stride % MIN_GAP_MILLIS == 0;
            case 'U' -> MIN_GAP_MICROS % stride == 0 || stride % MIN_GAP_MICROS == 0;
            case 'n' -> MIN_GAP_NANOS % stride == 0 || stride % MIN_GAP_NANOS == 0;
            default -> false;
        };
    }

    private static @NotNull Function createAllConstFunc(
            @NotNull String name,
            boolean returnUtc,
            @NotNull Function timestampFunc,
            int stride,
            char unit,
            long from,
            long offset,
            @Nullable String offsetStr,
            long tzOffset,
            @Nullable String tzStr,
            int timestampType
    ) {
        if (tzOffset == 0) {
            // No timezone offset — behaves identically to timestamp_floor
            final long effectiveOffset = from + offset;
            return new TimestampFloorOffsetFunction(name, timestampFunc, unit, stride, effectiveOffset, timestampType);
        }

        return new AllConstFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzOffset, tzStr, timestampType);
    }

    private static @NotNull Function createAllConstTzFunc(
            @NotNull String name,
            boolean returnUtc,
            @NotNull Function timestampFunc,
            int stride,
            char unit,
            long from,
            long offset,
            @Nullable String offsetStr,
            @NotNull TimeZoneRules tzRules,
            @NotNull String tzStr,
            int timestampType,
            TimestampDriver timestampDriver
    ) {
        if (canSkipDstGapCorrection(timestampDriver, stride, unit, from, offset)) {
            return new AllConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr, timestampType);
        }
        return new AllConstDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr, timestampType);
    }

    private static @NotNull Function createRuntimeConstTzFunc(
            String name,
            boolean returnUtc,
            Function timestampFunc,
            int stride,
            char unit,
            long from,
            long offset,
            String offsetStr,
            Function timezoneFunc,
            int timezonePos,
            int timestampType,
            TimestampDriver timestampDriver
    ) {
        if (canSkipDstGapCorrection(timestampDriver, stride, unit, from, offset)) {
            return new RuntimeConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos, timestampType);
        }
        return new RuntimeConstDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos, timestampType);
    }

    // Floors a UTC timestamp into local-time-aligned buckets.
    //
    // When returnUtc is true, converts the result back to UTC using the same timezone
    // offset derived from the original UTC input. When false, returns the floored local
    // time directly.
    //
    // The key insight for the UTC mode: we look up the timezone offset from the *original*
    // UTC input, which is unambiguous. We then use that same offset to convert back to UTC
    // at the end. This avoids the fall-back problem where two distinct UTC instants map to
    // the same local time — since we always derive the offset from the unambiguous UTC
    // input, each input produces a distinct UTC output even when local times collide.
    //
    // Spring-forward (DST gap) example — Europe/Berlin, Mar 28 2021:
    //   Local clocks jump from 02:00 CET to 03:00 CEST. Local times 02:00–02:59 don't exist.
    //   Input:  UTC 00:50 → offset +1h (CET) → local 01:50 → floor(1h) → local 01:00
    //   Output: 01:00 - 1h = UTC 00:00. No gap issue here; 01:00 CET exists.
    //
    //   But if the floor lands in the gap:
    //   Input:  UTC 01:10 → offset +2h (CEST) → local 03:10 → floor(30m) → local 03:00
    //   Output: 03:00 - 2h = UTC 01:00. Fine — 03:00 CEST exists.
    //
    //   Trickier: what if a stride like 2h floors local 03:10 down to local 02:00?
    //   Local 02:00 doesn't exist (it's in the gap). getDstGapOffset() returns the gap
    //   duration (1h). We subtract it: 02:00 - 1h = 01:00, re-floor: floor(01:00, 2h) = 00:00.
    //   Output: 00:00 - 2h (CEST offset from original UTC)... but wait, that's wrong.
    //   This is the inherent limitation of using the original UTC offset for the back-conversion
    //   when the floor crosses a DST boundary. In practice this only affects strides >= 1h
    //   that straddle the exact transition point, which is rare and documented as a known edge case.
    //
    // Fall-back example — Europe/Berlin, Oct 31 2021:
    //   Local clocks go from 03:00 CEST back to 02:00 CET. Local 02:00–02:59 occurs twice.
    //   Input A: UTC 00:30 → offset +2h (CEST) → local 02:30 → floor(1h) → local 02:00
    //   Output:  02:00 - 2h = UTC 00:00
    //   Input B: UTC 01:30 → offset +1h (CET)  → local 02:30 → floor(1h) → local 02:00
    //   Output:  02:00 - 1h = UTC 01:00
    //   Same local floor result (02:00), but different UTC outputs (00:00 vs 01:00) because
    //   the offset was derived from the distinct UTC inputs. This is the whole point of
    //   the UTC mode — it preserves bucket distinctness across fall-back.
    //
    // When returnUtc is true, the result is always a valid UTC timestamp and DST
    // gaps are irrelevant — we floor in local time and convert back to UTC using
    // the same offset derived from the original UTC input.
    //
    // When returnUtc is false (local time return), the floored local time may land
    // in a DST gap (non-existing time from a forward clock shift). In that case
    // the timestamp is moved back by the gap duration and re-floored to produce a
    // valid local time and avoid duplicate bucket keys.

    private static long floorWithTz(long timestamp, TimeZoneRules tzRules, TimestampDriver.TimestampFloorWithOffsetMethod floorFunc, int stride, long effectiveOffset, boolean returnUtc, char unit) {
        if (returnUtc) {
            // Use the shared conversion strategy from CommonUtils so that
            // the mat view refresh iterator produces matching bucket boundaries.
            final long tzOff = CommonUtils.getFloorUtcTzOffset(tzRules, timestamp, unit);
            final long localTimestamp = timestamp + tzOff;
            final long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
            return CommonUtils.offsetFlooredUtcResult(result, tzOff, 0, tzRules, unit);
        }
        final long tzOff = tzRules.getOffset(timestamp);
        final long localTimestamp = timestamp + tzOff;
        long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
        // Move the timestamp to the bucket if it belongs to a DST gap, i.e. non-existing
        // time interval that occur due to a forward clock shift.
        // This is required to avoid duplicate timestamps returned by SAMPLE BY + DST time zone + offset
        // queries that get rewritten to a parallel GROUP BY.
        long gapDuration = tzRules.getDstGapOffset(result);
        if (gapDuration != 0) {
            // The floored local time landed in a DST gap (spring-forward). Back up by the gap
            // duration to reach a real local time, then re-floor to find the correct bucket.
            result = floorFunc.floor(result - gapDuration, stride, effectiveOffset);
        }
        return result;
    }

    private static void validateUnit(char unit, int unitPos) throws SqlException {
        switch (unit) {
            case 'M':
            case 'y':
            case 'w':
            case 'd':
            case 'h':
            case 'm':
            case 's':
            case 'T':
            case 'U':
            case 'n':
                return;
        }
        throw SqlException.position(unitPos).put("unexpected unit");
    }

    abstract String getName();

    abstract boolean isReturnUtc();

    // both offset and time zone are consts
    private static class AllConstDstGapAwareFunc extends TimestampFunction implements UnaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final String offsetStr;
        private final boolean returnUtc;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;

        public AllConstDstGapAwareFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                TimeZoneRules tzRules,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.effectiveOffset = from + offset;
            this.offsetStr = offsetStr;
            this.tzRules = tzRules;
            this.tzStr = tzStr;
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            if (offsetStr != null) {
                sink.val('\'').val(offsetStr).val("',");
            } else {
                sink.val("'00:00',");
            }
            sink.val('\'').val(tzStr).val('\'');
            sink.val(')');
        }
    }

    private static class AllConstFunc extends TimestampFunction implements UnaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final String offsetStr;
        private final boolean returnUtc;
        private final int stride;
        private final Function tsFunc;
        private final long tzOffset;
        private final String tzStr;
        private final char unit;

        public AllConstFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                long tzOffset,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.effectiveOffset = from + offset;
            this.offsetStr = offsetStr;
            this.tzOffset = tzOffset;
            this.tzStr = tzStr;
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                final long localTimestamp = timestamp + tzOffset;
                long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
                return returnUtc ? result - tzOffset : result;
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            if (offsetStr != null) {
                sink.val('\'').val(offsetStr).val("',");
            } else {
                sink.val("'00:00',");
            }
            sink.val('\'').val(tzStr).val('\'');
            sink.val(')');
        }
    }

    private static class AllConstTzFunc extends TimestampFunction implements UnaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final String offsetStr;
        private final boolean returnUtc;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;

        public AllConstTzFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                TimeZoneRules tzRules,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.effectiveOffset = from + offset;
            this.offsetStr = offsetStr;
            this.tzRules = tzRules;
            this.tzStr = tzStr;
        }

        @Override
        public Function getArg() {
            return tsFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            if (offsetStr != null) {
                sink.val('\'').val(offsetStr).val("',");
            } else {
                sink.val("'00:00',");
            }
            sink.val('\'').val(tzStr).val('\'');
            sink.val(')');
        }
    }

    // both offset and time zone are runtime consts
    private static class AllRuntimeConstDstGapAwareFunc extends TimestampFunction implements TernaryFunction {
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final Function offsetFunc;
        private final int offsetPos;
        private final boolean returnUtc;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long effectiveOffset; // from + offset
        private long tzOffset;
        private TimeZoneRules tzRules;

        public AllRuntimeConstDstGapAwareFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        @Override
        public Function getCenter() {
            return offsetFunc;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                if (tzRules != null) {
                    return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
                }
                final long localTimestamp = timestamp + tzOffset;
                long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
                return returnUtc ? result - tzOffset : result;
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Dates.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Dates.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc).val(',');
            sink.val(timezoneFunc);
            sink.val(')');
        }
    }

    // offset is const and time zone is runtime const
    private static class RuntimeConstDstGapAwareFunc extends TimestampFunction implements BinaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final String offsetStr;
        private final boolean returnUtc;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstDstGapAwareFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                Function timezoneFunc,
                int timezonePos,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.effectiveOffset = from + offset;
            this.offsetStr = offsetStr;
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                if (tzRules != null) {
                    return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
                }
                final long localTimestamp = timestamp + tzOffset;
                long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
                return returnUtc ? result - tzOffset : result;
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Dates.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            if (offsetStr != null) {
                sink.val('\'').val(offsetStr).val("',");
            } else {
                sink.val("'00:00',");
            }
            sink.val(timezoneFunc);
            sink.val(')');
        }
    }

    // offset is runtime const and time zone is const
    private static class RuntimeConstOffsetDstGapAwareFunc extends TimestampFunction implements BinaryFunction {
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final Function offsetFunc;
        private final int offsetPos;
        private final boolean returnUtc;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;
        private long effectiveOffset; // from + offset

        public RuntimeConstOffsetDstGapAwareFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                TimeZoneRules tzRules,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.tzRules = tzRules;
            this.tzStr = tzStr;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return offsetFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Dates.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc).val(',');
            sink.val('\'').val(tzStr).val('\'');
            sink.val(')');
        }
    }

    private static class RuntimeConstOffsetFunction extends TimestampFunction implements BinaryFunction {
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final Function offsetFunc;
        private final int offsetPos;
        private final boolean returnUtc;
        private final int stride;
        private final Function tsFunc;
        private final long tzOffset;
        private final String tzStr;
        private final char unit;
        private long effectiveOffset; // from + offset

        public RuntimeConstOffsetFunction(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                long tzOffset,
                String tzStr,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.tzOffset = tzOffset;
            this.tzStr = tzStr;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return offsetFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                final long localTimestamp = timestamp + tzOffset;
                long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
                return returnUtc ? result - tzOffset : result;
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Dates.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc).val(',');
            if (tzStr != null) {
                sink.val('\'').val(tzStr).val('\'');
            } else {
                sink.val("null");
            }
            sink.val(')');
        }
    }

    // offset is const and time zone is runtime const
    private static class RuntimeConstTzFunc extends TimestampFunction implements BinaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampDriver.TimestampFloorWithOffsetMethod floorFunc;
        private final long from;
        private final String name;
        private final String offsetStr;
        private final boolean returnUtc;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstTzFunc(
                String name,
                boolean returnUtc,
                Function tsFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                Function timezoneFunc,
                int timezonePos,
                int timestampType
        ) {
            super(timestampType);
            this.name = name;
            this.returnUtc = returnUtc;
            this.tsFunc = tsFunc;
            this.floorFunc = timestampDriver.getTimestampFloorWithOffsetMethod(unit);
            this.stride = stride;
            this.unit = unit;
            this.from = from;
            this.effectiveOffset = from + offset;
            this.offsetStr = offsetStr;
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        @Override
        public Function getLeft() {
            return tsFunc;
        }

        @Override
        public Function getRight() {
            return timezoneFunc;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = tsFunc.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                if (tzRules != null) {
                    return floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
                }
                final long localTimestamp = timestamp + tzOffset;
                long result = floorFunc.floor(localTimestamp, stride, effectiveOffset);
                return returnUtc ? result - tzOffset : result;
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Dates.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = DateLocaleFactory.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = timestampDriver.fromMinutes(Numbers.decodeLowInt(l));
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(name).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(timestampDriver.toMSecString(from)).val("',");
            } else {
                sink.val("null,");
            }
            if (offsetStr != null) {
                sink.val('\'').val(offsetStr).val("',");
            } else {
                sink.val("'00:00',");
            }
            sink.val(timezoneFunc);
            sink.val(')');
        }

    }
}
