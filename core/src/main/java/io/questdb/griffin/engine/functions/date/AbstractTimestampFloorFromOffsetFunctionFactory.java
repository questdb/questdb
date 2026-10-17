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
import io.questdb.griffin.ResultTypes;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.TernaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
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
abstract class AbstractTimestampFloorFromOffsetFunctionFactory implements FunctionFactory, MonotonicTimestampFunctionFactory {
    private static final long MIN_GAP_MINUTES = 15;
    private static final long MIN_GAP_SECONDS = MIN_GAP_MINUTES * 60;
    private static final long MIN_GAP_MILLIS = MIN_GAP_SECONDS * 1000;
    private static final long MIN_GAP_MICROS = MIN_GAP_MILLIS * 1000;
    private static final long MIN_GAP_NANOS = MIN_GAP_MICROS * 1000;

    @Override
    public int getResultType(IntList argTypes) {
        return ResultTypes.timestampAtLeastMicros(argTypes.getQuick(1));
    }

    @Override
    public int getTimestampArgumentIndex(FunctionExpression call, ConstantArguments arguments) {
        return call.argumentAt(3) instanceof ConstantExpression && call.argumentAt(4) instanceof ConstantExpression ? 1 : -1;
    }

    @Override
    public int invertTimestampInterval(FunctionExpression call, Interval io, boolean isTimestampArgMonotonic, ConstantArguments arguments) throws SqlException {
        final CharSequence unitStr = arguments.constant(call.argumentAt(0)).getStrA(null);
        final int unitPos = call.argumentAt(0).getPosition();
        final int stride = CommonUtils.getStrideMultiple(unitStr, unitPos);
        final char unit = CommonUtils.getStrideUnit(unitStr, unitPos);
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(call.getDataType());
        final long effectiveOffset = TimestampFloorFromFunctionFactory.floorOrigin(timestampDriver, arguments.constant(call.argumentAt(2)).getTimestamp(null), call.argumentAt(2).getDataType())
                + constantOffset(arguments.constant(call.argumentAt(3)).getStrA(null), call.argumentAt(3).getPosition(), timestampDriver);
        final CharSequence tz = arguments.constant(call.argumentAt(4)).getStrA(null);
        final TimeZoneRules tzRules = constantZoneRules(tz, call.argumentAt(4).getPosition(), timestampDriver);
        final long tzOffset = constantZoneOffset(tz, tzRules, timestampDriver);
        if (tzRules != null && !tzRules.hasFixedOffset()) {
            return invertFloorNamedTz(io, timestampDriver, tzRules, timestampDriver.getTimestampFloorWithOffsetMethod(unit), unit, stride, effectiveOffset, isReturnUtc());
        }
        if (tzOffset == 0) {
            return TimestampFloorOffsetFunction.invert(io, timestampDriver, unit, stride, effectiveOffset);
        }
        return invertFloorFixedTz(io, timestampDriver, timestampDriver.getTimestampFloorWithOffsetMethod(unit), unit, stride, effectiveOffset, tzOffset, isReturnUtc());
    }

    @Override
    public boolean isConstructionDeferrable(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration) throws SqlException {
        final CharSequence unitStr = args.getQuick(0).getStrA(null);
        final int unitPos = argPositions.getQuick(0);
        CommonUtils.getStrideMultiple(unitStr, unitPos);
        final char unit = CommonUtils.getStrideUnit(unitStr, unitPos);
        args.getQuick(2).getTimestamp(null);
        validateUnit(unit, unitPos);
        final TimestampDriver timestampDriver = ColumnType.getTimestampDriver(ResultTypes.timestampAtLeastMicros(args.getQuick(1).getType()));
        final Function offsetFunc = args.getQuick(3);
        final int offsetPos = argPositions.getQuick(3);
        if (offsetFunc.isConstant()) {
            constantOffset(offsetFunc.getStrA(null), offsetPos, timestampDriver);
        }
        zoneRules(args.getQuick(4), argPositions.getQuick(4), timestampDriver);
        checkOffsetArgument(offsetFunc, offsetPos);
        return true;
    }

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
        final long from = TimestampFloorFromFunctionFactory.floorOrigin(timestampDriver, args.getQuick(2).getTimestamp(null), args.getQuick(2).getType());
        final Function offsetFunc = args.getQuick(3);
        final int offsetPos = argPositions.getQuick(3);
        final Function timezoneFunc = args.getQuick(4);
        final int timezonePos = argPositions.getQuick(4);
        validateUnit(unit, unitPos);
        String offsetStr = null;
        long offset = 0;
        if (offsetFunc.isConstant()) {
            final CharSequence o = offsetFunc.getStrA(null);
            offset = constantOffset(o, offsetPos, timestampDriver);
            offsetStr = Chars.toString(o);
        }
        TimeZoneRules tzRules = zoneRules(timezoneFunc, timezonePos, timestampDriver);
        checkOffsetArgument(offsetFunc, offsetPos);

        if (timezoneFunc.isConstant()) {
            final CharSequence tz = timezoneFunc.getStrA(null);
            final long tzOffset = constantZoneOffset(tz, tzRules, timestampDriver);
            if (tzRules != null && tzRules.hasFixedOffset()) {
                tzRules = null;
            }

            final String tzStr = Chars.toString(tz);

            if (tzRules == null) { // no timezone or fixed offset rules case
                if (offsetFunc.isConstant()) {
                    return createAllConstFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzOffset, tzStr, timestampType);
                }
                return new RuntimeConstOffsetFunction(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, tzOffset, tzStr, timestampType);
            }

            if (offsetFunc.isConstant()) {
                return createAllConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr, timestampType, timestampDriver);
            }
            return new RuntimeConstOffsetDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, tzRules, tzStr, timestampType);
        }

        if (offsetFunc.isConstant()) {
            return createRuntimeConstTzFunc(name, returnUtc, timestampFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos, timestampType, timestampDriver);
        }
        return new AllRuntimeConstDstGapAwareFunc(name, returnUtc, timestampFunc, stride, unit, from, offsetFunc, offsetPos, timezoneFunc, timezonePos, timestampType);
    }

    // Returns the width of the floor's buckets when add() steps from one bucket boundary to the
    // next one, zero otherwise. Calendar units yield zero. The bucket cache narrows the result
    // further, see TimestampFloorUtcBucketCachingFunction.
    static long computeFloorBucketWidth(
            TimestampDriver timestampDriver,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char unit,
            int stride,
            long offset
    ) {
        if (!CommonUtils.isFixedAlignedUnit(unit)) {
            return 0;
        }
        final long b0 = floorFunc.floor(offset, stride, offset);
        // add()/dateadd use the lowercase microsecond unit while ceil/floor use the uppercase one
        final long next = timestampDriver.add(b0, unit == 'U' ? 'u' : unit, stride);
        if (next > b0 && floorFunc.floor(next, stride, offset) == next && floorFunc.floor(next - 1, stride, offset) == b0) {
            return next - b0;
        }
        return 0;
    }

    static int invertFloorFixedTz(
            Interval io,
            TimestampDriver timestampDriver,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char unit,
            int stride,
            long effectiveOffset,
            long tzOffset,
            boolean returnUtc
    ) {
        if (!returnUtc
                && isFloorExactlyInvertible(timestampDriver, floorFunc, unit, stride, effectiveOffset)
                && floorPreimageShifted(io, timestampDriver, floorFunc, unit == 'U' ? 'u' : unit, stride, effectiveOffset, tzOffset)) {
            return MonotonicTimestampFunction.EXACT;
        }
        return invertFloorSuperset(io, timestampDriver, unit, stride, effectiveOffset);
    }

    // A named zone is a constant shift where no transition falls in the bound's window, so the
    // return-local floor inverts exactly there, otherwise it stays SUPERSET.
    static int invertFloorNamedTz(
            Interval io,
            TimestampDriver timestampDriver,
            TimeZoneRules tzRules,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char unit,
            int stride,
            long effectiveOffset,
            boolean returnUtc
    ) {
        if (!returnUtc
                && effectiveOffset == 0
                && isFloorExactlyInvertible(timestampDriver, floorFunc, unit, stride, effectiveOffset)
                && tryFloorNamedTzExact(io, timestampDriver, tzRules, floorFunc, unit, stride)) {
            return MonotonicTimestampFunction.EXACT;
        }
        return invertFloorSuperset(io, timestampDriver, unit, stride, effectiveOffset);
    }

    // 48h bounds any zone offset difference (e.g. Samoa's 2011 date-line shift); the
    // floor can drop the input by up to two buckets (DST-gap re-flooring).
    static int invertFloorSuperset(Interval io, TimestampDriver timestampDriver, char unit, int stride, long effectiveOffset) {
        if (!CommonUtils.isFixedAlignedUnit(unit)) {
            return MonotonicTimestampFunction.NONE;
        }
        final long margin = timestampDriver.fromDays(2);
        // add()/dateadd use the lowercase microsecond unit while ceil/floor use the uppercase one
        final long bucket = timestampDriver.add(0, unit == 'U' ? 'u' : unit, stride);
        if (bucket <= 0) {
            return MonotonicTimestampFunction.NONE;
        }
        long lo = io.getLo();
        long hi = io.getHi();
        if (lo != Numbers.LONG_NULL) {
            if (lo < Long.MIN_VALUE + margin) {
                lo = Numbers.LONG_NULL;
            } else {
                final long loWidened = lo - margin;
                // below the origin every timestamp floors to the origin
                lo = effectiveOffset != 0 && loWidened <= effectiveOffset ? Numbers.LONG_NULL : loWidened;
            }
        }
        if (hi != Long.MAX_VALUE) {
            if (bucket > (Long.MAX_VALUE - margin) / 2) {
                hi = Long.MAX_VALUE;
            } else {
                final long widen = 2 * bucket + margin;
                hi = hi > Long.MAX_VALUE - widen ? Long.MAX_VALUE : hi + widen;
            }
        }
        io.of(lo, hi);
        return MonotonicTimestampFunction.SUPERSET;
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

    /**
     * Raises the error for an offset argument that is neither constant nor runtime constant.
     */
    private static void checkOffsetArgument(Function offsetFunc, int offsetPos) throws SqlException {
        if (!offsetFunc.isConstant() && !offsetFunc.isRuntimeConstant()) {
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }
    }

    /**
     * The offset a constant offset argument spells, 0 for NULL; raises the error for an invalid offset.
     */
    private static long constantOffset(@Nullable CharSequence o, int offsetPos, TimestampDriver timestampDriver) throws SqlException {
        if (o == null) {
            return 0;
        }
        final long val = Dates.parseOffset(o);
        if (val == Numbers.LONG_NULL) {
            // bad value for offset
            throw SqlException.$(offsetPos, "invalid offset: ").put(o);
        }
        return timestampDriver.fromMinutes(Numbers.decodeLowInt(val));
    }

    private static long constantZoneOffset(@Nullable CharSequence tz, @Nullable TimeZoneRules tzRules, TimestampDriver timestampDriver) {
        if (tzRules == null) {
            return tz == null ? 0 : timestampDriver.fromMinutes(Numbers.decodeLowInt(Dates.parseOffset(tz, 0, tz.length())));
        }
        return tzRules.hasFixedOffset() ? tzRules.getOffset(0) : 0;
    }

    private static @Nullable TimeZoneRules constantZoneRules(@Nullable CharSequence tz, int timezonePos, TimestampDriver timestampDriver) throws SqlException {
        if (tz == null) {
            return null;
        }
        final int hi = tz.length();
        if (Dates.parseOffset(tz, 0, hi) != Long.MIN_VALUE) {
            return null;
        }
        try {
            return DateLocaleFactory.EN_LOCALE.getZoneRules(
                    Numbers.decodeLowInt(DateLocaleFactory.EN_LOCALE.matchZone(tz, 0, hi)), timestampDriver.getTZRuleResolution()
            );
        } catch (NumericException e) {
            throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
        }
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

    private static boolean floorPreimageShifted(
            Interval io,
            TimestampDriver timestampDriver,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char addUnit,
            int stride,
            long offset,
            long shift
    ) {
        long lo = io.getLo();
        long hi = io.getHi();
        if (offset != 0) {
            if (hi < offset) {
                io.of(Long.MAX_VALUE, Numbers.LONG_NULL);
                return true;
            }
            if (lo != Numbers.LONG_NULL && lo <= offset) {
                lo = Numbers.LONG_NULL;
            }
        }
        if (lo != Numbers.LONG_NULL) {
            final long b = floorFunc.floor(lo, stride, offset);
            long bound = lo;
            if (b != lo) {
                bound = timestampDriver.add(b, addUnit, stride);
                if (bound < lo) {
                    return false;
                }
            }
            if ((shift > 0 && bound < Long.MIN_VALUE + shift) || (shift < 0 && bound > Long.MAX_VALUE + shift)) {
                return false;
            }
            lo = bound - shift;
        }
        if (hi != Long.MAX_VALUE) {
            final long c = timestampDriver.add(floorFunc.floor(hi, stride, offset), addUnit, stride);
            if (c <= hi) {
                return false;
            }
            final long bound = c - 1;
            if ((shift > 0 && bound < Long.MIN_VALUE + shift) || (shift < 0 && bound > Long.MAX_VALUE + shift)) {
                return false;
            }
            hi = bound - shift;
        }
        io.of(lo, hi);
        return true;
    }

    // Floors a UTC timestamp on the local grid of a named time zone. The UTC result follows
    // CommonUtils.floorUtcToLocal/floorLocalToUtc. The local result of a day-or-longer stride is
    // the grid line of that bucket; a sub-day stride floors the wall-clock time and moves a line
    // that falls in a spring-forward gap back to the previous line.
    private static long floorWithTz(long timestamp, TimeZoneRules tzRules, TimestampDriver.TimestampFloorWithOffsetMethod floorFunc, int stride, long effectiveOffset, boolean returnUtc, char unit) {
        if (returnUtc) {
            final long local = CommonUtils.floorUtcToLocal(tzRules, timestamp, unit);
            return CommonUtils.floorLocalToUtc(tzRules, floorFunc.floor(local, stride, effectiveOffset), unit);
        }
        if (!CommonUtils.isSubDayUnit(unit)) {
            return floorFunc.floor(CommonUtils.floorUtcToLocal(tzRules, timestamp, unit), stride, effectiveOffset);
        }
        final long result = floorFunc.floor(timestamp + tzRules.getOffset(timestamp), stride, effectiveOffset);
        final long gapDuration = tzRules.getDstGapOffset(result);
        return gapDuration != 0 ? floorFunc.floor(result - gapDuration, stride, effectiveOffset) : result;
    }

    private static boolean isFloorExactlyInvertible(
            TimestampDriver timestampDriver,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char unit,
            int stride,
            long offset
    ) {
        // The helper returns next - b0 only when next > b0, so a non-zero width holds exactly
        // when add() reproduces the floor's bucket boundaries.
        return computeFloorBucketWidth(timestampDriver, floorFunc, unit, stride, offset) != 0;
    }

    private static boolean tryFloorNamedTzExact(
            Interval io,
            TimestampDriver timestampDriver,
            TimeZoneRules tzRules,
            TimestampDriver.TimestampFloorWithOffsetMethod floorFunc,
            char unit,
            int stride
    ) {
        final long loBound = io.getLo();
        final long hiBound = io.getHi();
        final boolean isLoFinite = loBound != Numbers.LONG_NULL;
        final boolean isHiFinite = hiBound != Long.MAX_VALUE;
        if (!isLoFinite && !isHiFinite) {
            return false;
        }
        final long c = tzRules.getOffset(isLoFinite ? loBound : hiBound);
        if (!floorPreimageShifted(io, timestampDriver, floorFunc, unit == 'U' ? 'u' : unit, stride, 0, c)) {
            return false;
        }
        final long tsLo = io.getLo();
        final long tsHi = io.getHi();
        if (tsLo > tsHi) {
            return true;
        }
        final long margin = timestampDriver.fromHours(24);
        long spanLo = Long.MAX_VALUE;
        long spanHi = Long.MIN_VALUE;
        if (isLoFinite) {
            spanLo = loBound;
            spanHi = loBound;
        }
        if (isHiFinite) {
            spanLo = Math.min(spanLo, hiBound);
            spanHi = Math.max(spanHi, hiBound);
        }
        if (tsLo != Numbers.LONG_NULL) {
            spanLo = Math.min(spanLo, tsLo);
            spanHi = Math.max(spanHi, tsLo);
        }
        if (tsHi != Long.MAX_VALUE) {
            spanLo = Math.min(spanLo, tsHi);
            spanHi = Math.max(spanHi, tsHi);
        }
        if (spanLo < Long.MIN_VALUE + margin || spanHi > Long.MAX_VALUE - margin || tzRules.getNextDST(spanLo - margin) <= spanHi + margin) {
            io.of(loBound, hiBound);
            return false;
        }
        return true;
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

    /**
     * The rules of the named time zone a constant time zone argument spells, null for a NULL or fixed-offset
     * spelling and for a runtime constant; raises the errors for an unknown zone and for an argument that is
     * neither constant nor runtime constant.
     */
    private static @Nullable TimeZoneRules zoneRules(Function timezoneFunc, int timezonePos, TimestampDriver timestampDriver) throws SqlException {
        if (timezoneFunc.isConstant()) {
            return constantZoneRules(timezoneFunc.getStrA(null), timezonePos, timestampDriver);
        }
        if (timezoneFunc.isRuntimeConstant()) {
            return null;
        }
        throw SqlException.$(timezonePos, "const or runtime const expected");
    }

    abstract String getName();

    abstract boolean isReturnUtc();

    // both offset and time zone are consts
    private static class AllConstDstGapAwareFunc extends TimestampFloorUtcBucketCachingFunction {
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
            // the return-local mode re-floors timestamps that land in a DST gap, so it stays uncached
            super(timestampType, unit, stride, from + offset, returnUtc);
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
            if (timestamp >= cachedLo && timestamp < cachedHi) {
                return cachedResult;
            }
            if (timestamp != Numbers.LONG_NULL) {
                return bucketWidth > 0
                        ? floorUtcAndCache(timestamp, tzRules, effectiveOffset, unit)
                        : floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public Function getTimestampArg() {
            return tsFunc;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            return invertFloorNamedTz(io, timestampDriver, tzRules, floorFunc, unit, stride, effectiveOffset, returnUtc);
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

    private static class AllConstFunc extends TimestampFunction implements UnaryFunction, MonotonicTimestampFunction {
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
        public Function getTimestampArg() {
            return tsFunc;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            return invertFloorFixedTz(io, timestampDriver, floorFunc, unit, stride, effectiveOffset, tzOffset, returnUtc);
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

    private static class AllConstTzFunc extends TimestampFloorUtcBucketCachingFunction {
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
            // the return-local mode re-floors timestamps that land in a DST gap, so it stays uncached
            super(timestampType, unit, stride, from + offset, returnUtc);
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
            if (timestamp >= cachedLo && timestamp < cachedHi) {
                return cachedResult;
            }
            if (timestamp != Numbers.LONG_NULL) {
                return bucketWidth > 0
                        ? floorUtcAndCache(timestamp, tzRules, effectiveOffset, unit)
                        : floorWithTz(timestamp, tzRules, floorFunc, stride, effectiveOffset, returnUtc, unit);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public Function getTimestampArg() {
            return tsFunc;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            return invertFloorNamedTz(io, timestampDriver, tzRules, floorFunc, unit, stride, effectiveOffset, returnUtc);
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
