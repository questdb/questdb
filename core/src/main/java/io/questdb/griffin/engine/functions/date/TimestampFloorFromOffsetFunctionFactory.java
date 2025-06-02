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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
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
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;


/**
 * Floors timestamps with modulo relative to a timestamp from 1970-01-01, as
 * well as an offset from the epoch start.
 * <p>
 * Fused variant of timestamp_floor() and to_timezone() functions meant
 * to be used in SAMPLE BY to parallel GROUP BY SQL rewrite.
 * <p>
 * When timezone is specified, the returned timestamps are in local time.
 */
public class TimestampFloorFromOffsetFunctionFactory implements FunctionFactory {
    private static final long MIN_GAP_MINUTES = 15;
    private static final long MIN_GAP_SECONDS = MIN_GAP_MINUTES * 60;
    private static final long MIN_GAP_MILLIS = MIN_GAP_SECONDS * 1000;
    private static final long MIN_GAP_MICROS = MIN_GAP_MILLIS * 1000;

    private static final TimestampFloorFunction floorDDFunc = Timestamps::floorDD;
    private static final TimestampFloorFunction floorHHFunc = Timestamps::floorHH;
    private static final TimestampFloorFunction floorMCFunc = Timestamps::floorMC;
    private static final TimestampFloorFunction floorMIFunc = Timestamps::floorMI;
    private static final TimestampFloorFunction floorMMFunc = Timestamps::floorMM;
    private static final TimestampFloorFunction floorMSFunc = Timestamps::floorMS;
    private static final TimestampFloorFunction floorSSFunc = Timestamps::floorSS;
    private static final TimestampFloorFunction floorWWFunc = Timestamps::floorWW;
    private static final TimestampFloorFunction floorYYYYFunc = Timestamps::floorYYYY;

    @Override
    public String getSignature() {
        return TimestampFloorFunctionFactory.NAME + "(sNnSS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final CharSequence unitStr = args.getQuick(0).getStrA(null);
        final int stride = Timestamps.getStrideMultiple(unitStr);
        final char unit = Timestamps.getStrideUnit(unitStr, argPositions.getQuick(0));
        final int unitPos = argPositions.getQuick(0);
        final Function timestampFunc = args.getQuick(1);
        long from = args.getQuick(2).getTimestamp(null);
        if (from == Numbers.LONG_NULL) {
            from = 0;
        }
        final Function offsetFunc = args.getQuick(3);
        final int offsetPos = argPositions.getQuick(3);
        final Function timezoneFunc = args.getQuick(4);
        final int timezonePos = argPositions.getQuick(4);

        final TimestampFloorFunction floorFunc = getFloorFunction(unit, unitPos);

        String offsetStr = null;
        long offset = 0;
        if (offsetFunc.isConstant()) {
            final CharSequence o = offsetFunc.getStrA(null);
            if (o != null) {
                final long val = Timestamps.parseOffset(o);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(o);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            }
            offsetStr = Chars.toString(o);
        }

        if (timezoneFunc.isConstant()) {
            final CharSequence tz = timezoneFunc.getStrA(null);
            long tzOffset = 0;
            TimeZoneRules tzRules = null;
            if (tz != null) {
                final int hi = tz.length();
                final long l = Timestamps.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = TimestampFormatUtils.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(tz, 0, hi)), RESOLUTION_MICROS
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
                    tzOffset = Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS;
                }
            }

            final String tzStr = Chars.toString(tz);

            if (tzRules == null) { // no timezone or fixed offset rules case
                if (offsetFunc.isConstant()) {
                    return createAllConstFunc(timestampFunc, floorFunc, stride, unit, unitPos, from, offset, offsetStr, tzOffset, tzStr);
                }
                if (offsetFunc.isRuntimeConstant()) {
                    return new RuntimeConstOffsetFunction(timestampFunc, floorFunc, stride, unit, from, offsetFunc, offsetPos, tzOffset, tzStr);
                }
                throw SqlException.$(offsetPos, "const or runtime const expected");
            }

            if (offsetFunc.isConstant()) {
                return createAllConstTzFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return new RuntimeConstOffsetDstGapAwareFunc(timestampFunc, floorFunc, stride, unit, from, offsetFunc, offsetPos, tzRules, tzStr);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        if (timezoneFunc.isRuntimeConstant()) {
            if (offsetFunc.isConstant()) {
                return createRuntimeConstTzFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return new AllRuntimeConstDstGapAwareFunc(timestampFunc, floorFunc, stride, unit, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        throw SqlException.$(timezonePos, "const or runtime const expected");
    }

    private static boolean canSkipDstGapCorrection(int stride, char unit, long from, long offset) {
        // require the effective offset to be aligned at day boundary;
        // we may relax this check in the future, if necessary
        if ((from + offset) % Timestamps.DAY_MICROS != 0) {
            return false;
        }

        switch (unit) {
            case 'M':
            case 'y':
            case 'w':
            case 'd':
            case 'h':
                return true;
            case 'm':
                // min DST gap is 15m, and it starts at the beginning of an hour
                return MIN_GAP_MINUTES % stride == 0 || stride % MIN_GAP_MINUTES == 0;
            case 's':
                return MIN_GAP_SECONDS % stride == 0 || stride % MIN_GAP_SECONDS == 0;
            case 'T':
                return MIN_GAP_MILLIS % stride == 0 || stride % MIN_GAP_MILLIS == 0;
            case 'U':
                return MIN_GAP_MICROS % stride == 0 || stride % MIN_GAP_MICROS == 0;
        }
        return false;
    }

    private static @NotNull Function createAllConstFunc(
            @NotNull Function timestampFunc,
            @NotNull TimestampFloorFunction floorFunc,
            int stride,
            char unit,
            int unitPos,
            long from,
            long offset,
            @Nullable String offsetStr,
            long tzOffset,
            @Nullable String tzStr
    ) throws SqlException {
        if (tzOffset == 0) {
            final long effectiveOffset = from + offset;
            switch (unit) {
                case 'M':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMMFunction(timestampFunc, stride, effectiveOffset);
                case 'y':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetYYYYFunction(timestampFunc, stride, effectiveOffset);
                case 'w':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetWWFunction(timestampFunc, stride, effectiveOffset);
                case 'd':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetDDFunction(timestampFunc, stride, effectiveOffset);
                case 'h':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetHHFunction(timestampFunc, stride, effectiveOffset);
                case 'm':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMIFunction(timestampFunc, stride, effectiveOffset);
                case 's':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetSSFunction(timestampFunc, stride, effectiveOffset);
                case 'T':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMSFunction(timestampFunc, stride, effectiveOffset);
                case 'U':
                    return new TimestampFloorOffsetFunctions.TimestampFloorOffsetMCFunction(timestampFunc, stride, effectiveOffset);
                default:
                    throw SqlException.position(unitPos).put("unexpected unit");
            }
        }

        return new AllConstFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, tzOffset, tzStr);
    }

    private static @NotNull Function createAllConstTzFunc(
            @NotNull Function timestampFunc,
            @NotNull TimestampFloorFunction floorFunc,
            int stride,
            char unit,
            long from,
            long offset,
            @Nullable String offsetStr,
            @NotNull TimeZoneRules tzRules,
            @NotNull String tzStr
    ) {
        if (canSkipDstGapCorrection(stride, unit, from, offset)) {
            return new AllConstTzFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr);
        }
        return new AllConstDstGapAwareFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, tzRules, tzStr);
    }

    private static @NotNull Function createRuntimeConstTzFunc(
            Function timestampFunc,
            TimestampFloorFunction floorFunc,
            int stride,
            char unit,
            long from,
            long offset,
            String offsetStr,
            Function timezoneFunc,
            int timezonePos
    ) {
        if (canSkipDstGapCorrection(stride, unit, from, offset)) {
            return new RuntimeConstTzFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos);
        }
        return new RuntimeConstDstGapAwareFunc(timestampFunc, floorFunc, stride, unit, from, offset, offsetStr, timezoneFunc, timezonePos);
    }

    private static long floorWithDstGapCorrection(long timestamp, TimestampFloorFunction floorFunc, int stride, long offset, TimeZoneRules tzRules) {
        final long localTimestamp = timestamp + tzRules.getOffset(timestamp);
        long flooredTimestamp = floorFunc.floor(localTimestamp, stride, offset);
        // Move the timestamp to the bucket if it belongs to a DST gap, i.e. non-existing
        // time interval that occur due to a forward clock shift.
        // This is required to avoid duplicate timestamps returned by SAMPLE BY + DST time zone + offset
        // queries that get rewritten to a parallel GROUP BY.
        long gapDuration = tzRules.getDstGapOffset(flooredTimestamp);
        if (gapDuration == 0) {
            return flooredTimestamp;
        }
        return floorFunc.floor(flooredTimestamp - gapDuration, stride, offset);
    }

    private static TimestampFloorFunction getFloorFunction(char unit, int unitPos) throws SqlException {
        switch (unit) {
            case 'M':
                return floorMMFunc;
            case 'y':
                return floorYYYYFunc;
            case 'w':
                return floorWWFunc;
            case 'd':
                return floorDDFunc;
            case 'h':
                return floorHHFunc;
            case 'm':
                return floorMIFunc;
            case 's':
                return floorSSFunc;
            case 'T':
                return floorMSFunc;
            case 'U':
                return floorMCFunc;
        }
        throw SqlException.position(unitPos).put("unexpected unit");
    }

    @FunctionalInterface
    private interface TimestampFloorFunction {
        long floor(long micros, int stride, long offset);
    }

    // both offset and time zone are consts
    private static class AllConstDstGapAwareFunc extends TimestampFunction implements UnaryFunction {
        private final long effectiveOffset; // from + offset
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final String offsetStr;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;

        public AllConstDstGapAwareFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                TimeZoneRules tzRules,
                String tzStr
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                return floorWithDstGapCorrection(timestamp, floorFunc, stride, effectiveOffset, tzRules);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final String offsetStr;
        private final int stride;
        private final Function tsFunc;
        private final long tzOffset;
        private final String tzStr;
        private final char unit;

        public AllConstFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                long tzOffset,
                String tzStr
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final String offsetStr;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;

        public AllConstTzFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                TimeZoneRules tzRules,
                String tzStr
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                final long localTimestamp = timestamp + tzRules.getOffset(timestamp);
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final Function offsetFunc;
        private final int offsetPos;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long effectiveOffset; // from + offset
        private long tzOffset;
        private TimeZoneRules tzRules;

        public AllRuntimeConstDstGapAwareFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                    return floorWithDstGapCorrection(timestamp, floorFunc, stride, effectiveOffset, tzRules);
                }
                final long localTimestamp = timestamp + tzOffset;
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Timestamps.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Timestamps.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = TimestampFormatUtils.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(tz, 0, hi)), RESOLUTION_MICROS
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS;
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final String offsetStr;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstDstGapAwareFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                Function timezoneFunc,
                int timezonePos
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                    return floorWithDstGapCorrection(timestamp, floorFunc, stride, effectiveOffset, tzRules);
                }
                final long localTimestamp = timestamp + tzOffset;
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Timestamps.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = TimestampFormatUtils.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(tz, 0, hi)), RESOLUTION_MICROS
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS;
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final Function offsetFunc;
        private final int offsetPos;
        private final int stride;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private final char unit;
        private long effectiveOffset; // from + offset

        public RuntimeConstOffsetDstGapAwareFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                TimeZoneRules tzRules,
                String tzStr
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                return floorWithDstGapCorrection(timestamp, floorFunc, stride, effectiveOffset, tzRules);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Timestamps.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc).val(',');
            sink.val('\'').val(tzStr).val('\'');
            sink.val(')');
        }
    }

    private static class RuntimeConstOffsetFunction extends TimestampFunction implements BinaryFunction {
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final Function offsetFunc;
        private final int offsetPos;
        private final int stride;
        private final Function tsFunc;
        private final long tzOffset;
        private final String tzStr;
        private final char unit;
        private long effectiveOffset; // from + offset

        public RuntimeConstOffsetFunction(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                Function offsetFunc,
                int offsetPos,
                long tzOffset,
                String tzStr
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
            long offset;
            if (offsetStr != null) {
                final long val = Timestamps.parseOffset(offsetStr);
                if (val == Numbers.LONG_NULL) {
                    // bad value for offset
                    throw SqlException.$(offsetPos, "invalid offset: ").put(offsetStr);
                }
                offset = Numbers.decodeLowInt(val) * Timestamps.MINUTE_MICROS;
            } else {
                offset = 0;
            }
            effectiveOffset = from + offset;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final String offsetStr;
        private final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private final char unit;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstTzFunc(
                Function tsFunc,
                TimestampFloorFunction floorFunc,
                int stride,
                char unit,
                long from,
                long offset,
                String offsetStr,
                Function timezoneFunc,
                int timezonePos
        ) {
            this.tsFunc = tsFunc;
            this.floorFunc = floorFunc;
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
                final long localTimestamp = tzRules != null
                        ? timestamp + tzRules.getOffset(timestamp)
                        : timestamp + tzOffset;
                return floorFunc.floor(localTimestamp, stride, effectiveOffset);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence tz = timezoneFunc.getStrA(null);
            if (tz != null) {
                final int hi = tz.length();
                final long l = Timestamps.parseOffset(tz, 0, hi);
                if (l == Long.MIN_VALUE) {
                    try {
                        tzRules = TimestampFormatUtils.EN_LOCALE.getZoneRules(
                                Numbers.decodeLowInt(TimestampFormatUtils.EN_LOCALE.matchZone(tz, 0, hi)), RESOLUTION_MICROS
                        );
                        tzOffset = 0;
                    } catch (NumericException e) {
                        throw SqlException.$(timezonePos, "invalid timezone: ").put(tz);
                    }
                } else {
                    tzOffset = Numbers.decodeLowInt(l) * Timestamps.MINUTE_MICROS;
                    tzRules = null;
                }
            } else {
                tzOffset = 0;
                tzRules = null;
            }
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            sink.val(stride);
            sink.val(unit).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
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
