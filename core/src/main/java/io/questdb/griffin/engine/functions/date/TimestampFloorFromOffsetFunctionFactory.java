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
        final char unit = Timestamps.getStrideUnit(unitStr);
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
                    return createAllConstFunc(timestampFunc, floorFunc, unit, unitPos, stride, from, offset, offsetStr, tzOffset, tzStr);
                }
                if (offsetFunc.isRuntimeConstant()) {
                    return createRuntimeConstOffsetFunc(unit, unitPos, timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
                }
                throw SqlException.$(offsetPos, "const or runtime const expected");
            }

            if (offsetFunc.isConstant()) {
                return createAllConstTzFunc(unit, unitPos, timestampFunc, stride, from + offset, tzRules, tzStr);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return createRuntimeConstOffsetTzFunc(unit, unitPos, timestampFunc, stride, from, tzRules, tzStr, offsetFunc, offsetPos);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        if (timezoneFunc.isRuntimeConstant()) {
            if (offsetFunc.isConstant()) {
                return createRuntimeConstTzFunc(unit, unitPos, timestampFunc, stride, from + offset, timezoneFunc, timezonePos);
            }
            if (offsetFunc.isRuntimeConstant()) {
                return createAllRuntimeConstTzFunc(unit, unitPos, timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            }
            throw SqlException.$(offsetPos, "const or runtime const expected");
        }

        throw SqlException.$(timezonePos, "const or runtime const expected");
    }

    private static @NotNull Function createAllConstFunc(
            Function timestampFunc,
            TimestampFloorFunction floorFunc,
            char unit,
            int unitPos,
            int stride,
            long from,
            long offset,
            String offsetStr,
            long tzOffset,
            String tzStr
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
            char unit,
            int unitPos,
            Function timestampFunc,
            int stride,
            long offset,
            TimeZoneRules tzRules,
            String tzStr
    ) throws SqlException {
        switch (unit) {
            case 'M':
                return new AllConstTzMMFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'y':
                return new AllConstTzYYYYFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'w':
                return new AllConstTzWWFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'd':
                return new AllConstTzDDFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'h':
                return new AllConstTzHHFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'm':
                return new AllConstTzMIFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 's':
                return new AllConstTzSSFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'T':
                return new AllConstTzMSFunc(timestampFunc, stride, offset, tzRules, tzStr);
            case 'U':
                return new AllConstTzMCFunc(timestampFunc, stride, offset, tzRules, tzStr);
            default:
                throw SqlException.position(unitPos).put("unexpected unit");
        }
    }

    private static @NotNull Function createAllRuntimeConstTzFunc(
            char unit,
            int unitPos,
            Function timestampFunc,
            int stride,
            long from,
            Function offsetFunc,
            int offsetPos,
            Function timezoneFunc,
            int timezonePos
    ) throws SqlException {
        switch (unit) {
            case 'M':
                return new AllRuntimeConstTzMMFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'y':
                return new AllRuntimeConstTzYYYYFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'w':
                return new AllRuntimeConstTzWWFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'd':
                return new AllRuntimeConstTzDDFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'h':
                return new AllRuntimeConstTzHHFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'm':
                return new AllRuntimeConstTzMIFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 's':
                return new AllRuntimeConstTzSSFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'T':
                return new AllRuntimeConstTzMSFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            case 'U':
                return new AllRuntimeConstTzMCFunc(timestampFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
            default:
                throw SqlException.position(unitPos).put("unexpected unit");
        }
    }

    private static @NotNull Function createRuntimeConstOffsetFunc(
            char unit,
            int unitPos,
            Function timestampFunc,
            int stride,
            long from,
            Function offsetFunc,
            int offsetPos,
            long tzOffset,
            String tzStr
    ) throws SqlException {
        switch (unit) {
            case 'M':
                return new RuntimeConstOffsetMMFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'y':
                return new RuntimeConstOffsetYYYYFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'w':
                return new RuntimeConstOffsetWWFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'd':
                return new TimestampFloorOffsetDDFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'h':
                return new TimestampFloorOffsetHHFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'm':
                return new TimestampFloorOffsetMIFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 's':
                return new TimestampFloorOffsetSSFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'T':
                return new TimestampFloorOffsetMSFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            case 'U':
                return new TimestampFloorOffsetMCFunction(timestampFunc, stride, from, offsetFunc, offsetPos, tzOffset, tzStr);
            default:
                throw SqlException.position(unitPos).put("unexpected unit");
        }
    }

    private static @NotNull Function createRuntimeConstOffsetTzFunc(
            char unit,
            int unitPos,
            Function timestampFunc,
            int stride,
            long from,
            TimeZoneRules tzRules,
            String tzStr,
            Function offsetFunc,
            int offsetPos
    ) throws SqlException {
        switch (unit) {
            case 'M':
                return new RuntimeConstOffsetTzMMFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'y':
                return new RuntimeConstOffsetTzYYYYFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'w':
                return new RuntimeConstOffsetTzWWFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'd':
                return new RuntimeConstOffsetTzDDFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'h':
                return new RuntimeConstOffsetTzHHFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'm':
                return new RuntimeConstOffsetTzMIFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 's':
                return new RuntimeConstOffsetTzSSFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'T':
                return new RuntimeConstOffsetTzMSFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            case 'U':
                return new RuntimeConstOffsetTzMCFunc(timestampFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
            default:
                throw SqlException.position(unitPos).put("unexpected unit");
        }
    }

    private static @NotNull Function createRuntimeConstTzFunc(
            char unit,
            int unitPos,
            Function timestampFunc,
            int stride,
            long offset,
            Function timezoneFunc,
            int timezonePos
    ) throws SqlException {
        switch (unit) {
            case 'M':
                return new RuntimeConstTzMMFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'y':
                return new RuntimeConstTzYYYYFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'w':
                return new RuntimeConstTzWWFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'd':
                return new RuntimeConstTzDDFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'h':
                return new RuntimeConstTzHHFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'm':
                return new RuntimeConstTzMIFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 's':
                return new RuntimeConstTzSSFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'T':
                return new RuntimeConstTzMSFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            case 'U':
                return new RuntimeConstTzMCFunc(timestampFunc, stride, offset, timezoneFunc, timezonePos);
            default:
                throw SqlException.position(unitPos).put("unexpected unit");
        }
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

    private static class AllConstFunc extends TimestampFunction implements UnaryFunction {
        private final long effectiveOffset;
        private final TimestampFloorFunction floorFunc;
        private final long from;
        private final long offset;
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
            this.offset = offset;
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
            if (stride != 1) {
                sink.val(stride);
            }
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
            if (tzStr != null) {
                sink.val('\'').val(tzStr).val('\'');
            } else {
                sink.val("null");
            }
            sink.val(')');
        }
    }

    private static class AllConstTzDDFunc extends AllConstTzFunc {

        public AllConstTzDDFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    // both offset and time zone are consts
    private static abstract class AllConstTzFunc extends TimestampFunction implements UnaryFunction {
        protected final long offset;
        protected final int stride;
        private final Function arg;
        private final TimeZoneRules tzRules;
        private final String tzStr;

        public AllConstTzFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            this.arg = arg;
            this.stride = stride;
            this.offset = offset;
            this.tzRules = tzRules;
            this.tzStr = tzStr;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public final long getTimestamp(Record rec) {
            final long timestamp = arg.getTimestamp(rec);
            if (timestamp != Numbers.LONG_NULL) {
                final long localTimestamp = timestamp + tzRules.getOffset(timestamp);
                return floor(localTimestamp);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            if (stride != 1) {
                sink.val(stride);
            }
            sink.val(getUnit()).val("',");
            sink.val(arg).val(',');
            if (offset != 0) {
                sink.val('\'').val(Timestamps.toString(offset)).val("\',");
            }
            if (tzStr != null) {
                sink.val(",'").val(tzStr).val('\'');
            } else {
                sink.val(",null");
            }
            sink.val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    private static class AllConstTzHHFunc extends AllConstTzFunc {

        public AllConstTzHHFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    private static class AllConstTzMCFunc extends AllConstTzFunc {

        public AllConstTzMCFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    private static class AllConstTzMIFunc extends AllConstTzFunc {

        public AllConstTzMIFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    private static class AllConstTzMMFunc extends AllConstTzFunc {

        public AllConstTzMMFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    private static class AllConstTzMSFunc extends AllConstTzFunc {

        public AllConstTzMSFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    private static class AllConstTzSSFunc extends AllConstTzFunc {

        public AllConstTzSSFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    private static class AllConstTzWWFunc extends AllConstTzFunc {

        public AllConstTzWWFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    private static class AllConstTzYYYYFunc extends AllConstTzFunc {

        public AllConstTzYYYYFunc(Function arg, int stride, long offset, TimeZoneRules tzRules, String tzStr) {
            super(arg, stride, offset, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

    private static class AllRuntimeConstTzDDFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzDDFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    // both offset and time zone are runtime consts
    private static abstract class AllRuntimeConstTzFunc extends TimestampFunction implements TernaryFunction {
        protected final long from;
        protected final int stride;
        private final Function offsetFunc;
        private final int offsetPos;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private long offset;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public AllRuntimeConstTzFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.timezoneFunc = timezoneFunc;
            this.timezonePos = timezonePos;
        }

        public long effectiveOffset() {
            return from + offset;
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
                final long localTimestamp = tzRules != null
                        ? timestamp + tzRules.getOffset(timestamp)
                        : timestamp + tzOffset;
                return floor(localTimestamp);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TernaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
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
            if (stride != 1) {
                sink.val(stride);
            }
            sink.val(getUnit()).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc);
            sink.val(timezoneFunc);
            sink.val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    private static class AllRuntimeConstTzHHFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzHHFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    private static class AllRuntimeConstTzMCFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzMCFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    private static class AllRuntimeConstTzMIFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzMIFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    private static class AllRuntimeConstTzMMFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzMMFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    private static class AllRuntimeConstTzMSFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzMSFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    private static class AllRuntimeConstTzSSFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzSSFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    private static class AllRuntimeConstTzWWFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzWWFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    private static class AllRuntimeConstTzYYYYFunc extends AllRuntimeConstTzFunc {

        public AllRuntimeConstTzYYYYFunc(
                Function tsFunc,
                int stride,
                long from,
                Function offsetFunc,
                int offsetPos,
                Function timezoneFunc,
                int timezonePos
        ) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

    private static abstract class RuntimeConstOffsetFunction extends TimestampFunction implements BinaryFunction {
        protected final int stride;
        private final long from;
        private final Function offsetFunc;
        private final int offsetPos;
        private final Function tsFunc;
        private final long tzOffset;
        private final String tzStr;
        private long offset;

        public RuntimeConstOffsetFunction(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.tzOffset = tzOffset;
            this.tzStr = tzStr;
        }

        public long effectiveOffset() {
            return from + offset;
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
                return floor(localTimestamp);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
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
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            if (stride != 1) {
                sink.val(stride);
            }
            sink.val(getUnit()).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc);
            if (tzStr != null) {
                sink.val(",'").val(tzStr).val('\'');
            } else {
                sink.val(",null");
            }
            sink.val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    private static class RuntimeConstOffsetMMFunc extends RuntimeConstOffsetFunction {

        public RuntimeConstOffsetMMFunc(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    private static class RuntimeConstOffsetTzDDFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzDDFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    // offset is runtime const and time zone is const
    private static abstract class RuntimeConstOffsetTzFunc extends TimestampFunction implements BinaryFunction {
        protected final long from;
        protected final int stride;
        private final Function offsetFunc;
        private final int offsetPos;
        private final Function tsFunc;
        private final TimeZoneRules tzRules;
        private final String tzStr;
        private long offset;

        public RuntimeConstOffsetTzFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.from = from;
            this.offsetFunc = offsetFunc;
            this.offsetPos = offsetPos;
            this.tzRules = tzRules;
            this.tzStr = tzStr;
        }

        public long effectiveOffset() {
            return from + offset;
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
                final long localTimestamp = timestamp + tzRules.getOffset(timestamp);
                return floor(localTimestamp);
            }
            return Numbers.LONG_NULL;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            BinaryFunction.super.init(symbolTableSource, executionContext);

            final CharSequence offsetStr = offsetFunc.getStrA(null);
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
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(TimestampFloorFunctionFactory.NAME).val("('");
            if (stride != 1) {
                sink.val(stride);
            }
            sink.val(getUnit()).val("',");
            sink.val(tsFunc).val(',');
            if (from != 0) {
                sink.val('\'').val(Timestamps.toString(from)).val("',");
            } else {
                sink.val("null,");
            }
            sink.val(offsetFunc);
            if (tzStr != null) {
                sink.val(",'").val(tzStr).val('\'');
            } else {
                sink.val(",null");
            }
            sink.val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    private static class RuntimeConstOffsetTzHHFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzHHFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    private static class RuntimeConstOffsetTzMCFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzMCFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    private static class RuntimeConstOffsetTzMIFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzMIFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    private static class RuntimeConstOffsetTzMMFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzMMFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    private static class RuntimeConstOffsetTzMSFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzMSFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    private static class RuntimeConstOffsetTzSSFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzSSFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    private static class RuntimeConstOffsetTzWWFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzWWFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    private static class RuntimeConstOffsetTzYYYYFunc extends RuntimeConstOffsetTzFunc {

        public RuntimeConstOffsetTzYYYYFunc(Function tsFunc, int stride, long from, Function offsetFunc, int offsetPos, TimeZoneRules tzRules, String tzStr) {
            super(tsFunc, stride, from, offsetFunc, offsetPos, tzRules, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

    private static class RuntimeConstOffsetWWFunction extends RuntimeConstOffsetFunction {

        public RuntimeConstOffsetWWFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    private static class RuntimeConstOffsetYYYYFunction extends RuntimeConstOffsetFunction {

        public RuntimeConstOffsetYYYYFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

    private static class RuntimeConstTzDDFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzDDFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    // offset is const and time zone is runtime const
    private static abstract class RuntimeConstTzFunc extends TimestampFunction implements BinaryFunction {
        protected final long offset;
        protected final int stride;
        private final Function timezoneFunc;
        private final int timezonePos;
        private final Function tsFunc;
        private long tzOffset;
        private TimeZoneRules tzRules;

        public RuntimeConstTzFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            this.tsFunc = tsFunc;
            this.stride = stride;
            this.offset = offset;
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
                return floor(localTimestamp);
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
            sink.val(TimestampFloorFunctionFactory.NAME).val("('")
                    .val(getUnit()).val("',")
                    .val(tsFunc).val(',')
                    .val(Timestamps.toString(offset))
                    .val(')');
        }

        abstract protected long floor(long timestamp);

        abstract CharSequence getUnit();
    }

    private static class RuntimeConstTzHHFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzHHFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    private static class RuntimeConstTzMCFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzMCFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    private static class RuntimeConstTzMIFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzMIFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    private static class RuntimeConstTzMMFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzMMFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMM(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "month";
        }
    }

    private static class RuntimeConstTzMSFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzMSFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    private static class RuntimeConstTzSSFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzSSFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }

    private static class RuntimeConstTzWWFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzWWFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorWW(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "week";
        }
    }

    private static class RuntimeConstTzYYYYFunc extends RuntimeConstTzFunc {

        public RuntimeConstTzYYYYFunc(Function tsFunc, int stride, long offset, Function timezoneFunc, int timezonePos) {
            super(tsFunc, stride, offset, timezoneFunc, timezonePos);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorYYYY(timestamp, stride, offset);
        }

        @Override
        CharSequence getUnit() {
            return "year";
        }
    }

    private static class TimestampFloorOffsetDDFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetDDFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorDD(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "day";
        }
    }

    private static class TimestampFloorOffsetHHFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetHHFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorHH(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "hour";
        }
    }

    private static class TimestampFloorOffsetMCFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetMCFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMC(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "microsecond";
        }
    }

    private static class TimestampFloorOffsetMIFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetMIFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMI(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "minute";
        }
    }

    private static class TimestampFloorOffsetMSFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetMSFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorMS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "millisecond";
        }
    }

    private static class TimestampFloorOffsetSSFunction extends RuntimeConstOffsetFunction {

        public TimestampFloorOffsetSSFunction(Function tsFunc, int stride, long offset, Function offsetFunc, int offsetPos, long tzOffset, String tzStr) {
            super(tsFunc, stride, offset, offsetFunc, offsetPos, tzOffset, tzStr);
        }

        @Override
        public long floor(long timestamp) {
            return Timestamps.floorSS(timestamp, stride, effectiveOffset());
        }

        @Override
        CharSequence getUnit() {
            return "second";
        }
    }
}
