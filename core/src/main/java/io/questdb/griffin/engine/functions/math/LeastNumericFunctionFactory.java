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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.MultiArgFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.cast.CastDoubleToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

public class LeastNumericFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<IntHashSet> tlSet = ThreadLocal.withInitial(IntHashSet::new);

    @Override
    public String getSignature() {
        return "least(V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final IntHashSet counters = tlSet.get();
        counters.clear();
        final int argCount;
        if (args == null || (argCount = args.size()) == 0) {
            throw SqlException.$(position, "at least one argument is required by LEAST(V)");
        }
        boolean allNull = true;
        for (int i = 0; i < argCount; i++) {
            final Function arg = args.getQuick(i);
            final int type = arg.getType();

            switch (ColumnType.tagOf(type)) {
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                case ColumnType.LONG:
                case ColumnType.INT:
                case ColumnType.SHORT:
                case ColumnType.BYTE:
                case ColumnType.DATE:
                case ColumnType.TIMESTAMP:
                    allNull = false;
                case ColumnType.NULL:
                    counters.add(type);
                    continue;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported type: ").put(ColumnType.nameOf(type));
            }
        }
        if (allNull) {
            return NullConstant.NULL;
        }

        // have to copy, args is mutable
        final Function retVal = getLeastFunction(new ObjList<>(args), counters);
        if (retVal != null) {
            return retVal;
        }
        throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
    }

    private static @Nullable Function getLeastFunction(ObjList<Function> args, IntHashSet set) {
        if (set.contains(ColumnType.DOUBLE)) {
            return new LeastDoubleRecordFunction(args);
        }

        if (set.contains(ColumnType.FLOAT)) {
            return new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(new LeastDoubleRecordFunction(args));
        }

        if (set.contains(ColumnType.TIMESTAMP_NANO)) {
            if (set.contains(ColumnType.DATE) || set.contains(ColumnType.TIMESTAMP_MICRO)) {
                return new LeastTimestampRecordFunction(args, ColumnType.TIMESTAMP_NANO);
            }
            return new CastLongToTimestampFunctionFactory.Func(new LeastLongRecordFunction(args), ColumnType.TIMESTAMP_NANO);
        }

        if (set.contains(ColumnType.TIMESTAMP_MICRO)) {
            if (set.contains(ColumnType.DATE)) {
                return new LeastTimestampRecordFunction(args, ColumnType.TIMESTAMP_MICRO);
            }
            return new CastLongToTimestampFunctionFactory.Func(new LeastLongRecordFunction(args), ColumnType.TIMESTAMP_MICRO);
        }

        if (set.contains(ColumnType.DATE)) {
            return new CastLongToDateFunctionFactory.CastLongToDateFunction(new LeastLongRecordFunction(args));
        }

        if (set.contains(ColumnType.LONG)) {
            return new LeastLongRecordFunction(args);
        }

        if (set.contains(ColumnType.INT)) {
            return new CastLongToIntFunctionFactory.CastLongToIntFunction(new LeastLongRecordFunction(args));
        }

        if (set.contains(ColumnType.SHORT)) {
            return new CastLongToShortFunctionFactory.CastLongToShortFunction(new LeastLongRecordFunction(args));
        }

        if (set.contains(ColumnType.BYTE)) {
            return new CastLongToByteFunctionFactory.CastLongToByteFunction(new LeastLongRecordFunction(args));
        }

        return null;
    }

    private static class LeastDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public LeastDoubleRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            double value = Double.POSITIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                final double v = args.getQuick(i).getDouble(rec);
                if (!Numbers.isNull(v)) {
                    value = Math.min(value, v);
                }
            }
            return value;
        }

        @Override
        public String getName() {
            return "least[DOUBLE]";
        }
    }

    private static class LeastLongRecordFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public LeastLongRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value = Long.MAX_VALUE;
            boolean foundValidValue = false;
            for (int i = 0; i < n; i++) {
                final long v = args.getQuick(i).getLong(rec);
                if (v != Numbers.LONG_NULL) {
                    foundValidValue = true;
                    value = Math.min(value, v);
                }
            }
            return foundValidValue ? value : Numbers.LONG_NULL;
        }

        @Override
        public String getName() {
            return "least[LONG]";
        }
    }

    private static class LeastTimestampRecordFunction extends TimestampFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;
        private final IntList timestampTypes;

        public LeastTimestampRecordFunction(ObjList<Function> args, int timestampType) {
            super(timestampType);
            this.args = args;
            this.n = args.size();
            timestampTypes = new IntList(n);
            timestampTypes.setPos(n);
            for (int i = 0; i < n; i++) {
                timestampTypes.setQuick(i, ColumnType.getTimestampType(args.getQuick(i).getType()));
            }
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public String getName() {
            return "least[TIMESTAMP]";
        }

        @Override
        public long getTimestamp(Record rec) {
            long value = Long.MAX_VALUE;
            boolean foundValidValue = false;
            for (int i = 0; i < n; i++) {
                final long v = timestampDriver.from(args.getQuick(i).getTimestamp(rec), timestampTypes.getQuick(i));
                if (v != Numbers.LONG_NULL) {
                    foundValidValue = true;
                    value = Math.min(value, v);
                }
            }
            return foundValidValue ? value : Numbers.LONG_NULL;
        }
    }
}
