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
import io.questdb.griffin.engine.functions.cast.CastDoubleToFloatFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToByteFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToDateFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToIntFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToShortFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastLongToTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public class LeastNumericFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<int[]> tlCounters = ThreadLocal.withInitial(() -> new int[ColumnType.NULL + 1]);

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
        final int[] counters = tlCounters.get();
        Arrays.fill(counters, 0);

        final int argCount;
        if (args == null || (argCount = args.size()) == 0) {
            throw SqlException.$(position, "at least one argument is required by LEAST(V)");
        }

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
                case ColumnType.NULL:
                    counters[type]++;
                    continue;
                default:
                    throw SqlException.position(argPositions.getQuick(i)).put("unsupported type: ").put(ColumnType.nameOf(type));
            }
        }

        if (counters[ColumnType.NULL] == argCount) {
            return NullConstant.NULL;
        }

        // have to copy, args is mutable
        final Function retVal = getLeastFunction(new ObjList<>(args), counters);
        if (retVal != null) {
            return retVal;
        }
        throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
    }

    private static @Nullable Function getLeastFunction(ObjList<Function> args, int[] counters) {
        final int argCount = args.size();
        
        // Use loop-unrolled variants for small argument counts
        if (argCount >= 2 && argCount <= 5) {
            boolean allDouble = true;
            boolean allLong = true;
            
            for (int i = 0; i < argCount; i++) {
                Function func = args.getQuick(i);
                if (func.getType() != ColumnType.DOUBLE) {
                    allDouble = false;
                }
                if (func.getType() != ColumnType.LONG) {
                    allLong = false;
                }
            }
            
            if (allDouble) {
                switch (argCount) {
                    case 2:
                        return new LeastDoubleFunction2(args);
                    case 3:
                        return new LeastDoubleFunction3(args);
                    case 4:
                        return new LeastDoubleFunction4(args);
                    case 5:
                        return new LeastDoubleFunction5(args);
                }
            } else if (allLong) {
                switch (argCount) {
                    case 2:
                        return new LeastLongFunction2(args);
                    case 3:
                        return new LeastLongFunction3(args);
                    case 4:
                        return new LeastLongFunction4(args);
                    case 5:
                        return new LeastLongFunction5(args);
                }
            }
        }
        
        // Fall back to original loop-based implementation
        if (counters[ColumnType.DOUBLE] > 0) {
            return new LeastDoubleRecordFunction(args);
        }

        if (counters[ColumnType.FLOAT] > 0) {
            return new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(new LeastDoubleRecordFunction(args));
        }

        if (counters[ColumnType.LONG] > 0) {
            return new LeastLongRecordFunction(args);
        }

        if (counters[ColumnType.DATE] > 0) {
            return new CastLongToDateFunctionFactory.CastLongToDateFunction(new LeastLongRecordFunction(args));
        }

        if (counters[ColumnType.TIMESTAMP] > 0) {
            return new CastLongToTimestampFunctionFactory.CastLongToTimestampFunction(new LeastLongRecordFunction(args));
        }

        if (counters[ColumnType.INT] > 0) {
            return new CastLongToIntFunctionFactory.CastLongToIntFunction(new LeastLongRecordFunction(args));
        }

        if (counters[ColumnType.SHORT] > 0) {
            return new CastLongToShortFunctionFactory.CastLongToShortFunction(new LeastLongRecordFunction(args));
        }

        if (counters[ColumnType.BYTE] > 0) {
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

    // Loop-unrolled double functions
    private static class LeastDoubleFunction2 extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastDoubleFunction2(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public double getDouble(Record rec) {
            double val1 = args.getQuick(0).getDouble(rec);
            double val2 = args.getQuick(1).getDouble(rec);
            
            double value = Double.POSITIVE_INFINITY;
            if (!Numbers.isNull(val1)) {
                value = Math.min(value, val1);
            }
            if (!Numbers.isNull(val2)) {
                value = Math.min(value, val2);
            }
            return value;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastDoubleFunction3 extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastDoubleFunction3(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public double getDouble(Record rec) {
            double val1 = args.getQuick(0).getDouble(rec);
            double val2 = args.getQuick(1).getDouble(rec);
            double val3 = args.getQuick(2).getDouble(rec);
            
            double value = Double.POSITIVE_INFINITY;
            if (!Numbers.isNull(val1)) {
                value = Math.min(value, val1);
            }
            if (!Numbers.isNull(val2)) {
                value = Math.min(value, val2);
            }
            if (!Numbers.isNull(val3)) {
                value = Math.min(value, val3);
            }
            return value;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastDoubleFunction4 extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastDoubleFunction4(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public double getDouble(Record rec) {
            double val1 = args.getQuick(0).getDouble(rec);
            double val2 = args.getQuick(1).getDouble(rec);
            double val3 = args.getQuick(2).getDouble(rec);
            double val4 = args.getQuick(3).getDouble(rec);
            
            double value = Double.POSITIVE_INFINITY;
            if (!Numbers.isNull(val1)) {
                value = Math.min(value, val1);
            }
            if (!Numbers.isNull(val2)) {
                value = Math.min(value, val2);
            }
            if (!Numbers.isNull(val3)) {
                value = Math.min(value, val3);
            }
            if (!Numbers.isNull(val4)) {
                value = Math.min(value, val4);
            }
            return value;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastDoubleFunction5 extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastDoubleFunction5(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public double getDouble(Record rec) {
            double val1 = args.getQuick(0).getDouble(rec);
            double val2 = args.getQuick(1).getDouble(rec);
            double val3 = args.getQuick(2).getDouble(rec);
            double val4 = args.getQuick(3).getDouble(rec);
            double val5 = args.getQuick(4).getDouble(rec);
            
            double value = Double.POSITIVE_INFINITY;
            if (!Numbers.isNull(val1)) {
                value = Math.min(value, val1);
            }
            if (!Numbers.isNull(val2)) {
                value = Math.min(value, val2);
            }
            if (!Numbers.isNull(val3)) {
                value = Math.min(value, val3);
            }
            if (!Numbers.isNull(val4)) {
                value = Math.min(value, val4);
            }
            if (!Numbers.isNull(val5)) {
                value = Math.min(value, val5);
            }
            return value;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    // Loop-unrolled long functions
    private static class LeastLongFunction2 extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastLongFunction2(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public long getLong(Record rec) {
            long val1 = args.getQuick(0).getLong(rec);
            long val2 = args.getQuick(1).getLong(rec);
            
            if (val1 == Numbers.LONG_NULL) return val1;
            if (val2 == Numbers.LONG_NULL) return val2;
            
            return Math.min(val1, val2);
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastLongFunction3 extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastLongFunction3(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public long getLong(Record rec) {
            long val1 = args.getQuick(0).getLong(rec);
            long val2 = args.getQuick(1).getLong(rec);
            long val3 = args.getQuick(2).getLong(rec);
            
            if (val1 == Numbers.LONG_NULL) return val1;
            if (val2 == Numbers.LONG_NULL) return val2;
            if (val3 == Numbers.LONG_NULL) return val3;
            
            return Math.min(Math.min(val1, val2), val3);
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastLongFunction4 extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastLongFunction4(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public long getLong(Record rec) {
            long val1 = args.getQuick(0).getLong(rec);
            long val2 = args.getQuick(1).getLong(rec);
            long val3 = args.getQuick(2).getLong(rec);
            long val4 = args.getQuick(3).getLong(rec);
            
            if (val1 == Numbers.LONG_NULL) return val1;
            if (val2 == Numbers.LONG_NULL) return val2;
            if (val3 == Numbers.LONG_NULL) return val3;
            if (val4 == Numbers.LONG_NULL) return val4;
            
            return Math.min(Math.min(val1, val2), Math.min(val3, val4));
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
    
    private static class LeastLongFunction5 extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LeastLongFunction5(ObjList<Function> args) {
            this.args = args;
        }

        @Override
        public long getLong(Record rec) {
            long val1 = args.getQuick(0).getLong(rec);
            long val2 = args.getQuick(1).getLong(rec);
            long val3 = args.getQuick(2).getLong(rec);
            long val4 = args.getQuick(3).getLong(rec);
            long val5 = args.getQuick(4).getLong(rec);
            
            if (val1 == Numbers.LONG_NULL) return val1;
            if (val2 == Numbers.LONG_NULL) return val2;
            if (val3 == Numbers.LONG_NULL) return val3;
            if (val4 == Numbers.LONG_NULL) return val4;
            if (val5 == Numbers.LONG_NULL) return val5;
            
            return Math.min(Math.min(Math.min(val1, val2), Math.min(val3, val4)), val5);
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }
    }
}
