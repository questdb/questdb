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

public class GreatestNumericFunctionFactory implements FunctionFactory {
    private static final ThreadLocal<int[]> tlCounters = ThreadLocal.withInitial(() -> new int[ColumnType.NULL + 1]);

    @Override
    public String getSignature() {
        return "greatest(V)";
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
            throw SqlException.$(position, "at least one argument is required by GREATEST(V)");
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
        final Function retVal = getGreatestFunction(new ObjList<>(args), counters);
        if (retVal != null) {
            return retVal;
        }
        throw SqlException.position(argPositions.getQuick(0)).put("unexpected argument types");
    }

    private static @Nullable Function getGreatestFunction(ObjList<Function> args, int[] counters) {
        final int argCount = args.size();
        
        if (counters[ColumnType.DOUBLE] > 0) {
            // Use loop-unrolled variants for common cases (2-5 args)
            switch (argCount) {
                case 2:
                    return new GreatestDoubleFunction2(args.getQuick(0), args.getQuick(1));
                case 3:
                    return new GreatestDoubleFunction3(args.getQuick(0), args.getQuick(1), args.getQuick(2));
                case 4:
                    return new GreatestDoubleFunction4(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
                case 5:
                    return new GreatestDoubleFunction5(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3), args.getQuick(4));
                default:
                    return new GreatestDoubleRecordFunction(args);
            }
        }

        if (counters[ColumnType.FLOAT] > 0) {
            Function doubleFunc;
            switch (argCount) {
                case 2:
                    doubleFunc = new GreatestDoubleFunction2(args.getQuick(0), args.getQuick(1));
                    break;
                case 3:
                    doubleFunc = new GreatestDoubleFunction3(args.getQuick(0), args.getQuick(1), args.getQuick(2));
                    break;
                case 4:
                    doubleFunc = new GreatestDoubleFunction4(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
                    break;
                case 5:
                    doubleFunc = new GreatestDoubleFunction5(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3), args.getQuick(4));
                    break;
                default:
                    doubleFunc = new GreatestDoubleRecordFunction(args);
                    break;
            }
            return new CastDoubleToFloatFunctionFactory.CastDoubleToFloatFunction(doubleFunc);
        }

        if (counters[ColumnType.LONG] > 0) {
            // Use loop-unrolled variants for common cases (2-5 args)
            switch (argCount) {
                case 2:
                    return new GreatestLongFunction2(args.getQuick(0), args.getQuick(1));
                case 3:
                    return new GreatestLongFunction3(args.getQuick(0), args.getQuick(1), args.getQuick(2));
                case 4:
                    return new GreatestLongFunction4(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
                case 5:
                    return new GreatestLongFunction5(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3), args.getQuick(4));
                default:
                    return new GreatestLongRecordFunction(args);
            }
        }

        if (counters[ColumnType.DATE] > 0) {
            return new CastLongToDateFunctionFactory.CastLongToDateFunction(new GreatestLongRecordFunction(args));
        }

        if (counters[ColumnType.TIMESTAMP] > 0) {
            return new CastLongToTimestampFunctionFactory.CastLongToTimestampFunction(new GreatestLongRecordFunction(args));
        }

        if (counters[ColumnType.INT] > 0) {
            return new CastLongToIntFunctionFactory.CastLongToIntFunction(new GreatestLongRecordFunction(args));
        }

        if (counters[ColumnType.SHORT] > 0) {
            return new CastLongToShortFunctionFactory.CastLongToShortFunction(new GreatestLongRecordFunction(args));
        }

        if (counters[ColumnType.BYTE] > 0) {
            return new CastLongToByteFunctionFactory.CastLongToByteFunction(new GreatestLongRecordFunction(args));
        }

        return null;
    }


    private static class GreatestDoubleRecordFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public GreatestDoubleRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            double value = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                final double v = args.getQuick(i).getDouble(rec);
                if (!Numbers.isNull(v)) {
                    value = Math.max(value, v);
                }
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }
    }

    private static class GreatestLongRecordFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int n;

        public GreatestLongRecordFunction(ObjList<Function> args) {
            this.args = args;
            this.n = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value = args.getQuick(0).getLong(rec);
            for (int i = 1; i < n; i++) {
                value = Math.max(value, args.getQuick(i).getLong(rec));
            }
            return value;
        }

        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }

    // Loop-unrolled implementations for performance optimization
    // These avoid loop overhead and array bounds checks for common use cases (2-5 arguments)
    
    private static class GreatestDoubleFunction2 extends DoubleFunction {
        private final Function arg1, arg2;
        
        public GreatestDoubleFunction2(Function arg1, Function arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }
        
        @Override
        public double getDouble(Record rec) {
            final double v1 = arg1.getDouble(rec);
            final double v2 = arg2.getDouble(rec);
            
            double value = Double.NEGATIVE_INFINITY;
            if (!Numbers.isNull(v1)) {
                value = Math.max(value, v1);
            }
            if (!Numbers.isNull(v2)) {
                value = Math.max(value, v2);
            }
            return value;
        }
        
        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }
    }
    
    private static class GreatestDoubleFunction3 extends DoubleFunction {
        private final Function arg1, arg2, arg3;
        
        public GreatestDoubleFunction3(Function arg1, Function arg2, Function arg3) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
        }
        
        @Override
        public double getDouble(Record rec) {
            final double v1 = arg1.getDouble(rec);
            final double v2 = arg2.getDouble(rec);
            final double v3 = arg3.getDouble(rec);
            
            double max = Double.NEGATIVE_INFINITY;
            boolean hasValidValue = false;
            
            if (!Numbers.isNull(v1)) {
                max = v1;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v2)) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v3)) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Double.NaN;
        }
        
        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }
    }
    
    private static class GreatestDoubleFunction4 extends DoubleFunction {
        private final Function arg1, arg2, arg3, arg4;
        
        public GreatestDoubleFunction4(Function arg1, Function arg2, Function arg3, Function arg4) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
            this.arg4 = arg4;
        }
        
        @Override
        public double getDouble(Record rec) {
            final double v1 = arg1.getDouble(rec);
            final double v2 = arg2.getDouble(rec);
            final double v3 = arg3.getDouble(rec);
            final double v4 = arg4.getDouble(rec);
            
            double max = Double.NEGATIVE_INFINITY;
            boolean hasValidValue = false;
            
            if (!Numbers.isNull(v1)) {
                max = v1;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v2)) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v3)) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v4)) {
                max = hasValidValue ? Math.max(max, v4) : v4;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Double.NaN;
        }
        
        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }
    }
    
    private static class GreatestDoubleFunction5 extends DoubleFunction {
        private final Function arg1, arg2, arg3, arg4, arg5;
        
        public GreatestDoubleFunction5(Function arg1, Function arg2, Function arg3, Function arg4, Function arg5) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
            this.arg4 = arg4;
            this.arg5 = arg5;
        }
        
        @Override
        public double getDouble(Record rec) {
            final double v1 = arg1.getDouble(rec);
            final double v2 = arg2.getDouble(rec);
            final double v3 = arg3.getDouble(rec);
            final double v4 = arg4.getDouble(rec);
            final double v5 = arg5.getDouble(rec);
            
            double max = Double.NEGATIVE_INFINITY;
            boolean hasValidValue = false;
            
            if (!Numbers.isNull(v1)) {
                max = v1;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v2)) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v3)) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v4)) {
                max = hasValidValue ? Math.max(max, v4) : v4;
                hasValidValue = true;
            }
            if (!Numbers.isNull(v5)) {
                max = hasValidValue ? Math.max(max, v5) : v5;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Double.NaN;
        }
        
        @Override
        public String getName() {
            return "greatest[DOUBLE]";
        }
    }
    
    private static class GreatestLongFunction2 extends LongFunction {
        private final Function arg1, arg2;
        
        public GreatestLongFunction2(Function arg1, Function arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }
        
        @Override
        public long getLong(Record rec) {
            final long v1 = arg1.getLong(rec);
            final long v2 = arg2.getLong(rec);
            
            if (v1 == Numbers.LONG_NULL) {
                return v2 == Numbers.LONG_NULL ? Numbers.LONG_NULL : v2;
            }
            if (v2 == Numbers.LONG_NULL) {
                return v1;
            }
            return Math.max(v1, v2);
        }
        
        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }
    
    private static class GreatestLongFunction3 extends LongFunction {
        private final Function arg1, arg2, arg3;
        
        public GreatestLongFunction3(Function arg1, Function arg2, Function arg3) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
        }
        
        @Override
        public long getLong(Record rec) {
            final long v1 = arg1.getLong(rec);
            final long v2 = arg2.getLong(rec);
            final long v3 = arg3.getLong(rec);
            
            long max = Long.MIN_VALUE;
            boolean hasValidValue = false;
            
            if (v1 != Numbers.LONG_NULL) {
                max = v1;
                hasValidValue = true;
            }
            if (v2 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (v3 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Numbers.LONG_NULL;
        }
        
        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }
    
    private static class GreatestLongFunction4 extends LongFunction {
        private final Function arg1, arg2, arg3, arg4;
        
        public GreatestLongFunction4(Function arg1, Function arg2, Function arg3, Function arg4) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
            this.arg4 = arg4;
        }
        
        @Override
        public long getLong(Record rec) {
            final long v1 = arg1.getLong(rec);
            final long v2 = arg2.getLong(rec);
            final long v3 = arg3.getLong(rec);
            final long v4 = arg4.getLong(rec);
            
            long max = Long.MIN_VALUE;
            boolean hasValidValue = false;
            
            if (v1 != Numbers.LONG_NULL) {
                max = v1;
                hasValidValue = true;
            }
            if (v2 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (v3 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            if (v4 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v4) : v4;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Numbers.LONG_NULL;
        }
        
        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }
    
    private static class GreatestLongFunction5 extends LongFunction {
        private final Function arg1, arg2, arg3, arg4, arg5;
        
        public GreatestLongFunction5(Function arg1, Function arg2, Function arg3, Function arg4, Function arg5) {
            this.arg1 = arg1;
            this.arg2 = arg2;
            this.arg3 = arg3;
            this.arg4 = arg4;
            this.arg5 = arg5;
        }
        
        @Override
        public long getLong(Record rec) {
            final long v1 = arg1.getLong(rec);
            final long v2 = arg2.getLong(rec);
            final long v3 = arg3.getLong(rec);
            final long v4 = arg4.getLong(rec);
            final long v5 = arg5.getLong(rec);
            
            long max = Long.MIN_VALUE;
            boolean hasValidValue = false;
            
            if (v1 != Numbers.LONG_NULL) {
                max = v1;
                hasValidValue = true;
            }
            if (v2 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v2) : v2;
                hasValidValue = true;
            }
            if (v3 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v3) : v3;
                hasValidValue = true;
            }
            if (v4 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v4) : v4;
                hasValidValue = true;
            }
            if (v5 != Numbers.LONG_NULL) {
                max = hasValidValue ? Math.max(max, v5) : v5;
                hasValidValue = true;
            }
            
            return hasValidValue ? max : Numbers.LONG_NULL;
        }
        
        @Override
        public String getName() {
            return "greatest[LONG]";
        }
    }
}
