/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.*;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public class CoalesceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "coalesce(V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (args.size() < 2) {
            throw SqlException.$(position, "coalesce can be used with 2 or more arguments");
        }
        if (args.size() > 2) {
            // copy, args collection will be reused by sql parser
            args = new ObjList<>(args);
        }

        // Similar to CASE function.
        // compute return type in this loop
        final int argsSize = args.size();
        int returnType = -1;
        for (int i = 0; i < argsSize; i++) {
            returnType = CaseCommon.getCommonType(returnType, args.getQuick(i).getType(), argPositions.getQuick(i));
        }
        switch (ColumnType.tagOf(returnType)) {
            case ColumnType.DOUBLE:
                return argsSize == 2 ? new TwoDoubleCoalesceFunction(args) : new DoubleCoalesceFunction(args, argsSize);
            case ColumnType.DATE:
                return argsSize == 2 ? new TwoDateCoalesceFunction(args) : new DateCoalesceFunction(args, argsSize);
            case ColumnType.TIMESTAMP:
                return argsSize == 2 ? new TwoTimestampCoalesceFunction(args) : new TimestampCoalesceFunction(args);
            case ColumnType.LONG:
                return argsSize == 2 ? new TwoLongCoalesceFunction(args) : new LongCoalesceFunction(args, argsSize);
            case ColumnType.LONG256:
                return argsSize == 2 ? new TwoLong256CoalesceFunction(args) : new Long256CoalesceFunction(args);
            case ColumnType.INT:
                return argsSize == 2 ? new TwoIntCoalesceFunction(args) : new IntCoalesceFunction(args, argsSize);
            case ColumnType.IPv4:
                return argsSize == 2 ? new TwoIPv4CoalesceFunction(args) : new IPv4CoalesceFunction(args, argsSize);
            case ColumnType.FLOAT:
                return argsSize == 2 ? new TwoFloatCoalesceFunction(args) : new FloatCoalesceFunction(args, argsSize);
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                if (argsSize == 2) {
                    int type0 = ColumnType.tagOf(args.getQuick(0).getType());
                    if (type0 != ColumnType.tagOf(args.getQuick(1).getType())) {
                        return new TwoSymStrCoalesceFunction(args);
                    } else if (type0 == ColumnType.SYMBOL) {
                        return new TwoSymCoalesceFunction(args);
                    } else {
                        return new TwoStrCoalesceFunction(args);
                    }
                }
                return new SymStrCoalesceFunction(args, argsSize);
            case ColumnType.UUID:
                return argsSize == 2 ? new TwoUuidCoalesceFunction(args) : new UuidCoalesceFunction(args, argsSize);
            case ColumnType.BOOLEAN:
            case ColumnType.SHORT:
            case ColumnType.BYTE:
            case ColumnType.CHAR:
                // Null on these data types not supported
                return args.getQuick(0);
            default:
                throw SqlException.$(position, "coalesce cannot be used with ")
                        .put(ColumnType.nameOf(returnType))
                        .put(" data type");
        }
    }

    private static boolean isNotNull(Long256 value) {
        return value != null &&
                value != Long256Impl.NULL_LONG256 && (value.getLong0() != Numbers.LONG_NaN ||
                value.getLong1() != Numbers.LONG_NaN ||
                value.getLong2() != Numbers.LONG_NaN ||
                value.getLong3() != Numbers.LONG_NaN);
    }

    private interface BinaryCoalesceFunction extends BinaryFunction {
        @Override
        default String getName() {
            return "coalesce";
        }
    }

    private interface MultiArgCoalesceFunction extends MultiArgFunction {
        @Override
        default String getName() {
            return "coalesce";
        }
    }

    private static class DateCoalesceFunction extends DateFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public DateCoalesceFunction(ObjList<Function> args, int size) {
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getDate(Record rec) {
            for (int i = 0; i < size; i++) {
                long value = args.getQuick(i).getDate(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class DoubleCoalesceFunction extends DoubleFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public DoubleCoalesceFunction(ObjList<Function> args, int size) {
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            for (int i = 0; i < size; i++) {
                double value = args.getQuick(i).getDouble(rec);
                if (value == value) {
                    return value;
                }
            }
            return Double.NaN;
        }
    }

    private static class FloatCoalesceFunction extends FloatFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public FloatCoalesceFunction(ObjList<Function> args, int size) {
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public float getFloat(Record rec) {
            for (int i = 0; i < size; i++) {
                float value = args.getQuick(i).getFloat(rec);
                if (value == value) {
                    return value;
                }
            }
            return Float.NaN;
        }
    }

    private static class IPv4CoalesceFunction extends IPv4Function implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public IPv4CoalesceFunction(ObjList<Function> args, int size) {
            super();
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public int getIPv4(Record rec) {
            for (int i = 0; i < size; i++) {
                int value = args.getQuick(i).getIPv4(rec);
                if (value != Numbers.IPv4_NULL) {
                    return value;
                }
            }
            return Numbers.IPv4_NULL;
        }
    }

    private static class IntCoalesceFunction extends IntFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public IntCoalesceFunction(ObjList<Function> args, int size) {
            super();
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            for (int i = 0; i < size; i++) {
                int value = args.getQuick(i).getInt(rec);
                if (value != Numbers.INT_NaN) {
                    return value;
                }
            }
            return Numbers.INT_NaN;
        }
    }

    private static class Long256CoalesceFunction extends Long256Function implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public Long256CoalesceFunction(ObjList<Function> args) {
            this.args = args;
            this.size = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            for (int i = 0; i < size; i++) {
                Long256 value = args.getQuick(i).getLong256A(rec);
                if (isNotNull(value)) {
                    Numbers.appendLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3(), sink);
                    return;
                }
            }
        }

        @Override
        public Long256 getLong256A(Record rec) {
            Long256 value = Long256Impl.NULL_LONG256;
            for (int i = 0; i < size; i++) {
                value = args.getQuick(i).getLong256A(rec);
                if (isNotNull(value)) {
                    return value;
                }
            }
            return value;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            Long256 value = Long256Impl.NULL_LONG256;
            for (int i = 0; i < size; i++) {
                value = args.getQuick(i).getLong256B(rec);
                if (isNotNull(value)) {
                    return value;
                }
            }
            return value;
        }
    }

    public static class LongCoalesceFunction extends LongFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public LongCoalesceFunction(ObjList<Function> args, int size) {
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            long value;
            for (int i = 0; i < size; i++) {
                value = args.getQuick(i).getLong(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class SymStrCoalesceFunction extends StrFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public SymStrCoalesceFunction(ObjList<Function> args, int size) {
            this.args = args;
            this.size = size;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }


        @Override
        public CharSequence getStr(Record rec) {
            for (int i = 0; i < size; i++) {
                Function arg = args.getQuick(i);
                CharSequence value = (ColumnType.isSymbol(arg.getType())) ? arg.getSymbol(rec) : arg.getStr(rec);
                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            for (int i = 0; i < size; i++) {
                Function arg = args.getQuick(i);
                CharSequence value = (ColumnType.isSymbol(arg.getType())) ? arg.getSymbolB(rec) : arg.getStrB(rec);
                if (value != null) {
                    return value;
                }
            }
            return null;
        }
    }

    private static class TimestampCoalesceFunction extends TimestampFunction implements MultiArgCoalesceFunction {
        private final ObjList<Function> args;
        private final int size;

        public TimestampCoalesceFunction(ObjList<Function> args) {
            this.args = args;
            this.size = args.size();
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getTimestamp(Record rec) {
            for (int i = 0; i < size; i++) {
                long value = args.getQuick(i).getTimestamp(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class TwoDateCoalesceFunction extends DateFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoDateCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public long getDate(Record rec) {
            long value = args0.getDate(rec);
            if (value != Numbers.LONG_NaN) {
                return value;
            }
            return args1.getDate(rec);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoDoubleCoalesceFunction extends DoubleFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoDoubleCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public double getDouble(Record rec) {
            double value = args0.getDouble(rec);
            if (value == value) {
                return value;
            }
            return args1.getDouble(rec);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoFloatCoalesceFunction extends FloatFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoFloatCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public float getFloat(Record rec) {
            float value = args0.getFloat(rec);
            if (value == value) {
                return value;
            }
            return args1.getFloat(rec);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoIPv4CoalesceFunction extends IPv4Function implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoIPv4CoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public int getIPv4(Record rec) {
            int value = args0.getIPv4(rec);
            if (value != Numbers.IPv4_NULL) {
                return value;
            }
            return args1.getIPv4(rec);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoIntCoalesceFunction extends IntFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoIntCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public int getInt(Record rec) {
            int value = args0.getInt(rec);
            if (value != Numbers.INT_NaN) {
                return value;
            }
            return args1.getInt(rec);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoLong256CoalesceFunction extends Long256Function implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoLong256CoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            Long256 value = args0.getLong256A(rec);
            if (!isNotNull(value)) {
                value = args1.getLong256A(rec);
            }
            Numbers.appendLong256(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3(), sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            Long256 value = args0.getLong256A(rec);
            if (isNotNull(value)) {
                return value;
            }
            return args1.getLong256A(rec);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            Long256 value = args0.getLong256B(rec);
            if (isNotNull(value)) {
                return value;
            }
            return args1.getLong256B(rec);
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    public static class TwoLongCoalesceFunction extends LongFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoLongCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public long getLong(Record rec) {
            long value = args0.getLong(rec);
            if (value != Numbers.LONG_NaN) {
                return value;
            }
            return args1.getLong(rec);
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class TwoStrCoalesceFunction extends StrFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoStrCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }

        @Override
        public CharSequence getStr(Record rec) {
            CharSequence value = args0.getStr(rec);
            if (value != null) {
                return value;
            }
            return args1.getStr(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            CharSequence value = args0.getStrB(rec);
            if (value != null) {
                return value;
            }
            return args1.getStrB(rec);
        }
    }

    private static class TwoSymCoalesceFunction extends StrFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoSymCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }

        @Override
        public CharSequence getStr(Record rec) {
            CharSequence value = args0.getSymbol(rec);
            if (value != null) {
                return value;
            }
            return args1.getSymbol(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            CharSequence value = args0.getSymbolB(rec);
            if (value != null) {
                return value;
            }
            return args1.getSymbolB(rec);
        }
    }

    private static class TwoSymStrCoalesceFunction extends StrFunction implements BinaryCoalesceFunction {
        private final boolean arg1IsSymbol;
        private final Function args0;
        private final boolean args0IsSymbol;
        private final Function args1;

        public TwoSymStrCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
            this.args0IsSymbol = ColumnType.isSymbol(args0.getType());
            this.arg1IsSymbol = ColumnType.isSymbol(args1.getType());
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }

        @Override
        public CharSequence getStr(Record rec) {
            CharSequence value = args0IsSymbol ? args0.getSymbol(rec) : args0.getStr(rec);
            if (value != null) {
                return value;
            }
            return arg1IsSymbol ? args1.getSymbol(rec) : args1.getStr(rec);
        }

        @Override
        public CharSequence getStrB(Record rec) {
            CharSequence value = args0IsSymbol ? args0.getSymbolB(rec) : args0.getStrB(rec);
            if (value != null) {
                return value;
            }
            return arg1IsSymbol ? args1.getSymbolB(rec) : args1.getStrB(rec);
        }
    }

    private static class TwoTimestampCoalesceFunction extends TimestampFunction implements BinaryCoalesceFunction {
        private final Function args0;
        private final Function args1;

        public TwoTimestampCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public Function getRight() {
            return args1;
        }

        @Override
        public long getTimestamp(Record rec) {
            long value = args0.getTimestamp(rec);
            if (value != Numbers.LONG_NaN) {
                return value;
            }
            return args1.getTimestamp(rec);
        }
    }

    private static class TwoUuidCoalesceFunction extends UuidFunction implements BinaryFunction {
        private final Function args0;
        private final Function args1;

        public TwoUuidCoalesceFunction(ObjList<Function> args) {
            assert args.size() == 2;
            this.args0 = args.getQuick(0);
            this.args1 = args.getQuick(1);
        }

        @Override
        public Function getLeft() {
            return args0;
        }

        @Override
        public long getLong128Hi(Record rec) {
            long hi0 = args0.getLong128Hi(rec);
            if (hi0 != Numbers.LONG_NaN) {
                return hi0; // if hi is not NaN then we know Long128 is not null
            }
            long lo0 = args0.getLong128Lo(rec);
            if (lo0 != Numbers.LONG_NaN) {
                return hi0; // if lo is not NaN then we know Long128 is not null and we can return hi0 even if it is NaN
            }
            // ok, both hi and lo are NaN, we use the value from the second argument
            return args1.getLong128Hi(rec);
        }

        @Override
        public long getLong128Lo(Record rec) {
            long lo0 = args0.getLong128Lo(rec);
            if (lo0 != Numbers.LONG_NaN) {
                return lo0; // lo is not NaN -> Long128 is not null, that's easy
            }
            long hi0 = args0.getLong128Hi(rec);
            if (hi0 != Numbers.LONG_NaN) {
                return lo0; // hi is not NaN  -> Long128 is not null -> we can return lo even if it is NaN
            }
            // ok, both hi and lo are NaN, we use the value from the second argument
            return args1.getLong128Lo(rec);
        }

        @Override
        public Function getRight() {
            return args1;
        }
    }

    private static class UuidCoalesceFunction extends UuidFunction implements MultiArgFunction {
        private final ObjList<Function> args;
        private final int size;

        public UuidCoalesceFunction(ObjList<Function> args, int argsSize) {
            this.args = args;
            this.size = argsSize;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong128Hi(Record rec) {
            long value = Numbers.LONG_NaN;
            for (int i = 0; i < size; i++) {
                value = args.getQuick(i).getLong128Hi(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
                long lo = args.getQuick(i).getLong128Lo(rec);
                if (lo != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return value;
        }

        @Override
        public long getLong128Lo(Record rec) {
            long value = Numbers.LONG_NaN;
            for (int i = 0; i < size; i++) {
                value = args.getQuick(i).getLong128Lo(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
                long hi = args.getQuick(i).getLong128Hi(rec);
                if (hi != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return value;
        }
    }
}
