/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class CoalesceFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "coalesce(V)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        // Similar to CASE function.
        // compute return type in this loop
        final int argsSize = args.size();
        int returnType = -1;
        for (int i = 0; i < argsSize; i++) {
            returnType = CaseCommon.getCommonType(returnType, args.getQuick(i).getType(), args.getQuick(i).getPosition());
        }

        switch (returnType) {
            case ColumnType.DOUBLE:
                return new DoubleCoalesceFunction(position, args);
            case ColumnType.DATE:
                return new DateCoalesceFunction(position, args);
            case ColumnType.TIMESTAMP:
                return new TimestampCoalesceFunction(position, args);
            case ColumnType.LONG:
                return new LongCoalesceFunction(position, args);
            case ColumnType.LONG256:
                return new Long256CoalesceFunction(position, args);
            case ColumnType.INT:
                return new IntCoalesceFunction(position, args);
            case ColumnType.FLOAT:
                return new FloatCoalesceFunction(position, args);
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                return new SymStrCoalesceFunction(position, args);
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

    private static class DoubleCoalesceFunction extends DoubleFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public DoubleCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                double value = args.getQuick(i).getDouble(rec);
                if (!Double.isNaN(value)) {
                    return value;
                }
            }
            return Double.NaN;
        }
    }

    private static class FloatCoalesceFunction extends FloatFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public FloatCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public float getFloat(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                float value = args.getQuick(i).getFloat(rec);
                if (!Float.isNaN(value)) {
                    return value;
                }
            }
            return Float.NaN;
        }
    }

    private static class DateCoalesceFunction extends DateFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public DateCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getDate(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                long value = args.getQuick(i).getDate(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class TimestampCoalesceFunction extends TimestampFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public TimestampCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getTimestamp(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                long value = args.getQuick(i).getTimestamp(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class LongCoalesceFunction extends LongFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public LongCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public long getLong(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                long value = args.getQuick(i).getLong(rec);
                if (value != Numbers.LONG_NaN) {
                    return value;
                }
            }
            return Numbers.LONG_NaN;
        }
    }

    private static class Long256CoalesceFunction extends Long256Function implements MultiArgFunction {
        private final ObjList<Function> args;

        public Long256CoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public void getLong256(Record rec, CharSink sink) {
            for (int i = 0, size = args.size(); i < size; i++) {
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
            for (int i = 0, size = args.size(); i < size; i++) {
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
            for (int i = 0, size = args.size(); i < size; i++) {
                value = args.getQuick(i).getLong256B(rec);
                if (isNotNull(value)) {
                    return value;
                }
            }
            return value;
        }

        private static boolean isNotNull(Long256 value) {
            return value != null &&
                    value != Long256Impl.NULL_LONG256 && (value.getLong0() != Numbers.LONG_NaN ||
                    value.getLong1() != Numbers.LONG_NaN ||
                    value.getLong2() != Numbers.LONG_NaN ||
                    value.getLong3() != Numbers.LONG_NaN);
        }
    }

    private static class IntCoalesceFunction extends IntFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public IntCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }

        @Override
        public int getInt(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                int value = args.getQuick(i).getInt(rec);
                if (value != Numbers.INT_NaN) {
                    return value;
                }
            }
            return Numbers.INT_NaN;
        }
    }

    private static class SymStrCoalesceFunction extends StrFunction implements MultiArgFunction {
        private final ObjList<Function> args;

        public SymStrCoalesceFunction(int position, ObjList<Function> args) {
            super(position);
            this.args = args;
        }

        @Override
        public ObjList<Function> getArgs() {
            return args;
        }


        @Override
        public CharSequence getStr(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                Function arg = args.getQuick(i);
                CharSequence value;
                if (arg.getType() == ColumnType.SYMBOL) {
                    value = arg.getSymbol(rec);
                } else {
                    value = arg.getStr(rec);
                }

                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            for (int i = 0, size = args.size(); i < size; i++) {
                Function arg = args.getQuick(i);
                CharSequence value;
                if (arg.getType() == ColumnType.SYMBOL) {
                    value = arg.getSymbol(rec);
                } else {
                    value = arg.getStrB(rec);
                }

                if (value != null) {
                    return value;
                }
            }
            return null;
        }
    }
}
