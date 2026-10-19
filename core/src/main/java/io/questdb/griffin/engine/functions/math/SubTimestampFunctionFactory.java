/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.Interval;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class SubTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "-(Nl)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new Func(args.getQuick(0), args.getQuick(1), ColumnType.getTimestampType(args.getQuick(0).getType()));
    }

    public static class Func extends TimestampFunction implements ArithmeticBinaryFunction, MonotonicTimestampFunction {
        private final Function left;
        private final Function right;

        public Func(Function left, Function right, int timestampType) {
            super(timestampType);
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return "-";
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public long getTimestamp(Record rec) {
            // The right operand is a LONG (signature "-(Nl)"), so read it with getLong().
            // Calling getTimestamp() here breaks for operands whose getTimestamp() is unsupported,
            // e.g. a CHAR constant implicitly cast to LONG ("now() - '1'"), which threw
            // UnsupportedOperationException. This mirrors AddLongToTimestampFunctionFactory.
            long l = left.getTimestamp(rec);
            long r = right.getLong(rec);

            if (l != Numbers.LONG_NULL && r != Numbers.LONG_NULL) {
                return l - r;
            }

            return Numbers.LONG_NULL;
        }

        @Override
        public Function getTimestampArg() {
            return left;
        }

        @Override
        public int invertTimestampInterval(Interval io) {
            if (!right.isConstant()) {
                return NONE;
            }
            final long k = right.getLong(null);
            if (k == Numbers.LONG_NULL) {
                return NONE;
            }
            // g(ts) = ts - k, so the constant shift is -k
            return MonotonicTimestampFunction.invertConstantShift(io, -k, shiftInputCeiling(getType()));
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
