/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

package io.questdb.griffin.engine.functions.conditional;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class NullIfLong256FunctionFactory implements FunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.LONG256;
    }

    @Override
    public String getSignature() {
        return "nullif(HH)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new NullIfLong256Function(args.getQuick(0), args.getQuick(1));
    }

    private static class NullIfLong256Function extends Long256Function implements BinaryFunction {
        private final Function left;
        private final Function right;

        public NullIfLong256Function(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            Numbers.appendLong256(getLong256A(rec), sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            // Read once: a second read of a non-deterministic argument is a fresh draw, and
            // returning that draw hands back the very value the comparison just excluded.
            final Long256 value = left.getLong256A(rec);
            return isEqual(value, right.getLong256B(rec)) ? Long256Impl.NULL_LONG256 : value;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            final Long256 value = left.getLong256B(rec);
            return isEqual(value, right.getLong256A(rec)) ? Long256Impl.NULL_LONG256 : value;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("nullif(").val(left).val(',').val(right).val(')');
        }

        private static boolean isEqual(Long256 a, Long256 b) {
            return a.getLong0() == b.getLong0() && a.getLong1() == b.getLong1()
                    && a.getLong2() == b.getLong2() && a.getLong3() == b.getLong3();
        }
    }
}
