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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.std.IntList;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import io.questdb.std.Long256Util;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;

public class AddLong256FunctionFactory implements FunctionFactory {
    /**
     * Adds {@code left} and {@code right} into {@code sum}, like {@link Long256Impl#add} without
     * its NULL test. The function tests both operands for NULL before it calls this method.
     */
    public static Long256Impl value(Long256Impl sum, Long256 left, Long256 right) {
        sum.copyFrom(left);
        Long256Util.addValue(sum, right.getLong0(), right.getLong1(), right.getLong2(), right.getLong3());
        return sum;
    }

    @Override
    public String getSignature() {
        return "+(HH)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new AddLong256Func(args.getQuick(0), args.getQuick(1));
    }

    private static class AddLong256Func extends Long256Function implements ArithmeticBinaryFunction {
        final Function left;
        final Long256Impl long256A = new Long256Impl();
        final Long256Impl long256B = new Long256Impl();
        final Function right;

        public AddLong256Func(Function left, Function right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            Long256Impl v = (Long256Impl) getLong256A(rec);
            v.toSink(sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            final Long256 l = left.getLong256A(rec);
            final Long256 r = right.getLong256A(rec);
            if (l.equals(Long256Impl.NULL_LONG256) || r.equals(Long256Impl.NULL_LONG256)) {
                return Long256Impl.NULL_LONG256;
            }
            return value(long256A, l, r);
        }

        @Override
        public Long256 getLong256B(Record rec) {
            final Long256 l = left.getLong256B(rec);
            final Long256 r = right.getLong256B(rec);
            if (l.equals(Long256Impl.NULL_LONG256) || r.equals(Long256Impl.NULL_LONG256)) {
                return Long256Impl.NULL_LONG256;
            }
            return value(long256B, l, r);
        }

        @Override
        public String getName() {
            return "+";
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
