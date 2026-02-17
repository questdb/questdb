/*******************************************************************************
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

package io.questdb.griffin.engine.functions.long256;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.Long256Function;
import io.questdb.griffin.engine.functions.QuaternaryFunction;
import io.questdb.griffin.engine.functions.constants.Long256Constant;
import io.questdb.std.*;
import io.questdb.std.str.CharSink;

public final class LongsToLong256FunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "to_long256(LLLL)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function l0 = args.getQuick(0);
        final Function l1 = args.getQuick(1);
        final Function l2 = args.getQuick(2);
        final Function l3 = args.getQuick(3);

        if (l0.isConstant() && l1.isConstant() && l2.isConstant() && l3.isConstant()) {
            return new Long256Constant(l0.getLong(null), l1.getLong(null), l2.getLong(null), l3.getLong(null));
        }
        return new LongsToLong256Function(l0, l1, l2, l3);
    }

    private static class LongsToLong256Function extends Long256Function implements QuaternaryFunction {

        private final Function l0;
        private final Function l1;
        private final Function l2;
        private final Function l3;

        private final Long256Impl long256a = new Long256Impl();
        private final Long256Impl long256b = new Long256Impl();

        public LongsToLong256Function(Function l0, Function l1, Function l2, Function l3) {
            this.l0 = l0;
            this.l1 = l1;
            this.l2 = l2;
            this.l3 = l3;
        }

        @Override
        public void close() {
            Misc.free(l0);
            Misc.free(l1);
            Misc.free(l2);
            Misc.free(l3);
        }

        @Override
        public Function getFunc0() {
            return l0;
        }

        @Override
        public Function getFunc1() {
            return l1;
        }

        @Override
        public Function getFunc2() {
            return l2;
        }

        @Override
        public Function getFunc3() {
            return l3;
        }

        @Override
        public void getLong256(Record rec, CharSink<?> sink) {
            Numbers.appendLong256(l0.getLong(rec), l1.getLong(rec), l2.getLong(rec), l3.getLong(rec), sink);
        }

        @Override
        public Long256 getLong256A(Record rec) {
            long l0 = this.l0.getLong(rec);
            long l1 = this.l1.getLong(rec);
            long l2 = this.l2.getLong(rec);
            long l3 = this.l3.getLong(rec);
            if (Long256Impl.isNull(l0, l1, l2, l3)) {
                return Long256Impl.NULL_LONG256;
            }
            long256a.setAll(l0, l1, l2, l3);
            return long256a;
        }

        @Override
        public Long256 getLong256B(Record rec) {
            long l0 = this.l0.getLong(rec);
            long l1 = this.l1.getLong(rec);
            long l2 = this.l2.getLong(rec);
            long l3 = this.l3.getLong(rec);
            if (Long256Impl.isNull(l0, l1, l2, l3)) {
                return Long256Impl.NULL_LONG256;
            }
            long256b.setAll(l0, l1, l2, l3);
            return long256b;
        }

        @Override
        public String getName() {
            return "to_long256";
        }

    }
}
