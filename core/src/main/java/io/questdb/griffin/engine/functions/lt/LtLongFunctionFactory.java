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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class LtLongFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "<(LL)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        final boolean unsigned64 = left.getType() == ColumnType.UINT64 || right.getType() == ColumnType.UINT64;
        return new LtLongFunction(left, right, unsigned64);
    }

    private static class LtLongFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;
        private final boolean unsigned64;

        public LtLongFunction(Function left, Function right, boolean unsigned64) {
            this.left = left;
            this.right = right;
            this.unsigned64 = unsigned64;
        }

        @Override
        public boolean getBool(Record rec) {
            if (this.left.isNull(rec) || this.right.isNull(rec)) {
                return false;
            }
            if (unsigned64) {
                return Numbers.lessThanUInt64(
                        this.left.getLong(rec),
                        this.right.getLong(rec),
                        negated
                );
            }
            return Numbers.lessThan(
                    this.left.getLong(rec),
                    this.right.getLong(rec),
                    negated
            );
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(left);
            if (negated) {
                sink.val(">=");
            } else {
                sink.val('<');
            }
            sink.val(right);
        }
    }
}
