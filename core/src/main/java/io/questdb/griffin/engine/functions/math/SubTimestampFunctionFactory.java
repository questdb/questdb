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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
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

    public static class Func extends TimestampFunction implements BinaryFunction {
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
            long l = left.getTimestamp(rec);
            long r = right.getTimestamp(rec);

            if (l != Numbers.LONG_NULL && r != Numbers.LONG_NULL) {
                return l - r;
            }

            return Numbers.LONG_NULL;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
