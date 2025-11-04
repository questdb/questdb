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

package io.questdb.griffin.engine.functions.date;

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

public class AddLongToTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "+(NL)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        Function arg = args.getQuick(0);
        return new AddLongFunc(arg, args.getQuick(1), ColumnType.getTimestampType(arg.getType()));
    }

    private static class AddLongFunc extends TimestampFunction implements BinaryFunction {
        final Function left;
        final Function right;

        public AddLongFunc(Function left, Function right, int type) {
            super(type);
            this.left = left;
            this.right = right;
        }

        @Override
        public Function getLeft() {
            return left;
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
        public long getTimestamp(Record rec) {
            final long l = left.getTimestamp(rec);
            final long r = right.getLong(rec);
            if (l == Numbers.LONG_NULL || r == Numbers.LONG_NULL) {
                return Numbers.LONG_NULL;
            }
            return l + r;
        }

        @Override
        public boolean isOperator() {
            return true;
        }
    }
}
