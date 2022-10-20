/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.QuarternaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class WidthBucketFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "width_bucket(DDDI)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new WidthBucketFunction(args.getQuick(0), args.getQuick(1), args.getQuick(2), args.getQuick(3));
    }

    private static class WidthBucketFunction extends IntFunction implements QuarternaryFunction {
        private final Function operand_func;
        private final Function low_func;
        private final Function high_func;
        private final Function count_func;

        public WidthBucketFunction(Function operand_func, Function low_func, Function high_func, Function count_func) {
            this.operand_func = operand_func;
            this.low_func = low_func;
            this.high_func = high_func;
            this.count_func = count_func;
        }

        @Override
        public Function getLeftEnd() {
            return operand_func;
        }

        @Override
        public Function getCenterLeft() {
            return low_func;
        }

        @Override
        public Function getCenterRight() {
            return high_func;
        }

        @Override
        public Function getRightEnd() {
            return count_func;
        }

        @Override
        public int getInt(Record rec) {
            double operand = operand_func.getDouble(rec);
            double low = low_func.getDouble(rec);
            double high = high_func.getDouble(rec);
            int count = count_func.getInt(rec);
            if (operand < low) {
                return 0;
            } else if (operand > high) {
                return (count + 1);
            } else {
                return (int) ((operand - low) / (high - low) * count) + 1;
            }
        }
    }
}
