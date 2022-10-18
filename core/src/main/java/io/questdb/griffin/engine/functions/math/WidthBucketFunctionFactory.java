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
        private final Function leftEnd;
        private final Function centerLeft;
        private final Function centerRight;
        private final Function rightEnd;

        public WidthBucketFunction(Function leftEnd, Function centerLeft, Function centerRight, Function rightEnd) {
            this.leftEnd = leftEnd;
            this.centerLeft = centerLeft;
            this.centerRight = centerRight;
            this.rightEnd = rightEnd;
        }

        @Override
        public Function getLeftEnd() {
            return leftEnd;
        }

        @Override
        public Function getCenterLeft() {
            return centerLeft;
        }

        @Override
        public Function getCenterRight() {
            return centerRight;
        }

        @Override
        public Function getRightEnd() {
            return rightEnd;
        }

        @Override
        public int getInt(Record rec) {
            double operand = leftEnd.getDouble(rec);
            double low = centerLeft.getDouble(rec);
            double high = centerRight.getDouble(rec);
            int count = rightEnd.getInt(rec);
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
