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

package io.questdb.griffin.engine.functions.lt;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.TIMESTAMP_FORMAT_MIN_LENGTH;

public class LtTimestampStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(NS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function rightFn = args.getQuick(1);
        if (rightFn.isConstant()) {
            try {
                long rightTimestamp = parseFullOrPartialTimestamp(rightFn.getStr(null));
                return new LtTimestampStrConstantFunction(position, args.getQuick(0), rightTimestamp);
            } catch (NumericException e) {
                throw SqlException.invalidDate(rightFn.getPosition());
            }
        }
        return new LtTimestampStrFunction(position, args.getQuick(0), rightFn);
    }

    private static long parseFullOrPartialTimestamp(CharSequence seq) throws NumericException {
        if (seq.length() >= TIMESTAMP_FORMAT_MIN_LENGTH) {
            return TimestampFormatUtils.parseTimestamp(seq);
        } else {
            return IntervalUtils.parseFloorPartialDate(seq);
        }
    }

    private static class LtTimestampStrConstantFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function left;
        private final long right;

        public LtTimestampStrConstantFunction(int position, Function left, long right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated
                    ? left.getTimestamp(rec) >= right
                    : left.getTimestamp(rec) < right;
        }

        @Override
        public Function getArg() {
            return left;
        }
    }

    private static class LtTimestampStrFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public LtTimestampStrFunction(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence timestampAsString = right.getStr(rec);
            try {
                long rightTimestamp = parseFullOrPartialTimestamp(timestampAsString);
                return negated
                        ? left.getTimestamp(rec) >= rightTimestamp
                        : left.getTimestamp(rec) < rightTimestamp;
            } catch (NumericException e) {
                return false;
            }
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }
}
