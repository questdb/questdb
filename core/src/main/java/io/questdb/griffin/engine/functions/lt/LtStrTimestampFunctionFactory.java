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

public class LtStrTimestampFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "<(SN)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function leftFn = args.getQuick(0);
        if (leftFn.isConstant()) {
            try {
                long leftTimestamp = parseFullOrPartialTimestamp(leftFn.getStr(null));
                return new LtStrConstantTimestampFunction(position, leftTimestamp, args.getQuick(1));
            } catch (NumericException e) {
                throw SqlException.$(position, "could not parse timestamp [value='").put(leftFn.getStr(null)).put("']");
            }
        }
        return new LtStrTimestampFunction(position, leftFn, args.getQuick(1));
    }

    private static long parseFullOrPartialTimestamp(CharSequence seq) throws NumericException {
        if (seq.length() >= TIMESTAMP_FORMAT_MIN_LENGTH) {
            return TimestampFormatUtils.parseTimestamp(seq);
        } else {
            return IntervalUtils.parseCCPartialDate(seq) - 1;
        }
    }

    private static class LtStrConstantTimestampFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final long left;
        private final Function right;

        public LtStrConstantTimestampFunction(int position, long left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            return negated
                    ? left >= right.getTimestamp(rec)
                    : left < right.getTimestamp(rec);
        }

        @Override
        public Function getArg() {
            return right;
        }
    }

    private static class LtStrTimestampFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;

        public LtStrTimestampFunction(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence timestampAsString = left.getStr(rec);
            try {
                long leftTimestamp = parseFullOrPartialTimestamp(timestampAsString);
                return negated
                        ? leftTimestamp >= right.getTimestamp(rec)
                        : leftTimestamp < right.getTimestamp(rec);
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
