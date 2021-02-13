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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.model.IntervalOperation;
<<<<<<< HEAD
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.std.LongList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;

import static io.questdb.std.datetime.microtime.TimestampFormatUtils.TIMESTAMP_FORMAT_MIN_LENGTH;
=======
import io.questdb.std.LongList;
import io.questdb.std.ObjList;

import static io.questdb.griffin.model.IntervalUtils.*;
>>>>>>> upstream/master

public class EqTimestampStrFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "=(NS)";
    }

    @Override
    public boolean isBoolean() {
        return true;
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        Function rightFn = args.getQuick(1);
        if (rightFn.isConstant()) {
<<<<<<< HEAD
            try {
                return new EqTimestampStrConstantFunction(position, args.getQuick(0), rightFn.getStr(null));
            } catch (NumericException e) {
                throw SqlException.$(position, "could not parse timestamp [value='").put(rightFn.getStr(null)).put("']");
            }
=======
            return new EqTimestampStrConstantFunction(position, args.getQuick(0), rightFn.getStr(null), rightFn.getPosition());
>>>>>>> upstream/master
        }
        return new EqTimestampStrFunction(position, args.getQuick(0), rightFn);
    }

    private static class EqTimestampStrConstantFunction extends NegatableBooleanFunction implements UnaryFunction {
        private final Function left;
<<<<<<< HEAD
        private final long beginning;
        private final long end;

        public EqTimestampStrConstantFunction(int position, Function left, CharSequence right) throws NumericException {
            super(position);
            this.left = left;
            if (right.length() >= TIMESTAMP_FORMAT_MIN_LENGTH) {
                beginning = end = TimestampFormatUtils.parseTimestamp(right);
            } else {
                LongList out = new LongList();
                IntervalUtils.parseInterval(right, 0, right.length(), IntervalOperation.INTERSECT, out);
                beginning = IntervalUtils.getEncodedPeriodLo(out, 0);
                end = IntervalUtils.getEncodedPeriodHi(out, 0);
            }
=======
        private final LongList intervals = new LongList();

        public EqTimestampStrConstantFunction(int position, Function left, CharSequence right, int rightPosition) throws SqlException {
            super(position);
            this.left = left;
            parseAndApplyIntervalEx(right, intervals, rightPosition);
>>>>>>> upstream/master
        }

        @Override
        public boolean getBool(Record rec) {
<<<<<<< HEAD
            return negated != isTimestampInRange(left.getTimestamp(rec), beginning, end);
=======
            return negated != isInIntervals(intervals, left.getTimestamp(rec));
>>>>>>> upstream/master
        }

        @Override
        public Function getArg() {
            return left;
        }
    }

    private static class EqTimestampStrFunction extends NegatableBooleanFunction implements BinaryFunction {
        private final Function left;
        private final Function right;
        private final LongList intervals = new LongList();

        public EqTimestampStrFunction(int position, Function left, Function right) {
            super(position);
            this.left = left;
            this.right = right;
        }

        @Override
        public boolean getBool(Record rec) {
            CharSequence timestampAsString = right.getStr(rec);
<<<<<<< HEAD
            try {
                intervals.clear();
                long beginning;
                long end;
                if (timestampAsString.length() >= TIMESTAMP_FORMAT_MIN_LENGTH) {
                    beginning = end = TimestampFormatUtils.parseTimestamp(timestampAsString);
                } else {
                    IntervalUtils.parseInterval(timestampAsString, 0, timestampAsString.length(), IntervalOperation.INTERSECT, intervals);
                    beginning = IntervalUtils.getEncodedPeriodLo(intervals, 0);
                    end = IntervalUtils.getEncodedPeriodHi(intervals, 0);
                }
                return negated != isTimestampInRange(left.getTimestamp(rec), beginning, end);
            } catch (NumericException e) {
                return false;
            }
=======
            intervals.clear();
            try {
                parseAndApplyIntervalEx(timestampAsString, intervals, right.getPosition());
            } catch (SqlException e) {
                return false;
            }
            return negated != isInIntervals(intervals, left.getTimestamp(rec));
>>>>>>> upstream/master
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

<<<<<<< HEAD
    private static boolean isTimestampInRange(long timestamp, long begin, long end) {
        return timestamp >= begin && timestamp <= end;
=======
    private static void parseAndApplyIntervalEx(CharSequence seq, LongList out, int position) throws SqlException {
        parseIntervalEx(seq, 0, seq.length(), position, out, IntervalOperation.INTERSECT);
        applyLastEncodedIntervalEx(out);
>>>>>>> upstream/master
    }
}
