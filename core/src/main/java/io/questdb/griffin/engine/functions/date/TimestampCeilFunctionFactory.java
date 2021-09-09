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

package io.questdb.griffin.engine.functions.date;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Timestamps;

public class TimestampCeilFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "timestamp_ceil(sN)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) throws SqlException {
        final Function kind = args.getQuick(0);

        final char c = kind.getChar(null);
        switch (c) {
            case 'd':
                return new TimestampCeilDDFunction(args.getQuick(1));
            case 'M':
                return new TimestampCeilMMFunction(args.getQuick(1));
            case 'y':
                return new TimestampCeilYYYYFunction(args.getQuick(1));
            case 'h':
                return new TimestampCeilHHFunction(args.getQuick(1));
            case 'm':
                return new TimestampCeilMIFunction(args.getQuick(1));
            case 's':
                return new TimestampCeilSSFunction(args.getQuick(1));
            case 'T':
                return new TimestampCeilMSFunction(args.getQuick(1));
            case 0:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid kind 'null'");
            default:
                throw SqlException.position(argPositions.getQuick(0)).put("invalid kind '").put(c).put('\'');
        }
    }

    private abstract static class AbstractTimestampCeilFunction extends TimestampFunction implements UnaryFunction {
        private final Function arg;

        public AbstractTimestampCeilFunction(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        final public long getTimestamp(Record rec) {
            long micros = arg.getTimestamp(rec);
            return micros == Numbers.LONG_NaN ? Numbers.LONG_NaN : ceil(micros);
        }

        abstract long ceil(long timestamp);
    }

    public static class TimestampCeilDDFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilDDFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilDD(timestamp);
        }
    }

    public static class TimestampCeilMMFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilMMFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMM(timestamp);
        }
    }

    public static class TimestampCeilYYYYFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilYYYYFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilYYYY(timestamp);
        }
    }

    public static class TimestampCeilHHFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilHHFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilHH(timestamp);
        }
    }

    public static class TimestampCeilMIFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilMIFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMI(timestamp);
        }
    }

    public static class TimestampCeilSSFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilSSFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilSS(timestamp);
        }
    }

    public static class TimestampCeilMSFunction extends AbstractTimestampCeilFunction {
        public TimestampCeilMSFunction(Function arg) {
            super(arg);
        }

        @Override
        public long ceil(long timestamp) {
            return Timestamps.ceilMS(timestamp);
        }
    }
}
