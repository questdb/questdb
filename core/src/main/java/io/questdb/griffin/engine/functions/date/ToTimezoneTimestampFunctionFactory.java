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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.IntList;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;

import static io.questdb.griffin.engine.functions.date.ToUTCTimestampFunctionFactory.getTimestampFunction;

public class ToTimezoneTimestampFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "to_timezone(NS)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function timestamp = args.getQuick(0);
        Function timezone = args.getQuick(1);

        if (timezone.isConstant()) {
            return getTimestampFunction(argPositions, timestamp, timezone, 1);
        } else {
            return new ToTimezoneFunctionVar(timestamp, timezone);
        }
    }

    private static class ToTimezoneFunctionVar extends TimestampFunction implements BinaryFunction {
        private final Function timestamp;
        private final Function timezone;

        public ToTimezoneFunctionVar(Function timestamp, Function timezone) {
            this.timestamp = timestamp;
            this.timezone = timezone;
        }

        @Override
        public Function getLeft() {
            return timestamp;
        }

        @Override
        public String getName() {
            return "to_timezone";
        }

        @Override
        public Function getRight() {
            return timezone;
        }

        @Override
        public long getTimestamp(Record rec) {
            final long timestampValue = timestamp.getTimestamp(rec);
            try {
                final CharSequence tz = timezone.getStrA(rec);
                return tz != null ? Timestamps.toTimezone(timestampValue, TimestampFormatUtils.EN_LOCALE, tz) : timestampValue;
            } catch (NumericException e) {
                return timestampValue;
            }
        }
    }
}
