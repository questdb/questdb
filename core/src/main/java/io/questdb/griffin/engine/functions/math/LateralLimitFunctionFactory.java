/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.math;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

/**
 * A LIMIT bound of a correlated LATERAL body: the first argument, or the constant second one when the first is NULL.
 */
public class LateralLimitFunctionFactory implements FunctionFactory {
    @Override
    public int getResultType(IntList argTypes) {
        return ColumnType.LONG;
    }

    @Override
    public String getSignature() {
        return "__lateral_limit(Ll)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new LateralLimitFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class LateralLimitFunction extends LongFunction implements BinaryFunction {
        private final Function limit;
        private final Function nullLimit;

        public LateralLimitFunction(Function limit, Function nullLimit) {
            this.limit = limit;
            this.nullLimit = nullLimit;
        }

        @Override
        public Function getLeft() {
            return limit;
        }

        @Override
        public long getLong(Record rec) {
            final long value = limit.getLong(rec);
            if (value == Numbers.LONG_NULL) {
                return nullLimit.getLong(rec);
            }
            if (value < 0) {
                throw CairoException.nonCritical()
                        .put("negative LIMIT is not supported in a correlated lateral sub-query [limit=")
                        .put(value)
                        .put(']');
            }
            return value;
        }

        @Override
        public String getName() {
            return "__lateral_limit";
        }

        @Override
        public Function getRight() {
            return nullLimit;
        }
    }
}
