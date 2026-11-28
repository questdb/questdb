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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BinaryFunction;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * Compares two timestamps the way a join key compares them. SqlOptimiser filters a table with it when
 * join keys imply that a TIMESTAMP and a TIMESTAMP_NS column of the table are equal. Their keys compare
 * nanoseconds, and RecordSinkFactory.widenMicrosToNanos() widens the TIMESTAMP: it keeps NULL, so NULL
 * equals NULL, and it does not fail on a TIMESTAMP outside the TIMESTAMP_NS range, which = does.
 */
public class EqTimestampKeyFunctionFactory implements FunctionFactory {
    public static final String NAME = "__timestamp_key_eq";

    @Override
    public String getSignature() {
        return NAME + "(NN)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        final Function left = args.getQuick(0);
        final Function right = args.getQuick(1);
        final int leftType = left.getType();
        final int rightType = right.getType();
        final boolean isNanos = ColumnType.isTimestampNano(leftType) || ColumnType.isTimestampNano(rightType);
        return new Func(
                left,
                right,
                isNanos && ColumnType.isTimestampMicro(leftType),
                isNanos && ColumnType.isTimestampMicro(rightType)
        );
    }

    private static class Func extends BooleanFunction implements BinaryFunction {
        private final boolean isLeftWidened;
        private final boolean isRightWidened;
        private final Function left;
        private final Function right;

        public Func(Function left, Function right, boolean isLeftWidened, boolean isRightWidened) {
            this.left = left;
            this.right = right;
            this.isLeftWidened = isLeftWidened;
            this.isRightWidened = isRightWidened;
        }

        @Override
        public boolean getBool(Record rec) {
            final long l = left.getTimestamp(rec);
            final long r = right.getTimestamp(rec);
            return (isLeftWidened ? RecordSinkFactory.widenMicrosToNanos(l) : l)
                    == (isRightWidened ? RecordSinkFactory.widenMicrosToNanos(r) : r);
        }

        @Override
        public Function getLeft() {
            return left;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public Function getRight() {
            return right;
        }
    }
}
