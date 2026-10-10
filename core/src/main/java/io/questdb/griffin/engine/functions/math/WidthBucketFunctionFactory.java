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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;

public class WidthBucketFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "width_bucket(DDDI)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {

        final Function operand = args.getQuick(0);
        final Function lowerBound = args.getQuick(1);
        final Function upperBound = args.getQuick(2);
        final Function bucketCount = args.getQuick(3);

        if (bucketCount.isConstant()) {
            final int count = bucketCount.getInt(null);

            if (count != Numbers.INT_NULL && count <= 0) {
                throw SqlException.$(
                        argPositions.getQuick(3),
                        "bucket count must be greater than 0"
                );
            }
        }

        if (lowerBound.isConstant() && upperBound.isConstant()) {
            final double lower = lowerBound.getDouble(null);
            final double upper = upperBound.getDouble(null);

            if (!Double.isNaN(lower)
                    && !Double.isNaN(upper)
                    && lower == upper) {

                throw SqlException.$(
                        argPositions.getQuick(2),
                        "bounds cannot be equal"
                );
            }
        }

        return new WidthBucketFunction(
                operand,
                lowerBound,
                upperBound,
                bucketCount
        );
    }

    private static class WidthBucketFunction extends IntFunction {

        private final Function operand;
        private final Function lowerBound;
        private final Function upperBound;
        private final Function bucketCount;

        private WidthBucketFunction(
                Function operand,
                Function lowerBound,
                Function upperBound,
                Function bucketCount
        ) {
            this.operand = operand;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.bucketCount = bucketCount;
        }

        @Override
        public int getInt(Record rec) {

            final double value = operand.getDouble(rec);
            final double lower = lowerBound.getDouble(rec);
            final double upper = upperBound.getDouble(rec);
            final int count = bucketCount.getInt(rec);

            if (Double.isNaN(value)
                    || Double.isNaN(lower)
                    || Double.isNaN(upper)
                    || count == Numbers.INT_NULL) {

                return Numbers.INT_NULL;
            }

            if (count <= 0 || lower == upper) {
                return Numbers.INT_NULL;
            }

            if (lower < upper) {

                if (value < lower) {
                    return 0;
                }

                if (value >= upper) {
                    return count + 1;
                }

                return (int) Math.floor(
                        ((value - lower) / (upper - lower)) * count
                ) + 1;
            }

            if (value > lower) {
                return 0;
            }

            if (value <= upper) {
                return count + 1;
            }

            return (int) Math.floor(
                    ((lower - value) / (lower - upper)) * count
            ) + 1;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("width_bucket(")
                    .val(operand)
                    .val(',')
                    .val(lowerBound)
                    .val(',')
                    .val(upperBound)
                    .val(',')
                    .val(bucketCount)
                    .val(')');
        }
    }
}
