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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class StdDevSampleGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "stddev_samp(D)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new StdDevSampleGroupByFunction(args.getQuick(0));
    }

    private static class StdDevSampleGroupByFunction extends AbstractStdDevGroupByFunction {

        public StdDevSampleGroupByFunction(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 2);
            if (count - 1 > 0) {
                double sum = rec.getDouble(valueIndex + 1);
                double variance = sum / (count - 1);
                return Math.sqrt(variance);
            }
            return Double.NaN;
        }

        @Override
        public String getName() {
            return "stddev_samp";
        }

        // Chan et al. [CGL82; CGL83]
        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcMean = srcValue.getDouble(valueIndex);
            double srcSum = srcValue.getDouble(valueIndex + 1);
            long srcCount = srcValue.getLong(valueIndex + 2);

            double destMean = destValue.getDouble(valueIndex);
            double destSum = destValue.getDouble(valueIndex + 1);
            long destCount = destValue.getLong(valueIndex + 2);

            long mergedCount = srcCount + destCount;
            double delta = destMean - srcMean;

            // This is only valid when countA is much larger than countB.
            // If both are large and similar sizes, delta is not scaled down.
            // double mergedMean = srcMean + delta * ((double) destCount / mergedCount);

            // So we use this instead:
            double mergedMean = (srcCount * srcMean + destCount * destMean) / mergedCount;
            double mergedSum = srcSum + destSum + (delta * delta) * ((double) (srcCount * destCount) / mergedCount);

            destValue.putDouble(valueIndex, mergedMean);
            destValue.putDouble(valueIndex + 1, mergedSum);
            destValue.putLong(valueIndex + 2, mergedCount);
        }

        @Override
        public boolean supportsParallelism() {
            return true;
        }
    }
}
