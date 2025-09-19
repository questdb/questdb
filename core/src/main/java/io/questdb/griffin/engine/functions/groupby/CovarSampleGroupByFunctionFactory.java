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

public class CovarSampleGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "covar_samp(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new CovarSampleGroupByFunction(args.getQuick(0), args.getQuick(1));
    }

    public static class CovarSampleGroupByFunction extends AbstractCovarGroupByFunction {

        protected CovarSampleGroupByFunction(@NotNull Function arg0, @NotNull Function arg1) {
            super(arg0, arg1);
        }

        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 3);
            if (count - 1 > 0) {
                double sumXY = rec.getDouble(valueIndex + 2);
                return sumXY / (count - 1);
            }
            return Double.NaN;
        }

        @Override
        public String getName() {
            return "covar_samp";
        }

        // Chan et al. [CGL82; CGL83]
        @Override
        public void merge(MapValue destValue, MapValue srcValue) {
            double srcMeanY = srcValue.getDouble(valueIndex);
            double srcMeanX = srcValue.getDouble(valueIndex + 1);
            double srcSumXY = srcValue.getDouble(valueIndex + 2);
            long srcCount = srcValue.getLong(valueIndex + 3);

            double destMeanY = destValue.getDouble(valueIndex);
            double destMeanX = destValue.getDouble(valueIndex + 1);
            double destSumXY = destValue.getDouble(valueIndex + 2);
            long destCount = destValue.getLong(valueIndex + 3);

            long mergedCount = srcCount + destCount;
            double deltaY = destMeanY - srcMeanY;
            double deltaX = destMeanX - srcMeanX;

            // This is only valid when countA is much larger than countB.
            // If both are large and similar sizes, delta is not scaled down.
            // double mergedMean = srcMean + delta * ((double) destCount / mergedCount);

            // So we use this instead:
            double mergedMeanY = (srcCount * srcMeanY + destCount * destMeanY) / mergedCount;
            double mergedMeanX = (srcCount * srcMeanX + destCount * destMeanX) / mergedCount;
            double mergedSumXY = srcSumXY + destSumXY + (deltaY * deltaX) * ((double) srcCount * destCount / mergedCount);

            destValue.putDouble(valueIndex, mergedMeanY);
            destValue.putDouble(valueIndex + 1, mergedMeanX);
            destValue.putDouble(valueIndex + 2, mergedSumXY);
            destValue.putLong(valueIndex + 3, mergedCount);
        }

        @Override
        public boolean supportsParallelism() {
            return true;
        }
    }
}
