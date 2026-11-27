/*******************************************************************************
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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

/**
 * {@code regr_slope_tstat(y, x)} - the t-statistic for the slope of the simple
 * linear regression of {@code y} on {@code x}, testing the null hypothesis that
 * the true slope is zero. It is {@code slope / SE(slope)} where
 * {@code slope = Sxy / Sxx} and the standard error of the slope is
 * {@code SE(slope) = sqrt( (SSE / (n - 2)) / Sxx )}, with
 * {@code SSE = Syy - Sxy^2 / Sxx} the residual sum of squares. The statistic has
 * {@code n - 2} degrees of freedom, so at least three points are required.
 */
public class RegressionSlopeTStatFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_slope_tstat(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionSlopeTStatFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class RegressionSlopeTStatFunction extends AbstractRegressionGroupByFunction {

        public RegressionSlopeTStatFunction(@NotNull Function arg0, @NotNull Function arg1) {
            super(arg0, arg1);
        }

        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 5);
            // The t-statistic has n - 2 degrees of freedom, so it needs n >= 3.
            if (count < 3) {
                return Double.NaN;
            }
            double sumX = rec.getDouble(valueIndex + 3);
            if (sumX == 0) {
                return Double.NaN;
            }
            double sumY = rec.getDouble(valueIndex + 1);
            double sumXY = rec.getDouble(valueIndex + 4);
            double sse = sumY - (sumXY * sumXY) / sumX;
            // A perfect fit (SSE = 0) leaves the standard error at zero and the
            // t-statistic undefined; treat it as NULL rather than +/-Infinity.
            if (sse <= 0) {
                return Double.NaN;
            }
            double slope = sumXY / sumX;
            double seSlope = Math.sqrt((sse / (count - 2)) / sumX);
            return slope / seSlope;
        }

        @Override
        public String getName() {
            return "regr_slope_tstat";
        }
    }
}
