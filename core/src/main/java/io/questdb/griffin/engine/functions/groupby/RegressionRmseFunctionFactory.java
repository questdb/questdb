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
 * {@code regr_rmse(y, x)} - the root mean square error of the simple linear
 * regression of {@code y} on {@code x}, i.e. {@code sqrt(SSE / n)} where
 * {@code SSE = Syy - Sxy^2 / Sxx} is the residual sum of squares. This is the
 * square root of the mean of the squared residuals; it divides by {@code n}
 * rather than {@code n - 2}, so it is a biased estimator of the error standard
 * deviation but matches the literal "root mean square error" definition.
 */
public class RegressionRmseFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_rmse(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionRmseFunction(args.getQuick(0), args.getQuick(1));
    }

    private static class RegressionRmseFunction extends AbstractRegressionGroupByFunction {

        public RegressionRmseFunction(@NotNull Function arg0, @NotNull Function arg1) {
            super(arg0, arg1);
        }

        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 5);
            if (count <= 0) {
                return Double.NaN;
            }
            // Sxx = 0 means X does not vary, so no regression line exists.
            // Also covers count = 1. Matches regr_slope / regr_r2.
            double sumX = rec.getDouble(valueIndex + 3);
            if (sumX == 0) {
                return Double.NaN;
            }
            double sumY = rec.getDouble(valueIndex + 1);
            double sumXY = rec.getDouble(valueIndex + 4);
            double sse = sumY - (sumXY * sumXY) / sumX;
            // SSE is non-negative in exact arithmetic; clamp tiny negative
            // rounding results so sqrt() does not return NaN for a near-perfect
            // fit.
            if (sse < 0) {
                sse = 0;
            }
            return Math.sqrt(sse / count);
        }

        @Override
        public String getName() {
            return "regr_rmse";
        }
    }
}
