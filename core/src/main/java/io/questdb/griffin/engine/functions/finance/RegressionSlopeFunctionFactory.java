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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.groupby.AbstractCovarGroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class RegressionSlopeFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "regr_slope(DD)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new RegressionSlopeFunction(args.getQuick(0), args.getQuick(1));
    }


    private static class RegressionSlopeFunction extends AbstractCovarGroupByFunction {

        public RegressionSlopeFunction(@NotNull Function arg0, @NotNull Function arg1) {
            super(arg0, arg1);

        }


        @Override
        public double getDouble(Record rec) {
            long count = rec.getLong(valueIndex + 3);
            double covXY = rec.getDouble(valueIndex + 2) / count;
            double varX = rec.getDouble(valueIndex + 4) / count;

            if (varX == 0) {
                return Double.NaN;
            }

            return covXY / varX;
        }

        @Override
        public String getName() {
            return "regr_slope";
        }
    }
}
