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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

public class KurtosisSampleGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "kurtosis_samp(D)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new KurtosisSampleGroupByFunction(args.getQuick(0));
    }

    private static class KurtosisSampleGroupByFunction extends AbstractKurtosisGroupByFunction {

        public KurtosisSampleGroupByFunction(@NotNull Function arg) {
            super(arg);
        }

        @Override
        public double getDouble(Record rec) {
            long n = rec.getLong(valueIndex + 4);
            if (n < 4) {
                return Double.NaN;
            }
            double m2 = rec.getDouble(valueIndex + 1);
            if (m2 == 0.0) {
                // all observations equal: kurtosis is undefined
                return Double.NaN;
            }
            double m4 = rec.getDouble(valueIndex + 3);
            double nd = n;
            // population excess kurtosis, then Fisher-Pearson bias correction
            double g2 = nd * m4 / (m2 * m2) - 3.0;
            return ((nd - 1) / ((nd - 2) * (nd - 3))) * ((nd + 1) * g2 + 6);
        }

        @Override
        public String getName() {
            return "kurtosis_samp";
        }

        @Override
        public int getSampleByFlags() {
            return GroupByFunction.SAMPLE_BY_FILL_ALL;
        }
    }
}
