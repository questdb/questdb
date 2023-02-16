/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CotDoubleFunctionFactory implements FunctionFactory {
    public static final String SYMBOL = "cot";

    @Override
    public String getSignature() {
        return SYMBOL + "(D)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new CotFunction(args.getQuick(0));
    }

    private static class CotFunction extends DoubleFunction implements ScalarFunction, UnaryFunction {
        final Function angleRad;

        public CotFunction(Function angleRad) {
            this.angleRad = angleRad;
        }

        @Override
        public Function getArg() {
            return angleRad;
        }

        @Override
        public double getDouble(Record rec) {
            double angle = angleRad.getDouble(rec);
            if (Double.isNaN(angle) || Double.isInfinite(angle)) {
                return Double.NaN;
            }
            return 1.0 / Math.tan(angle);
        }

        @Override
        public String getName() {
            return SYMBOL;
        }
    }
}
