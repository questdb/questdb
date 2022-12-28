/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class CotDoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "cot(D)";
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function angleRad = args.getQuick(0);
        if (angleRad.isConstant()) {
            return new DoubleConstant(cot(angleRad.getDouble(null)));
        }
        return new CotFunction(args.getQuick(0));
    }

    private static double cot(double angle) {
        if (Double.isNaN(angle)) {
            return Double.NaN;
        }
        double tan = StrictMath.tan(angle);
        if (Math.abs(tan) >= 0.000000000000001) {
            return 1.0 / tan;
        }
        return tan > 0.0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
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
            return cot(angleRad.getDouble(rec));
        }
    }
}
