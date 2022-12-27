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
import io.questdb.griffin.engine.functions.constants.DoubleConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class Atan2DoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "atan2(DD)"; // y, x
    }

    @Override
    public Function newInstance(
            int position, ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        Function angleY = args.getQuick(0); // radians
        Function angleX = args.getQuick(1); // radians
        if (angleY.isConstant() && angleX.isConstant()) {
            return new DoubleConstant(Math.atan2(angleY.getDouble(null), angleX.getDouble(null)));
        }
        if (angleY.isConstant()) {
            return new Atan2Function(new DoubleConstant(angleY.getDouble(null)), angleX);
        }
        if (angleX.isConstant()) {
            return new Atan2Function(angleY, new DoubleConstant(angleX.getDouble(null)));
        }

        return new Atan2Function(angleY, angleX);
    }

    private static class Atan2Function extends DoubleFunction implements ScalarFunction {
        final Function xFunction;
        final Function yFunction;

        public Atan2Function(Function yFunction, Function xFunction) {
            this.yFunction = yFunction;
            this.xFunction = xFunction;
        }


        @Override
        public double getDouble(Record rec) {
            return Math.atan2(yFunction.getDouble(rec), xFunction.getDouble(rec));
        }
    }
}
