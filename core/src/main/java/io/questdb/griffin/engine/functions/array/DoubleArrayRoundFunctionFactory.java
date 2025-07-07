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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

public class DoubleArrayRoundFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "round(D[]I)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        final Function scale = args.getQuick(1);
        return new Func(arg, scale, configuration);
    }

    private static class Func extends DoubleArrayAndScalarIntArrayOperator {
        public Func(Function arrayArg, Function scalarArg, CairoConfiguration configuration) {
            super("round", arrayArg, scalarArg, configuration);
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            memory.putDouble(roundNc(view.getDouble(index), scalarValue));
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            FlatArrayView flatView = view.flatView();
            for (int i = view.getFlatViewOffset(), n = view.getFlatViewOffset() + view.getFlatViewLength(); i < n; i++) {
                memory.putDouble(roundNc(flatView.getDoubleAtAbsIndex(i), scalarValue));
            }
        }

        private double roundNc(double d, int i) {
            if (Numbers.isNull(d) || Numbers.INT_NULL == i) {
                return Double.NaN;
            }

            try {
                return Numbers.roundHalfUp(d, i);
            } catch (NumericException ignore) {
                return Double.NaN;
            }
        }
    }
}
