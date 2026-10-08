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

package io.questdb.griffin.engine.functions.array;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DoubleList;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DoubleArrayElemAvgFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array_elem_avg(D[]V)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int resolvedDims = AbstractDoubleArrayElemFunction.validateArgsAndResolveDims(
                position, args, argPositions, "array_elem_avg"
        );
        return new Func(configuration, new ObjList<>(args), resolvedDims);
    }

    private static final class Func extends AbstractDoubleArrayElemFunction {
        private final DoubleList compensation = new DoubleList();
        private final IntList counts = new IntList();

        Func(CairoConfiguration configuration, ObjList<Function> args, int resolvedDims) {
            super(configuration, args, resolvedDims);
        }

        @Override
        public String getName() {
            return "array_elem_avg";
        }

        @Override
        protected void accumulate(int outIndex, double val) {
            kahanAccumulate(outIndex, val, compensation);
            counts.increment(outIndex);
        }

        @Override
        protected void beforeAccumulation(int totalFlatLen) {
            counts.clear();
            counts.setPos(totalFlatLen);
            counts.zero(0);
            compensation.setPos(totalFlatLen);
            compensation.fill(0, totalFlatLen, 0.0);
        }

        @Override
        protected void postProcess(int totalFlatLen) {
            for (int i = 0; i < totalFlatLen; i++) {
                int c = counts.getQuick(i);
                if (c > 0) {
                    arrayOut.putDouble(i, arrayOut.getDouble(i) / c);
                }
            }
        }
    }
}
