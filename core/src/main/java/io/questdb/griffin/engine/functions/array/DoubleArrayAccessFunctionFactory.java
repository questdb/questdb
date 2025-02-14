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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

public class DoubleArrayAccessFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "[](D[]V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        Function arrayArg = args.getQuick(0);
        args.remove(0);
        return new DoubleArrayAccessFunction(arrayArg, args);
    }

    private static class DoubleArrayAccessFunction extends DoubleFunction {

        private final Function arrayArg;
        private final ObjList<Function> indexArgs;

        DoubleArrayAccessFunction(Function arrayArg, ObjList<Function> indexArgs) {
            this.arrayArg = arrayArg;
            this.indexArgs = indexArgs;
        }

        @Override
        public double getDouble(Record rec) {
            ArrayView array = arrayArg.getArray(rec);
            int flatIndex = 0;
            for (int n = indexArgs.size(), dim = 0; dim < n; dim++) {
                int indexAtDim = indexArgs.getQuick(dim).getInt(rec);
                int strideAtDim = array.getStride(dim);
                flatIndex += strideAtDim * indexAtDim;
            }
            return array.getDoubleAtFlatIndex(flatIndex);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("[](").val(arrayArg);
            for (int n = indexArgs.size(), i = 0; i < n; i++) {
                sink.val(',').val(indexArgs.getQuick(i));
            }
            sink.val(')');
        }
    }
}
