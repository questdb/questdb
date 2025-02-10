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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.HeapLongArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndLongArrayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_long_array(i)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int dimensionCount = args.getQuick(0).getInt(null);
        if (dimensionCount <= 0) {
            return NullConstant.NULL;
        }
        return new RndLongArrayFunction(dimensionCount);
    }

    public static class RndLongArrayFunction extends ArrayFunction {
        private static final int MAX_DIM_LEN = 16;
        private final HeapLongArray array;
        private Rnd rnd;

        public RndLongArrayFunction(int nDims) {
            this.array = new HeapLongArray(nDims);
        }

        @Override
        public ArrayView getArray(Record rec) {
            regenerate();
            return array;
        }

        @Override
        public int getType() {
            return array.getType();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.rnd = executionContext.getRandom();
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_long_array").val('(').val(array.getDimCount()).val(')');
        }

        private void regenerate() {
            for (int n = array.getDimCount(), i = 0; i < n; i++) {
                array.setDimLen(i, (rnd.nextPositiveInt() % (MAX_DIM_LEN - 1)) + 1);
            }
            array.applyShape();
            for (int n = array.getFlatElemCount(), i = 0; i < n; i++) {
                array.setLongAtFlatIndex(i, rnd.nextLong());
            }
        }
    }
}
