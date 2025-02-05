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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndDoubleArrayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "rnd_double_array(ii)";
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
        int nanRate = args.getQuick(1).getInt(null);
        if (nanRate < 0) {
            throw SqlException.$(argPositions.getQuick(0), "invalid NaN rate [nanRate=").put(nanRate).put(']');
        }
        return new RndDoubleArrayFunction(dimensionCount, nanRate);
    }

    public static class ArrayArrayView implements ArrayView {

        private static final int MAX_LEN = 16;
        private final int dimensionCount;
        private final int nanRate;
        private final int[] shape;
        private final int[] strides;
        private final int type;
        private Rnd rnd;
        private int size;
        private double[] values;

        public ArrayArrayView(int dimensionCount, int nanRate, int type) {
            this.dimensionCount = dimensionCount;
            this.shape = new int[dimensionCount];
            this.strides = new int[dimensionCount];
            this.type = type;
            this.nanRate = nanRate;
        }

        @Override
        public void appendRowMajor(MemoryA mem) {
            for (int i = 0; i < size; i++) {
                mem.putDouble(values[i]);
            }
        }

        @Override
        public int getDim() {
            return dimensionCount;
        }

        @Override
        public int getDimLength(int dim) {
            return shape[dim];
        }

        @Override
        public double getDoubleFromRowMajor(int flatIndex) {
            return values[flatIndex];
        }

        @Override
        public long getLongFromRowMajor(int flatIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSize() {
            return size;
        }

        @Override
        public int getStride(int dimension) {
            return strides[dimension];
        }

        @Override
        public int getType() {
            return type;
        }

        public void setRnd(Rnd rnd) {
            this.rnd = rnd;
        }

        private ArrayArrayView refresh() {
            size = 1;
            for (int i = 0; i < dimensionCount; i++) {
                shape[i] = (rnd.nextPositiveInt() % (MAX_LEN - 1)) + 1;
                size *= shape[i];
            }

            int stride = 1;
            for (int i = shape.length - 1; i >= 0; i--) {
                strides[i] = stride;
                stride *= shape[i];
            }

            if (values == null || values.length < size) {
                values = new double[size];
            }

            for (int i = 0; i < size; i++) {
                double val;
                if ((rnd.nextInt() % nanRate) == 1) {
                    val = Double.NaN;
                } else {
                    val = rnd.nextDouble();
                }
                values[i] = val;
            }
            return this;
        }
    }

    public static class RndDoubleArrayFunction extends ArrayFunction {
        private final ArrayArrayView arrayView;
        private final int type;

        public RndDoubleArrayFunction(int dimensionCount, int nanRate) {
            this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensionCount);
            this.arrayView = new ArrayArrayView(dimensionCount, nanRate + 1, this.type);
        }

        @Override
        public ArrayView getArray(Record rec) {
            return arrayView.refresh();
        }

        @Override
        public int getType() {
            return type;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            arrayView.setRnd(executionContext.getRandom());
        }

        @Override
        public void toPlan(PlanSink sink) {
            // todo: include nanRate in plan
            sink.val("rnd_double_array").val('(').val(arrayView.dimensionCount).val(')');
        }
    }
}
