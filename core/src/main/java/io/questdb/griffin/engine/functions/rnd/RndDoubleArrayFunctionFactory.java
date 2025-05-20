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
import io.questdb.cairo.arr.DirectArray;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Transient;

public class RndDoubleArrayFunctionFactory implements FunctionFactory {
    private static final int MAX_DIM_LEN = 16;

    @Override
    public String getSignature() {
        return "rnd_double_array(v)";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        // The following forms are supported:
        // - rnd_double_array(dimensionCount) - 1 arg, generates random dim lengths up to 16 and random element values. No NaNs.
        // - rnd_double_array(dimensionCount, nanRate) - 2 args, generates random dim lengths up to 16 and random element values. NaN frequency is using (rndInt() % nanRate == 0)
        // - rnd_double_array(dimensionCount, nanRate, maxDimLength) - 3 args, same as the previous, except maxDimLen is not 16 but whatever is provided as the arg
        // - rnd_double_array(dimensionCount, nanRate, 0, dim1Len, dim2Len, dim3Len, ...) - 4+ args - generates fixed size array with random elements, NaN rate is as the above
        final int dimensionCount;
        if (args == null || args.size() == 0 || (dimensionCount = args.getQuick(0).getInt(null)) <= 0) {
            return NullConstant.NULL;
        }

        if (args.size() == 1) {
            return new RndDoubleArrayRndDimLenFunction(configuration, dimensionCount, 0, MAX_DIM_LEN, position);
        }

        final int nanRate = args.getQuick(1).getInt(null);
        if (nanRate < 0) {
            throw SqlException.$(argPositions.getQuick(1), "invalid NaN rate [nanRate=").put(nanRate).put(']');
        }

        if (args.size() == 2) {
            return new RndDoubleArrayRndDimLenFunction(configuration, dimensionCount, nanRate, MAX_DIM_LEN, position);
        }

        final int maxDimLen = args.getQuick(2).getInt(null);
        // ignore validation for signature where user provides fixed size array
        if (maxDimLen <= 0 && args.size() == 3) {
            throw SqlException.$(argPositions.getQuick(2), "maxDimLen must be positive int [maxDimLen=").put(maxDimLen).put(']');
        }

        if (args.size() == 3) {
            return new RndDoubleArrayRndDimLenFunction(configuration, dimensionCount, nanRate, maxDimLen, position);
        }

        if (args.size() < dimensionCount + 3) {
            throw SqlException.$(argPositions.getQuick(args.size() - 1), "not enough values for dim length [dimensionCount=").put(dimensionCount)
                    .put(", dimLengths=").put(args.size() - 3)
                    .put(']');
        }

        if (args.size() > dimensionCount + 3) {
            throw SqlException.$(argPositions.getQuick(dimensionCount + 3), "too many values for dim length [dimensionCount=").put(dimensionCount)
                    .put(", dimLengths=").put(args.size() - 3)
                    .put(']');
        }

        final IntList dimLens = new IntList(dimensionCount);
        for (int i = 3, n = args.size(); i < n; i++) {
            dimLens.add(args.getQuick(i).getInt(null));
        }

        return new RndDoubleArrayFixDimLenFunction(configuration, dimensionCount, nanRate, dimLens, position);
    }

    public static class RndDoubleArrayFixDimLenFunction extends ArrayFunction {
        private final IntList dimLens;
        private final int functionPosition;
        private final int nDims;
        private final int nanRate;
        private DirectArray array;
        private Rnd rnd;

        public RndDoubleArrayFixDimLenFunction(CairoConfiguration configuration, int nDims, int nanRate, IntList dimLens, int functionPosition) {
            try {
                this.nanRate = nanRate + 1;
                this.nDims = nDims;
                this.array = new DirectArray(configuration);
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
                this.array.setType(type);
                this.dimLens = dimLens;
                this.functionPosition = functionPosition;
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            this.array = Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            regenerate();
            return array;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_double_array").val('(')
                    .val(array.getDimCount()).val(',')
                    .val(nanRate - 1).val(',')
                    .val("ignored,");
            for (int i = 0, n = dimLens.size(); i < n; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val(dimLens.getQuick(i));
            }
            sink.val(')');
        }

        private void regenerate() {
            rnd.nextDoubleArray(nDims, array, nanRate, dimLens, functionPosition);
        }
    }

    public static class RndDoubleArrayRndDimLenFunction extends ArrayFunction {
        private final int functionPosition;
        private final int maxDimLen;
        private final int nDims;
        private final int nanRate;
        private DirectArray array;
        private Rnd rnd;

        public RndDoubleArrayRndDimLenFunction(CairoConfiguration configuration, int nDims, int nanRate, int maxDimLen, int functionPosition) {
            try {
                this.nanRate = nanRate + 1;
                this.nDims = nDims;
                this.array = new DirectArray(configuration);
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
                this.array.setType(type);
                this.maxDimLen = maxDimLen;
                this.functionPosition = functionPosition;
            } catch (Throwable th) {
                close();
                throw th;
            }
        }

        @Override
        public void close() {
            this.array = Misc.free(array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            regenerate();
            return array;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_double_array").val('(')
                    .val(array.getDimCount()).val(',')
                    .val(nanRate - 1).val(',')
                    .val(maxDimLen)
                    .val(')');
        }

        private void regenerate() {
            rnd.nextDoubleArray(nDims, array, nanRate, maxDimLen, functionPosition);
        }
    }
}
