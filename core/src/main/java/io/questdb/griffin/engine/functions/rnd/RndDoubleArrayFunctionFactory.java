/*******************************************************************************
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
        return "rnd_double_array(lv)";
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
        // - rnd_double_array(nDims) - 1 arg, generates random dim lengths up to 16 and random element values. No NaNs.
        // - rnd_double_array(nDims, nanRate) - 2 args, generates random dim lengths up to 16 and random element values. NaN frequency is using (rndInt() % nanRate == 0)
        // - rnd_double_array(nDims, nanRate, maxDimLength) - 3 args, same as the previous, except maxDimLen is not 16 but whatever is provided as the arg
        // - rnd_double_array(nDims, nanRate, 0, dim1Len, dim2Len, dim3Len, ...) - 4+ args - generates fixed size array with random elements, NaN rate is as the above

        final int nDims = validateAndGetArg(args, argPositions, 0, "nDims");
        if (nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
            throw SqlException.$(argPositions.getQuick(0), "maximum for nDims is ").put(ColumnType.ARRAY_NDIMS_LIMIT)
                    .put(" [nDims=").put(nDims).put(']');
        }
        if (nDims == 0) {
            return NullConstant.NULL;
        }
        if (args.size() == 1) {
            return new RndDimLenFunc(configuration, nDims, 0, MAX_DIM_LEN, position);
        }

        final int nanRate = validateAndGetArg(args, argPositions, 1, "nanRate");
        if (args.size() == 2) {
            return new RndDimLenFunc(configuration, nDims, nanRate, MAX_DIM_LEN, position);
        }

        final int maxDimLen = validateAndGetArg(args, argPositions, 2, "maxDimLength");
        if (args.size() == 3) {
            if (maxDimLen <= 0) {
                throw SqlException.$(argPositions.getQuick(2), "maxDimLength must be a positive integer [maxDimLength=").put(maxDimLen).put(']');
            }
            return new RndDimLenFunc(configuration, nDims, nanRate, maxDimLen, position);
        }

        if (args.size() < nDims + 3) {
            throw SqlException.$(argPositions.getQuick(args.size() - 1), "not enough dim lengths [nDims=").put(nDims)
                    .put(", nDimLengths=").put(args.size() - 3).put(']');
        }
        if (args.size() > nDims + 3) {
            throw SqlException.$(argPositions.getQuick(nDims + 3), "too many dim lengths [nDims=").put(nDims)
                    .put(", nDimLengths=").put(args.size() - 3).put(']');
        }

        final IntList dimLens = new IntList(nDims);
        for (int i = 3, n = args.size(); i < n; i++) {
            dimLens.add(validateAndGetArg(args, argPositions, i, "dimLength"));
        }

        return new FixDimLenFunc(configuration, nDims, nanRate, dimLens, position);
    }

    private static int validateAndGetArg(
            ObjList<Function> args,
            IntList argPositions,
            int argIndex,
            String argName
    ) throws SqlException {
        Function arg = args.getQuick(argIndex);
        int argPosition = argPositions.getQuick(argIndex);
        if (!ColumnType.isSameOrBuiltInWideningCast(arg.getType(), ColumnType.LONG)) {
            throw SqlException.$(argPosition, argName).put(" must be an integer");
        }
        final long valueLong = arg.getLong(null);
        if (valueLong < 0 || valueLong > Integer.MAX_VALUE) {
            throw SqlException.$(argPosition, "invalid ").put(argName).put(" [").put(argName)
                    .put('=').put(valueLong).put(']');
        }
        return (int) valueLong;
    }

    private static class FixDimLenFunc extends ArrayFunction {
        private final IntList dimLens;
        private final int nDims;
        private final int nanRate;
        private final int position;
        private DirectArray array;
        private Rnd rnd;

        public FixDimLenFunc(
                CairoConfiguration configuration,
                int nDims,
                int nanRate,
                IntList dimLens,
                int position
        ) {
            try {
                this.nanRate = nanRate;
                this.nDims = nDims;
                this.array = new DirectArray(configuration);
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
                this.array.setType(type);
                this.dimLens = dimLens;
                this.position = position;
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
            rnd.nextDoubleArray(nDims, array, nanRate, dimLens, position);
            return array;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public boolean shouldMemoize() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_double_array").val('(')
                    .val(array.getDimCount()).val(',')
                    .val(nanRate).val(',')
                    .val("ignored,");
            for (int i = 0, n = dimLens.size(); i < n; i++) {
                if (i > 0) {
                    sink.val(',');
                }
                sink.val(dimLens.getQuick(i));
            }
            sink.val(')');
        }
    }

    private static class RndDimLenFunc extends ArrayFunction {
        private final int maxDimLen;
        private final int nDims;
        private final int nanRate;
        private final int position;
        private DirectArray array;
        private Rnd rnd;

        public RndDimLenFunc(CairoConfiguration configuration, int nDims, int nanRate, int maxDimLen, int position) {
            try {
                this.nanRate = nanRate;
                this.nDims = nDims;
                this.array = new DirectArray(configuration);
                this.type = ColumnType.encodeArrayType(ColumnType.DOUBLE, nDims);
                this.array.setType(type);
                this.maxDimLen = maxDimLen;
                this.position = position;
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
            rnd.nextDoubleArray(nDims, array, nanRate, maxDimLen, position);
            return array;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            this.rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public boolean shouldMemoize() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_double_array").val('(')
                    .val(array.getDimCount()).val(',')
                    .val(nanRate).val(',')
                    .val(maxDimLen)
                    .val(')');
        }
    }
}
