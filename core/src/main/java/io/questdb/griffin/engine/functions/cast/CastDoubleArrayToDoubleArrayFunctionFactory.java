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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DerivedArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public final class CastDoubleArrayToDoubleArrayFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(D[]d[])";
    }

    @Override
    public Function newInstance(
            int position,
            @Transient ObjList<Function> args,
            @Transient IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function fromFunc = args.getQuick(0);
        final int fromType = fromFunc.getType();
        final int toType = args.getQuick(1).getType();

        final int dimsTo = ColumnType.decodeWeakArrayDimensionality(toType);
        final int dimsFrom = ColumnType.decodeWeakArrayDimensionality(fromType);
        if (dimsTo == -1) {
            throw SqlException.$(position, "cannot cast array to array with unknown number of dimensions");
        }
        if (dimsFrom == -1) {
            return new WeakDimsFunc(fromFunc, toType, argPositions.getQuick(0));
        }
        final int dimsToAdd = dimsTo - dimsFrom;
        if (dimsToAdd < 0) {
            throw SqlException.$(position, "cannot cast array to lower dimension [from=").put(ColumnType.nameOf(fromType))
                    .put(" (").put(ColumnType.decodeWeakArrayDimensionality(fromType)).put("D)")
                    .put(", to=").put(ColumnType.nameOf(toType))
                    .put(" (").put(ColumnType.decodeWeakArrayDimensionality(toType)).put("D)")
                    .put("]. Use array flattening operation (e.g. 'flatten(arr)') instead");
        }
        if (dimsToAdd == 0) {
            // nothing to do
            return fromFunc;
        }
        return new Func(fromFunc, toType, dimsToAdd);
    }

    public static final class Func extends ArrayFunction implements UnaryFunction {
        private final Function arg;
        private final DerivedArrayView derivedArray = new DerivedArrayView();
        private final int dimsToAdd;

        public Func(Function arg, int toType, int dimsToAdd) {
            assert dimsToAdd > 0;
            this.type = toType;
            this.arg = arg;
            this.dimsToAdd = dimsToAdd;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(derivedArray);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            final ArrayView array = arg.getArray(rec);
            derivedArray.of(array);
            derivedArray.prependDimensions(dimsToAdd);
            return derivedArray;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    public static final class WeakDimsFunc extends ArrayFunction implements UnaryFunction {
        private final Function arg;
        private final DerivedArrayView derivedArray = new DerivedArrayView();
        private final int position;
        private int dimsToAdd;

        public WeakDimsFunc(Function arg, int toType, int position) {
            this.type = toType;
            this.arg = arg;
            this.position = position;
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(derivedArray);
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            final ArrayView array = arg.getArray(rec);
            derivedArray.of(array);
            derivedArray.prependDimensions(dimsToAdd);
            return derivedArray;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            UnaryFunction.super.init(symbolTableSource, executionContext);
            final int dimsFrom = ColumnType.decodeArrayElementType(arg.getType());
            final int dimsTo = ColumnType.decodeArrayElementType(type);
            dimsToAdd = dimsTo - dimsFrom;
            if (dimsToAdd < 0) {
                throw SqlException.$(position, "cannot cast array to lower dimension [from=").put(ColumnType.nameOf(arg.getType()))
                        .put(" (").put(ColumnType.decodeArrayDimensionality(arg.getType())).put("D)")
                        .put(", to=").put(ColumnType.nameOf(type))
                        .put(" (").put(ColumnType.decodeArrayDimensionality(type)).put("D)")
                        .put("]. Use array flattening operation (e.g. 'flatten(arr)') instead");
            }
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }
}
