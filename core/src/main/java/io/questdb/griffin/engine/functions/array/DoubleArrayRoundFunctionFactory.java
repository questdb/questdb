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
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FlatArrayView;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;

import static io.questdb.std.Numbers.pow10max;

public class DoubleArrayRoundFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "round(D[]I)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final Function arg = args.getQuick(0);
        final Function scale = args.getQuick(1);
        if (scale.isConstant()) {
            int scaleValue = scale.getInt(null);
            if (scaleValue > -1) {
                if (scaleValue + 2 < pow10max) {
                    return new DoubleArrayRoundPositiveScaleFunction(arg, scaleValue, configuration);
                }
                return new DoubleArrayRoundDegenerateScaleFunction(arg, configuration);
            }
        }
        return new DoubleArrayRoundVarScaleFunction(arg, scale, configuration);
    }

    private static class DoubleArrayRoundDegenerateScaleFunction extends ArrayFunction implements DoubleUnaryArrayAccessor, UnaryFunction {
        protected final Function arrayArg;
        private final DirectArray array;
        private final String name;
        protected MemoryA memory;

        public DoubleArrayRoundDegenerateScaleFunction(Function arrayArg, CairoConfiguration configuration) {
            this.name = "round";
            this.arrayArg = arrayArg;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            memory.putDouble(Double.NaN);
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            for (int i = view.getFlatViewOffset(), n = view.getFlatViewOffset() + view.getFlatViewLength(); i < n; i++) {
                memory.putDouble(Double.NaN);
            }
        }

        @Override
        public void applyToNullArray() {
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            array.setType(getType());
            array.copyShapeFrom(arr);
            array.applyShape();
            memory = array.startMemoryA();
            calculate(arr);
            return array;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }
    }

    private static class DoubleArrayRoundPositiveScaleFunction extends ArrayFunction implements DoubleUnaryArrayAccessor, UnaryFunction {
        protected final Function arrayArg;
        protected final int scale;
        private final DirectArray array;
        private final String name;
        protected MemoryA memory;

        public DoubleArrayRoundPositiveScaleFunction(Function arrayArg, int scale, CairoConfiguration configuration) {
            this.name = "round";
            this.arrayArg = arrayArg;
            this.scale = scale;
            this.type = arrayArg.getType();
            this.array = new DirectArray(configuration);
        }

        @Override
        public void applyToElement(ArrayView view, int index) {
            memory.putDouble(roundNc(view.getDouble(index)));
        }

        @Override
        public void applyToEntireVanillaArray(ArrayView view) {
            FlatArrayView flatView = view.flatView();
            for (int i = view.getFlatViewOffset(), n = view.getFlatViewOffset() + view.getFlatViewLength(); i < n; i++) {
                memory.putDouble(roundNc(flatView.getDoubleAtAbsIndex(i)));
            }
        }

        @Override
        public void applyToNullArray() {
        }

        @Override
        public void close() {
            UnaryFunction.super.close();
            Misc.free(array);
        }

        @Override
        public Function getArg() {
            return arrayArg;
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView arr = arrayArg.getArray(rec);
            if (arr.isNull()) {
                array.ofNull();
                return array;
            }

            array.setType(getType());
            array.copyShapeFrom(arr);
            array.applyShape();
            memory = array.startMemoryA();
            calculate(arr);
            return array;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean isOperator() {
            return true;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private double roundNc(double d) {
            if (Numbers.isFinite(d)) {
                return Numbers.roundHalfUpPosScale(d, scale);
            }
            return Double.NaN;
        }
    }

    private static class DoubleArrayRoundVarScaleFunction extends DoubleArrayAndScalarIntArrayOperator {
        public DoubleArrayRoundVarScaleFunction(Function arrayArg, Function scalarArg, CairoConfiguration configuration) {
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
