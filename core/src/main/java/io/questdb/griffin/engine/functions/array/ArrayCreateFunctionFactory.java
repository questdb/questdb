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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.DirectArray;
import io.questdb.cairo.arr.FunctionArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.ArrayConstant;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.ColumnType.commonWideningType;
import static io.questdb.cairo.ColumnType.decodeArrayElementType;

public class ArrayCreateFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "array(V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        int outerDimLen = args == null ? 0 : args.size();
        if (outerDimLen == 0) {
            return ArrayConstant.emptyUntyped(1);
        }
        Function arg0 = args.getQuick(0);
        int arg0Pos = argPositions.getQuick(0);
        int type0 = arg0.getType();
        short commonElemType = (short) type0;
        boolean isConstant = arg0.isConstant();
        if (!ColumnType.isArray(type0)) {
            for (int i = 1; i < outerDimLen; i++) {
                Function argI = args.getQuick(i);
                short typeI = (short) argI.getType();
                if (ColumnType.isArray(typeI)) {
                    throw SqlException.$(argPositions.getQuick(i), "mixed array and non-array elements");
                }
                isConstant &= argI.isConstant();
                // use commonWideningType(commonElemType, typeI) once we support more than the DOUBLE array type:
                commonElemType = ColumnType.DOUBLE;
            }
            if (!ColumnType.isSupportedArrayElementType(commonElemType)) {
                throw SqlException.position(arg0Pos)
                        .put("unsupported array element type [type=")
                        .put(ColumnType.nameOf(commonElemType))
                        .put(']');
            }
            FunctionArray array = new FunctionArray(commonElemType, 1);
            array.setDimLen(0, outerDimLen);
            array.applyShape(configuration, arg0Pos);
            for (int i = 0; i < outerDimLen; i++) {
                Function argI = args.getQuick(i);
                array.putFunction(i, argI);
            }
            return new FunctionArrayFunction(array, isConstant);
        }

        // First argument is an array, validate that all of them arrays. Mixed array and
        // non-array arguments aren't allowed, because they must all come together to form
        // a new array with one more dimension.
        commonElemType = ColumnType.decodeArrayElementType(type0);
        final int nestedNDims = ColumnType.decodeArrayDimensionality(arg0.getType());
        for (int n = args.size(), i = 1; i < n; i++) {
            Function argI = args.getQuick(i);
            int typeI = argI.getType();
            int argPosI = argPositions.getQuick(i);
            if (!ColumnType.isArray(typeI)) {
                throw SqlException.$(argPosI, "mixed array and non-array elements");
            }
            if (ColumnType.decodeArrayDimensionality(typeI) != nestedNDims) {
                throw SqlException.$(argPosI, "sub-arrays don't match in number of dimensions");
            }
            commonElemType = commonWideningType(commonElemType, decodeArrayElementType(typeI));
            isConstant &= argI.isConstant();
        }

        function_array:
        if (arg0 instanceof FunctionArrayFunction) {
            for (int i = 1; i < outerDimLen; i++) {
                if (!(args.getQuick(i) instanceof FunctionArrayFunction)) {
                    break function_array;
                }
            }
            // All arguments are of type FunctionArrayFunction, we can gather all their
            // functions into a new single FunctionArrayFunction
            FunctionArray array0 = (FunctionArray) arg0.getArray(null);
            final int nestedElemCount = array0.getFlatViewLength();
            for (int n = args.size(), i = 1; i < n; i++) {
                if (args.getQuick(i).getArray(null).getFlatViewLength() != nestedElemCount) {
                    throw SqlException.$(argPositions.getQuick(i), "element counts in sub-arrays don't match");
                }
            }
            FunctionArray array = new FunctionArray(commonElemType, nestedNDims + 1);
            array.setDimLen(0, outerDimLen);
            for (int i = 0; i < nestedNDims; i++) {
                array.setDimLen(i + 1, array0.getDimLen(i));
            }
            array.applyShape(configuration, arg0Pos);
            int flatIndex = 0;
            for (int i = 0; i < outerDimLen; i++) {
                FunctionArray arrayI = (FunctionArray) args.getQuick(i).getArray(null);
                for (int j = 0; j < nestedElemCount; j++) {
                    array.putFunction(flatIndex++, arrayI.getFunctionAtFlatIndex(j));
                }
            }
            return new FunctionArrayFunction(array, isConstant);
        }

        // Arguments aren't all FunctionArrayFunctions, treat them generically as some kind of array functions.
        return new ArrayFunctionArrayFunction(
                configuration,
                new ObjList<>(args),
                new IntList(argPositions),
                commonElemType,
                nestedNDims,
                isConstant
        );
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) {
        return ColumnType.ARRAY;
    }

    private static class ArrayFunctionArrayFunction extends ArrayFunction {
        private final @NotNull IntList argPositions;
        private final @NotNull ObjList<Function> args;
        private final boolean isConstant;
        private DirectArray arrayOut;

        public ArrayFunctionArrayFunction(
                @NotNull CairoConfiguration configuration,
                @NotNull ObjList<Function> args,
                @NotNull IntList argPositions,
                short commonElemType,
                int nestedNDims,
                boolean isConstant
        ) {
            this.type = ColumnType.encodeArrayType(commonElemType, nestedNDims + 1);
            this.isConstant = isConstant;
            this.args = args;
            this.argPositions = argPositions;
            this.arrayOut = new DirectArray(configuration);
            arrayOut.setType(type);
        }

        @Override
        public void assignType(int type, BindVariableService bindVariableService) {
            this.type = type;
            arrayOut.setType(type);
        }

        @Override
        public void close() {
            this.arrayOut = Misc.free(this.arrayOut);
            Misc.freeObjList(args);
        }

        @Override
        public ArrayView getArray(Record rec) {
            ArrayView array0 = args.getQuick(0).getArray(rec);
            short type0 = array0.getElemType();
            short outType = arrayOut.getElemType();
            if (type0 != ColumnType.UNDEFINED && type0 != outType) {
                throw CairoException.nonCritical().position(argPositions.getQuick(0))
                        .put("sub-array has different type [subArrayType=").put(type0)
                        .put(", thisArrayType=").put(outType);
            }
            int nDims = array0.getDimCount();
            arrayOut.clear();
            arrayOut.setDimLen(0, args.size());
            for (int dim = 0; dim < nDims; dim++) {
                arrayOut.setDimLen(dim + 1, array0.getDimLen(dim));
            }
            arrayOut.applyShape(argPositions.getQuick(0));
            MemoryA memA = arrayOut.startMemoryA();
            array0.appendToMem(memA);
            for (int n = args.size(), i = 1; i < n; i++) {
                ArrayView arrayI = args.getQuick(i).getArray(rec);
                int argPosI = argPositions.getQuick(i);
                for (int dim = 0; dim < nDims; dim++) {
                    if (arrayI.getDimLen(dim) != arrayOut.getDimLen(dim + 1)) {
                        throw CairoException.nonCritical().position(argPosI)
                                .put("array shapes don't match");
                    }
                }
                arrayI.appendToMem(memA);
            }
            return arrayOut;
        }

        @Override
        public boolean isConstant() {
            return isConstant;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("ARRAY[");
            String comma = "";
            for (int n = args.size(), i = 0; i < n; i++) {
                sink.val(comma);
                sink.val(args.getQuick(i));
                comma = ",";
            }
            sink.val(']');
        }
    }

    private static class FunctionArrayFunction extends ArrayFunction {
        private final boolean isConstant;
        private FunctionArray array;

        public FunctionArrayFunction(FunctionArray array, boolean isConstant) {
            this.array = array;
            this.isConstant = isConstant;
            this.type = array.getType();
        }

        @Override
        public void assignType(int type, BindVariableService bindVariableService) {
            assert array.isEmpty() : "array is not empty";
            this.type = type;
            array.setType(type);
        }

        @Override
        public void close() {
            this.array = Misc.free(this.array);
        }

        @Override
        public ArrayView getArray(Record rec) {
            array.setRecord(rec);
            return array;
        }

        @Override
        public boolean isConstant() {
            return isConstant;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("ARRAY");
            array.toPlan(sink);
        }
    }
}
