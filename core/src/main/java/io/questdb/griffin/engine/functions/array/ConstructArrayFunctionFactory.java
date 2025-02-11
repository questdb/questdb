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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.FunctionArray;
import io.questdb.cairo.arr.HeapLongArray;
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import static io.questdb.cairo.ColumnType.commonWideningType;
import static io.questdb.cairo.ColumnType.decodeArrayElementType;

public class ConstructArrayFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "[,](V)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return new FunctionArrayFunction(args, argPositions);
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) {
        return ColumnType.ARRAY;
    }

    private static class ConstantArrayFunction extends ArrayFunction {
        private final HeapLongArray array;

        public ConstantArrayFunction(ObjList<Function> args, IntList argPositions) throws SqlException {
            int outerDimLen = args.size();
            Function arg0 = args.getQuick(0);
            if (ColumnType.isArray(arg0.getType())) {
                ArrayView array0 = arg0.getArray(null);
                final int nestedNDims = array0.getDimCount();
                final int nestedElemCount = array0.getFlatElemCount();
                for (int n = args.size(), i = 1; i < n; i++) {
                    Function argI = args.getQuick(i);
                    if (!ColumnType.isArray(argI.getType())) {
                        throw SqlException.$(argPositions.getQuick(i), "not an array");
                    }
                    ArrayView arrayI = argI.getArray(null);
                    if (arrayI.getDimCount() != nestedNDims) {
                        throw SqlException.$(argPositions.getQuick(i), "mismatched array shape");
                    }
                    assert arrayI.getFlatElemCount() == nestedElemCount : "flat element counts don't match";
                }
                this.array = new HeapLongArray(nestedNDims + 1);
                array.setDimLen(0, outerDimLen);
                for (int i = 0; i < nestedNDims; i++) {
                    array.setDimLen(i + 1, array0.getDimLen(i));
                }
                array.applyShape();
                int flatIndex = 0;
                for (int i = 0; i < outerDimLen; i++) {
                    ArrayView arrayI = args.getQuick(i).getArray(null);
                    for (int j = 0; j < nestedElemCount; j++) {
                        array.putLong(flatIndex++, arrayI.getLongAtFlatIndex(j));
                    }
                }
            } else {
                this.array = new HeapLongArray(1);
                array.setDimLen(0, outerDimLen);
                array.applyShape();
                for (int i = 0; i < outerDimLen; i++) {
                    array.putLong(i, args.getQuick(i).getLong(null));
                }
            }
            this.type = array.getType();
        }

        @Override
        public void assignType(int type, BindVariableService bindVariableService) {
        }

        @Override
        public ArrayView getArray(Record rec) {
            return array;
        }
    }

    private static class FunctionArrayFunction extends ArrayFunction {
        private final FunctionArray array;

        public FunctionArrayFunction(ObjList<Function> args, IntList argPositions) throws SqlException {
            int outerDimLen = args.size();
            Function arg0 = args.getQuick(0);
            short type0 = (short) arg0.getType();
            short commonElemType = type0;
            if (ColumnType.isArray(type0)) {
                commonElemType = decodeArrayElementType(type0);
                FunctionArray array0 = (FunctionArray) arg0.getArray(null);
                final int nestedNDims = array0.getDimCount();
                final int nestedElemCount = array0.getFlatElemCount();
                for (int n = args.size(), i = 1; i < n; i++) {
                    Function argI = args.getQuick(i);
                    int typeI = argI.getType();
                    if (!ColumnType.isArray(typeI)) {
                        throw SqlException.$(argPositions.getQuick(i), "not an array");
                    }
                    commonElemType = commonWideningType(commonElemType, decodeArrayElementType(typeI));
                    ArrayView arrayI = argI.getArray(null);
                    if (arrayI.getDimCount() != nestedNDims) {
                        throw SqlException.$(argPositions.getQuick(i), "mismatched array shape");
                    }
                    assert arrayI.getFlatElemCount() == nestedElemCount : "flat element counts don't match";
                }
                this.array = new FunctionArray(commonElemType, nestedNDims + 1);
                array.setDimLen(0, outerDimLen);
                for (int i = 0; i < nestedNDims; i++) {
                    array.setDimLen(i + 1, array0.getDimLen(i));
                }
                array.applyShape();
                int flatIndex = 0;
                for (int i = 0; i < outerDimLen; i++) {
                    FunctionArray arrayI = (FunctionArray) args.getQuick(i).getArray(null);
                    for (int j = 0; j < nestedElemCount; j++) {
                        array.putFunction(flatIndex++, arrayI.getFunctionAtFlatIndex(j));
                    }
                }
            } else {
                for (int i = 1; i < outerDimLen; i++) {
                    short typeI = (short) args.getQuick(i).getType();
                    if (ColumnType.isArray(typeI)) {
                        throw SqlException.$(argPositions.getQuick(i), "array out of place");
                    }
                    commonElemType = commonWideningType(commonElemType, typeI);
                }
                this.array = new FunctionArray(commonElemType, 1);
                array.setDimLen(0, outerDimLen);
                array.applyShape();
                for (int i = 0; i < outerDimLen; i++) {
                    array.putFunction(i, args.getQuick(i));
                }
            }
            this.type = array.getType();
        }

        @Override
        public ArrayView getArray(Record rec) {
            array.setRecord(rec);
            return array;
        }
    }
}
