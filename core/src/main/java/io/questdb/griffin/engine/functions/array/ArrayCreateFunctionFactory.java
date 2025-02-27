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
import io.questdb.cairo.sql.ArrayFunction;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

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
        return new FunctionArrayFunction(args, argPositions);
    }

    @Override
    public int resolvePreferredVariadicType(int sqlPos, int argPos, ObjList<Function> args) {
        return ColumnType.ARRAY;
    }

    private static class FunctionArrayFunction extends ArrayFunction {
        private FunctionArray array;

        public FunctionArrayFunction(ObjList<Function> args, IntList argPositions) throws SqlException {
            try {
                int outerDimLen = args.size();
                Function arg0 = args.getQuick(0);
                short type0 = (short) arg0.getType();
                short commonElemType = type0;
                if (ColumnType.isArray(type0)) {
                    commonElemType = decodeArrayElementType(type0);
                    FunctionArray array0 = (FunctionArray) arg0.getArray(null);
                    final int nestedNDims = array0.getDimCount();
                    final int nestedElemCount = array0.getFlatViewLength();
                    for (int n = args.size(), i = 1; i < n; i++) {
                        Function argI = args.getQuick(i);
                        int typeI = argI.getType();
                        int argPos = argPositions.getQuick(i);
                        if (!ColumnType.isArray(typeI)) {
                            throw SqlException.$(argPos, "mixed array and non-array elements");
                        }
                        commonElemType = commonWideningType(commonElemType, decodeArrayElementType(typeI));
                        ArrayView arrayI = argI.getArray(null);
                        if (arrayI.getDimCount() != nestedNDims) {
                            throw SqlException.$(argPos, "mismatched array shape");
                        }
                        if (arrayI.getFlatViewLength() != nestedElemCount) {
                            throw SqlException.$(argPos, "element counts in sub-arrays don't match");
                        }
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
                            throw SqlException.$(argPositions.getQuick(i), "mixed array and non-array elements");
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
            } finally {
                for (int n = args.size(), i = 0; i < n; i++) {
                    Function arg = args.getQuick(i);
                    if (ColumnType.isArray(arg.getType())) {
                        arg.close();
                    }
                }
            }
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
        public void toPlan(PlanSink sink) {
            sink.val("ARRAY[]");
        }
    }
}
