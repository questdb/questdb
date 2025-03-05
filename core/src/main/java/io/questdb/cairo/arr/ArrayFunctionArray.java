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

package io.questdb.cairo.arr;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class ArrayFunctionArray extends MutableArray implements FlatArrayView {

    private final IntList argPositions;
    private final ObjList<Function> arrayFunctions;
    private final ObjList<ArrayView> arrays;

    public ArrayFunctionArray(
            ObjList<Function> arrayFunctions,
            IntList argPositions,
            int arrayType
    ) {
        this.arrayFunctions = arrayFunctions;
        this.arrays = new ObjList<>(arrayFunctions.size());
        this.argPositions = argPositions;
        setType(arrayType);
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem) {
        for (int n = arrays.size(), i = 0; i < n; i++) {
            arrays.getQuick(i).flatView.appendToMemFlat(mem);
        }
    }

    @Override
    public void close() {
        Misc.freeObjList(arrayFunctions);
    }

    @Override
    public short elemType() {
        return ColumnType.decodeArrayElementType(this.type);
    }

    @Override
    public double getDouble(int flatIndex) {
        int stride0 = getStride(0);
        int arrayIndex = flatIndex / stride0;
        validateIndex(arrayIndex, flatIndex);
        return arrays.getQuick(arrayIndex).flatView.getDouble(flatIndex % stride0);
    }

    @Override
    public long getLong(int flatIndex) {
        int stride0 = getStride(0);
        int arrayIndex = flatIndex / stride0;
        validateIndex(arrayIndex, flatIndex);
        return arrays.getQuick(arrayIndex).flatView.getLong(flatIndex % stride0);
    }

    @Override
    public int length() {
        return flatViewLength;
    }

    public void setRecord(Record rec) {
        shape.clear();
        shape.add(arrayFunctions.size());
        arrays.clear();
        ArrayView array0 = arrayFunctions.getQuick(0).getArray(rec);
        arrays.add(array0);
        for (int i = 0, nDims = array0.getDimCount(); i < nDims; i++) {
            shape.add(array0.getDimLen(i));
        }
        resetToDefaultStrides();
        int flatLength0 = array0.getFlatViewLength();
        for (int n = arrayFunctions.size(), i = 1; i < n; i++) {
            ArrayView array = arrayFunctions.getQuick(i).getArray(rec);
            int flatLengthI = array.getFlatViewLength();
            int argPosI = argPositions.getQuick(i);
            if (flatLengthI != flatLength0) {
                throw CairoException.nonCritical().position(argPosI)
                        .put("array element counts don't match [count0=").put(flatLength0)
                        .put(", count").put(i).put('=').put(flatLengthI).put(']');
            }
            arrays.add(array);
        }
        this.flatViewLength = flatLength0 * arrayFunctions.size();
    }

    private void validateIndex(int arrayIndex, int flatIndex) {
        if (arrayIndex < 0 || arrayIndex > arrays.size()) {
            throw CairoException.nonCritical().put("flatIndex out of range [flatIndex=").put(flatIndex);
        }
    }
}
