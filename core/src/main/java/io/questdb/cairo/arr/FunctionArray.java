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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;

public class FunctionArray implements ArrayView {

    private final int[] shape;
    private final int[] strides;
    private Function[] functions;
    private Record record;
    private int type;

    public FunctionArray(short elementType, int nDims) {
        this.shape = new int[nDims];
        this.strides = new int[nDims];
        this.type = ColumnType.encodeArrayType(elementType, nDims);
    }

    @Override
    public void appendWithDefaultStrides(MemoryA mem) {
        short elemType = ColumnType.decodeArrayElementType(type);
        for (int n = getFlatElemCount(), i = 0; i < n; i++) {
            Function f = functions[i];
            switch (elemType) {
                case ColumnType.BYTE:
                    mem.putByte(f.getByte(record));
                    break;
                case ColumnType.SHORT:
                    mem.putShort(f.getShort(record));
                    break;
                case ColumnType.INT:
                    mem.putInt(f.getInt(record));
                    break;
                case ColumnType.LONG:
                    mem.putLong(f.getLong(record));
                    break;
                case ColumnType.DATE:
                    mem.putLong(f.getDate(record));
                    break;
                case ColumnType.TIMESTAMP:
                    mem.putLong(f.getTimestamp(record));
                    break;
                case ColumnType.FLOAT:
                    mem.putFloat(f.getFloat(record));
                    break;
                case ColumnType.DOUBLE:
                    mem.putDouble(f.getDouble(record));
                    break;
                case ColumnType.LONG256:
                    throw new UnsupportedOperationException("long256");
                case ColumnType.UUID:
                    throw new UnsupportedOperationException("uuid");
                case ColumnType.IPv4:
                    mem.putInt(f.getIPv4(record));
                    break;
                default:
                    throw new AssertionError("impossible array element type");
            }
        }
    }

    public void applyShape() {
        int stride = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            int dimLen = shape[i];
            if (dimLen == 0) {
                throw new IllegalStateException("Zero dimLen at " + i);
            }
            strides[i] = stride;
            stride *= dimLen;
        }
        if (functions == null || functions.length < stride) {
            functions = new Function[stride];
        }
    }

    @Override
    public int getDimCount() {
        return shape.length;
    }

    @Override
    public int getDimLen(int dimension) {
        return shape[dimension];
    }

    @Override
    public double getDoubleAtFlatIndex(int flatIndex) {
        return functions[flatIndex].getDouble(record);
    }

    @Override
    public int getFlatElemCount() {
        return functions.length;
    }

    public Function getFunctionAtFlatIndex(int flatIndex) {
        return functions[flatIndex];
    }

    @Override
    public long getLongAtFlatIndex(int flatIndex) {
        return functions[flatIndex].getLong(record);
    }

    @Override
    public int getStride(int dimension) {
        return strides[dimension];
    }

    @Override
    public int getType() {
        return type;
    }

    public void putFunction(int flatIndex, Function f) {
        functions[flatIndex] = f;
    }

    public void setDimLen(int dim, int len) {
        shape[dim] = len;
    }

    public void setRecord(Record rec) {
        this.record = rec;
    }

    public void setType(int type) {
        this.type = type;
    }
}
