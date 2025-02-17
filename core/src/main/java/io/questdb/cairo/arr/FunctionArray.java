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
import io.questdb.std.Long256;
import io.questdb.std.Misc;

import java.io.Closeable;
import java.io.IOException;

public class FunctionArray implements ArrayView, FlatArrayView, Closeable {

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
        final short elemType = ColumnType.decodeArrayElementType(type);
        final Function[] functions = functions();
        switch (elemType) {
            case ColumnType.BYTE:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putByte(functions[i].getByte(record));
                }
                break;
            case ColumnType.SHORT:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putShort(functions[i].getShort(record));
                }
                break;
            case ColumnType.INT:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putInt(functions[i].getInt(record));
                }
                break;
            case ColumnType.LONG:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putLong(functions[i].getLong(record));
                }
                break;
            case ColumnType.DATE:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putLong(functions[i].getDate(record));
                }
                break;
            case ColumnType.TIMESTAMP:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putLong(functions[i].getTimestamp(record));
                }
                break;
            case ColumnType.FLOAT:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putFloat(functions[i].getFloat(record));
                }
                break;
            case ColumnType.DOUBLE:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putDouble(functions[i].getDouble(record));
                }
                break;
            case ColumnType.LONG256:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    Long256 v = functions[i].getLong256A(record);
                    mem.putLong256(v.getLong0(), v.getLong1(), v.getLong2(), v.getLong3());
                }
                break;
            case ColumnType.UUID:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putLong128(functions[i].getLong128Lo(record), functions[i].getLong128Hi(record));
                }
                break;
            case ColumnType.IPv4:
                for (int n = getFlatElemCount(), i = 0; i < n; i++) {
                    mem.putInt(functions[i].getIPv4(record));
                }
                break;
            default:
                throw new AssertionError("impossible array element type");
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
    public void close() throws IOException {
        Function[] functions = functions();
        for (int n = functions.length, i = 0; i < n; i++) {
            functions[i] = Misc.free(functions[i]);
        }
    }

    @Override
    public FlatArrayView flatView() {
        return this;
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
    public double getDouble(int flatIndex) {
        return functions()[flatIndex].getDouble(record);
    }

    @Override
    public int getFlatElemCount() {
        return functions().length;
    }

    public Function getFunctionAtFlatIndex(int flatIndex) {
        return functions()[flatIndex];
    }

    @Override
    public long getLong(int flatIndex) {
        return functions()[flatIndex].getLong(record);
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
        functions()[flatIndex] = f;
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

    private Function[] functions() {
        try {
            return functions;
        } catch (NullPointerException e) {
            throw new IllegalStateException("FunctionArray used before calling applyShape()");
        }
    }
}
