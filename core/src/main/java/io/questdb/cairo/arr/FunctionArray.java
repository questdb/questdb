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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.std.Long256;
import io.questdb.std.Misc;

public class FunctionArray extends MutableArray implements FlatArrayView {

    private Function[] functions;
    private Record record;

    public FunctionArray(short elementType, int nDims) {
        setType(ColumnType.encodeArrayType(elementType, nDims));
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem) {
        final short elemType = elemType();
        final Function[] functions = functions();
        int elemCount = getFlatViewLength();
        switch (elemType) {
            case ColumnType.BYTE:
                for (int i = 0; i < elemCount; i++) {
                    mem.putByte(functions[i].getByte(record));
                }
                break;
            case ColumnType.SHORT:
                for (int i = 0; i < elemCount; i++) {
                    mem.putShort(functions[i].getShort(record));
                }
                break;
            case ColumnType.INT:
                for (int i = 0; i < elemCount; i++) {
                    mem.putInt(functions[i].getInt(record));
                }
                break;
            case ColumnType.LONG:
                for (int i = 0; i < elemCount; i++) {
                    mem.putLong(functions[i].getLong(record));
                }
                break;
            case ColumnType.DATE:
                for (int i = 0; i < elemCount; i++) {
                    mem.putLong(functions[i].getDate(record));
                }
                break;
            case ColumnType.TIMESTAMP:
                for (int i = 0; i < elemCount; i++) {
                    mem.putLong(functions[i].getTimestamp(record));
                }
                break;
            case ColumnType.FLOAT:
                for (int i = 0; i < elemCount; i++) {
                    mem.putFloat(functions[i].getFloat(record));
                }
                break;
            case ColumnType.DOUBLE:
                for (int i = 0; i < elemCount; i++) {
                    mem.putDouble(functions[i].getDouble(record));
                }
                break;
            case ColumnType.LONG256:
                for (int i = 0; i < elemCount; i++) {
                    Long256 v = functions[i].getLong256A(record);
                    mem.putLong256(v.getLong0(), v.getLong1(), v.getLong2(), v.getLong3());
                }
                break;
            case ColumnType.UUID:
                for (int i = 0; i < elemCount; i++) {
                    mem.putLong128(functions[i].getLong128Lo(record), functions[i].getLong128Hi(record));
                }
                break;
            case ColumnType.IPv4:
                for (int i = 0; i < elemCount; i++) {
                    mem.putInt(functions[i].getIPv4(record));
                }
                break;
            default:
                throw new AssertionError("impossible array element type");
        }
    }

    public void applyShape(CairoConfiguration configuration, int errorPos) {
        resetToDefaultStrides(configuration.maxArrayElementCount(), errorPos);
        if (functions == null || functions.length < flatViewLength) {
            functions = new Function[flatViewLength];
        }
    }

    @Override
    public void close() {
        Function[] functions = functions();
        for (int n = functions.length, i = 0; i < n; i++) {
            functions[i] = Misc.free(functions[i]);
        }
    }

    @Override
    public short elemType() {
        return ColumnType.decodeArrayElementType(this.type);
    }

    @Override
    public double getDouble(int flatIndex) {
        validateFlatIndex(flatIndex);
        return functions[flatIndex].getDouble(record);
    }

    public Function getFunctionAtFlatIndex(int flatIndex) {
        return functions()[flatIndex];
    }

    @Override
    public long getLong(int flatIndex) {
        validateFlatIndex(flatIndex);
        return functions()[flatIndex].getLong(record);
    }

    @Override
    public int length() {
        return functions.length;
    }

    public void putFunction(int flatIndex, Function f) {
        functions()[flatIndex] = f;
    }

    public void setRecord(Record rec) {
        this.record = rec;
    }

    private Function[] functions() {
        try {
            return functions;
        } catch (NullPointerException e) {
            throw new IllegalStateException("FunctionArray used before calling applyShape()");
        }
    }

    private void validateFlatIndex(int flatIndex) {
        if (flatIndex < 0 || flatIndex > functions.length) {
            throw CairoException.nonCritical().put("flatIndex out of range [flatIndex=").put(flatIndex);
        }
    }
}
