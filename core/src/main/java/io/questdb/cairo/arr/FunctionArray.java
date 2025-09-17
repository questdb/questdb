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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.griffin.PlanSink;
import io.questdb.std.Long256;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class FunctionArray extends MutableArray implements FlatArrayView {
    private ObjList<Function> functions;
    private Record record;

    public FunctionArray(int elementType, int nDims) {
        setType(ColumnType.encodeArrayType(elementType, nDims));
        this.flatView = this;
    }

    @Override
    public void appendToMemFlat(MemoryA mem, int offset, int length) {
        final short elemType = getElemType();
        switch (ColumnType.tagOf(elemType)) {
            case ColumnType.BYTE:
                for (int i = 0; i < length; i++) {
                    mem.putByte(functions.getQuick(offset + i).getByte(record));
                }
                break;
            case ColumnType.SHORT:
                for (int i = 0; i < length; i++) {
                    mem.putShort(functions.getQuick(offset + i).getShort(record));
                }
                break;
            case ColumnType.INT:
                for (int i = 0; i < length; i++) {
                    mem.putInt(functions.getQuick(offset + i).getInt(record));
                }
                break;
            case ColumnType.LONG:
                for (int i = 0; i < length; i++) {
                    mem.putLong(functions.getQuick(offset + i).getLong(record));
                }
                break;
            case ColumnType.DATE:
                for (int i = 0; i < length; i++) {
                    mem.putLong(functions.getQuick(offset + i).getDate(record));
                }
                break;
            case ColumnType.TIMESTAMP:
                for (int i = 0; i < length; i++) {
                    mem.putLong(functions.getQuick(offset + i).getTimestamp(record));
                }
                break;
            case ColumnType.FLOAT:
                for (int i = 0; i < length; i++) {
                    mem.putFloat(functions.getQuick(offset + i).getFloat(record));
                }
                break;
            case ColumnType.DOUBLE:
                for (int i = 0; i < length; i++) {
                    mem.putDouble(functions.getQuick(offset + i).getDouble(record));
                }
                break;
            case ColumnType.LONG256:
                for (int i = 0; i < length; i++) {
                    Long256 v = functions.getQuick(offset + i).getLong256A(record);
                    mem.putLong256(v.getLong0(), v.getLong1(), v.getLong2(), v.getLong3());
                }
                break;
            case ColumnType.UUID:
                for (int i = 0; i < length; i++) {
                    mem.putLong128(functions.getQuick(offset + i).getLong128Lo(record),
                            functions.getQuick(offset + i).getLong128Hi(record));
                }
                break;
            case ColumnType.IPv4:
                for (int i = 0; i < length; i++) {
                    mem.putInt(functions.getQuick(offset + i).getIPv4(record));
                }
                break;
            default:
                throw new AssertionError("impossible array element type");
        }
    }

    public void applyShape(CairoConfiguration configuration, int errorPos) {
        resetToDefaultStrides(configuration.maxArrayElementCount(), errorPos);
        if (functions == null) {
            functions = new ObjList<>(flatViewLength);
        }
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(functions);
    }

    @Override
    public double getDoubleAtAbsIndex(int flatIndex) {
        return functions.getQuick(flatIndex).getDouble(record);
    }

    public Function getFunctionAtFlatIndex(int flatIndex) {
        return functions.getQuick(flatIndex);
    }

    public ObjList<Function> getFunctions() {
        return functions;
    }

    @Override
    public long getLongAtAbsIndex(int flatIndex) {
        return functions.getQuick(flatIndex).getLong(record);
    }

    @Override
    public int length() {
        return functions.size();
    }

    public void putFunction(int flatIndex, Function f) {
        functions.extendAndSet(flatIndex, f);
    }

    public void setRecord(Record rec) {
        this.record = rec;
    }

    public void toPlan(PlanSink sink) {
        sink.val('[');
        String comma = "";
        for (int i = 0, n = functions.size(); i < n; i++) {
            sink.val(comma);
            sink.val(functions.getQuick(i));
            comma = ",";
        }
        sink.val(']');
    }
}
