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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;

public final class MaxVarcharGroupByFunction extends VarcharFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final GroupByUtf8Sink sinkA = new GroupByUtf8Sink();
    private final GroupByUtf8Sink sinkB = new GroupByUtf8Sink();
    private int valueIndex;

    public MaxVarcharGroupByFunction(Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        sinkA.of(0);
        sinkB.of(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final Utf8Sequence val = arg.getVarcharA(record);
        if (val == null) {
            mapValue.putLong(valueIndex, 0);
        } else {
            sinkA.of(0).put(val);
            mapValue.putLong(valueIndex, sinkA.ptr());
        }
    }


    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final Utf8Sequence val = arg.getVarcharA(record);
        if (val != null) {
            final long ptr = mapValue.getLong(valueIndex);
            if (ptr == 0) {
                sinkA.of(0).put(val);
                mapValue.putLong(valueIndex, sinkA.ptr());
                return;
            }

            sinkA.of(ptr);
            if (Utf8s.compare(sinkA, val) < 0) {
                sinkA.clear();
                sinkA.put(val);
                mapValue.putLong(valueIndex, sinkA.ptr());
            }
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public Utf8Sequence getVarcharA(Record rec) {
        final long ptr = rec.getLong(valueIndex);
        return ptr == 0 ? null : sinkA.of(ptr);
    }

    @Override
    public Utf8Sequence getVarcharB(Record rec) {
        return getVarcharA(rec);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcPtr = srcValue.getLong(valueIndex);
        if (srcPtr == 0) {
            return;
        }
        long destPtr = destValue.getLong(valueIndex);
        if (destPtr == 0 || Utf8s.compare(sinkA.of(destPtr), sinkB.of(srcPtr)) < 0) {
            destValue.putLong(valueIndex, srcPtr);
        }
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        sinkA.setAllocator(allocator);
        sinkB.setAllocator(allocator);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("max(").val(arg).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }
}
