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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.functions.VarcharFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByUtf8Sink;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class FirstVarcharGroupByFunction extends VarcharFunction implements GroupByFunction, UnaryFunction {
    protected final Function arg;
    protected final GroupByUtf8Sink sink = new GroupByUtf8Sink();
    protected int valueIndex;

    public FirstVarcharGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
    }

    @Override
    public void clear() {
        sink.of(0);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putLong(valueIndex, rowId);
        final Utf8Sequence val = arg.getVarcharA(record);
        if (val == null) {
            mapValue.putLong(valueIndex + 1, 0);
            mapValue.putBool(valueIndex + 2, true);
        } else {
            sink.of(0).put(val);
            mapValue.putLong(valueIndex + 1, sink.ptr());
            mapValue.putBool(valueIndex + 2, false);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        // empty
    }

    @Override
    public Function getArg() {
        return this.arg;
    }

    @Override
    public String getName() {
        return "first";
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
    }

    @Override
    public void getVarchar(Record rec, Utf8Sink utf8Sink) {
        utf8Sink.put(getVarcharA(rec));
    }

    @Override
    public @Nullable Utf8Sequence getVarcharA(Record rec) {
        final boolean nullValue = rec.getBool(valueIndex + 2);
        if (nullValue) {
            return null;
        }
        final long ptr = rec.getLong(valueIndex + 1);
        return ptr == 0 ? null : sink.of(ptr);
    }

    @Override
    public @Nullable Utf8Sequence getVarcharB(Record rec) {
        return getVarcharA(rec);
    }

    @Override
    public void initValueIndex(int valueIndex) {
        this.valueIndex = valueIndex;
    }

    @Override
    public void initValueTypes(ArrayColumnTypes columnTypes) {
        this.valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);    // row id
        columnTypes.add(ColumnType.LONG);    // sink pointer
        columnTypes.add(ColumnType.BOOLEAN); // null flag
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isReadThreadSafe() {
        return false;
    }

    @Override
    public boolean isScalar() {
        return false;
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId != Numbers.LONG_NaN && (srcRowId < destRowId || destRowId == Numbers.LONG_NaN)) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcValue.getLong(valueIndex + 1));
            destValue.putBool(valueIndex + 2, srcValue.getBool(valueIndex + 2));
        }
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        sink.setAllocator(allocator);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, Numbers.LONG_NaN);
        mapValue.putLong(valueIndex + 1, 0);
        mapValue.putBool(valueIndex + 2, true);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }
}