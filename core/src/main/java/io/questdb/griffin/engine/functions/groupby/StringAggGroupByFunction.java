/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByCharSink;

class StringAggGroupByFunction extends StrFunction implements UnaryFunction, GroupByFunction {
    private final Function arg;
    private final char delimiter;
    private final int functionPosition;
    private final int maxBytes;

    private final GroupByCharSink sinkA = new GroupByCharSink();
    private final GroupByCharSink sinkB = new GroupByCharSink();

    private int totalMemoryUsed;
    private int valueIndex;

    public StringAggGroupByFunction(Function arg, int functionPosition, char delimiter, int maxBytes) {
        this.arg = arg;
        this.delimiter = delimiter;
        this.functionPosition = functionPosition;
        this.maxBytes = maxBytes;
    }

    @Override
    public void clear() {
        sinkA.of(0);
        sinkB.of(0);
        totalMemoryUsed = 0;
    }

    @Override
    public void close() {
        sinkA.clear();
        sinkB.clear();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        final CharSequence str = arg.getStrA(record);

        if (str != null) {
            //Non-null value: initialize a new sink
            sinkA.of(0);
            sinkA.put(str);

            totalMemoryUsed += sinkA.length() * 2;  // UTF-16: 2 bytes per char
            assertSizeCompliance();

            // Store the sink pointer in the map at valueIndex
            mapValue.putLong(valueIndex, sinkA.ptr());
        } else {
            // First value is null: mark group as null
            mapValue.putLong(valueIndex, 0);
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final CharSequence str = arg.getStrA(record);

        if (str == null) {
            return;
        }

        final long ptr = mapValue.getLong(valueIndex);

        if (ptr == 0) {
            // First non-null value for this group
            sinkA.of(0);
            sinkA.put(str);

            totalMemoryUsed += sinkA.length() * 2;
            assertSizeCompliance();

            mapValue.putLong(valueIndex, sinkA.ptr());
        } else {
            // Group already has data, append with delimiter
            sinkA.of(ptr);

            int oldLen = sinkA.length();

            sinkA.putAscii(delimiter);
            sinkA.put(str);

            int newLen = sinkA.length();
            int chars = newLen - oldLen;
            totalMemoryUsed += chars * 2;
            assertSizeCompliance();

            mapValue.putLong(valueIndex, sinkA.ptr());
        }
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        final long srcPtr = srcValue.getLong(valueIndex);

        if (srcPtr == 0) {
            // Nothing to do
            return;
        }

        final long destPtr = destValue.getLong(valueIndex);

        if (destPtr == 0) {
            // Just copy source pointer
            destValue.putLong(valueIndex, srcPtr);
            return;
        }

        // If we got here, both groups have data - need to merge them
        sinkA.of(destPtr);
        sinkB.of(srcPtr);

        int oldLen = sinkA.length();

        sinkA.putAscii(delimiter);
        sinkA.put(sinkB);

        int newLen = sinkA.length();
        int chars = newLen - oldLen;
        totalMemoryUsed += chars * 2;
        assertSizeCompliance();

        // Update destination with merged result pointer
        destValue.putLong(valueIndex, sinkA.ptr());
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public CharSequence getStrA(Record rec) {
        final long ptr = rec.getLong(valueIndex);
        return ptr == 0 ? "" : sinkA.of(ptr);
    }

    @Override
    public CharSequence getStrB(Record rec) {
        final long ptr = rec.getLong(valueIndex);
        return ptr == 0 ? "" : sinkB.of(ptr);
    }

    @Override
    public int getValueIndex() {
        return valueIndex;
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
    public boolean isScalar() {
        return false;
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putLong(valueIndex, 0);
    }

    @Override
    public boolean supportsParallelism() {
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }

    @Override
    public void setAllocator(GroupByAllocator allocator) {
        sinkA.setAllocator(allocator);
        sinkB.setAllocator(allocator);
    }

    private void assertSizeCompliance() {
        if (totalMemoryUsed > maxBytes) {
            throw CairoException.nonCritical()
                    .position(functionPosition)
                    .put("string_agg() result exceeds max size of ")
                    .put(maxBytes)
                    .put(" bytes");
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val("string_agg(").val(arg).val(',').val(delimiter).val(')');
    }

    @Override
    public void toTop() {
        UnaryFunction.super.toTop();
    }
}
