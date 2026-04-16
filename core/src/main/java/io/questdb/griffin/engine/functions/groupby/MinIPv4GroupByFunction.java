/*+*****************************************************************************
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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.IPv4Function;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class MinIPv4GroupByFunction extends IPv4Function implements GroupByFunction, UnaryFunction {
    private static final long IPv4_NULL_AS_LONG = Numbers.ipv4ToLong(Numbers.IPv4_NULL);
    private final Function arg;
    private final int argColumnIndex;
    private int valueIndex;

    public MinIPv4GroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.IPv4);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            final long hi = dataAddr + rowCount * (long) Integer.BYTES;
            long min = IPv4_NULL_AS_LONG;
            for (; dataAddr < hi; dataAddr += Integer.BYTES) {
                long value = Numbers.ipv4ToLong(Unsafe.getUnsafe().getInt(dataAddr));
                if (value != IPv4_NULL_AS_LONG && (value < min || min == IPv4_NULL_AS_LONG)) {
                    min = value;
                }
            }
            if (min != IPv4_NULL_AS_LONG) {
                final long existing = Numbers.ipv4ToLong(mapValue.getIPv4(valueIndex));
                if (min < existing || existing == IPv4_NULL_AS_LONG) {
                    mapValue.putInt(valueIndex, (int) min);
                }
            }
        }
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putInt(valueIndex, arg.getIPv4(record));
    }

    @Override
    public void computeKeyedBatch(
            PageFrameMemoryRecord record,
            FlyweightPackedMapValue mapValue,
            long baseValueAddr,
            long batchAddr,
            long rowCount,
            long baseRowId
    ) {
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        // Fast path: arg is a direct IPv4 column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final int value = Unsafe.getUnsafe().getInt(argAddr + (rowIndex << 2));
                if (value != Numbers.IPv4_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final int current = Unsafe.getUnsafe().getInt(addr);
                    final long valueAsLong = Numbers.ipv4ToLong(value);
                    final long currentAsLong = Numbers.ipv4ToLong(current);
                    if (current == Numbers.IPv4_NULL || valueAsLong < currentAsLong) {
                        Unsafe.getUnsafe().putInt(addr, value);
                    }
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final int value = arg.getIPv4(record);
                if (value != Numbers.IPv4_NULL) {
                    final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                    final int current = Unsafe.getUnsafe().getInt(addr);
                    final long valueAsLong = Numbers.ipv4ToLong(value);
                    final long currentAsLong = Numbers.ipv4ToLong(current);
                    if (current == Numbers.IPv4_NULL || valueAsLong < currentAsLong) {
                        Unsafe.getUnsafe().putInt(addr, value);
                    }
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long min = Numbers.ipv4ToLong(mapValue.getIPv4(valueIndex));
        long next = Numbers.ipv4ToLong(arg.getIPv4(record));
        if (next != Numbers.IPv4_NULL && (next < min || min == Numbers.IPv4_NULL)) {
            mapValue.putInt(valueIndex, (int) next);
        }
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public int getIPv4(Record rec) {
        return rec.getIPv4(valueIndex);
    }

    @Override
    public String getName() {
        return "min";
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
        columnTypes.add(ColumnType.IPv4);
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean isThreadSafe() {
        return UnaryFunction.super.isThreadSafe();
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        long srcMin = Numbers.ipv4ToLong(srcValue.getIPv4(valueIndex));
        long destMin = Numbers.ipv4ToLong(destValue.getIPv4(valueIndex));
        if (srcMin != Numbers.IPv4_NULL && (srcMin < destMin || destMin == Numbers.IPv4_NULL)) {
            destValue.putInt(valueIndex, (int) srcMin);
        }
    }

    @Override
    public void setInt(MapValue mapValue, int value) {
        mapValue.putInt(valueIndex, value);
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putInt(valueIndex, Numbers.IPv4_NULL);
    }

    @Override
    public boolean supportsBatchComputation() {
        return true;
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
