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
import io.questdb.griffin.engine.functions.ByteFunction;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class BitXorByteGroupByFunction extends ByteFunction implements GroupByFunction, UnaryFunction {
    private final Function arg;
    private final int argColumnIndex;
    private int valueIndex;

    public BitXorByteGroupByFunction(@NotNull Function arg) {
        this.arg = arg;
        this.argColumnIndex = GroupByUtils.directArgColumnIndex(arg, ColumnType.BYTE);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        mapValue.putByte(valueIndex, arg.getByte(record));
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
        // The etalon (all-zeros) is the identity element for XOR, so we can fold
        // the first row and every subsequent row through the same branchless update.
        final long valueColumnOffset = mapValue.getOffset(valueIndex);
        // Fast path: arg is a direct byte column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final byte value = Unsafe.getByte(argAddr + rowIndex);
                final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                Unsafe.putByte(addr, (byte) (Unsafe.getByte(addr) ^ value));
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                record.setRowIndex(Map.decodeBatchRowIndex(encoded));
                final byte value = arg.getByte(record);
                final long addr = baseValueAddr + Map.decodeBatchOffset(encoded) + valueColumnOffset;
                Unsafe.putByte(addr, (byte) (Unsafe.getByte(addr) ^ value));
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        final byte value = arg.getByte(record);
        final byte current = mapValue.getByte(valueIndex);
        mapValue.putByte(valueIndex, (byte) (current ^ value));
    }

    @Override
    public Function getArg() {
        return arg;
    }

    @Override
    public byte getByte(Record rec) {
        return rec.getByte(valueIndex);
    }

    @Override
    public String getName() {
        return "bit_xor";
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
        columnTypes.add(ColumnType.BYTE);
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
        final byte srcVal = srcValue.getByte(valueIndex);
        final byte destVal = destValue.getByte(valueIndex);
        destValue.putByte(valueIndex, (byte) (destVal ^ srcVal));
    }

    @Override
    public void setNull(MapValue mapValue) {
        mapValue.putByte(valueIndex, (byte) 0);
    }

    @Override
    public boolean supportsParallelism() {
        return UnaryFunction.super.supportsParallelism();
    }
}
