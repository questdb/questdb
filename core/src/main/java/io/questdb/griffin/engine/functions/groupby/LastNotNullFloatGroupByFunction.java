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

import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

public class LastNotNullFloatGroupByFunction extends FirstFloatGroupByFunction {

    public LastNotNullFloatGroupByFunction(@NotNull Function arg) {
        super(arg);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            long hi = dataAddr + (rowCount - 1) * 4L;
            long offset = rowCount - 1;
            for (; hi >= dataAddr; hi -= 4L) {
                float value = Unsafe.getUnsafe().getFloat(hi);
                if (!Numbers.isNull(value)) {
                    long rowId = startRowId + offset;
                    long existingRowId = mapValue.getLong(valueIndex);
                    if (rowId > existingRowId || existingRowId == Numbers.LONG_NULL) {
                        mapValue.putLong(valueIndex, rowId);
                        mapValue.putFloat(valueIndex + 1, value);
                    }
                    break;
                }
                offset--;
            }
        }
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
        // setEmpty pre-seeds rowId = LONG_NULL and value = NaN. Null (NaN/Infinity) input
        // is skipped; non-null input wins when the stored value is still null or has an
        // earlier rowId.
        final long rowIdOffset = mapValue.getOffset(valueIndex);
        final long valueColumnOffset = mapValue.getOffset(valueIndex + 1);
        // Fast path: arg is a direct float column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final float value = Unsafe.getUnsafe().getFloat(argAddr + (rowIndex << 2));
                // Mirror computeFirst semantics on new entries (write through even for
                // null values) so the state matches what the per-row path produces.
                if (!Numbers.isNull(value) || Map.isNewBatchEntry(encoded)) {
                    final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                    final long rowId = baseRowId + rowIndex;
                    final float existingValue = Unsafe.getUnsafe().getFloat(entryBase + valueColumnOffset);
                    if (Numbers.isNull(existingValue) || rowId > Unsafe.getUnsafe().getLong(entryBase + rowIdOffset)) {
                        Unsafe.getUnsafe().putLong(entryBase + rowIdOffset, rowId);
                        Unsafe.getUnsafe().putFloat(entryBase + valueColumnOffset, value);
                    }
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getUnsafe().getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                record.setRowIndex(rowIndex);
                final float value = arg.getFloat(record);
                // Mirror computeFirst semantics on new entries (write through even for
                // null values) so the state matches what the per-row path produces.
                if (!Numbers.isNull(value) || Map.isNewBatchEntry(encoded)) {
                    final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                    final long rowId = baseRowId + rowIndex;
                    final float existingValue = Unsafe.getUnsafe().getFloat(entryBase + valueColumnOffset);
                    if (Numbers.isNull(existingValue) || rowId > Unsafe.getUnsafe().getLong(entryBase + rowIdOffset)) {
                        Unsafe.getUnsafe().putLong(entryBase + rowIdOffset, rowId);
                        Unsafe.getUnsafe().putFloat(entryBase + valueColumnOffset, value);
                    }
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (!Numbers.isNull(arg.getFloat(record))) {
            if (Numbers.isNull(mapValue.getFloat(valueIndex + 1)) || rowId > mapValue.getLong(valueIndex)) {
                computeFirst(mapValue, record, rowId);
            }
        }
    }

    @Override
    public String getName() {
        return "last_not_null";
    }

    @Override
    public void merge(MapValue destValue, MapValue srcValue) {
        float srcVal = srcValue.getFloat(valueIndex + 1);
        if (Numbers.isNull(srcVal)) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId > destRowId) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putFloat(valueIndex + 1, srcVal);
        }
    }
}
