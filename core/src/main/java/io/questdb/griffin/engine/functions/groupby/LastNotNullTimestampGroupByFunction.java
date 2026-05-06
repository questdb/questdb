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

public class LastNotNullTimestampGroupByFunction extends FirstTimestampGroupByFunction {
    public LastNotNullTimestampGroupByFunction(@NotNull Function arg, int timestampType) {
        super(arg, timestampType);
    }

    @Override
    public void computeBatch(MapValue mapValue, long dataAddr, int rowCount, long startRowId) {
        if (rowCount > 0) {
            long hi = dataAddr + (rowCount - 1) * (long) Long.BYTES;
            long offset = rowCount - 1;
            for (; hi >= dataAddr; hi -= Long.BYTES) {
                long value = Unsafe.getLong(hi);
                if (value != Numbers.LONG_NULL) {
                    long rowId = startRowId + offset;
                    long existingRowId = mapValue.getLong(valueIndex);
                    if (rowId > existingRowId || existingRowId == Numbers.LONG_NULL) {
                        mapValue.putLong(valueIndex, rowId);
                        mapValue.putLong(valueIndex + 1, value);
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
        // setEmpty pre-seeds both slots to LONG_NULL. Null input is skipped; non-null
        // input wins when the stored value is still null or has an earlier rowId.
        final long rowIdOffset = mapValue.getOffset(valueIndex);
        final long valueColumnOffset = mapValue.getOffset(valueIndex + 1);
        // Fast path: arg is a direct timestamp column with data on the current frame.
        // Zero page address means a column top; fall through to the record-based path.
        final long argAddr = argColumnIndex >= 0 ? record.getPageAddress(argColumnIndex) : 0;
        if (argAddr != 0) {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                final long value = Unsafe.getLong(argAddr + (rowIndex << 3));
                // Mirror computeFirst semantics on new entries (write through even for
                // null values) so the state matches what the per-row path produces.
                if (value != Numbers.LONG_NULL || Map.isNewBatchEntry(encoded)) {
                    final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                    final long rowId = baseRowId + rowIndex;
                    final long existingValue = Unsafe.getLong(entryBase + valueColumnOffset);
                    if (existingValue == Numbers.LONG_NULL || rowId > Unsafe.getLong(entryBase + rowIdOffset)) {
                        Unsafe.putLong(entryBase + rowIdOffset, rowId);
                        Unsafe.putLong(entryBase + valueColumnOffset, value);
                    }
                }
            }
        } else {
            for (long i = 0; i < rowCount; i++) {
                final long encoded = Unsafe.getLong(batchAddr + (i << 3));
                final long rowIndex = Map.decodeBatchRowIndex(encoded);
                record.setRowIndex(rowIndex);
                final long value = arg.getTimestamp(record);
                // Mirror computeFirst semantics on new entries (write through even for
                // null values) so the state matches what the per-row path produces.
                if (value != Numbers.LONG_NULL || Map.isNewBatchEntry(encoded)) {
                    final long entryBase = baseValueAddr + Map.decodeBatchOffset(encoded);
                    final long rowId = baseRowId + rowIndex;
                    final long existingValue = Unsafe.getLong(entryBase + valueColumnOffset);
                    if (existingValue == Numbers.LONG_NULL || rowId > Unsafe.getLong(entryBase + rowIdOffset)) {
                        Unsafe.putLong(entryBase + rowIdOffset, rowId);
                        Unsafe.putLong(entryBase + valueColumnOffset, value);
                    }
                }
            }
        }
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        if (arg.getTimestamp(record) != Numbers.LONG_NULL) {
            if (mapValue.getTimestamp(valueIndex + 1) == Numbers.LONG_NULL || rowId > mapValue.getLong(valueIndex)) {
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
        long srcVal = srcValue.getTimestamp(valueIndex + 1);
        if (srcVal == Numbers.LONG_NULL) {
            return;
        }
        long srcRowId = srcValue.getLong(valueIndex);
        long destRowId = destValue.getLong(valueIndex);
        if (srcRowId > destRowId) {
            destValue.putLong(valueIndex, srcRowId);
            destValue.putLong(valueIndex + 1, srcVal);
        }
    }
}
