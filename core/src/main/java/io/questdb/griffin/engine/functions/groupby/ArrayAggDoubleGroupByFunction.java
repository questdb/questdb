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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * Collects double values into a 1D {@code DOUBLE[]} array during GROUP BY / SAMPLE BY.
 * See {@link AbstractArrayAggDoubleGroupByFunction} for the shared parallelism strategy
 * and buffer layout.
 */
public class ArrayAggDoubleGroupByFunction extends AbstractArrayAggDoubleGroupByFunction {

    public ArrayAggDoubleGroupByFunction(@NotNull Function arg, int maxArrayElementCount) {
        super(arg, maxArrayElementCount);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record, long rowId) {
        long ptr = allocator.malloc(HEADER_SIZE + INITIAL_CAPACITY * ENTRY_SIZE);
        Unsafe.getUnsafe().putInt(ptr, 1);
        Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, INITIAL_CAPACITY);
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE, rowId);
        Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + VALUE_OFFSET, arg.getDouble(record));
        mapValue.putLong(valueIndex, ptr);
    }

    @Override
    public void computeNext(MapValue mapValue, Record record, long rowId) {
        long ptr = mapValue.getLong(valueIndex);
        int count = Unsafe.getUnsafe().getInt(ptr);
        checkCapacityLimit(count + 1);
        int capacity = Unsafe.getUnsafe().getInt(ptr + CAPACITY_OFFSET);
        if (count == capacity) {
            if (capacity > (Integer.MAX_VALUE >> 1)) {
                throw CairoException.nonCritical().put("array_agg: array exceeds maximum capacity");
            }
            int newCapacity = capacity << 1;
            long oldSize = HEADER_SIZE + (long) capacity * ENTRY_SIZE;
            long newSize = HEADER_SIZE + (long) newCapacity * ENTRY_SIZE;
            ptr = allocator.realloc(ptr, oldSize, newSize);
            Unsafe.getUnsafe().putInt(ptr + CAPACITY_OFFSET, newCapacity);
            mapValue.putLong(valueIndex, ptr);
        }
        Unsafe.getUnsafe().putLong(ptr + HEADER_SIZE + (long) count * ENTRY_SIZE, rowId);
        Unsafe.getUnsafe().putDouble(ptr + HEADER_SIZE + (long) count * ENTRY_SIZE + VALUE_OFFSET, arg.getDouble(record));
        Unsafe.getUnsafe().putInt(ptr, count + 1);
    }
}
