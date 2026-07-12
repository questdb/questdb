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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.groupby.LastNotNullTimestampGroupByFunction;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers {@code computeBatch} for the TIMESTAMP aggregators. The sibling per-type classes cover the
 * other types; TIMESTAMP had none, which left {@code LastNotNullTimestampGroupByFunction.computeBatch}
 * with no test of its own.
 */
public class TimestampGroupByFunctionBatchTest {
    private static final int COLUMN_INDEX = 431;
    // Stands in for a row whose column is NULL, as a column-top row reads.
    private static final Record NULL_RECORD = new Record() {
        @Override
        public long getTimestamp(int col) {
            return Numbers.LONG_NULL;
        }
    };
    private long lastAllocated;
    private long lastSize;

    @After
    public void tearDown() {
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
            lastAllocated = 0;
            lastSize = 0;
        }
    }

    @Test
    public void testLastNotNullTimestampBatch() {
        GroupByFunction function = newLastNotNullTimestampFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, 10_000L, Numbers.LONG_NULL, 20_000L);
            function.computeBatch(value, ptr, 4, 0);

            Assert.assertEquals(20_000L, function.getTimestamp(value));
            Assert.assertTrue(function.supportsBatchComputation());
        }
    }

    @Test
    public void testLastNotNullTimestampBatchAllNull() {
        GroupByFunction function = newLastNotNullTimestampFunction();
        try (SimpleMapValue value = prepare(function)) {
            function.setNull(value);

            long ptr = allocateLongs(Numbers.LONG_NULL, Numbers.LONG_NULL);
            function.computeBatch(value, ptr, 2, 0);

            Assert.assertEquals(Numbers.LONG_NULL, function.getTimestamp(value));
        }
    }

    @Test
    public void testLastNotNullTimestampBatchReplacesStoredNull() {
        GroupByFunction function = newLastNotNullTimestampFunction();
        try (SimpleMapValue value = prepare(function)) {
            // The row-by-row fallback for a frame with column tops calls computeFirst, which writes
            // through unconditionally - NULL included - leaving a real rowId next to a NULL value.
            // Frames reach a worker out of order, so a later frame's non-null at a LOWER rowId must
            // still replace that stored NULL, as computeNext already does.
            function.computeFirst(value, NULL_RECORD, 100);

            long ptr = allocateLongs(42_000L);
            function.computeBatch(value, ptr, 1, 10);

            Assert.assertEquals(42_000L, function.getTimestamp(value));
        }
    }

    private long allocateLongs(long... values) {
        if (values.length == 0) {
            return 0;
        }
        if (lastAllocated != 0) {
            Unsafe.free(lastAllocated, lastSize, MemoryTag.NATIVE_DEFAULT);
        }
        lastSize = (long) values.length * Long.BYTES;
        lastAllocated = Unsafe.malloc(lastSize, MemoryTag.NATIVE_DEFAULT);
        long addr = lastAllocated;
        for (long value : values) {
            Unsafe.putLong(addr, value);
            addr += Long.BYTES;
        }
        return lastAllocated;
    }

    private GroupByFunction newLastNotNullTimestampFunction() {
        return new LastNotNullTimestampGroupByFunction(
                TimestampColumn.newInstance(COLUMN_INDEX, ColumnType.TIMESTAMP),
                ColumnType.TIMESTAMP
        );
    }

    private SimpleMapValue prepare(GroupByFunction function) {
        var columnTypes = new ArrayColumnTypes();
        function.initValueTypes(columnTypes);
        SimpleMapValue value = new SimpleMapValue(columnTypes.getColumnCount());
        function.initValueIndex(0);
        function.setEmpty(value);
        return value;
    }
}
