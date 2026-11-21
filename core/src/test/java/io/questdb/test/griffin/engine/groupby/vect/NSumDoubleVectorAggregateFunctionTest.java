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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.groupby.vect.NSumDoubleVectorAggregateFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Rosti;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.griffin.SqlCodeGenerator.GKK_VANILLA_INT;

public class NSumDoubleVectorAggregateFunctionTest extends AbstractCairoTest {
    private static final double INF = Double.POSITIVE_INFINITY;

    @Test
    public void testKeyedMergeAndWrapUpKeepCompensation() throws Exception {
        assertMemoryLeak(() -> {
            try (NSumDoubleVectorAggregateFunction function = new NSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 2)) {
                final ArrayColumnTypes types = new ArrayColumnTypes();
                types.add(ColumnType.INT);
                function.pushValueTypes(types);
                final long a = newRosti(function, types);
                final long b = newRosti(function, types);
                try {
                    aggregateKeyed(function, a, 1, 1, -1e16, 1e16);
                    aggregateKeyed(function, a, 2, -1e16);
                    aggregateKeyed(function, b, 2, 1e16, 1);
                    aggregateKeyed(function, a, 3, 1e16, 1, -1e16, 1);
                    aggregateKeyed(function, b, 3, -1e16);
                    aggregateKeyed(function, a, 4, INF, 2, -INF, Double.NaN);
                    aggregateKeyed(function, b, 4, INF);
                    aggregateKeyed(function, b, 5, Double.NaN, -INF);
                    aggregateKeyed(function, a, Numbers.INT_NULL, 2);
                    aggregateFrame(function, 0, 1e16, 1);
                    aggregateFrame(function, 1, -1e16);
                    Assert.assertTrue(function.merge(a, b));
                    Assert.assertTrue(function.wrapUp(a));

                    Assert.assertEquals(1, valueOf(function, a, 1), 0);
                    Assert.assertEquals(1, valueOf(function, a, 2), 0);
                    Assert.assertEquals(2 - 1e16, valueOf(function, a, 3), 0);
                    Assert.assertEquals(2, valueOf(function, a, 4), 0);
                    Assert.assertTrue(Double.isNaN(valueOf(function, a, 5)));
                    Assert.assertEquals(3, valueOf(function, a, Numbers.INT_NULL), 0);
                } finally {
                    Rosti.free(a);
                    Rosti.free(b);
                }
            }
        });
    }

    @Test
    public void testKeyedWrapUpInsertsCompensatedNullKey() throws Exception {
        assertMemoryLeak(() -> {
            try (NSumDoubleVectorAggregateFunction function = new NSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 2)) {
                final ArrayColumnTypes types = new ArrayColumnTypes();
                types.add(ColumnType.INT);
                function.pushValueTypes(types);
                final long a = newRosti(function, types);
                try {
                    aggregateKeyed(function, a, 1, 5);
                    aggregateFrame(function, 0, 1e16, 1);
                    aggregateFrame(function, 1, -1e16);
                    Assert.assertTrue(function.wrapUp(a));

                    Assert.assertEquals(5, valueOf(function, a, 1), 0);
                    Assert.assertEquals(1, valueOf(function, a, Numbers.INT_NULL), 0);
                } finally {
                    Rosti.free(a);
                }
            }
        });
    }

    @Test
    public void testNotKeyedKeepsCompensationAcrossFramesAndWorkers() throws Exception {
        assertMemoryLeak(() -> {
            try (NSumDoubleVectorAggregateFunction function = new NSumDoubleVectorAggregateFunction(GKK_VANILLA_INT, 0, -1, 3)) {
                aggregateFrame(function, 0, 1e16, 1, 1, 1, 1, 1, 1, 1);
                aggregateFrame(function, 1, -1e16);
                Assert.assertEquals(7, function.getDouble(null), 0);

                function.clear();
                aggregateFrame(function, 0, 1);
                aggregateFrame(function, 0, -1e16);
                aggregateFrame(function, 0, 1e16);
                Assert.assertEquals(1, function.getDouble(null), 0);

                function.clear();
                aggregateFrame(function, 0, 1);
                aggregateFrame(function, 1, -1e16);
                aggregateFrame(function, 2, 1e16);
                Assert.assertEquals(1, function.getDouble(null), 0);

                function.clear();
                aggregateFrame(function, 0, 1, INF, -INF, Double.NaN, 2);
                aggregateFrame(function, 1, Double.NaN, INF);
                Assert.assertEquals(3, function.getDouble(null), 0);

                function.clear();
                aggregateFrame(function, 2, Double.NaN, -INF);
                Assert.assertTrue(Double.isNaN(function.getDouble(null)));
                function.close();
            }
        });
    }

    private static void aggregateFrame(NSumDoubleVectorAggregateFunction function, int workerId, double... values) {
        final long size = (long) values.length * Double.BYTES;
        final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.putDouble(address + (long) i * Double.BYTES, values[i]);
            }
            function.aggregate(address, values.length, workerId);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void aggregateKeyed(NSumDoubleVectorAggregateFunction function, long pRosti, int key, double... values) {
        final long keysSize = (long) values.length * Integer.BYTES;
        final long valuesSize = (long) values.length * Double.BYTES;
        final long keys = Unsafe.malloc(keysSize, MemoryTag.NATIVE_DEFAULT);
        final long address = Unsafe.malloc(valuesSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.putInt(keys + (long) i * Integer.BYTES, key);
                Unsafe.putDouble(address + (long) i * Double.BYTES, values[i]);
            }
            Assert.assertTrue(function.aggregate(pRosti, keys, address, values.length));
        } finally {
            Unsafe.free(keys, keysSize, MemoryTag.NATIVE_DEFAULT);
            Unsafe.free(address, valuesSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long newRosti(NSumDoubleVectorAggregateFunction function, ArrayColumnTypes types) {
        final long pRosti = Rosti.alloc(types, 16);
        Unsafe.putInt(Rosti.getInitialValueSlot(pRosti, 0), Numbers.INT_NULL);
        function.initRosti(pRosti);
        return pRosti;
    }

    private static double valueOf(NSumDoubleVectorAggregateFunction function, long pRosti, int key) {
        final long ctrl = Rosti.getCtrl(pRosti);
        final long slots = Rosti.getSlots(pRosti);
        final long shift = Rosti.getSlotShift(pRosti);
        final long valueOffset = Unsafe.getInt(Rosti.getValueOffsets(pRosti) + function.getValueOffset() * 4L);
        for (long i = 0, n = Rosti.getCapacity(pRosti); i < n; i++) {
            if (Unsafe.getByte(ctrl + i) > -1) {
                final long slot = slots + (i << shift);
                if (Unsafe.getInt(slot) == key) {
                    return Unsafe.getDouble(slot + valueOffset);
                }
            }
        }
        throw new AssertionError("key not found: " + key);
    }
}
