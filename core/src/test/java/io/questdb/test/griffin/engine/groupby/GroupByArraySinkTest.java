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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByArraySink;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GroupByArraySinkTest extends AbstractCairoTest {

    @Test
    public void testPutSingleElement() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                ArrayView expected = createArray(allocator, 42.0);
                sink.put(expected);

                ArrayView result = sink.getArray();
                TestUtils.assertEquals(expected, result);
            }
        });
    }

    @Test
    public void testPut() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                ArrayView expected = createArray(allocator, 1.0, 2.0, 3.0);
                sink.put(expected);

                ArrayView result = sink.getArray();
                TestUtils.assertEquals(expected, result);
            }
        });
    }

    @Test
    public void testPutNull() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                sink.put(null);

                Assert.assertNull(sink.getArray());
            }
        });
    }

    @Test
    public void testPutEmptyArray() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                ArrayView expected = createArray(allocator);
                sink.put(expected);

                ArrayView result = sink.getArray();
                TestUtils.assertEquals(expected, result);
            }
        });
    }

    @Test
    public void testClear() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                sink.put(createArray(allocator, 1.0, 2.0));
                sink.clear();
                Assert.assertNull(sink.getArray());

                sink.put(createArray(allocator, 3.0, 4.0));
                Assert.assertNotNull(sink.getArray());
            }
        });
    }

    @Test
    public void testReplace() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                sink.put(createArray(allocator, 1.0, 2.0));
                ArrayView expected = createArray(allocator, 3.0, 4.0, 5.0, 6.0, 7.0);
                sink.put(expected);

                TestUtils.assertEquals(expected, sink.getArray());
            }
        });
    }

    @Test
    public void testOf() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink1 = new GroupByArraySink();
                sink1.setAllocator(allocator);

                ArrayView expected = createArray(allocator, 1.0, 2.0, 3.0);
                sink1.put(expected);

                GroupByArraySink sink2 = new GroupByArraySink();
                sink2.of(sink1.ptr());

                TestUtils.assertEquals(expected, sink2.getArray());
            }
        });
    }

    @Test
    public void testGrowthFromSmallToLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                sink.put(createArray(allocator, 1.0, 2.0));

                double[] largeArray = new double[1000];
                for (int i = 0; i < largeArray.length; i++) {
                    largeArray[i] = i;
                }
                ArrayView expected = createArray(allocator, largeArray);
                sink.put(expected);

                TestUtils.assertEquals(expected, sink.getArray());
            }
        });
    }

    @Test
    public void testPutMultidimensionalArray() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByArraySink sink = new GroupByArraySink();
                sink.setAllocator(allocator);

                ArrayView array = create2DArray(allocator, 2, 3, new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0});
                sink.put(array);

                TestUtils.assertEquals(array, sink.getArray());
            }
        });
    }

    private ArrayView createArray(GroupByAllocator allocator, double... values) {
        long totalSize = 4 + 4 + (values.length * 8L);
        long ptr = allocator.malloc(8 + totalSize);

        Unsafe.getUnsafe().putLong(ptr, totalSize);
        Unsafe.getUnsafe().putInt(ptr + 8, ColumnType.encodeArrayType(ColumnType.DOUBLE, 1, true));
        Unsafe.getUnsafe().putInt(ptr + 12, values.length);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putDouble(ptr + 16 + i * 8L, values[i]);
        }

        return ArrayTypeDriver.getPlainValue(ptr, new BorrowedArray());
    }

    private ArrayView create2DArray(GroupByAllocator allocator, int dim0, int dim1, double[] values) {
        long totalSize = 4 + 4 + 4 + (values.length * 8L);
        long ptr = allocator.malloc(8 + totalSize);

        Unsafe.getUnsafe().putLong(ptr, totalSize);
        Unsafe.getUnsafe().putInt(ptr + 8, ColumnType.encodeArrayType(ColumnType.DOUBLE, 2, true));
        Unsafe.getUnsafe().putInt(ptr + 12, dim0);
        Unsafe.getUnsafe().putInt(ptr + 16, dim1);
        for (int i = 0; i < values.length; i++) {
            Unsafe.getUnsafe().putDouble(ptr + 20 + i * 8L, values[i]);
        }

        return ArrayTypeDriver.getPlainValue(ptr, new BorrowedArray());
    }
}
