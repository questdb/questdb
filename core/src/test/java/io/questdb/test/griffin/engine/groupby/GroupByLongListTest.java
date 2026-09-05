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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByLongList;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class GroupByLongListTest extends AbstractCairoTest {

    @Test
    public void testQuickSelectAllEqual() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                for (int i = 0; i < 100; i++) {
                    list.add(42L);
                }

                // Should complete without degenerate behavior on all-equal data
                list.quickSelect(0, 99, 50);
                Assert.assertEquals(42L, list.getQuick(50));
            }
        });
    }

    @Test
    public void testQuickSelectBoundsHiTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(0, 5, 2);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for hi >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsKGreaterThanHi() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(0, 3, 4);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for k > hi");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsKLessThanLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(2, 4, 1);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for k < lo");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectBoundsNegativeLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelect(-1, 4, 2);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo < 0");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectFirst() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select minimum element (k=0)
                list.quickSelect(0, 4, 0);
                Assert.assertEquals(1L, list.getQuick(0));
            }
        });
    }

    @Test
    public void testQuickSelectLast() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select maximum element (k=4)
                list.quickSelect(0, 4, 4);
                Assert.assertEquals(5L, list.getQuick(4));
            }
        });
    }

    @Test
    public void testQuickSelectMedian() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select median element (k=2)
                list.quickSelect(0, 4, 2);
                Assert.assertEquals(3L, list.getQuick(2));
            }
        });
    }

    @Test
    public void testQuickSelectMultipleBoundsHiTooLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelectMultiple(0, 5, new int[]{0, 2, 4}, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for hi >= size()");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectMultipleBoundsNegativeLo() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                try {
                    list.quickSelectMultiple(-1, 4, new int[]{0, 2, 4}, 0, 3);
                    Assert.fail("Should throw ArrayIndexOutOfBoundsException for lo < 0");
                } catch (ArrayIndexOutOfBoundsException e) {
                    // Expected
                }
            }
        });
    }

    @Test
    public void testQuickSelectMultipleThreeIndices() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                long[] values = {5, 3, 1, 4, 2};
                for (long v : values) {
                    list.add(v);
                }

                // Select elements at indices 0, 2, 4 (min, median, max)
                list.quickSelectMultiple(0, 4, new int[]{0, 2, 4}, 0, 3);
                Assert.assertEquals(1L, list.getQuick(0));
                Assert.assertEquals(3L, list.getQuick(2));
                Assert.assertEquals(5L, list.getQuick(4));
            }
        });
    }

    @Test
    public void testSmoke() throws Exception {
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new FastGroupByAllocator(64, Numbers.SIZE_1GB)) {
                GroupByLongList list = new GroupByLongList(16, 0L);
                list.setAllocator(allocator);
                list.of(0);

                Assert.assertEquals(0, list.size());
                Assert.assertEquals(16, list.capacity());

                final int N = 1000;

                for (int i = 0; i < N; i++) {
                    list.add(i);
                }
                Assert.assertEquals(N, list.size());
                Assert.assertTrue(list.capacity() >= N);
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, list.get(i));
                }
            }
        });
    }
}
