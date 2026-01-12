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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.engine.groupby.FastGroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.griffin.engine.groupby.GroupByAllocatorFactory;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.Bytes;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

@RunWith(Parameterized.class)
public class GroupByAllocatorTest extends AbstractCairoTest {
    private final AllocatorType allocatorType;

    public GroupByAllocatorTest(AllocatorType allocatorType) {
        this.allocatorType = allocatorType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {AllocatorType.FAST_ALLOCATOR},
        });
    }

    @Test
    public void testAlignment() throws Exception {
        assertMemoryLeak(() -> {
            long usedSpaceAligned;
            int N = 5000;
            try (GroupByAllocator alignedAllocator = new FastGroupByAllocator(1024, 1024, true)) {
                for (int i = 0; i < N; i++) {
                    long ptr = alignedAllocator.malloc(1);
                    Assert.assertEquals(Bytes.align8b(ptr), ptr);
                }
                usedSpaceAligned = alignedAllocator.allocated();
            }

            try (GroupByAllocator unalignedAllocator = new FastGroupByAllocator(1024, 1024, false)) {
                for (int i = 0; i < N; i++) {
                    unalignedAllocator.malloc(1);
                }
                Assert.assertTrue(unalignedAllocator.allocated() < usedSpaceAligned);
            }
        });
    }

    @Test
    public void testCanBeUsedAfterClear() throws Exception {
        final int N = 100;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                long ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }

                allocator.clear();

                ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }
            }
        });
    }

    @Test
    public void testCanBeUsedAfterClose() throws Exception {
        final int N = 100;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                long ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }

                allocator.close();
                allocator.reopen();

                ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }
            }
        });
    }

    @Test
    public void testFree() throws Exception {
        final int N = 100;
        final int minChunkSize = 64;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return minChunkSize;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                long ptr = allocator.malloc(minChunkSize / 4);
                // Touch the first byte to make sure the memory is allocated.
                Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                Assert.assertEquals(minChunkSize, allocator.allocated());

                // This call should be ignored since the size is smaller than the min chunk size.
                allocator.free(ptr, minChunkSize / 4);
                Assert.assertEquals(minChunkSize, allocator.allocated());

                allocator.close();
                allocator.reopen();

                ptr = allocator.malloc(2 * minChunkSize);
                // Touch the first byte to make sure the memory is allocated.
                Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                Assert.assertEquals(2 * minChunkSize, allocator.allocated());

                // This call should be ignored since the pointer is not at the beginning of the chunk.
                allocator.free(ptr + 1, 2 * minChunkSize - 1);
                Assert.assertEquals(2 * minChunkSize, allocator.allocated());

                allocator.close();
                allocator.reopen();

                for (int i = 0; i < N; i++) {
                    ptr = allocator.malloc(minChunkSize + i);
                    // Touch the first byte to make sure the memory is allocated.
                    Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                    Assert.assertEquals(minChunkSize + i, allocator.allocated());

                    allocator.free(ptr, minChunkSize + i);
                    Assert.assertEquals(0, allocator.allocated());
                }
            }
        });
    }

    @Test
    public void testMalloc() throws Exception {
        final int N = 10_000;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                for (int i = 0; i < N; i++) {
                    long ptr = allocator.malloc(i + 1);
                    for (int j = 0; j < i + 1; j++) {
                        // Touch the memory to make sure it's allocated.
                        Unsafe.getUnsafe().putByte(ptr + j, (byte) j);
                    }
                }
            }
        });
    }

    @Test
    public void testRealloc() throws Exception {
        final int N = 1000;
        final int M = 16;
        final int minChunkSize = 64;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorDefaultChunkSize() {
                return minChunkSize;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                int size = M;
                long ptr = allocator.malloc(size);
                // Touch the first byte to make sure the memory is allocated.
                Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                ptr = allocator.malloc(size);
                Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                Assert.assertEquals(minChunkSize, allocator.allocated());

                // This should be no-op.
                Assert.assertEquals(ptr, allocator.realloc(ptr, size, size));

                // This call should lead to slow path (malloc + memcpy).
                ptr = allocator.realloc(ptr, size, size + minChunkSize);
                Unsafe.getUnsafe().putByte(ptr, (byte) 42);
                Assert.assertEquals(2 * minChunkSize + size, allocator.allocated());

                allocator.close();
                allocator.reopen();

                ptr = allocator.malloc(size);
                for (int i = 0; i < size; i++) {
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) 42);
                }

                for (int i = 0; i < N; i++) {
                    ptr = allocator.realloc(ptr, size, ++size);
                    for (int j = 0; j < M; j++) {
                        Assert.assertEquals(42, Unsafe.getUnsafe().getByte(ptr + j));
                    }
                    for (int j = M; j < size; j++) {
                        // Touch the tail part of the memory to make sure it's allocated.
                        Unsafe.getUnsafe().putByte(ptr + j, (byte) j);
                    }
                }
            }
        });
    }

    @Test
    public void testThrowsOnTooLargeMallocRequest() throws Exception {
        final long maxRequest = 64;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorMaxChunkSize() {
                return maxRequest;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                allocator.malloc(maxRequest + 1);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    @Test
    public void testThrowsOnTooLargeReallocRequest() throws Exception {
        final long maxRequest = 64;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public long getGroupByAllocatorMaxChunkSize() {
                return maxRequest;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = createAllocator(config)) {
                long ptr = allocator.malloc(maxRequest - 1);
                allocator.realloc(ptr, maxRequest - 1, maxRequest + 1);
                Assert.fail();
            } catch (CairoException ignore) {
            }
        });
    }

    private GroupByAllocator createAllocator(CairoConfiguration config) {
        if (Objects.requireNonNull(allocatorType) == AllocatorType.FAST_ALLOCATOR) {
            return GroupByAllocatorFactory.createAllocator(config);
        }
        throw new IllegalArgumentException("Unexpected allocator type: " + allocatorType);
    }

    public enum AllocatorType {
        FAST_ALLOCATOR
    }
}
