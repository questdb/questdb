/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.engine.groupby.GroupByAllocator;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class GroupByAllocatorTest extends AbstractCairoTest {

    @Test
    public void testCanBeUsedAfterClose() throws Exception {
        final int N = 100;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public int getGroupByAllocatorChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                long ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }

                allocator.close();

                ptr = allocator.malloc(N);
                for (int i = 0; i < N; i++) {
                    // Touch the memory to make sure it's allocated.
                    Unsafe.getUnsafe().putByte(ptr + i, (byte) i);
                }
            }
        });
    }

    @Test
    public void testConcurrent() throws Exception {
        final int N = 1000;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public int getGroupByAllocatorChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                final int threads = 8;

                final AtomicInteger errors = new AtomicInteger();
                final SOCountDownLatch latch = new SOCountDownLatch(threads);
                final CyclicBarrier barrier = new CyclicBarrier(threads);
                for (int i = 0; i < threads; i++) {
                    int finalI = i;
                    new Thread(() -> {
                        try {
                            barrier.await();

                            for (int j = 0; j < N; j++) {
                                long ptr = allocator.malloc(j + 1);
                                // Touch the memory to make sure it's allocated.
                                for (int k = 0; k < j + 1; k++) {
                                    Unsafe.getUnsafe().putByte(ptr + k, (byte) finalI);
                                }
                                // Assert that no one else touched the memory.
                                for (int k = 0; k < j + 1; k++) {
                                    Assert.assertEquals((byte) finalI, Unsafe.getUnsafe().getByte(ptr + k));
                                }
                            }
                        } catch (Throwable th) {
                            th.printStackTrace();
                            errors.incrementAndGet();
                        } finally {
                            latch.countDown();
                        }
                    }).start();
                }

                latch.await();
                Assert.assertEquals(0, errors.get());
            }
        });
    }

    @Test
    public void testMalloc() throws Exception {
        final int N = 10_000;
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public int getGroupByAllocatorChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
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
        final CairoConfiguration config = new DefaultCairoConfiguration(root) {
            @Override
            public int getGroupByAllocatorChunkSize() {
                return 64;
            }
        };
        assertMemoryLeak(() -> {
            try (GroupByAllocator allocator = new GroupByAllocator(config)) {
                int size = M;
                long ptr = allocator.malloc(size);
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
}
