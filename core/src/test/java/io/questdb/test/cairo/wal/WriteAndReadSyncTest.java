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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteAndReadSyncTest extends AbstractCairoTest {

    private final static Log LOG = LogFactory.getLog(WriteAndReadSyncTest.class);

    @Test
    public void testVanilla() throws IOException, BrokenBarrierException, InterruptedException {
        final long longsPerPage = Files.PAGE_SIZE / 8;
        for (int loop = 0; loop < 10; loop++) {
            Rnd rnd = new Rnd();
            // increments randomly by [0..512] up to three pages
            for (long longCountIncr = longsPerPage, limit = longsPerPage * 3; longCountIncr < limit; longCountIncr += (long) (rnd.nextDouble() * 512)) {
                long longCount = longCountIncr;
                final CountDownLatch readLatch = new CountDownLatch(1);
                final File file = temp.newFile();
                try (Path path = new Path()) {
                    path.of(file.getAbsolutePath()).$();
                    FilesFacade ff = TestFilesFacadeImpl.INSTANCE;

                    // barrier to make sure both threads kick in at the same time;
                    final CyclicBarrier barrier = new CyclicBarrier(2);
                    final AtomicInteger errorCount = new AtomicInteger();
                    long fd1 = TableUtils.openRW(ff, path.$(), LOG, configuration.getWriterFileOpenOpts());
                    long size = longCount * 8 / Files.PAGE_SIZE + 1;

                    // have this thread write another page
                    Thread th = new Thread(() -> {
                        try {
                            barrier.await();
                            // over allocate
                            long mem = TableUtils.mapRW(ff, fd1, (size) * Files.PAGE_SIZE, MemoryTag.NATIVE_DEFAULT);
                            for (int i = 0; i < longCount; i++) {
                                Unsafe.getUnsafe().putLong(mem + i * 8L, i);
                            }
                            readLatch.countDown();
                            ff.munmap(mem, (size) * Files.PAGE_SIZE, MemoryTag.NATIVE_DEFAULT);
                            ff.truncate(fd1, longCount * 8);
                            TestFilesFacadeImpl.INSTANCE.close(fd1);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            e.printStackTrace();
                        }
                    });
                    th.start();
                    barrier.await();

                    long fd2 = TableUtils.openRO(ff, path.$(), LOG);
                    try {
                        readLatch.await();
                        long mem = TableUtils.mapRO(ff, fd2, longCount * 8, MemoryTag.NATIVE_DEFAULT);
                        try {
                            for (int i = 0; i < longCount; i++) {
                                long value = Unsafe.getUnsafe().getLong(mem + i * 8L);
                                if (i != value) {
                                    Assert.fail("value " + value + ",offset " + i + ", size " + longCount + ", mapped " + size * Files.PAGE_SIZE);
                                }
                            }
                        } finally {
                            ff.munmap(mem, longCount * 8, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        ff.close(fd2);
                    }
                    Assert.assertEquals(0, errorCount.get());
                    th.join();
                }
            }
        }
    }
}
