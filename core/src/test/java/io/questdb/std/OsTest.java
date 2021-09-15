/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.std;

import io.questdb.cairo.TableUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OsTest {
    @Test
    public void testAffinity() throws Exception {
        if (Os.type != Os.OSX_ARM64) {
            Assert.assertEquals(0, Os.setCurrentThreadAffinity(0));

            AtomicInteger result = new AtomicInteger(-1);
            CountDownLatch threadHalt = new CountDownLatch(1);

            new Thread(() -> {
                result.set(Os.setCurrentThreadAffinity(1));
                threadHalt.countDown();
            }).start();

            Assert.assertTrue(threadHalt.await(1, TimeUnit.SECONDS));
            Assert.assertEquals(0, result.get());

            Assert.assertEquals(0, Os.setCurrentThreadAffinity(-1));
        }
    }

    @Test
    public void testCurrentTimeMicros() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeMicros();
        long delta = actual / 1000 - reference;
        Assert.assertTrue(delta < 200);
    }

    @Test
    public void testCurrentTimeNanos() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeNanos();
        Assert.assertTrue(actual > 0);
        long delta = actual / 1_000_000 - reference;
        Assert.assertTrue(delta < 200);
    }


    private final static Log LOG = LogFactory.getLog(OsTest.class);

    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testVanilla() throws IOException, BrokenBarrierException, InterruptedException {
        long pagesPerLong = Files.PAGE_SIZE / 8;
        for(int loop = 0; loop < 200000; loop++) {
            Random rnd = new Random();
            for (long longCountIncr = pagesPerLong; longCountIncr < pagesPerLong * 3; longCountIncr += rnd.nextDouble() * 512) {
                long longCount = longCountIncr;
                final var readLatch = new CountDownLatch(1);
                final File file = temp.newFile();
                try (Path path = new Path()) {
                    path.of(file.getAbsolutePath()).$();
                    FilesFacade ff = FilesFacadeImpl.INSTANCE;


                    // barrier to make sure both threads kick in at the same time;
                    final CyclicBarrier barrier = new CyclicBarrier(2);
                    final AtomicInteger errorCount = new AtomicInteger();
                    long fd1 = TableUtils.openRW(ff, path, LOG);
                    long size = longCount * 8 / Files.PAGE_SIZE + 1;
                    ff.truncate(fd1, size * Files.PAGE_SIZE);

                    // have this thread write another page
                    Thread th = new Thread(() -> {
                        try {
                            barrier.await();
                            // over allocate
                            long mem = TableUtils.mapRW(ff, fd1, (size) * Files.PAGE_SIZE);
                            for (int i = 0; i < longCount; i++) {
                                Unsafe.getUnsafe().putLong(mem + i * 8L, i);
                            }
                            readLatch.countDown();
                            ff.munmap(mem, (size) * Files.PAGE_SIZE);
                            ff.truncate(mem, longCount * 8);
                            FilesFacadeImpl.INSTANCE.close(fd1);
                        } catch (Throwable e) {
                            errorCount.incrementAndGet();
                            e.printStackTrace();
                        }
                    });
                    th.start();

                    barrier.await();

                    long fd2 = TableUtils.openRO(ff, path, LOG);
                    try {
                        readLatch.await();
                        long mem = TableUtils.mapRO(ff, fd2, longCount * 8);
                        try {
                            for (int i = 0; i < longCount; i++) {
                                long value = Unsafe.getUnsafe().getLong(mem + i * 8L);
                                if (i != value) {
                                    Assert.fail("value " + value + ",offset " + i + ", size " + longCount + ", mapped " + size * Files.PAGE_SIZE);
                                }
                            }
                        } finally {
                            ff.munmap(mem, longCount * 8);
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