/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

public class ColumnVersionWriterTest extends AbstractCairoTest {
    public static void assertEqual(LongList expected, LongList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    @Test
    public void testColumnTop() {
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader().ofRO(FilesFacadeImpl.INSTANCE, path)
        ) {
            for (int i = 0; i < 100; i += 2) {
                w.upsert(i, i % 10, -1, i * 10L);
            }

            w.commit();

            r.readSafe(configuration.getMicrosecondClock(), 1000);
            for (int i = 0; i < 100; i++) {
                long colTop = r.getColumnTop(i, i % 10);
                Assert.assertEquals(i % 2 == 0 ? i * 10 : 0, colTop);
            }

            assertEqual(w.getCachedList(), r.getCachedList());
        }
    }

    @Test
    public void testFuzz() {
        final Rnd rnd = new Rnd();
        final int N = 100_000;
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader().ofRO(FilesFacadeImpl.INSTANCE, path)
        ) {
            w.upsert(1, 2, 3, -1);

            for (int i = 0; i < N; i++) {
                // increment from 0 to 4 columns
                int increment = rnd.nextInt(32);

                for (int j = 0; j < increment; j++) {
                    w.upsert(rnd.nextLong(20), rnd.nextInt(10), i, -1);
                }

                w.commit();
                r.readSafe(configuration.getMicrosecondClock(), 1000);
                Assert.assertTrue(w.getCachedList().size() > 0);
                assertEqual(w.getCachedList(), r.getCachedList());
                // assert list is ordered by (timestamp,column_index)

                LongList list = r.getCachedList();
                long prevTimestamp = -1;
                long prevColumnIndex = -1;
                for (int j = 0, n = list.size(); j < n; j += ColumnVersionWriter.BLOCK_SIZE) {
                    long timestamp = list.getQuick(j);
                    long columnIndex = list.getQuick(j + 1);

                    if (prevTimestamp < timestamp) {
                        prevTimestamp = timestamp;
                        prevColumnIndex = columnIndex;
                        continue;
                    }

                    if (prevTimestamp == timestamp) {
                        Assert.assertTrue(prevColumnIndex < columnIndex);
                        prevColumnIndex = columnIndex;
                        continue;
                    }

                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testFuzzConcurrent() throws InterruptedException {
        final int N = 10_000;
        try (
                Path path = new Path();
                ColumnVersionWriter w = new ColumnVersionWriter(FilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                ColumnVersionReader r = new ColumnVersionReader().ofRO(FilesFacadeImpl.INSTANCE, path)
        ) {
            CyclicBarrier barrier = new CyclicBarrier(2);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicLong done = new AtomicLong();

            Thread writer = new Thread(() -> {
                Rnd rnd = new Rnd();
                try {
                    barrier.await();
                    for (int txn = 0; txn < N; txn++) {
                        int increment = rnd.nextInt(32);
                        for (int j = 0; j < increment; j++) {
                            w.upsert(rnd.nextLong(20), rnd.nextInt(10), txn, -1);
                        }
                        LongList list = w.getCachedList();
                        for (int j = 0, n = list.size(); j < n; j += ColumnVersionWriter.BLOCK_SIZE) {
                            long timestamp = list.getQuick(j);
                            int index = (int) list.getQuick(j + 1);
                            w.upsert(timestamp, index, txn, -1);
                        }
                        w.commit();
                    }
                } catch (Throwable th) {
                    exceptions.add(th);
                } finally {
                    done.incrementAndGet();
                }
            });

            Thread reader = new Thread(() -> {
                try {
                    barrier.await();
                    while (done.get() == 0) {
                        r.readSafe(configuration.getMicrosecondClock(), 5_000_000);
                        long txn = -1;
                        LongList list = r.getCachedList();
                        long prevTimestamp = -1;
                        long prevColumnIndex = -1;

                        for (int i = 0, n = list.size(); i < n; i += ColumnVersionWriter.BLOCK_SIZE) {
                            long timestamp = list.getQuick(i);
                            long columnIndex = list.getQuick(i + 1);

                            if (prevTimestamp < timestamp) {
                                prevTimestamp = timestamp;
                                prevColumnIndex = columnIndex;
                                continue;
                            } else {
                                if (prevTimestamp == timestamp) {
                                    Assert.assertTrue(prevColumnIndex < columnIndex);
                                    prevColumnIndex = columnIndex;
                                } else {
                                    Assert.fail();
                                }
                            }

                            long txn2 = list.getQuick(i + 2);
                            if (txn == -1) {
                                txn = txn2;
                            } else if (txn != txn2) {
                                // All txn must be same.
                                Assert.assertEquals("index " + i / ColumnVersionWriter.BLOCK_SIZE + ", version " + r.getVersion(), txn, txn2);
                            }
                        }
                    }
                } catch (Throwable th) {
                    exceptions.add(th);
                }
            });

            writer.start();
            reader.start();

            writer.join();
            reader.join();

            if (exceptions.size() != 0) {
                Assert.fail(exceptions.poll().toString());
            }
        }

    }
}