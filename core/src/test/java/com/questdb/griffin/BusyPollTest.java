/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.std.BinarySequence;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BusyPollTest extends AbstractCairoTest {
    private final static Engine engine = new Engine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine, configuration);
    private final static BindVariableService bindVariableService = new BindVariableService();

    @Test
    public void testByDay() throws Exception {
        testBusyPoll(
                0L,
                10000000,
                300_000,
                128,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by DAY"
        );
    }

    @Test
    public void testByMonth() throws Exception {
        testBusyPoll(
                0L,
                40000000,
                300_000,
                128,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by MONTH"
        );
    }

    @Test
    public void testByYear() throws Exception {
        testBusyPoll(
                0L,
                480000000,
                300_000,
                128,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by YEAR"
        );
    }

    @Test
    public void testNonPartitioned() throws Exception {
        testBusyPoll(
                0L,
                10000,
                3_000_000,
                128,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by NONE"
        );
    }

    private void testBusyPoll(long timestamp, long timestampIncrement, int n, int blobSize, String createStatement) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile(createStatement, bindVariableService);
            final AtomicInteger errorCount = new AtomicInteger();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final CountDownLatch latch = new CountDownLatch(2);
            try {
                new Thread(() -> {
                    try (TableWriter writer = engine.getWriter("xyz")) {
                        barrier.await();
                        long ts = timestamp;
                        long addr = Unsafe.malloc(blobSize);
                        try {
                            Rnd rnd = new Rnd();
                            for (int i = 0; i < n; i++) {
                                TableWriter.Row row = writer.newRow(ts);
                                row.putInt(0, i);
                                for (int k = 0; k < blobSize; k++) {
                                    Unsafe.getUnsafe().putByte(addr + k, rnd.nextByte());
                                }
                                row.putBin(1, addr, blobSize);
                                row.putLong(2, rnd.nextLong());
                                row.append();
                                writer.commit();
                                ts += timestampIncrement;
                            }
                        } finally {
                            Unsafe.free(addr, blobSize);
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try (TableReader reader = engine.getReader("xyz", 0)) {
                        Rnd rnd = new Rnd();
                        int count = 0;
                        final TableReaderIncrementalRecordCursor cursor = new TableReaderIncrementalRecordCursor();
                        cursor.of(reader);
                        final Record record = cursor.getRecord();
                        barrier.await();
                        while (count < n) {
                            if (cursor.reload()) {
                                while (cursor.hasNext()) {
                                    Assert.assertEquals(count, record.getInt(0));
                                    BinarySequence binarySequence = record.getBin(1);
                                    for (int i = 0; i < blobSize; i++) {
                                        Assert.assertEquals(rnd.nextByte(), binarySequence.byteAt(i));
                                    }
                                    Assert.assertEquals(rnd.nextLong(), record.getLong(2));
                                    count++;
                                }
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                        errorCount.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                }).start();

                Assert.assertTrue(latch.await(600, TimeUnit.SECONDS));
                Assert.assertEquals(0, errorCount.get());
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }
}
