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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.std.BinarySequence;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TableReaderTailRecordCursorTest extends AbstractGriffinTest {

    @Test
    public void testBusyPollByDay() throws Exception {
        testBusyPollFromMidTable(PartitionBy.DAY, 3000000000L);
    }

    @Test
    public void testBusyPollByMonth() throws Exception {
        testBusyPollFromMidTable(PartitionBy.MONTH, 50000000000L);
    }

    @Test
    public void testBusyPollByNone() throws Exception {
        testBusyPollFromMidTable(PartitionBy.NONE, 10000L);
    }

    @Test
    public void testBusyPollByYear() throws Exception {
        testBusyPollFromMidTable(PartitionBy.YEAR, 365 * 500000000L);
    }

    @Test
    public void testBusyPollFromBottomByDay() throws Exception {
        testBusyPollFromBottomOfTable(PartitionBy.DAY, 3000000000L);
    }

    @Test
    public void testBusyPollFromBottomByMonth() throws Exception {
        testBusyPollFromBottomOfTable(PartitionBy.MONTH, 50000000000L);
    }

    @Test
    public void testBusyPollFromBottomByNone() throws Exception {
        testBusyPollFromBottomOfTable(PartitionBy.NONE, 10000L);
    }

    @Test
    public void testBusyPollFromBottomByYear() throws Exception {
        testBusyPollFromBottomOfTable(PartitionBy.YEAR, 365 * 500000000L);
    }

    @Test
    public void testByDay() throws Exception {
        testBusyPoll(
                1000000,
                300_000,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by DAY"
        );
    }

    @Test
    public void testByMonth() throws Exception {
        testBusyPoll(
                40000000,
                300_000,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by MONTH"
        );
    }

    @Test
    public void testByYear() throws Exception {
        testBusyPoll(
                480000000,
                300_000,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by YEAR"
        );
    }

    @Test
    public void testNonPartitioned() throws Exception {
        testBusyPoll(
                10000,
                3_000_000,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by NONE"
        );
    }

    private void appendRecords(int start, int n, long timestampIncrement, TableWriter writer, long ts, long addr, Rnd rnd) {
        for (int i = 0; i < n; i++) {
            TableWriter.Row row = writer.newRow(ts);
            row.putInt(0, i);
            for (int k = 0; k < 1024; k++) {
                Unsafe.getUnsafe().putByte(addr + k, rnd.nextByte());
            }
            row.putBin(1, addr, 1024);
            row.putLong(2, start + n - i);
            row.append();
            writer.commit();
            ts += timestampIncrement;
        }
    }

    private void testBusyPoll(long timestampIncrement, int n, String createStatement) throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(createStatement, sqlExecutionContext);
            final AtomicInteger errorCount = new AtomicInteger();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final CountDownLatch latch = new CountDownLatch(2);
            new Thread(() -> {
                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz", "testing")) {
                    barrier.await();
                    long ts = 0;
                    long addr = Unsafe.malloc(128);
                    try {
                        Rnd rnd = new Rnd();
                        for (int i = 0; i < n; i++) {
                            TableWriter.Row row = writer.newRow(ts);
                            row.putInt(0, i);
                            for (int k = 0; k < 128; k++) {
                                Unsafe.getUnsafe().putByte(addr + k, rnd.nextByte());
                            }
                            row.putBin(1, addr, 128);
                            row.putLong(2, rnd.nextLong());
                            row.append();
                            writer.commit();
                            ts += timestampIncrement;
                        }
                    } finally {
                        Unsafe.free(addr, 128);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();

            new Thread(() -> {
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION)) {
                    Rnd rnd = new Rnd();
                    int count = 0;
                    final TableReaderTailRecordCursor cursor = new TableReaderTailRecordCursor();
                    cursor.of(reader);
                    final Record record = cursor.getRecord();
                    barrier.await();
                    while (count < n) {
                        if (cursor.reload()) {
                            while (cursor.hasNext()) {
                                Assert.assertEquals(count, record.getInt(0));
                                BinarySequence binarySequence = record.getBin(1);
                                for (int i = 0; i < 128; i++) {
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
        });
    }

    private void testBusyPollFromBottomOfTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy),
                    sqlExecutionContext
            );

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz", "testing")) {
                long ts = 0;
                long addr = Unsafe.malloc(blobSize);
                try {

                    Rnd rnd = new Rnd();
                    appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                    ts = n * timestampIncrement;
                    try (
                            TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
                            TableReaderTailRecordCursor cursor = new TableReaderTailRecordCursor()
                    ) {
                        cursor.of(reader);
                        cursor.toBottom();

                        Assert.assertFalse(cursor.reload());
                        Assert.assertFalse(cursor.hasNext());

                        appendRecords(n, n, timestampIncrement, writer, ts, addr, rnd);
                        Assert.assertTrue(cursor.reload());

                        int count = 0;
                        final Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            Assert.assertEquals(n + n - count, record.getLong(2));
                            count++;
                        }

                        writer.truncate();
                        Assert.assertTrue(cursor.reload());
                        Assert.assertFalse(cursor.hasNext());

                        appendRecords(n * 2, n / 2, timestampIncrement, writer, ts, addr, rnd);
                        Assert.assertTrue(cursor.reload());

                        count = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(n * 2 + n / 2 - count, record.getLong(2));
                            count++;
                        }

                        Assert.assertEquals(n / 2, count);
                    }
                } finally {
                    Unsafe.free(addr, blobSize);
                }
            }
        });
    }

    private void testBusyPollFromMidTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy),
                    sqlExecutionContext
            );

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz", "testing")) {
                long ts = 0;
                long addr = Unsafe.malloc(blobSize);
                try {

                    Rnd rnd = new Rnd();
                    appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                    ts = n * timestampIncrement;
                    try (
                            TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
                            TableReaderTailRecordCursor cursor = new TableReaderTailRecordCursor()
                    ) {
                        cursor.of(reader);
                        Assert.assertTrue(cursor.reload());
                        int count = 0;
                        Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            Assert.assertEquals(n - count, record.getLong(2));
                            count++;
                        }

                        Assert.assertFalse(cursor.reload());
                        Assert.assertFalse(cursor.hasNext());

                        appendRecords(n, n, timestampIncrement, writer, ts, addr, rnd);
                        Assert.assertTrue(cursor.reload());

                        count = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(n + n - count, record.getLong(2));
                            count++;
                        }

                        writer.truncate();
                        Assert.assertTrue(cursor.reload());
                        Assert.assertFalse(cursor.hasNext());

                        appendRecords(n * 2, n / 2, timestampIncrement, writer, ts, addr, rnd);
                        Assert.assertTrue(cursor.reload());

                        count = 0;
                        while (cursor.hasNext()) {
                            Assert.assertEquals(n * 2 + n / 2 - count, record.getLong(2));
                            count++;
                        }

                        Assert.assertEquals(n / 2, count);
                    }
                } finally {
                    Unsafe.free(addr, blobSize);
                }
            }
        });
    }
}
