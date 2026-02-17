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

package io.questdb.test.cairo;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TableReaderTailRecordCursorTest extends AbstractCairoTest {

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
    public void testBusyPollByWeek() throws Exception {
        testBusyPollFromMidTable(PartitionBy.WEEK, 7 * 3000000000L);
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
    public void testBusyPollFromBottomByWeek() throws Exception {
        testBusyPollFromBottomOfTable(PartitionBy.WEEK, 7 * 3000000000L);
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
    public void testByWeek() throws Exception {
        testBusyPoll(
                10000000,
                300_000,
                "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by WEEK"
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
            execute(createStatement);
            final AtomicInteger errorCount = new AtomicInteger();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final CountDownLatch latch = new CountDownLatch(2);
            TableToken tableToken = engine.verifyTableName("xyz");
            new Thread(() -> {
                try (TableWriter writer = getWriter(tableToken)) {
                    barrier.await();
                    long ts = 0;
                    long addr = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Rnd rnd = new Rnd();
                        for (int i = 0; i < n; i++) {
                            if (errorCount.get() > 0) {
                                // Reader already failed
                                return;
                            }
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
                        Unsafe.free(addr, 128, MemoryTag.NATIVE_DEFAULT);
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    latch.countDown();
                }
            }).start();

            new Thread(() -> {
                try (
                        TableReader reader = engine.getReader(tableToken);
                        TestTableReaderTailRecordCursor cursor = new TestTableReaderTailRecordCursor().of(reader)
                ) {
                    Rnd rnd = new Rnd();
                    int count = 0;
                    final Record record = cursor.getRecord();
                    barrier.await();
                    while (count < n) {
                        if (cursor.reload()) {
                            while (cursor.hasNext()) {
                                if (count != record.getInt(0)) {
                                    errorCount.incrementAndGet();
                                    StringSink ss = new StringSink();
                                    ss.put("[");
                                    for (int i = 0; i < reader.getPartitionCount(); i++) {
                                        ss.put(reader.getPartitionRowCount(i));
                                        ss.put(",");
                                    }
                                    ss.put("]:").put(reader.getTxn());
                                    Assert.assertEquals(ss.toString(), count, record.getInt(0));
                                }
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
                    Path.clearThreadLocals();
                    latch.countDown();
                }
            }).start();

            Assert.assertTrue(latch.await(600, TimeUnit.SECONDS));
            Assert.assertEquals(0, errorCount.get());
            Os.sleep(1000);
        });
    }

    private void testBusyPollFromBottomOfTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        assertMemoryLeak(() -> {
            execute("create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy));

            TableToken tableToken = engine.verifyTableName("xyz");
            try (TableWriter writer = getWriter(tableToken)) {
                long ts = 0;
                long addr = Unsafe.malloc(blobSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Rnd rnd = new Rnd();
                    appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                    ts = n * timestampIncrement;
                    try (
                            TableReader reader = engine.getReader(tableToken, TableUtils.ANY_TABLE_VERSION);
                            TestTableReaderTailRecordCursor cursor = new TestTableReaderTailRecordCursor().of(reader)
                    ) {
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
                    Unsafe.free(addr, blobSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    private void testBusyPollFromMidTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        assertMemoryLeak(() -> {
            execute(
                    "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy),
                    sqlExecutionContext
            );

            TableToken tableToken = engine.verifyTableName("xyz");
            try (TableWriter writer = getWriter(tableToken)) {
                long ts = 0;
                long addr = Unsafe.malloc(blobSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    Rnd rnd = new Rnd();
                    appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                    ts = n * timestampIncrement;
                    try (
                            TableReader reader = engine.getReader(tableToken, TableUtils.ANY_TABLE_VERSION);
                            TestTableReaderTailRecordCursor cursor = new TestTableReaderTailRecordCursor().of(reader)
                    ) {
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
                    Unsafe.free(addr, blobSize, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }
}
