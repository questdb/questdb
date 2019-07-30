/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.cairo;

import com.questdb.cairo.security.AllowAllCairoSecurityContext;
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.AbstractGriffinTest;
import com.questdb.griffin.SqlCompiler;
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

public class TableReaderTailRecordCursorTest extends AbstractGriffinTest {
    private final static CairoEngine engine = new CairoEngine(configuration);
    private final static SqlCompiler compiler = new SqlCompiler(engine);

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
                10000000,
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
        TestUtils.assertMemoryLeak(() -> {
            compiler.compile(createStatement);
            final AtomicInteger errorCount = new AtomicInteger();
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final CountDownLatch latch = new CountDownLatch(2);
            try {
                new Thread(() -> {
                    try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz")) {
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
                    try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_VERSION)) {
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
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }

    private void testBusyPollFromBottomOfTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy)
                );

                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz")) {
                    long ts = 0;
                    long addr = Unsafe.malloc(blobSize);
                    try {

                        Rnd rnd = new Rnd();
                        appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                        ts = n * timestampIncrement;
                        try (
                                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_VERSION);
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
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }

    private void testBusyPollFromMidTable(int partitionBy, long timestampIncrement) throws Exception {
        final int blobSize = 1024;
        final int n = 1000;
        TestUtils.assertMemoryLeak(() -> {
            try {
                compiler.compile(
                        "create table xyz (sequence INT, event BINARY, ts LONG, stamp TIMESTAMP) timestamp(stamp) partition by " + PartitionBy.toString(partitionBy)
                );

                try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "xyz")) {
                    long ts = 0;
                    long addr = Unsafe.malloc(blobSize);
                    try {

                        Rnd rnd = new Rnd();
                        appendRecords(0, n, timestampIncrement, writer, ts, addr, rnd);
                        ts = n * timestampIncrement;
                        try (
                                TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "xyz", TableUtils.ANY_TABLE_VERSION);
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
            } finally {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
            }
        });
    }
}
