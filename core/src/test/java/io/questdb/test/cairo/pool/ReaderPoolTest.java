/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.cairo.TestFilesFacade;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

@SuppressWarnings("CallToPrintStackTrace")
public class ReaderPoolTest extends AbstractCairoTest {
    private TableToken uTableToken;

    @Before
    public void setUpInstance() {
        TableModel model = new TableModel(configuration, "u", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        uTableToken = engine.verifyTableName("u");
    }

    @Test
    public void testAllocate() throws Exception {
        assertWithPool(pool -> {
            // has to be less than the available entries in the pool, default is 160
            final int numOfThreads = 50;

            final java.util.concurrent.ConcurrentHashMap<Integer, Throwable> errors = new ConcurrentHashMap<>();
            final CyclicBarrier start = new CyclicBarrier(numOfThreads);
            final SOCountDownLatch end = new SOCountDownLatch(numOfThreads);
            final AtomicInteger readerCount = new AtomicInteger();

            for (int i = 0; i < numOfThreads; i++) {
                final int threadIndex = i;
                new Thread(() -> {
                    TestUtils.await(start);
                    try {
                        try (TableReader ignored = pool.get(uTableToken)) {
                            readerCount.incrementAndGet();
                        }
                    } catch (Throwable th) {
                        errors.put(threadIndex, th);
                    } finally {
                        Path.clearThreadLocals();
                    }
                    end.countDown();
                }).start();
            }
            end.await();

            if (!errors.isEmpty()) {
                for (Map.Entry<Integer, Throwable> entry : errors.entrySet()) {
                    LOG.error().$("Error in thread [id=").$(entry.getKey()).$("] ").$(entry.getValue()).$();
                }
                fail("Error in threads");
            }

            Assert.assertEquals(numOfThreads, readerCount.get());
        });
    }

    @Test
    public void testAllocateAndClear() throws Exception {
        assertWithPool(pool -> {
            int n = 2;
            final CyclicBarrier barrier = new CyclicBarrier(n);
            final CountDownLatch halt = new CountDownLatch(n);
            final AtomicInteger errors = new AtomicInteger();
            final AtomicInteger readerCount = new AtomicInteger();

            new Thread(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        try (TableReader ignored = pool.get(uTableToken)) {
                            readerCount.incrementAndGet();
                        } catch (EntryUnavailableException ignored) {
                        }

                        if (i == 1) {
                            barrier.await();
                        }
                        Os.pause();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    halt.countDown();
                }
            }).start();

            new Thread(() -> {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        pool.releaseInactive();
                        Os.pause();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    Path.clearThreadLocals();
                    halt.countDown();
                }
            }).start();

            halt.await();

            Assert.assertTrue(readerCount.get() > 0);
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testBasicCharSequence() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        sink.clear();
        sink.put("x");
        TableToken xTableToken = engine.verifyTableName(sink);
        assertWithPool(pool -> {
            sink.clear();
            sink.put("x");

            TableReader reader1 = pool.get(engine.verifyTableName(sink));
            Assert.assertNotNull(reader1);
            reader1.close();

            // mutate sink
            sink.clear();
            sink.put("y");

            try (TableReader reader2 = pool.get(xTableToken)) {
                Assert.assertSame(reader1, reader2);
            }
        });
    }

    @Test
    public void testClosePoolWhenReaderIsOut() throws Exception {
        assertWithPool(pool -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
            AbstractCairoTest.create(model);

            try (TableReader reader = pool.get(engine.verifyTableName("x"))) {
                Assert.assertNotNull(reader);
                try {
                    pool.close();
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(),
                            "table is left behind on pool shutdown [table=" + reader.getTableToken().getDirName() + "]");
                }
                Assert.assertTrue(reader.isOpen());
            }
        });
    }

    @Test
    public void testCloseReaderWhenPoolClosed() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.get(uTableToken);
            Assert.assertNotNull(reader);
            try {
                pool.close();
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(),
                        "table is left behind on pool shutdown [table=" + reader.getTableToken().getDirName() + "]");
            }
            Assert.assertTrue(reader.isOpen());
            reader.close();
            reader.close();
        });
    }

    @Test
    public void testCloseWithActiveReader() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.get(uTableToken);
            Assert.assertNotNull(reader);
            try {
                pool.close();
                Assert.fail();
            } catch (CairoException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "is left behind");
            }

            Assert.assertTrue(reader.isOpen());
            reader.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testCloseWithInactiveReader() throws Exception {
        assertWithPool(pool -> {
            TableReader reader = pool.get(uTableToken);
            Assert.assertNotNull(reader);
            reader.close();
            Assert.assertTrue(reader.isOpen());
            pool.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testConcurrentGetAndGetCopyOfFuzz() throws Exception {
        final int readerThreadCount = 4;
        final int readerIterations = 200;
        final int writerIterations = 50;
        final int writerBatchSize = 10;
        final String tableName = "test";

        final Rnd seedRnd = TestUtils.generateRandom(LOG);

        assertWithPool((ReaderPool pool) -> {
            TableModel model = new TableModel(configuration, tableName, PartitionBy.HOUR)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            final CyclicBarrier barrier = new CyclicBarrier(readerThreadCount + 1);
            final CountDownLatch halt = new CountDownLatch(readerThreadCount + 1);
            final AtomicInteger errors = new AtomicInteger();

            final Rnd writerRnd = new Rnd(seedRnd.nextLong(), seedRnd.nextLong());
            new Thread(() -> {
                try {
                    barrier.await();
                    try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                        boolean columnsAdded = false;
                        boolean columnTypeChanged = false;
                        boolean columnRenamed = false;
                        for (int i = 0; i < writerIterations; i++) {
                            final int prob = writerRnd.nextInt(100);
                            if (prob >= 95 && columnsAdded) {
                                writer.removeColumn("sym2");
                                writer.removeColumn("int2");
                                writer.removeColumn(columnRenamed ? "bool3" : "bool2");
                                columnsAdded = false;
                                columnTypeChanged = false;
                                columnRenamed = false;
                            } else if (prob >= 90 && !columnsAdded) {
                                writer.addColumn("sym2", ColumnType.SYMBOL, 256, true, true, 256, false);
                                writer.addColumn("int2", ColumnType.INT);
                                writer.addColumn("bool2", ColumnType.BOOLEAN);
                                columnsAdded = true;
                            } else if (prob >= 85 && columnsAdded && !columnTypeChanged) {
                                writer.changeColumnType("sym2", ColumnType.STRING, 0, false, false, 0, false, null);
                                columnTypeChanged = true;
                            } else if (prob >= 80 && columnsAdded && !columnRenamed) {
                                writer.renameColumn("bool2", "bool3");
                                columnRenamed = true;
                            } else {
                                for (int j = 0; j < writerBatchSize; j++) {
                                    TableWriter.Row r = writer.newRow(Micros.SECOND_MICROS * i);
                                    r.putSym(0, writerRnd.nextString(writerRnd.nextInt(32)));
                                    r.append();
                                }
                                writer.commit();
                            }
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                    e.printStackTrace(System.out);
                } finally {
                    Path.clearThreadLocals();
                    halt.countDown();
                }
            }).start();

            final TableToken tableToken = engine.verifyTableName(tableName);
            for (int t = 0; t < readerThreadCount; t++) {
                final Rnd readerRnd = new Rnd(seedRnd.nextLong(), seedRnd.nextLong());
                new Thread(() -> {
                    final StringSink sink = new StringSink();
                    try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()) {
                        barrier.await();
                        for (int i = 0; i < readerIterations; i++) {
                            TableReader copiedReader = null;
                            TableReader reader = null;
                            try {
                                if (readerRnd.nextBoolean()) {
                                    reader = pool.get(tableToken);
                                } else {
                                    copiedReader = pool.get(tableToken);
                                    Os.sleep(1); // wait a bit
                                    reader = pool.getCopyOf(copiedReader);
                                }

                                // Just read the table contents.
                                cursor.of(reader);
                                println(reader.getMetadata(), cursor, sink);
                            } finally {
                                Misc.free(copiedReader);
                                Misc.free(reader);
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                        e.printStackTrace(System.out);
                    } finally {
                        Path.clearThreadLocals();
                        halt.countDown();
                    }
                }).start();
            }

            halt.await();
            Assert.assertEquals(0, halt.getCount());
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testConcurrentOpenAndClose() throws Exception {
        final int readerCount = 5;
        int threadCount = 2;
        final int iterations = 1000;

        final TableToken[] names = new TableToken[readerCount];
        for (int i = 0; i < readerCount; i++) {
            String name = "x" + i;
            TableModel model = new TableModel(configuration, name, PartitionBy.NONE).col("ts", ColumnType.DATE);
            AbstractCairoTest.create(model);
            names[i] = engine.verifyTableName(name);
        }

        assertWithPool(pool -> {
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch halt = new CountDownLatch(threadCount);
            final AtomicInteger errors = new AtomicInteger();

            for (int i = 0; i < threadCount; i++) {
                final int x = i;
                new Thread(() -> {
                    Rnd rnd = new Rnd(x, -x);
                    try {
                        barrier.await();

                        for (int i1 = 0; i1 < iterations; i1++) {
                            TableToken m = names[rnd.nextPositiveInt() % readerCount];
                            try (TableReader ignored = pool.get(m)) {
                                Os.pause();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        errors.incrementAndGet();
                    } finally {
                        Path.clearThreadLocals();
                        halt.countDown();
                    }
                }).start();
            }

            halt.await();
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testConcurrentRead() throws Exception {
        final int readerCount = 5;
        int threadCount = 2;
        final int iterations = 1000;
        Rnd dataRnd = new Rnd();

        final TableToken[] names = new TableToken[readerCount];
        final String[] expectedRows = new String[readerCount];
        final CharSequenceObjHashMap<String> expectedRowsMap = new CharSequenceObjHashMap<>();

        for (int i = 0; i < readerCount; i++) {
            String name = "x" + i;
            TableModel model = new TableModel(configuration, name, PartitionBy.NONE).col("ts", ColumnType.DATE);
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, name)) {
                for (int k = 0; k < 10; k++) {
                    TableWriter.Row r = writer.newRow();
                    r.putDate(0, dataRnd.nextLong());
                    r.append();
                }
                writer.commit();
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, name);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                println(reader.getMetadata(), cursor);
            }
            expectedRows[i] = sink.toString();
            expectedRowsMap.put(name, expectedRows[i]);
            names[i] = engine.verifyTableName(name);
        }

        assertWithPool((ReaderPool pool) -> {
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch halt = new CountDownLatch(threadCount);
            final AtomicInteger errors = new AtomicInteger();

            for (int k = 0; k < threadCount; k++) {
                new Thread(new Runnable() {
                    final ObjHashSet<TableReader> readers = new ObjHashSet<>();
                    final StringSink sink = new StringSink();

                    @Override
                    public void run() {
                        Rnd rnd = new Rnd();
                        try {
                            barrier.await();
                            TableToken name;

                            // on each iteration thread will do between 1 and 3 things:
                            // 1. it will open a random reader
                            // 2. it will read from one of readers it has opened
                            // 3. it will close one of readers it has opened
                            for (int i = 0; i < iterations; i++) {
                                if (readers.isEmpty() || (readers.size() < 40 && rnd.nextPositiveInt() % 4 == 0)) {
                                    name = names[rnd.nextPositiveInt() % readerCount];
                                    try {
                                        Assert.assertTrue(readers.add(pool.get(name)));
                                    } catch (EntryUnavailableException ignore) {
                                    }
                                }

                                Os.pause();

                                if (readers.isEmpty()) {
                                    continue;
                                }

                                int index = rnd.nextPositiveInt() % readers.size();
                                TableReader reader = readers.get(index);
                                Assert.assertTrue(reader.isOpen());

                                // read rows
                                String expectedRows = expectedRowsMap.get(reader.getTableToken().getTableName());
                                Assert.assertNotNull(expectedRows);
                                TestUtils.assertReader(
                                        expectedRows,
                                        reader,
                                        sink
                                );

                                Os.pause();

                                if (!readers.isEmpty() && rnd.nextPositiveInt() % 4 == 0) {
                                    TableReader r2 = readers.get(rnd.nextPositiveInt() % readers.size());
                                    Assert.assertTrue(r2.isOpen());
                                    r2.close();
                                    Assert.assertTrue(readers.remove(r2));
                                }

                                Os.pause();
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            e.printStackTrace();
                        } finally {
                            for (int i = 0; i < readers.size(); i++) {
                                readers.get(i).close();
                            }
                            Path.clearThreadLocals();
                            halt.countDown();
                        }
                    }
                }).start();
            }

            halt.await();
            Assert.assertEquals(0, halt.getCount());
            Assert.assertEquals(0, errors.get());
        });
    }

    @Test
    public void testCopyOfCreatesNewReader() throws Exception {
        assertWithPool(pool -> {
            final String tableName = "test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                TableWriter.Row r = writer.newRow(1);
                r.putSym(0, "foo");
                r.append();
                writer.commit();
            }

            final TableToken tableToken = engine.verifyTableName(tableName);
            try (
                    TableReader ogReader = pool.get(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
            ) {
                try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                    TableWriter.Row r = writer.newRow(Micros.DAY_MICROS + 1);
                    r.putSym(0, "bar");
                    r.append();
                    writer.commit();
                }

                cursor.of(ogReader);
                println(ogReader.getMetadata(), cursor);
                final String expected = sink.toString();

                TableReader copyReaderRef;
                try (TableReader copyReader = pool.getCopyOf(ogReader)) {
                    copyReaderRef = copyReader;
                    cursor.of(copyReader);
                    println(copyReader.getMetadata(), cursor);
                    final String copyActual = sink.toString();

                    Assert.assertEquals(expected, copyActual);
                }

                // This is the same instance as copyReader, but this time it's fully up-to-date.
                try (TableReader newerTxnReader = pool.get(tableToken)) {
                    Assert.assertSame(copyReaderRef, newerTxnReader);
                    cursor.of(newerTxnReader);
                    println(newerTxnReader.getMetadata(), cursor);
                    final String newerTxnActual = sink.toString();

                    Assert.assertNotEquals(expected, newerTxnActual);
                }
            }
        });
    }

    @Test
    public void testCopyOfEmptyTable() throws Exception {
        assertWithPool(pool -> {
            final String tableName = "test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            final TableToken tableToken = engine.verifyTableName(tableName);
            try (
                    TableReader ogReader = pool.get(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
            ) {
                cursor.of(ogReader);
                println(ogReader.getMetadata(), cursor);
                final String expected = sink.toString();

                try (TableReader copyReader = pool.getCopyOf(ogReader)) {
                    cursor.of(copyReader);
                    println(copyReader.getMetadata(), cursor);
                    final String copyActual = sink.toString();

                    Assert.assertEquals(expected, copyActual);
                }
            }
        });
    }

    @Test
    public void testCopyOfReloadsNewerTxnReader() throws Exception {
        assertWithPool(pool -> {
            final String tableName = "test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                TableWriter.Row r = writer.newRow(1);
                r.putSym(0, "foo");
                r.append();
                writer.commit();
            }

            final TableToken tableToken = engine.verifyTableName(tableName);
            try (
                    TableReader ogReader = pool.get(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
            ) {
                cursor.of(ogReader);
                println(ogReader.getMetadata(), cursor);
                final String expected = sink.toString();

                try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                    TableWriter.Row r = writer.newRow(Micros.DAY_MICROS + 1);
                    r.putSym(0, "bar");
                    r.append();
                    writer.commit();
                }

                TableReader newerTxnReaderRef;
                try (TableReader newerTxnReader = pool.get(tableToken)) {
                    newerTxnReaderRef = newerTxnReader;
                    cursor.of(newerTxnReader);
                    println(newerTxnReader.getMetadata(), cursor);
                    final String newerTxnActual = sink.toString();

                    Assert.assertNotEquals(expected, newerTxnActual);
                }

                try (TableReader copyReader = pool.getCopyOf(ogReader)) {
                    Assert.assertSame(newerTxnReaderRef, copyReader);
                    cursor.of(copyReader);
                    println(copyReader.getMetadata(), cursor);
                    final String copyActual = sink.toString();

                    Assert.assertEquals(expected, copyActual);
                }
            }
        });
    }

    @Test
    public void testCopyOfReloadsOlderTxnReader() throws Exception {
        assertWithPool(pool -> {
            final String tableName = "test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                TableWriter.Row r = writer.newRow(1);
                r.putSym(0, "foo");
                r.append();
                writer.commit();
            }

            final TableToken tableToken = engine.verifyTableName(tableName);
            // Grab a couple of readers, so that we have a stale reader when we acquire the OG reader.
            try (
                    TableReader ignore1 = pool.get(tableToken);
                    TableReader ignore2 = pool.get(tableToken)
            ) {
            }

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                TableWriter.Row r = writer.newRow(Micros.DAY_MICROS + 1);
                r.putSym(0, "bar");
                r.append();
                writer.commit();
            }

            try (
                    TableReader ogReader = pool.get(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
            ) {
                try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                    TableWriter.Row r = writer.newRow(2 * Micros.DAY_MICROS + 1);
                    r.putSym(0, "baz");
                    r.append();
                    writer.commit();
                }

                cursor.of(ogReader);
                println(ogReader.getMetadata(), cursor);
                final String expected = sink.toString();

                try (TableReader copyReader = pool.getCopyOf(ogReader)) {
                    cursor.of(copyReader);
                    println(copyReader.getMetadata(), cursor);
                    final String copyActual = sink.toString();

                    Assert.assertEquals(expected, copyActual);
                }
            }
        });
    }

    @Test
    public void testCopyOfReturnsSameTxnReader() throws Exception {
        assertWithPool(pool -> {
            final String tableName = "test";
            TableModel model = new TableModel(configuration, tableName, PartitionBy.DAY)
                    .col("sym", ColumnType.SYMBOL)
                    .timestamp("ts");
            AbstractCairoTest.create(model);

            try (TableWriter writer = newOffPoolWriter(configuration, tableName)) {
                TableWriter.Row r = writer.newRow(1);
                r.putSym(0, "foo");
                r.append();
                writer.commit();
            }

            final TableToken tableToken = engine.verifyTableName(tableName);
            try (
                    TableReader ogReader = pool.get(tableToken);
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()
            ) {
                cursor.of(ogReader);
                println(ogReader.getMetadata(), cursor);
                final String expected = sink.toString();

                TableReader sameTxnReaderRef;
                try (TableReader sameTxnReader = pool.get(tableToken)) {
                    sameTxnReaderRef = sameTxnReader;
                    cursor.of(sameTxnReader);
                    println(sameTxnReader.getMetadata(), cursor);
                    final String sameTxnActual = sink.toString();

                    Assert.assertEquals(expected, sameTxnActual);
                }

                // We should get the same reader as before since getCopyOf() is no-op in this case.
                try (TableReader copyReader = pool.getCopyOf(ogReader)) {
                    Assert.assertSame(sameTxnReaderRef, copyReader);
                    cursor.of(copyReader);
                    println(copyReader.getMetadata(), cursor);
                    final String copyActual = sink.toString();

                    Assert.assertEquals(expected, copyActual);
                }
            }
        });
    }

    @Test
    public void testDetachedRefCount() throws Exception {
        TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        TableToken tableToken = engine.verifyTableName("x");
        assertWithPool(pool -> {
            ReaderPool.R reader = pool.get(tableToken);
            Assert.assertEquals(0, reader.getDetachedRefCount());

            pool.detach(reader);
            Assert.assertEquals(0, reader.getDetachedRefCount());

            for (int i = 0; i < 10; i++) {
                reader.incrementDetachedRefCount();
                Assert.assertEquals(i + 1, reader.getDetachedRefCount());
            }
            for (int i = 9; i > -1; i--) {
                reader.close();
                Assert.assertEquals(i, reader.getDetachedRefCount());
            }

            pool.attach(reader);
            reader.close();
            Assert.assertEquals(0, reader.getDetachedRefCount());
        });
    }

    @Test
    public void testDoubleLock() throws Exception {
        TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        TableToken xyzTableToken = engine.verifyTableName("xyz");

        assertWithPool(pool -> {
            Assert.assertTrue(pool.lock(xyzTableToken));
            Assert.assertTrue(pool.lock(xyzTableToken));

            try {
                pool.get(xyzTableToken);
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }

            pool.unlock(xyzTableToken);

            try (TableReader reader = pool.get(xyzTableToken)) {
                Assert.assertNotNull(reader);
            }
        });
    }

    @Test
    public void testGetAndCloseRace() throws Exception {
        TableModel model = new TableModel(configuration, "xyz", PartitionBy.NONE).col("ts", ColumnType.DATE);
        AbstractCairoTest.create(model);
        TableToken xyzTableToken = engine.verifyTableName("xyz");

        for (int i = 0; i < 100; i++) {
            assertWithPool(pool -> {
                AtomicInteger exceptionCount = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(2);
                CountDownLatch stopLatch = new CountDownLatch(2);

                new Thread(() -> {
                    try {
                        barrier.await();
                        pool.close();
                    } catch (CairoException e) {
                        // "table is left behind" exception is a valid outcome, ignore it
                        if (!Chars.contains(e.getFlyweightMessage(), "table is left behind on pool shutdown")) {
                            exceptionCount.incrementAndGet();
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        Path.clearThreadLocals();
                        stopLatch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    try {
                        barrier.await();
                        try (TableReader reader = pool.get(xyzTableToken)) {
                            Assert.assertNotNull(reader);
                        } catch (PoolClosedException ignore) {
                            // this can also happen when this thread is delayed enough for pool close to complete
                        }
                    } catch (Exception e) {
                        exceptionCount.incrementAndGet();
                        e.printStackTrace();
                    } finally {
                        Path.clearThreadLocals();
                        stopLatch.countDown();
                    }
                }).start();

                Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
                Assert.assertEquals(0, exceptionCount.get());
            });
        }
    }

    @Test
    public void testGetMultipleReaders() throws Exception {
        assertWithPool(pool -> {
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < 64; i++) {
                Assert.assertTrue(readers.add(pool.get(uTableToken)));
            }
            for (int i = 0, n = readers.size(); i < n; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }
        });
    }

    @Test
    public void testGetReaderFailure() throws Exception {
        final int N = 3;
        final int K = 40;
        AtomicInteger locked = new AtomicInteger(1);

        TestFilesFacade ff = new TestFilesFacade() {
            int count = N;

            @Override
            public long openRO(LPSZ name) {
                count--;
                if (Utf8s.endsWithAscii(name, TableUtils.META_FILE_NAME) && locked.get() == 1) {
                    return -1;
                }
                return super.openRO(name);
            }

            @Override
            public boolean wasCalled() {
                return count < N;
            }
        };

        assertWithPool(pool -> {
            for (int i = 0; i < 3; i++) {
                try {
                    pool.get(uTableToken);
                    Assert.fail();
                } catch (CairoException ignored) {
                }

                Assert.assertEquals(0, pool.getBusyCount());
            }

            locked.set(0);
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            for (int i = 0; i < K; i++) {
                Assert.assertTrue(readers.add(pool.get(uTableToken)));
            }

            Assert.assertEquals(K, pool.getBusyCount());
            Assert.assertEquals(K, readers.size());

            for (int i = 0; i < K; i++) {
                TableReader reader = readers.get(i);
                Assert.assertTrue(reader.isOpen());
                reader.close();
            }
        }, new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull FilesFacade getFilesFacade() {
                return ff;
            }

            @Override
            public long getSpinLockTimeout() {
                return 1;
            }
        });

        Assert.assertTrue(ff.wasCalled());
    }

    @Test
    public void testGetReaderWhenPoolClosed() throws Exception {
        assertWithPool(pool -> {
            pool.close();

            try {
                pool.get(uTableToken);
                Assert.fail();
            } catch (PoolClosedException ignored) {
            }
        });
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Test
    public void testGetReadersBeforeFailure() throws Exception {
        assertWithPool(pool -> {
            ObjList<TableReader> readers = new ObjList<>();
            try {
                do {
                    readers.add(pool.get(uTableToken));
                } while (true);
            } catch (EntryUnavailableException e) {
                Assert.assertEquals(pool.getMaxEntries(), readers.size());
            } finally {
                for (int i = 0, n = readers.size(); i < n; i++) {
                    readers.getQuick(i).close();
                }
            }
        });
    }

    @Test
    public void testLockBusyReader() throws Exception {
        assertWithPool(new PoolAwareCode() {

            @Override
            public void run(ReaderPool pool) throws Exception {
                TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
                AbstractCairoTest.create(model);
                final TableToken nameX = engine.verifyTableName("x");

                final int N = 10_000;
                for (int i = 0; i < N; i++) {
                    testLockBusyReaderRollTheDice(pool, nameX);
                }
            }

            private void testLockBusyReaderRollTheDice(ReaderPool pool, TableToken nameX) throws BrokenBarrierException, InterruptedException {
                final CyclicBarrier start = new CyclicBarrier(2);
                final SOCountDownLatch halt = new SOCountDownLatch(1);
                final AtomicReference<TableReader> ref = new AtomicReference<>();
                new Thread(() -> {
                    try {
                        // start together with main thread
                        start.await();
                        // try to get reader from pool
                        ref.set(pool.get(nameX));
                    } catch (Throwable ignored) {
                    } finally {
                        Path.clearThreadLocals();
                        // the end
                        halt.countDown();
                    }
                }).start();

                // start together with the thread
                start.await();

                // get a lock
                boolean couldLock = pool.lock(nameX);

                // wait until thread stops
                halt.await();

                // assert
                if (couldLock) {
                    pool.unlock(nameX);
                    Assert.assertNull(ref.get());
                } else {
                    TableReader reader = ref.get();
                    Assert.assertNotNull(reader);
                    reader.close();
                }
            }
        });
    }

    @Test
    public void testLockMultipleReaders() throws Exception {
        assertWithPool(pool -> {
            ObjHashSet<TableReader> readers = new ObjHashSet<>();
            try {
                for (int i = 0; i < 64; i++) {
                    Assert.assertTrue(readers.add(pool.get(uTableToken)));
                }
                Assert.assertEquals(64, pool.getBusyCount());

                for (int i = 0, n = readers.size(); i < n; i++) {
                    TableReader reader = readers.get(i);
                    Assert.assertTrue(reader.isOpen());
                    reader.close();
                }

                Assert.assertTrue(pool.lock(uTableToken));
                Assert.assertEquals(0, pool.getBusyCount());
                for (int i = 0, n = readers.size(); i < n; i++) {
                    Assert.assertFalse(readers.get(i).isOpen());
                }
                pool.unlock(uTableToken);
            } finally {
                // Release readers on failure
                // In OSX the number of shared memory system-wide can be quite small
                // close readers to release shared memory
                for (int i = 0, n = readers.size(); i < n; i++) {
                    TableReader reader = readers.get(i);
                    if (reader.isOpen()) {
                        reader.close();
                    }
                }
            }
        });
    }

    @Test
    public void testLockRace() throws Exception {
        assertWithPool(pool -> {
            AtomicInteger successCount = new AtomicInteger();
            AtomicInteger failureCount = new AtomicInteger();
            AtomicInteger exceptionCount = new AtomicInteger();
            CyclicBarrier barrier = new CyclicBarrier(2);
            CountDownLatch stopLatch = new CountDownLatch(2);

            TableToken xTableToken = new TableToken("x", "x", null, 123, false, false, false);

            final Runnable runnable = () -> {
                try {
                    barrier.await();
                    if (pool.lock(xTableToken)) {
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    exceptionCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    stopLatch.countDown();
                }
            };

            new Thread(runnable).start();
            new Thread(runnable).start();

            Assert.assertTrue(stopLatch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(0, exceptionCount.get());
            Assert.assertEquals(1, successCount.get());
            Assert.assertEquals(1, failureCount.get());
        });
    }

    @Test
    public void testLockRaceAgainstGet() throws Exception {
        assertWithPool(pool -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE);
            AbstractCairoTest.create(model);

            TableToken xTableToken = engine.verifyTableName("x");
            for (int k = 0; k < 10; k++) {
                // allocate 32 readers to get to the start race at edge of next entry
                int n = 64;
                TableReader[] readers = new TableReader[n];
                try {
                    for (int i = 0; i < n; i++) {
                        readers[i] = pool.get(xTableToken);
                        Assert.assertNotNull(readers[i]);
                    }

                    CyclicBarrier barrier = new CyclicBarrier(2);
                    CountDownLatch latch = new CountDownLatch(1);

                    new Thread(() -> {
                        try {
                            barrier.await();
                            boolean locked = pool.lock(xTableToken);
                            if (locked) {
                                pool.unlock(xTableToken);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    }).start();

                    barrier.await();
                    try {
                        pool.get(xTableToken).close();
                    } catch (EntryLockedException ignore) {
                    }
                    Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
                } finally {
                    for (int i = 0; i < n; i++) {
                        readers[i].close();
                    }
                }
            }
        });
    }

    @Test
    public void testLockUnlock() throws Exception {
        // create tables
        AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.NONE).col("ts", ColumnType.DATE));
        AbstractCairoTest.create(new TableModel(configuration, "y", PartitionBy.NONE).col("ts", ColumnType.DATE));

        assertWithPool(pool -> {
            TableReader x, y;
            TableToken xTableToken = engine.verifyTableName("x");
            x = pool.get(xTableToken);
            Assert.assertNotNull(x);

            y = pool.get(uTableToken);
            Assert.assertNotNull(y);

            // expect lock to fail because we have "x" open
            Assert.assertFalse(pool.lock(xTableToken));

            x.close();

            // expect lock to succeed after we closed "x"
            Assert.assertTrue(pool.lock(xTableToken));

            // expect "x" to be physically closed
            Assert.assertFalse(x.isOpen());

            // "x" is locked, expect this to fail
            try {
                Assert.assertNull(pool.get(xTableToken));
            } catch (EntryLockedException ignored) {
            }

            pool.unlock(xTableToken);

            x = pool.get(xTableToken);
            Assert.assertNotNull(x);
            x.close();

            Assert.assertTrue(x.isOpen());

            Assert.assertTrue(y.isOpen());
            y.close();

            pool.close();

            Assert.assertFalse(y.isOpen());

            // "x" was not busy and should be closed by factory
            Assert.assertFalse(x.isOpen());
        });
    }

    @Test
    public void testLockUnlockMultiple() throws Exception {
        assertWithPool(pool -> {
            TableReader r1 = pool.get(uTableToken);
            TableReader r2 = pool.get(uTableToken);
            r1.close();
            Assert.assertFalse(pool.lock(uTableToken));
            r2.close();
            Assert.assertTrue(pool.lock(uTableToken));
            pool.unlock(uTableToken);
        });
    }

    @Test
    public void testReaderDoubleClose() throws Exception {
        assertWithPool(pool -> {
            class Listener implements PoolListener {
                private final IntList events = new IntList();
                private final ObjList<TableToken> names = new ObjList<>();

                @Override
                public void onEvent(byte factoryType, long thread, TableToken tableToken, short event, short segment, short position) {
                    names.add(tableToken);
                    events.add(event);
                }
            }

            Listener listener = new Listener();
            pool.setPoolListener(listener);

            TableReader reader = pool.get(uTableToken);

            Assert.assertNotNull(reader);
            Assert.assertTrue(reader.isOpen());
            Assert.assertEquals(1, pool.getBusyCount());
            reader.close();
            Assert.assertEquals(0, pool.getBusyCount());
            try {
                reader.close();
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "double close");
            }
        });
    }

    @Test
    public void testSerialOpenClose() throws Exception {
        assertWithPool(pool -> {
            TableReader firstReader = null;
            for (int i = 0; i < 1000; i++) {
                try (TableReader reader = pool.get(uTableToken)) {
                    if (firstReader == null) {
                        firstReader = reader;
                    }
                    Assert.assertNotNull(reader);
                    Assert.assertSame(firstReader, reader);
                }
            }
        });
    }

    @Test
    public void testUnlockByAnotherThread() throws Exception {
        assertWithPool(pool -> {
            TableToken tableToken = new TableToken("Ургант", "Ургант", null, 123, false, false, false);
            Assert.assertTrue(pool.lock(tableToken));
            AtomicInteger errors = new AtomicInteger();

            CountDownLatch latch = new CountDownLatch(1);
            new Thread(() -> {
                try {
                    try {
                        pool.unlock(tableToken);
                        Assert.fail();
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "Not the lock owner of Ургант");
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            }).start();

            Assert.assertTrue(latch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(0, errors.get());

            try {
                pool.get(tableToken);
                Assert.fail();
            } catch (EntryLockedException ignore) {
            }
            pool.unlock(tableToken);
        });
    }

    @Test
    public void testUnlockNonExisting() throws Exception {
        assertWithPool(pool -> {
            AtomicInteger counter = new AtomicInteger();
            pool.setPoolListener((factoryType, thread, name, event, segment, position) -> {
                if (event == PoolListener.EV_NOT_LOCKED) {
                    counter.incrementAndGet();
                }
            });

            TableToken tableToken = new TableToken("xyz", "xyz", null, 123, false, false, false);
            pool.unlock(tableToken);
            Assert.assertEquals(1, counter.get());
        });
    }

    private void assertWithPool(PoolAwareCode code) throws Exception {
        assertWithPool(code, configuration);
    }

    private void assertWithPool(PoolAwareCode code, final CairoConfiguration configuration) throws Exception {
        assertMemoryLeak(() -> {
            try (ReaderPool pool = new ReaderPool(configuration, engine.getTxnScoreboardPool(), messageBus)) {
                code.run(pool);
            }
        });
    }

    private interface PoolAwareCode {
        void run(ReaderPool pool) throws Exception;
    }
}
