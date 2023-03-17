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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Formatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.cairo.ColumnVersionReader.HEADER_SIZE;

public class ColumnVersionWriterTest extends AbstractCairoTest {

    @Test
    public void testColumnAddRemove() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0)
            ) {
                long partitionTimestamp = Timestamps.DAY_MICROS * 2;
                int columnIndex = 3;

                // Add column
                w.upsert(partitionTimestamp, columnIndex, 123, 987);
                w.upsertDefaultTxnName(columnIndex, 123, partitionTimestamp);

                // Verify
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex + 1));
                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(partitionTimestamp, columnIndex));
                int recordIndex = w.getRecordIndex(partitionTimestamp, columnIndex);
                Assert.assertEquals(123, w.getColumnNameTxnByIndex(recordIndex));
                Assert.assertEquals(987, w.getColumnTopByIndex(recordIndex));

                // Remove non-existing column top
                w.removeColumnTop(partitionTimestamp, columnIndex + 1);
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex + 1));

                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(partitionTimestamp, columnIndex));

                // Remove existing column top
                w.removeColumnTop(partitionTimestamp, columnIndex);

                Assert.assertEquals(partitionTimestamp, w.getColumnTopPartitionTimestamp(columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(partitionTimestamp, columnIndex));
                Assert.assertEquals(0, w.getColumnTopQuick(partitionTimestamp, columnIndex));
            }
        });
    }

    @Test
    public void testColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(TestFilesFacadeImpl.INSTANCE, path)
            ) {
                for (int i = 0; i < 100; i += 2) {
                    w.upsert(i, i % 10, -1, i * 10L);
                }

                w.commit();

                r.readSafe(configuration.getMillisecondClock(), 1);
                for (int i = 0; i < 100; i++) {
                    long colTop = r.getColumnTopQuick(i, i % 10);
                    Assert.assertEquals(i % 2 == 0 ? i * 10 : 0, colTop);
                }

                TestUtils.assertEquals(w.getCachedList(), r.getCachedList());
            }
        });
    }

    @Test
    public void testColumnTopChangedInO3() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0)
            ) {
                long day1 = 0;
                long day2 = Timestamps.DAY_MICROS;
                long day3 = Timestamps.DAY_MICROS * 2;
                int columnIndex = 3;
                int columnIndex1 = 1;

                // Add column
                w.upsert(day3, columnIndex, 123, 987);
                w.upsertDefaultTxnName(columnIndex, 123, day3);

                // Simulate O3 write to day1, day2
                w.upsertColumnTop(day1, columnIndex, 15);
                w.upsertColumnTop(day2, columnIndex, 0);
                w.upsertColumnTop(day1, columnIndex1, 15);
                w.upsertColumnTop(day2, columnIndex1, 0);

                // Check column top, txn name
                Assert.assertEquals(123, w.getColumnNameTxn(day1, columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(day2, columnIndex));
                Assert.assertEquals(123, w.getColumnNameTxn(day3, columnIndex));

                Assert.assertEquals(-1, w.getColumnNameTxn(day1, columnIndex1));
                Assert.assertEquals(-1, w.getColumnNameTxn(day2, columnIndex1));
                Assert.assertEquals(-1, w.getColumnNameTxn(day3, columnIndex1));

                // Check column top values
                Assert.assertEquals(15, w.getColumnTopQuick(day1, columnIndex));
                Assert.assertEquals(0, w.getColumnTopQuick(day2, columnIndex));
                Assert.assertEquals(987, w.getColumnTopQuick(day3, columnIndex));

                Assert.assertEquals(15, w.getColumnTopQuick(day1, columnIndex1));
                Assert.assertEquals(0, w.getColumnTopQuick(day2, columnIndex1));
                Assert.assertEquals(0, w.getColumnTopQuick(day3, columnIndex1));
            }
        });
    }

    @Test
    public void testColumnTruncate() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(TestFilesFacadeImpl.INSTANCE, path)
            ) {
                Rnd rnd = TestUtils.generateRandom(LOG);
                int columnCount = 27;
                for (int i = 0; i < columnCount; i++) {
                    w.upsertDefaultTxnName(i, i, Timestamps.DAY_MICROS * i);
                    w.upsertColumnTop(Timestamps.DAY_MICROS * i, i, i * 100);
                }

                w.commit();
                w.truncate(true);
                r.readUnsafe();

                for (int i = 0; i < columnCount; i++) {
                    for (int j = 0; j < 100; j++) {
                        Assert.assertEquals(0, r.getColumnTop(rnd.nextLong(), i));
                        Assert.assertEquals(i, r.getDefaultColumnNameTxn(i));
                        Assert.assertEquals(Long.MIN_VALUE, r.getColumnTopPartitionTimestamp(i));
                    }
                }
            }
        });
    }

    @Test
    public void testColumnVersionReaderReuse() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(ff, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, path)
            ) {
                for (int i = 0; i < 100; i += 2) {
                    w.upsert(i, i % 10, -1, i * 10L);
                }

                w.commit();

                r.readSafe(configuration.getMillisecondClock(), 1);
                for (int i = 0; i < 100; i++) {
                    long colTop = r.getColumnTopQuick(i, i % 10);
                    Assert.assertEquals(i % 2 == 0 ? i * 10 : 0, colTop);
                }

                TestUtils.assertEquals(w.getCachedList(), r.getCachedList());

                r.ofRO(ff, path);
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(w.getCachedList(), r.getCachedList());

                MemoryCMR mem = Vm.getCMRInstance();
                mem.of(ff, path, 0, HEADER_SIZE, MemoryTag.MMAP_TABLE_READER);
                r.ofRO(mem);
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(w.getCachedList(), r.getCachedList());
                mem.close();
            }
        });
    }

    @Test
    public void testFuzz() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = new Rnd();
            final int N = 100_000;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(TestFilesFacadeImpl.INSTANCE, path)
            ) {
                w.upsert(1, 2, 3, -1);

                for (int i = 0; i < N; i++) {
                    // increment from 0 to 4 columns
                    int increment = rnd.nextInt(32);

                    for (int j = 0; j < increment; j++) {
                        w.upsert(rnd.nextLong(20), rnd.nextInt(10), i, -1);
                    }

                    w.commit();
                    r.readSafe(configuration.getMillisecondClock(), 1);
                    Assert.assertTrue(w.getCachedList().size() > 0);
                    TestUtils.assertEquals(w.getCachedList(), r.getCachedList());
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
        });
    }

    @Test
    public void testFuzzConcurrent() throws Exception {
        testFuzzConcurrent(0);
    }

    @Test
    public void testFuzzWithTimeout() throws Exception {
        testFuzzConcurrent(5_000);
    }

    @Test
    public void testRemovePartitionColumns() throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(ff, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, path)
            ) {
                CVStringTable.setupColumnVersionWriter(w, "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2      -1      10\n" +
                        "       0       3      -1      10\n" +
                        "       0       5      -1      10\n" +
                        "       1       0      -1      10\n" +
                        "       1       2      -1      10\n" +
                        "       2       2      -1      10\n" +
                        "       2      11      -1      10\n" +
                        "       2      15      -1      10\n" +
                        "       3       0      -1      10\n"
                );

                w.commit();
                w.removePartition(0);
                w.commit();

                String expected = "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       1       0      -1      10\n" +
                        "       1       2      -1      10\n" +
                        "       2       2      -1      10\n" +
                        "       2      11      -1      10\n" +
                        "       2      15      -1      10\n" +
                        "       3       0      -1      10\n";

                TestUtils.assertEquals(expected, CVStringTable.asTable(w.getCachedList()));
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(expected, CVStringTable.asTable(r.getCachedList()));
            }
        });
    }

    @Test
    public void testUpsertPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2      -1      10\n" +
                        "       0       3      -1      10\n" +
                        "       0       5      -1      10\n" +
                        "       1       0      -1      10\n" +
                        "       1       2      -1      10\n" +
                        "       2       2      -1      10\n" +
                        "       2      11      -1      10\n" +
                        "       2      15      -1      10\n" +
                        "       3       0      -1      10\n" +
                        "       4       7      -1      10\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n" +
                        "       1       0      -1      10\n" +
                        "       2       2       1     111\n" +
                        "       2      11       2       1\n" +
                        "       2      15       2    1001\n" +
                        "       3       0       3     110\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2      -1      10\n" +
                        "       0       3      -1      10\n" +
                        "       0       5      -1      10\n" +
                        "       1       0      -1      10\n" +
                        "       2       2      -1      10\n" +
                        "       2      11      -1      10\n" +
                        "       2      15      -1      10\n" +
                        "       3       0       3     110\n" +
                        "       4       7      -1      10\n",
                0, 2, 4
        );
    }

    @Test
    public void testUpsertPartitionDstContainsPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       2      11       0      99\n" +
                        "       2      12       1      17\n" +
                        "       3      11       1       8\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n" +
                        "       2      11       5      12\n" +
                        "       2      12       5      12\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n" +
                        "       2      11       0      99\n" +
                        "       2      12       1      17\n",
                2
        );
    }

    @Test
    public void testUpsertPartitionDstDoesNotContainPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       2      11       1      10\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n" +
                        "       2      11       1      10\n", // Gets added
                2
        );
    }

    @Test
    public void testUpsertPartitionSrcDoesNotContainPartition() throws Exception {
        assertUpsertPartitionFromSourceCV(
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       2      11       1      10\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" +
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n",
                "" +
                        "     pts  colIdx  colTxn  colTop\n" + // No changes
                        "       0       2       3       1\n" +
                        "       0       3       1     101\n",
                0
        );
    }

    private static void assertUpsertPartitionFromSourceCV(
            String srcExpected,
            String dstExpected,
            String dstUpsertFromSrcExpected,
            long... partitionTimestamp
    ) throws Exception {
        assertMemoryLeak(() -> {
            FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w1 = new ColumnVersionWriter(ff, path.of(root).concat("_cv1").$(), 0);
                    ColumnVersionWriter w2 = new ColumnVersionWriter(ff, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(ff, path)
            ) {
                CVStringTable.setupColumnVersionWriter(w1, srcExpected);
                CVStringTable.setupColumnVersionWriter(w2, dstExpected);
                for (long p : partitionTimestamp) {
                    w2.copyPartition(p, w1);
                }
                TestUtils.assertEquals(dstUpsertFromSrcExpected, CVStringTable.asTable(w2.getCachedList()));
                w2.commit();
                r.readSafe(configuration.getMillisecondClock(), 1);
                TestUtils.assertEquals(dstUpsertFromSrcExpected, CVStringTable.asTable(r.getCachedList()));
            }
        });
    }

    private void testFuzzConcurrent(int spinLockTimeout) throws Exception {
        assertMemoryLeak(() -> {
            final int N = 10_000;
            try (
                    Path path = new Path();
                    ColumnVersionWriter w = new ColumnVersionWriter(TestFilesFacadeImpl.INSTANCE, path.of(root).concat("_cv").$(), 0);
                    ColumnVersionReader r = new ColumnVersionReader().ofRO(TestFilesFacadeImpl.INSTANCE, path)
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
                            try {
                                r.readSafe(configuration.getMillisecondClock(), spinLockTimeout);
                            } catch (CairoException ex) {
                                if (spinLockTimeout == 0 && Chars.contains(ex.getFlyweightMessage(), "timeout")) {
                                    continue;
                                }
                                throw ex;
                            }
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
        });
    }

    private static abstract class CVStringTable {
        private static final Formatter strF = new Formatter(new SinkFormatterAdapter());

        private static long[] parseColumnVersionTable(String table) {
            String[] rows = table.split("\n");
            long[] values = new long[(rows.length - 1) * ColumnVersionWriter.BLOCK_SIZE]; // minus header
            for (int i = 1, k = 0; i < rows.length; i++) {
                String[] columns = rows[i].split("\\s+");
                assert columns.length == 5;
                for (int j = 1; j < columns.length; j++) {
                    values[k++] = Long.parseLong(columns[j]);
                }
            }
            assert values.length > 0 && values.length % ColumnVersionWriter.BLOCK_SIZE == 0;
            return values;
        }

        static String asTable(LongList cachedList) {
            sink.clear();
            strF.format("%8s%8s%8s%8s\n", "pts", "colIdx", "colTxn", "colTop");
            for (int i = 0; i < cachedList.size(); i++) {
                strF.format("%8d", cachedList.getQuick(i));
                if (i > 0 && (i + 1) % ColumnVersionWriter.BLOCK_SIZE == 0) {
                    sink.put('\n');
                }
            }
            return sink.toString();
        }

        static void setupColumnVersionWriter(ColumnVersionWriter w, String expectedTable) {
            long[] values = parseColumnVersionTable(expectedTable);
            for (int i = 0; i < values.length; i += ColumnVersionWriter.BLOCK_SIZE) {
                w.upsert(
                        values[i],
                        (int) values[i + ColumnVersionWriter.COLUMN_INDEX_OFFSET],
                        values[i + ColumnVersionWriter.COLUMN_NAME_TXN_OFFSET],
                        values[i + ColumnVersionWriter.COLUMN_TOP_OFFSET]);
            }
            TestUtils.assertEquals(expectedTable, asTable(w.getCachedList()));
        }

        private static final class SinkFormatterAdapter extends StringSink implements Appendable {
            @Override
            public Appendable append(CharSequence csq) {
                sink.put(csq);
                return this;
            }

            @Override
            public Appendable append(CharSequence csq, int start, int end) {
                sink.put(csq, start, end);
                return this;
            }

            @Override
            public Appendable append(char c) {
                sink.put(c);
                return this;
            }
        }
    }
}