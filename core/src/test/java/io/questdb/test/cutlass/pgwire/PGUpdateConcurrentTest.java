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

package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT;
import static io.questdb.PropertyKey.CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT;

public class PGUpdateConcurrentTest extends BasePGTest {
    private static final ThreadLocal<StringSink> readerSink = new ThreadLocal<>(StringSink::new);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 256);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_WRITER_COMMAND_QUEUE_CAPACITY, 256);
        super.setUp();
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.SINGLE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 10, 25, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 10, 40, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencySingleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 10, 40, PartitionMode.SINGLE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 1, 30, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 1, 50, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencySingleWriterSingleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 1, 50, PartitionMode.SINGLE);
    }

    @Test
    public void testUpdateTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PGServer server1 = createPGServer(1);
                    WorkerPool workerPool = server1.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " 0 as x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                }

                TableWriter lockedWriter = getWriter(
                        "testUpdateTimeout"
                );

                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? WHERE x != 4");
                    update.setInt(1, 4);

                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "Timeout expired");
                    }

                    lockedWriter.close();
                    update.setInt(1, 5);
                    update.executeUpdate();
                    update.close();
                }

                assertSql("ts\tx\n" +
                        "1970-01-01T00:00:00.000000Z\t5\n" +
                        "1970-01-01T00:00:01.000000Z\t5\n" +
                        "1970-01-01T00:00:02.000000Z\t5\n" +
                        "1970-01-01T00:00:03.000000Z\t5\n" +
                        "1970-01-01T00:00:04.000000Z\t5\n", "testUpdateTimeout");
            }
        });
    }

    private void assertReader(TableReader rdr, IntObjHashMap<CharSequence[]> expectedValues, IntObjHashMap<Validator> validators) throws SqlException {
        final RecordMetadata metadata = rdr.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            validators.get(i).reset();
        }
        try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()) {
            cursor.of(rdr);
            final Record record = cursor.getRecord();
            int recordIndex = 0;
            while (cursor.hasNext()) {
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    final StringSink readerSink = PGUpdateConcurrentTest.readerSink.get();
                    readerSink.clear();
                    CursorPrinter.printColumn(record, metadata, i, readerSink);
                    CharSequence[] expectedValueArray = expectedValues.get(i);
                    CharSequence expectedValue = expectedValueArray != null ? expectedValueArray[recordIndex] : null;
                    if (!validators.get(i).validate(expectedValue, readerSink)) {
                        throw SqlException.$(0, "assertSql failed, recordIndex=").put(recordIndex)
                                .put(", columnIndex=").put(i)
                                .put(", expected=").put(expectedValue)
                                .put(", actual=").put(readerSink);
                    }
                }
                recordIndex++;
            }
        }
    }

    private void testConcurrency(int numOfWriters, int numOfReaders, int numOfUpdates, PartitionMode partitionMode) throws Exception {
        setProperty(CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT, 20_000L); // On in CI Windows updates are particularly slow
        node1.setProperty(CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT, 90_000L);
        node1.setProperty(PropertyKey.CAIRO_SPIN_LOCK_TIMEOUT, 20_000L);
        spinLockTimeout = 20_000L;
        assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(numOfWriters + numOfReaders);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger current = new AtomicInteger();
            ObjList<Thread> threads = new ObjList<>(numOfWriters + numOfReaders + 1);

            try (
                    final PGServer pgServer = createPGServer(2);
                    WorkerPool workerPool = pgServer.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(pgServer.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table up as" +
                            " (select timestamp_sequence(0, " + PartitionMode.getTimestampSeq(partitionMode) + ") ts," +
                            " 0 as x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)" +
                            (PartitionMode.isPartitioned(partitionMode) ? " partition by DAY WAL" : ""));
                    create.execute();
                    create.close();

                    TestUtils.drainWalQueue(engine);
                }

                Thread tick = new Thread(() -> {
                    while (current.get() < numOfWriters * numOfUpdates && exceptions.isEmpty()) {
                        try (TableWriter tableWriter = getWriter("up")) {
                            tableWriter.tick();
                        } catch (EntryUnavailableException ignored) {
                        }
                        Os.sleep(100);
                    }
                });
                threads.add(tick);
                tick.start();

                for (int k = 0; k < numOfWriters; k++) {
                    Thread writer = new Thread(() -> {
                        try {
                            try (final Connection connection = getConnection(pgServer.getPort(), false, true)) {
                                barrier.await();
                                PreparedStatement update = connection.prepareStatement("UPDATE up SET x = ?");
                                for (int i = 0; i < numOfUpdates; i++) {
                                    update.setInt(1, i);
                                    // update against WAL table will return txn instead of row count
                                    update.executeUpdate();
                                    current.incrementAndGet();
                                }
                                update.close();
                            }
                        } catch (Throwable th) {
                            LOG.error().$("writer error ").$(th).$();
                            exceptions.add(th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    });
                    threads.add(writer);
                    writer.start();
                }

                for (int k = 0; k < numOfReaders; k++) {
                    Thread reader = new Thread(() -> {
                        IntObjHashMap<CharSequence[]> expectedValues = new IntObjHashMap<>();
                        expectedValues.put(0, PartitionMode.getExpectedTimestamps(partitionMode));

                        IntObjHashMap<Validator> validators = new IntObjHashMap<>();
                        validators.put(0, Chars::equals);
                        validators.put(1, new Validator() {
                            private CharSequence value;

                            @Override
                            public void reset() {
                                value = null;
                            }

                            @Override
                            public boolean validate(CharSequence expected, CharSequence actual) {
                                if (value == null) {
                                    value = actual.toString();
                                }
                                return actual.equals(value);
                            }
                        });

                        try {
                            barrier.await();
                            try (TableReader rdr = getReader("up")) {
                                while (current.get() < numOfWriters * numOfUpdates && exceptions.isEmpty()) {
                                    rdr.reload();
                                    assertReader(rdr, expectedValues, validators);
                                }
                            }
                        } catch (Throwable th) {
                            LOG.error().$("reader error ").$(th).$();
                            exceptions.add(th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    });
                    threads.add(reader);
                    reader.start();
                }

                for (int i = 0; i < threads.size(); i++) {
                    threads.get(i).join();
                }
            }

            if (!exceptions.isEmpty()) {
                Assert.fail(exceptions.poll().toString());
            }
        });
    }

    private enum PartitionMode {
        NONE, SINGLE, MULTIPLE;

        static CharSequence[] getExpectedTimestamps(PartitionMode mode) {
            return mode == MULTIPLE ?
                    new CharSequence[]{
                            "1970-01-01T00:00:00.000000Z",
                            "1970-01-01T12:00:00.000000Z",
                            "1970-01-02T00:00:00.000000Z",
                            "1970-01-02T12:00:00.000000Z",
                            "1970-01-03T00:00:00.000000Z"
                    } :
                    new CharSequence[]{
                            "1970-01-01T00:00:00.000000Z",
                            "1970-01-01T00:00:01.000000Z",
                            "1970-01-01T00:00:02.000000Z",
                            "1970-01-01T00:00:03.000000Z",
                            "1970-01-01T00:00:04.000000Z"
                    };
        }

        static long getTimestampSeq(PartitionMode mode) {
            return mode == MULTIPLE ? 43200000000L : 1000000L;
        }

        static boolean isPartitioned(PartitionMode mode) {
            return mode != NONE;
        }
    }

    private interface Validator {
        default void reset() {
        }

        boolean validate(CharSequence expected, CharSequence actual);
    }
}
