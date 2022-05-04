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

package io.questdb.cutlass.pgwire;

import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.*;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

public class PGUpdateConcurrentTest extends BasePGTest {
    private final ThreadLocal<StringSink> readerSink = new ThreadLocal<>(StringSink::new);

    @BeforeClass
    public static void setUpStatic() {
        writerCommandQueueCapacity = 256;
        AbstractGriffinTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        writerCommandQueueCapacity = 256;
        super.setUp();
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.MULTIPLE);
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PartitionMode.NONE);
    }

    @Test
    public void testConcurrencyMultipleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(4, 20, 8, PartitionMode.SINGLE);
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
            try (PGWireServer ignore1 = createPGServer(1)) {
                try (final Connection connection = getConnection(false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 1000000) ts," +
                            " 0 as x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                }

                TableWriter lockedWriter = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        "testUpdateTimeout",
                        "test");

                try (final Connection connection = getConnection(false, true)) {
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

                assertSql("testUpdateTimeout", "ts\tx\n" +
                        "1970-01-01T00:00:00.000000Z\t5\n" +
                        "1970-01-01T00:00:01.000000Z\t5\n" +
                        "1970-01-01T00:00:02.000000Z\t5\n" +
                        "1970-01-01T00:00:03.000000Z\t5\n" +
                        "1970-01-01T00:00:04.000000Z\t5\n");
            }
        });
    }

    @Test
    public void testUpdateWithQueryTimeout() throws Exception {
        assertMemoryLeak(() -> {
            writerAsyncCommandBusyWaitTimeout = 20_000_000L; // On in CI Windows updates are particularly slow
            writerAsyncCommandMaxTimeout = 30_000_000L;
            try (PGWireServer ignore1 = createPGServer(1)) {
                try (final Connection connection = getConnection(false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 60 * 1000000L) ts," +
                            " 0 as x" +
                            " from long_sequence(2000))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                }

                TableWriter lockedWriter = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        "testUpdateTimeout",
                        "test");

                // Non-simple connection
                try (final Connection connection = getConnection(false, true, 1L)) {
                    PreparedStatement update = connection.prepareStatement("" +
                            "UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setQueryTimeout(1);
                    update.setInt(1, 4);

                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }

                    lockedWriter.close();

                    // Check that timeout of 1ms is too tough anyway to execute even with writer closed
                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }
                }

                lockedWriter = engine.getWriter(
                        sqlExecutionContext.getCairoSecurityContext(),
                        "testUpdateTimeout",
                        "test");

                // Simple connection
                try (final Connection connection = getConnection(true, true, 1L)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setQueryTimeout(1);
                    update.setInt(1, 4);

                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }

                    lockedWriter.close();

                    // Check that timeout of 1ms is too tough anyway to execute even with writer closed
                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }
                }

                // Connection with default timeout
                try (final Connection connection = getConnection(false, true)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setInt(1, 5);
                    update.executeUpdate();
                }

                assertSql("select count() from testUpdateTimeout where x = 5",
                        "count\n" +
                                "2000\n");
            }
        });
    }

    private void assertSql(String sql, IntObjHashMap<CharSequence[]> expectedValues, IntObjHashMap<Validator> validators, SqlCompiler readerCompiler) throws SqlException {
        try (RecordCursorFactory factory = readerCompiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            final RecordMetadata metadata = factory.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                validators.get(i).reset();
            }
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int recordIndex = 0;
                while (cursor.hasNext()) {
                    for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                        final StringSink readerSink = this.readerSink.get();
                        readerSink.clear();
                        TestUtils.printColumn(record, metadata, i, readerSink);
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
    }

    private void testConcurrency(int numOfWriters, int numOfReaders, int numOfUpdates, PartitionMode partitionMode) throws Exception {
        writerAsyncCommandBusyWaitTimeout = 20_000_000L; // On in CI Windows updates are particularly slow
        writerAsyncCommandMaxTimeout = 30_000_000L;
        assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(numOfWriters + numOfReaders);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger current = new AtomicInteger();
            ObjList<Thread> threads = new ObjList<>(numOfWriters + numOfReaders + 1);

            final PGWireServer pgServer = createPGServer(2);
            try (final Connection connection = getConnection(false, true)) {
                PreparedStatement create = connection.prepareStatement("create table up as" +
                        " (select timestamp_sequence(0, " + PartitionMode.getTimestampSeq(partitionMode) + ") ts," +
                        " 0 as x" +
                        " from long_sequence(5))" +
                        " timestamp(ts)" +
                        (PartitionMode.isPartitioned(partitionMode) ? " partition by DAY" : ""));
                create.execute();
                create.close();
            }

            Thread tick = new Thread(() -> {
                while (current.get() < numOfWriters * numOfUpdates && exceptions.size() == 0) {
                    try (TableWriter tableWriter = engine.getWriter(
                            sqlExecutionContext.getCairoSecurityContext(),
                            "up",
                            "test")) {
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
                    try (final Connection connection = getConnection(false, true)) {
                        barrier.await();
                        PreparedStatement update = connection.prepareStatement("UPDATE up SET x = ?");
                        for (int i = 0; i < numOfUpdates; i++) {
                            update.setInt(1, i);
                            Assert.assertEquals(5, update.executeUpdate());
                            current.incrementAndGet();
                        }
                        update.close();
                    } catch (Throwable th) {
                        LOG.error().$("writer error ").$(th).$();
                        exceptions.add(th);
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
                        public boolean validate(CharSequence expected, CharSequence actual) {
                            if (value == null) {
                                value = actual.toString();
                            }
                            return actual.equals(value);
                        }

                        @Override
                        public void reset() {
                            value = null;
                        }
                    });

                    try {
                        final SqlCompiler readerCompiler = new SqlCompiler(engine, null, snapshotAgent);
                        barrier.await();
                        while (current.get() < numOfWriters * numOfUpdates && exceptions.size() == 0) {
                            assertSql("up", expectedValues, validators, readerCompiler);
                        }
                        readerCompiler.close();
                    } catch (Throwable th) {
                        LOG.error().$("reader error ").$(th).$();
                        exceptions.add(th);
                    }
                });
                threads.add(reader);
                reader.start();
            }

            for (int i = 0; i < threads.size(); i++) {
                threads.get(i).join();
            }
            pgServer.close();

            if (exceptions.size() != 0) {
                Assert.fail(exceptions.poll().toString());
            }
        });
    }

    private enum PartitionMode {
        NONE, SINGLE, MULTIPLE;

        static boolean isPartitioned(PartitionMode mode) {
            return mode != NONE;
        }

        static long getTimestampSeq(PartitionMode mode) {
            return mode == MULTIPLE ? 43200000000L : 1000000L;
        }

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
    }

    private interface Validator {
        default void reset() {
        }

        boolean validate(CharSequence expected, CharSequence actual);
    }
}
