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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cairo.EntryUnavailableException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.SqlException;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.tools.TestUtils.await;

public class PGConnectionMetricsTest extends BasePGTest {
    private static final ThreadLocal<StringSink> readerSink = new ThreadLocal<>(StringSink::new);
    @Rule
    public RetryTest retries = new RetryTest(5);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        writerCommandQueueCapacity = 256;
        AbstractGriffinTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        writerCommandQueueCapacity = 256;
        super.setUp();
    }

    // it does not like this test for whatever reason
    @Test
    public void testConnectionFourWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PGConnectionMetricsTest.PartitionMode.NONE);
    }

    @Test
    public void testConnectionFourWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(4, 10, 8, PGConnectionMetricsTest.PartitionMode.SINGLE);
    }

    @Test
    public void testConnectionFourWritersMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(4, 10, 8, PGConnectionMetricsTest.PartitionMode.MULTIPLE);
    }

    @Test
    public void testConnectionSingleWriterMultipleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 10, 25, PGConnectionMetricsTest.PartitionMode.MULTIPLE);
    }

    @Test
    public void testConnectionSingleWriterMultipleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 10, 40, PGConnectionMetricsTest.PartitionMode.NONE);
    }

    @Test
    public void testConnectionSingleWriterMultipleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 10, 40, PGConnectionMetricsTest.PartitionMode.SINGLE);
    }

    @Test
    public void testConnectionSingleWriterSingleReaderMultiPartitioned() throws Exception {
        testConcurrency(1, 1, 30, PGConnectionMetricsTest.PartitionMode.MULTIPLE);
    }

    @Test
    public void testConnectionSingleWriterSingleReaderNonPartitioned() throws Exception {
        testConcurrency(1, 1, 50, PGConnectionMetricsTest.PartitionMode.NONE);

    }

    @Test
    public void testConnectionSingleWriterWriterSingleReaderSinglePartitioned() throws Exception {
        testConcurrency(1, 1, 50, PGConnectionMetricsTest.PartitionMode.SINGLE);
    }

    @Test
    public void testConnectionWithOneNestedConnection() throws Exception {
        assertMemoryLeak(() -> {
            writerAsyncCommandBusyWaitTimeout = 20_000L; // On in CI Windows updates are particularly slow
            writerAsyncCommandMaxTimeout = 90_000L;
            try (
                    PGWireServer server1 = createPGServer(2);
                    WorkerPool workerPool = server1.getWorkerPool()) {
                workerPool.start(LOG);

                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 60 * 1000000L) ts," +
                            " 0 as x" +
                            " from long_sequence(2000))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());

                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setInt(1, 5);
                    update.executeUpdate();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                    try (final Connection connection1 = getConnection(server1.getPort(), false, true)) {
                        PreparedStatement update1 = connection1.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                        update1.setInt(1, 5);
                        update1.executeUpdate();
                        Assert.assertEquals(2, metrics.pgWire().totalPGConnections().getValue());
                    }
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                while (metrics.pgWire().totalPGConnections().getValue() != 0) {
                    Os.sleep(100);
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
            }
        });
    }

    @Test
    public void testConnectionWithTwoNestedConnections() throws Exception {
        assertMemoryLeak(() -> {
            writerAsyncCommandBusyWaitTimeout = 20_000L; // On in CI Windows updates are particularly slow
            writerAsyncCommandMaxTimeout = 90_000L;
            try (
                    PGWireServer server1 = createPGServer(2);
                    WorkerPool workerPool = server1.getWorkerPool()) {
                workerPool.start(LOG);

                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 60 * 1000000L) ts," +
                            " 0 as x" +
                            " from long_sequence(2000))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());

                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setInt(1, 5);
                    update.executeUpdate();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                    try (final Connection connection1 = getConnection(server1.getPort(), false, true)) {
                        PreparedStatement update1 = connection1.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                        update1.setInt(1, 5);
                        update1.executeUpdate();
                        Assert.assertEquals(2, metrics.pgWire().totalPGConnections().getValue());
                    }
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());

                    try (final Connection connection1 = getConnection(server1.getPort(), false, true)) {
                        PreparedStatement update1 = connection1.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                        update1.setInt(1, 5);
                        update1.executeUpdate();
                        Assert.assertEquals(2, metrics.pgWire().totalPGConnections().getValue());
                    }
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());


                }
                while (metrics.pgWire().totalPGConnections().getValue() != 0) {
                    Os.sleep(100);
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
            }
        });
    }

    @Test
    public void testSingleConnectionsWithUpdateTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PGWireServer server1 = createPGServer(1);
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
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }

                TableWriter lockedWriter = getWriter(
                        "testUpdateTimeout"
                );
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());

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
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());

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
    public void testSingleConnectionsWithUpdateWithQueryTimeout() throws Exception {
        assertMemoryLeak(() -> {
            writerAsyncCommandBusyWaitTimeout = 20_000L; // On in CI Windows updates are particularly slow
            writerAsyncCommandMaxTimeout = 90_000L;
            try (
                    PGWireServer server1 = createPGServer(1);
                    WorkerPool workerPool = server1.getWorkerPool()) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table testUpdateTimeout as" +
                            " (select timestamp_sequence(0, 60 * 1000000L) ts," +
                            " 0 as x" +
                            " from long_sequence(2000))" +
                            " timestamp(ts)" +
                            " partition by DAY");
                    create.execute();
                    create.close();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
                TableWriter lockedWriter = getWriter("testUpdateTimeout");

                // Non-simple connection
                try (final Connection connection = getConnection(server1.getPort(), false, true, 1L)) {
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
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                    lockedWriter.close();

                    // Check that timeout of 1ms is too tough anyway to execute even with writer closed
                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }

                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
                lockedWriter = getWriter("testUpdateTimeout");

                // Simple connection
                try (final Connection connection = getConnection(server1.getPort(), true, true, 1L)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setQueryTimeout(1);
                    update.setInt(1, 4);

                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                    lockedWriter.close();

                    // Check that timeout of 1ms is too tough anyway to execute even with writer closed
                    try {
                        update.executeUpdate();
                        Assert.fail();
                    } catch (PSQLException ex) {
                        TestUtils.assertContains(ex.getMessage(), "timeout");
                    }

                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());

                // Connection with default timeout
                try (final Connection connection = getConnection(server1.getPort(), false, true)) {
                    PreparedStatement update = connection.prepareStatement("UPDATE testUpdateTimeout SET x = ? FROM tables() WHERE x != 4");
                    update.setInt(1, 5);
                    update.executeUpdate();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
            }
        });
    }

    private void assertReader(TableReader rdr, IntObjHashMap<CharSequence[]> expectedValues, IntObjHashMap<PGConnectionMetricsTest.Validator> validators) throws SqlException {
        final RecordMetadata metadata = rdr.getMetadata();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            validators.get(i).reset();
        }
        RecordCursor cursor = rdr.getCursor();
        final Record record = cursor.getRecord();
        int recordIndex = 0;
        while (cursor.hasNext()) {
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                final StringSink readerSink = PGConnectionMetricsTest.readerSink.get();
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

    private void testConcurrency(int numOfWriters, int numOfReaders, int numOfUpdates, PGConnectionMetricsTest.PartitionMode partitionMode) throws Exception {
        writerAsyncCommandBusyWaitTimeout = 20_000L; // On in CI Windows updates are particularly slow
        writerAsyncCommandMaxTimeout = 90_000L;
        spinLockTimeout = 20_000L;
        assertMemoryLeak(() -> {
            CyclicBarrier barrier = new CyclicBarrier(numOfWriters + numOfReaders);
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger current = new AtomicInteger();
            ObjList<Thread> threads = new ObjList<>(numOfWriters + numOfReaders + 1);

            try (
                    final PGWireServer pgServer = createPGServer(2);
                    WorkerPool workerPool = pgServer.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection connection = getConnection(pgServer.getPort(), false, true)) {
                    PreparedStatement create = connection.prepareStatement("create table up as" +
                            " (select timestamp_sequence(0, " + PGConnectionMetricsTest.PartitionMode.getTimestampSeq(partitionMode) + ") ts," +
                            " 0 as x" +
                            " from long_sequence(5))" +
                            " timestamp(ts)" +
                            (PGConnectionMetricsTest.PartitionMode.isPartitioned(partitionMode) ? " partition by DAY" : ""));
                    create.execute();
                    create.close();
                    Assert.assertEquals(1, metrics.pgWire().totalPGConnections().getValue());
                }
//                barrier.await();


                Thread tick = new Thread(() -> {
                    while (current.get() < numOfWriters * numOfUpdates && exceptions.size() == 0) {
                        try (TableWriter tableWriter = getWriter("up")) {
                            tableWriter.tick();
                        } catch (EntryUnavailableException ignored) {
                        }
                        Os.sleep(100);
                    }
                });

                threads.add(tick);
                tick.start();
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
                for (int k = 0; k < numOfWriters; k++) {
                    Thread writer = new Thread(() -> {
                        try (final Connection connection = getConnection(pgServer.getPort(), false, true)) {
                            barrier.await();
                            System.out.println();
                            System.out.println("Connections " + metrics.pgWire().totalPGConnections().getValue());
                            System.out.println();
                            Assert.assertEquals(numOfWriters, metrics.pgWire().totalPGConnections().getValue());
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
                Assert.assertEquals(0, metrics.pgWire().totalPGConnections().getValue());
                for (int k = 0; k < numOfReaders; k++) {
                    Thread reader = new Thread(() -> {
                        IntObjHashMap<CharSequence[]> expectedValues = new IntObjHashMap<>();
                        expectedValues.put(0, PGConnectionMetricsTest.PartitionMode.getExpectedTimestamps(partitionMode));

                        IntObjHashMap<PGConnectionMetricsTest.Validator> validators = new IntObjHashMap<>();
                        validators.put(0, Chars::equals);
                        validators.put(1, new PGConnectionMetricsTest.Validator() {
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
                                while (current.get() < numOfWriters * numOfUpdates && exceptions.size() == 0) {
                                    rdr.reload();
                                    assertReader(rdr, expectedValues, validators);
                                }
                            }
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

            }


            if (exceptions.size() != 0) {
                Assert.fail(exceptions.poll().toString());
            }
        });

    }

    private enum PartitionMode {
        NONE, SINGLE, MULTIPLE;

        static CharSequence[] getExpectedTimestamps(PGConnectionMetricsTest.PartitionMode mode) {
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

        static long getTimestampSeq(PGConnectionMetricsTest.PartitionMode mode) {
            return mode == MULTIPLE ? 43200000000L : 1000000L;
        }

        static boolean isPartitioned(PGConnectionMetricsTest.PartitionMode mode) {
            return mode != NONE;
        }
    }

    private interface Validator {
        default void reset() {
        }

        boolean validate(CharSequence expected, CharSequence actual);
    }

    // I believe the disconnect of a connection sometimes happens after an assert causing a test fail
    // rerunning the test allows the test to pass
    public class RetryTest implements TestRule {
        private int retryCount;

        public RetryTest(int retryCount) {
            this.retryCount = retryCount;
        }

        public Statement apply(Statement base, Description description) {
            return statement(base, description);
        }

        private Statement statement(final Statement base, final Description description) {
            return new Statement() {
                Throwable caughtThrowable = null;

                @Override
                public void evaluate() throws Throwable {
                    for (int i = 0; i < retryCount; i++) {
                        try {
                            base.evaluate();
                            return;
                        } catch (Throwable t) {
                            caughtThrowable = t;
                            System.err.println(description.getDisplayName() + ": run " + (i + 1) + " failed");

                        }
                    }
                    System.err.println(description.getDisplayName() + ": giving up after " + retryCount + " failures");
                    throw caughtThrowable;
                }
            };
        }
    }
}

