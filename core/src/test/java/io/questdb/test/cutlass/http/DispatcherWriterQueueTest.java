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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.QueryFutureUpdateListener;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.network.Net;
import io.questdb.std.Os;
import io.questdb.std.datetime.Clock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.URLEncoder;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DispatcherWriterQueueTest extends AbstractCairoTest {
    @Rule
    public Timeout timeout = Timeout.builder()
            .withTimeout(10 * 60 * 1000, TimeUnit.MILLISECONDS)
            .withLookingForStuckThread(true)
            .build();
    private Error error = null;

    @Test
    public void testAlterTableAddCacheAlterCache() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue("Column s must exist", columnIndex >= 0);
                    Assert.assertTrue(reader.getSymbolMapReader(columnIndex).isCached());
                },
                1,
                0,
                "alter+table+<x>+alter+column+s+cache"
        );
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(2, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                1,
                0,
                "alter+table+<x>+add+column+y+int"
        );
    }

    @Test
    public void testAlterTableAddDisconnect() throws Exception {
        SOCountDownLatch alterAckReceived = new SOCountDownLatch(1);
        SOCountDownLatch disconnectLatch = new SOCountDownLatch(1);

        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withQueryFutureUpdateListener(waitUntilCommandStarted(alterAckReceived))
                .withAlterTableStartWaitTimeout(30_000)
                .withAlterTableMaxWaitTimeout(50_000)
                .withFilesFacade(new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "default/s.v") || Utf8s.endsWithAscii(name, "default\\s.v")) {
                            alterAckReceived.await();
                            disconnectLatch.countDown();
                        }
                        return super.openRW(name, opts);
                    }
                });

        runAlterOnBusyTable(
                (writer, reader) -> {
                    // Wait command execution
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    for (int i = 0; i < 100 && !writer.getMetadata().isColumnIndexed(columnIndex); i++) {
                        writer.tick(true);
                        Os.sleep(100);
                    }
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                0,
                queryTestBuilder,
                disconnectLatch,
                "alter+table+<x>+alter+column+s+add+index"
        );
    }

    @Test
    public void testAlterTableAddIndex() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(2, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                1,
                0,
                "alter+table+<x>+add+column+y+int"
        );
    }

    @Test
    public void testAlterTableAddIndexContinuesAfterStartTimeoutExpired() throws Exception {
        SOCountDownLatch alterAckReceived = new SOCountDownLatch(1);
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withQueryFutureUpdateListener(waitUntilCommandStarted(alterAckReceived))
                .withAlterTableStartWaitTimeout(30_000)
                .withAlterTableMaxWaitTimeout(50_000)
                .withFilesFacade(new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "/default/s.v") || Utf8s.endsWithAscii(name, "default\\s.v")) {
                            alterAckReceived.await();
                        }
                        return super.openRW(name, opts);
                    }
                });

        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                0,
                queryTestBuilder,
                null,
                "alter+table+<x>+alter+column+s+add+index"
        );
    }

    @Test
    public void testAlterTableAddIndexContinuesAfterStartTimeoutExpiredAndTimeout() throws Exception {
        SOCountDownLatch alterAckReceived = new SOCountDownLatch(1);

        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(100)
                .withAlterTableMaxWaitTimeout(10)
                .withQueryFutureUpdateListener(waitUntilCommandStarted(alterAckReceived))
                .withFilesFacade(new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "/default/s.v") || Utf8s.endsWithAscii(name, "\\default\\s.v")) {
                            alterAckReceived.await();
                            Os.sleep(500);
                        }
                        return super.openRW(name, opts);
                    }
                });

        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                1,
                queryTestBuilder,
                null,
                "alter+table+<x>+alter+column+s+add+index"
        );
    }

    @Test
    public void testAlterTableAddNocacheAlterCache() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue("Column s must exist", columnIndex >= 0);
                    Assert.assertFalse(reader.getSymbolMapReader(columnIndex).isCached());
                },
                1,
                0,
                "alter+table+<x>+alter+column+s+nocache"
        );
    }

    @Test
    public void testAlterTableAddRenameColumn() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertTrue("Column y must exist", columnIndex > 0);
                    int columnIndex2 = metadata.getColumnIndex("s2");
                    Assert.assertTrue("Column s2 must exist", columnIndex2 > 0);
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex2));
                    Assert.assertFalse(reader.getSymbolMapReader(columnIndex2).isCached());
                },
                2,
                0,
                "alter+table+<x>+add+column+y+int",
                "alter+table+<x>+add+column+s2+symbol+capacity+512+nocache+index"
        );
    }

    @Test
    public void testAlterTableCacheAndNocache() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE foo ( a SYMBOL )");
            drainWalQueue();

            String header = "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n";
            String left = "a\tSYMBOL\tfalse\t256\t";
            String right = "\t128\t0\tfalse\tfalse\n";

            // check its true by default
            assertSql(header + left + "true" + right, "table_columns('foo')");

            execute("ALTER TABLE foo ALTER COLUMN a NOCACHE");
            drainWalQueue();
            // check its false now
            assertSql(header + left + "false" + right, "table_columns('foo')");

            execute("ALTER TABLE foo ALTER COLUMN a CACHE");
            drainWalQueue();

            // check its true again
            assertSql(header + left + "true" + right, "table_columns('foo')");
        });
    }

    @Test
    public void testAlterTableFailsToUpgradeConcurrently() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndexQuiet("y");
                    int columnIndex2 = metadata.getColumnIndexQuiet("x");

                    Assert.assertTrue(columnIndex > -1 || columnIndex2 > -1);
                    Assert.assertTrue(columnIndex == -1 || columnIndex2 == -1);
                },
                2,
                1,
                "alter+table+<x>+rename+column+s+to+y",
                "alter+table+<x>+rename+column+s+to+x"
        );
    }

    @Test
    public void testCanReuseSameJsonContextForMultipleAlterRuns() throws Exception {
        runAlterOnBusyTable(
                (writer, reader) -> {
                    TableRecordMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndexQuiet("y");
                    int columnIndex2 = metadata.getColumnIndexQuiet("x");

                    Assert.assertTrue(columnIndex > -1 && columnIndex2 > -1);
                    Assert.assertEquals(-1, metadata.getColumnIndexQuiet("s"));
                },
                1,
                0,
                "alter+table+<x>+add+y+long256,x+timestamp",
                "alter+table+<x>+drop+column+s"
        );
    }

    @Test
    public void testUpdateBusyTable() throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(30_000);

        // this is JDK8 thing:
        //noinspection CharsetObjectCanBeUsed
        runUpdateOnBusyTable(
                (writer, reader) -> TestUtils.assertReader(
                        "s\tx\tts\n" +
                                "b\t10\t1970-01-01T00:00:00.000001Z\n" +
                                "c\t2\t1970-01-01T00:00:00.000002Z\n" +
                                "a\t1\t1970-01-01T00:00:00.000003Z\n" +
                                "b\t10\t1970-01-01T00:00:00.000004Z\n" +
                                "c\t5\t1970-01-01T00:00:00.000005Z\n" +
                                "a\t1\t1970-01-01T00:00:00.000006Z\n" +
                                "b\t10\t1970-01-01T00:00:00.000007Z\n" +
                                "c\t8\t1970-01-01T00:00:00.000008Z\n" +
                                "a\t1\t1970-01-01T00:00:00.000009Z\n",
                        reader,
                        new StringSink()
                ),
                writer -> {
                },
                queryTestBuilder,
                null,
                null,
                -1L,
                3,
                URLEncoder.encode("update x set x=1 where s = 'a'", "UTF8"),
                URLEncoder.encode("update x set x=10 where s = 'b'", "UTF8")
        );
    }

    @SuppressWarnings("CharsetObjectCanBeUsed")
    @Test
    public void testUpdateConnectionDropOnColumnRewrite() throws Exception {
        SOCountDownLatch disconnectLatch = new SOCountDownLatch(1);

        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(30_000)
                .withFilesFacade(new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "x.d.1")) {
                            disconnectLatch.countDown();
                        }
                        return super.openRW(name, opts);
                    }
                });

        runUpdateOnBusyTable(
                (wrt, rdr) -> {
                    // Test no resources leak, update can go through or not, it is not deterministic
                },
                writer -> {
                },
                queryTestBuilder,
                disconnectLatch,
                null,
                1000,
                0,
                URLEncoder.encode("update x set x=1 from tables()", "UTF8")
        );
    }

    @Test
    public void testUpdateContinuesAfterAfterReaderOutOfDateExceptionWithRetry() throws Exception {
        testUpdateSucceedsAfterReaderOutOfDateException(2, 1L);
    }

    @Test
    public void testUpdateContinuesAfterStartTimeoutExpiredAndFailsAfterReaderOutOfDateException() throws Exception {
        testUpdateFailsAfterReaderOutOfDateException(2, 1_000L);
    }

    @Test
    public void testUpdateContinuesAfterStartTimeoutExpiredAndSucceedsAfterReaderOutOfDateException() throws Exception {
        testUpdateSucceedsAfterReaderOutOfDateException(2, 1_000L);
    }

    @Test
    public void testUpdateFailsAfterReaderOutOfDateException() throws Exception {
        testUpdateFailsAfterReaderOutOfDateException(1, 30_000L);
    }

    @Test
    public void testUpdateSucceedsAfterReaderOutOfDateException() throws Exception {
        testUpdateSucceedsAfterReaderOutOfDateException(1, 30_000L);
    }

    private void runAlterOnBusyTable(
            AlterVerifyAction alterVerifyAction,
            int errorsExpected,
            HttpQueryTestBuilder queryTestBuilder,
            SOCountDownLatch waitToDisconnect,
            final String... httpAlterQueries
    ) throws Exception {
        queryTestBuilder.run((engine, sqlExecutionContext) -> {
            TableWriter writer = null;
            try {
                String tableName = "x";
                engine.execute(
                        "create table IF NOT EXISTS " + tableName + " as (" +
                                " select rnd_symbol('a', 'b', 'c') as s," +
                                " cast(x as timestamp) ts" +
                                " from long_sequence(10)" +
                                " )",
                        sqlExecutionContext
                );
                writer = TestUtils.getWriter(engine, tableName);
                SOCountDownLatch finished = new SOCountDownLatch(httpAlterQueries.length);
                AtomicInteger errors = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(httpAlterQueries.length);

                for (int i = 0; i < httpAlterQueries.length; i++) {
                    String httpAlterQuery = httpAlterQueries[i].replace("<x>", tableName);
                    Thread thread = new Thread(() -> {
                        try {
                            barrier.await();
                            if (waitToDisconnect != null) {
                                long fd = new SendAndReceiveRequestBuilder()
                                        .connectAndSendRequest(
                                                "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n"
                                                        + SendAndReceiveRequestBuilder.RequestHeaders
                                        );
                                waitToDisconnect.await();
                                Net.close(fd);
                            } else {
                                new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                        "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n",
                                        "0c\r\n" +
                                                "{\"ddl\":\"OK\"}\r\n" +
                                                "00\r\n" +
                                                "\r\n"
                                );
                            }
                        } catch (Error e) {
                            if (errorsExpected == 0) {
                                error = e;
                            }
                            errors.getAndIncrement();
                        } catch (Throwable e) {
                            errors.getAndIncrement();
                        } finally {
                            finished.countDown();
                        }
                    });
                    thread.start();
                }

                Clock microsecondClock = engine.getConfiguration().getMicrosecondClock();
                long startTimeMicro = microsecondClock.getTicks();
                // Wait 1 min max for completion
                while (microsecondClock.getTicks() - startTimeMicro < 60_000_000 && finished.getCount() > 0 && errors.get() <= errorsExpected) {
                    writer.tick(true);
                    finished.await(1_000_000);
                }

                if (error != null) {
                    throw error;
                }
                Assert.assertEquals(errorsExpected, errors.get());
                Assert.assertEquals(0, finished.getCount());
                engine.releaseInactive();
                try (TableReader reader = engine.getReader(tableName)) {
                    alterVerifyAction.run(writer, reader);
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        });
    }

    private void runAlterOnBusyTable(
            final AlterVerifyAction alterVerifyAction,
            int httpWorkers,
            int errorsExpected,
            final String... httpAlterQueries
    ) throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(httpWorkers)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(30_000);

        runAlterOnBusyTable(alterVerifyAction, errorsExpected, queryTestBuilder, null, httpAlterQueries);
    }

    private void runUpdateOnBusyTable(
            AlterVerifyAction alterVerifyAction,
            OnTickAction onTick,
            HttpQueryTestBuilder queryTestBuilder,
            SOCountDownLatch waitToDisconnect,
            String errorHeader,
            long statementTimeout,
            int updatedCount,
            final String... httpUpdateQueries
    ) throws Exception {
        queryTestBuilder.run((engine, sqlExecutionContext) -> {
            TableWriter writer = null;
            try {
                String tableName = "x";
                engine.execute("create table IF NOT EXISTS " + tableName + " as (" +
                                " select case when x%3 = 0 then 'a' when x%3 = 1 then 'b' else 'c' end as s," +
                                " x," +
                                " cast(x as timestamp) ts" +
                                " from long_sequence(9)" +
                                " )",
                        sqlExecutionContext
                );
                writer = TestUtils.getWriter(engine, tableName);
                SOCountDownLatch finished = new SOCountDownLatch(httpUpdateQueries.length);
                AtomicInteger errors = new AtomicInteger();
                CyclicBarrier barrier = new CyclicBarrier(httpUpdateQueries.length);

                for (int i = 0; i < httpUpdateQueries.length; i++) {
                    String httpUpdateQuery = httpUpdateQueries[i];
                    Thread thread = new Thread(() -> {
                        try {
                            barrier.await();
                            if (waitToDisconnect != null) {
                                long fd = new SendAndReceiveRequestBuilder()
                                        .withStatementTimeout(statementTimeout)
                                        .connectAndSendRequestWithHeaders(
                                                "GET /query?query=" + httpUpdateQuery + " HTTP/1.1\r\n"
                                        );
                                waitToDisconnect.await();
                                Net.close(fd);
                            } else {
                                if (errorHeader != null) {
                                    new SendAndReceiveRequestBuilder()
                                            .withStatementTimeout(statementTimeout)
                                            .executeWithStandardRequestHeaders(
                                                    "GET /query?query=" + httpUpdateQuery + " HTTP/1.1\r\n",
                                                    errorHeader
                                            );
                                } else {
                                    new SendAndReceiveRequestBuilder()
                                            .withStatementTimeout(statementTimeout)
                                            .executeWithStandardHeaders(
                                                    "GET /query?query=" + httpUpdateQuery + " HTTP/1.1\r\n",
                                                    "18\r\n" +
                                                            "{\"dml\":\"OK\",\"updated\":" + updatedCount + "}\r\n" +
                                                            "00\r\n" +
                                                            "\r\n"
                                            );
                                }
                            }
                        } catch (Error e) {
                            error = e;
                            errors.getAndIncrement();
                        } catch (Throwable e) {
                            errors.getAndIncrement();
                        } finally {
                            finished.countDown();
                        }
                    });
                    thread.start();
                }

                Clock microsecondClock = engine.getConfiguration().getMicrosecondClock();
                long startTimeMicro = microsecondClock.getTicks();
                // Wait 1 min max for completion
                while (microsecondClock.getTicks() - startTimeMicro < 60_000_000 && finished.getCount() > 0 && errors.get() <= 0) {
                    onTick.run(writer);
                    writer.tick(true);
                    finished.await(1_000_000);
                }

                if (error != null) {
                    throw error;
                }
                Assert.assertEquals(0, errors.get());
                Assert.assertEquals(0, finished.getCount());
                engine.releaseAllReaders();
                try (TableReader reader = engine.getReader(tableName)) {
                    alterVerifyAction.run(writer, reader);
                }
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        });
    }

    // JDK8 related
    @SuppressWarnings("CharsetObjectCanBeUsed")
    private void testUpdateAfterReaderOutOfDateException(
            AlterVerifyAction alterVerifyAction,
            OnTickAction onTick,
            SOCountDownLatch updateScheduled,
            long startWaitTimeout,
            String errorHeader,
            long statementTimeout,
            int updatedCount
    ) throws Exception {
        final SOCountDownLatch updateAckReceived = new SOCountDownLatch(1);

        final HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withQueryFutureUpdateListener(waitUntilCommandStarted(updateAckReceived, updateScheduled))
                .withAlterTableStartWaitTimeout(startWaitTimeout)
                .withAlterTableMaxWaitTimeout(50_000L)
                .withFilesFacade(new TestFilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name, int opts) {
                        if (Utf8s.endsWithAscii(name, "default/ts.d.2") || Utf8s.endsWithAscii(name, "default\\ts.d.2")) {
                            updateAckReceived.await();
                        }
                        return super.openRW(name, opts);
                    }
                });

        runUpdateOnBusyTable(
                alterVerifyAction,
                onTick,
                queryTestBuilder,
                null,
                errorHeader,
                statementTimeout,
                updatedCount,
                URLEncoder.encode("update x set ts=123", "UTF8")
        );
    }

    private void testUpdateFailsAfterReaderOutOfDateException(
            int updateScheduledCount,
            long startWaitTimeout
    ) throws Exception {
        final SOCountDownLatch updateScheduled = new SOCountDownLatch(updateScheduledCount);

        testUpdateAfterReaderOutOfDateException(
                (writer, reader) -> {
                    try {
                        reader.getMetadata().getColumnIndex("ts");
                        Assert.fail("InvalidColumnException is expected");
                    } catch (InvalidColumnException ignored) {
                    } catch (Throwable th) {
                        Assert.fail("InvalidColumnException is expected instead");
                    }
                },
                new OnTickAction() {
                    private boolean first = true;

                    @Override
                    public void run(TableWriter writer) {
                        if (first) {
                            updateScheduled.await();
                            // removing a new column before calling writer.tick() will result in ReaderOutOfDateException
                            // thrown from UpdateOperator as this changes table structure
                            // recompile will fail because the column UPDATE refers to is removed
                            writer.removeColumn("ts");
                            first = false;
                        }
                    }
                },
                updateScheduled,
                startWaitTimeout,
                "HTTP/1.1 400 Bad request\r\n" +
                        "Server: questDB/1.0\r\n" +
                        "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                        "Transfer-Encoding: chunked\r\n" +
                        "Content-Type: application/json; charset=utf-8\r\n" +
                        "Keep-Alive: timeout=5, max=10000\r\n" +
                        "\r\n" +
                        "4a\r\n" +
                        "{\"query\":\"update x set ts=123\",\"error\":\"Invalid column: ts\",\"position\":13}\r\n" +
                        "00\r\n" +
                        "\r\n",
                -1L,
                0
        );
    }

    private void testUpdateSucceedsAfterReaderOutOfDateException(
            int updateScheduledCount,
            long startWaitTimeout
    ) throws Exception {
        SOCountDownLatch updateScheduled = new SOCountDownLatch(updateScheduledCount);

        testUpdateAfterReaderOutOfDateException(
                (writer, reader) -> {
                    try (TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor()) {
                        cursor.of(reader);
                        int colIndex = reader.getMetadata().getColumnIndex("ts");
                        while (cursor.hasNext()) {
                            long value = cursor.getRecord().getLong(colIndex);
                            Assert.assertEquals(123L, value);
                        }
                    }
                },
                new OnTickAction() {
                    private boolean first = true;

                    @Override
                    public void run(TableWriter writer) {
                        if (first) {
                            updateScheduled.await();
                            // adding a new column before calling writer.tick() will result in ReaderOutOfDateException
                            // thrown from UpdateOperator as this changes table structure
                            // recompile should be successful so the UPDATE completes
                            writer.addColumn("newCol", ColumnType.INT);
                            first = false;
                        }
                    }
                },
                updateScheduled,
                startWaitTimeout,
                null,
                120_000_000L,
                9
        );
    }

    private QueryFutureUpdateListener waitUntilCommandStarted(SOCountDownLatch ackReceived) {
        return waitUntilCommandStarted(ackReceived, null);
    }

    private QueryFutureUpdateListener waitUntilCommandStarted(SOCountDownLatch ackReceived, SOCountDownLatch scheduled) {
        return new QueryFutureUpdateListener() {
            @Override
            public void reportBusyWaitExpired(TableToken tableToken, long commandId) {
                if (scheduled != null) {
                    scheduled.countDown();
                }
            }

            @Override
            public void reportProgress(long commandId, int status) {
                if (status == OperationFuture.QUERY_STARTED) {
                    ackReceived.countDown();
                }
            }

            @Override
            public void reportStart(TableToken tableToken, long commandId) {
                if (scheduled != null) {
                    scheduled.countDown();
                }
            }
        };
    }

    @FunctionalInterface
    interface AlterVerifyAction {
        void run(TableWriter writer, TableReader rdr);
    }

    @FunctionalInterface
    interface OnTickAction {
        void run(TableWriter writer);
    }
}
