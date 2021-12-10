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

package io.questdb.cutlass.http;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class DispatcherWriterQueueTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private SqlCompiler compiler;
    private SqlExecutionContextImpl sqlExecutionContext;
    private Error error = null;

    public void setupSql(CairoEngine engine) {
        compiler = new SqlCompiler(engine);
        BindVariableServiceImpl bindVariableService = new BindVariableServiceImpl(engine.getConfiguration());
        sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                .with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        bindVariableService,
                        null,
                        -1,
                        null);
        bindVariableService.clear();
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(2, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                1,
                0,
                "alter+table+x+add+column+y+int");
    }

    @Test
    public void testAlterTableAddIndex() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(2, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                1,
                0,
                "alter+table+x+add+column+y+int");
    }

    @Test
    public void testAlterTableAddRenameColumn() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertTrue("Column y must exist", columnIndex > 0);
                    int columnIndex2 = metadata.getColumnIndex("s2");
                    Assert.assertTrue("Column s2 must exist", columnIndex2 > 0);
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex2));
                    Assert.assertFalse(rdr.getSymbolMapReader(columnIndex2).isCached());
                },
                2,
                0,
                "alter+table+x+add+column+y+int",
                "alter+table+x+add+column+s2+symbol+capacity+512+nocache+index");
    }

    @Test
    public void testAlterTableAddNocacheAlterCache() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue("Column s must exist", columnIndex >= 0);
                    Assert.assertFalse(rdr.getSymbolMapReader(columnIndex).isCached());
                },
                1,
                0,
                "alter+table+x+alter+column+s+nocache");
    }

    @Test
    public void testAlterTableAddCacheAlterCache() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue("Column s must exist", columnIndex >= 0);
                    Assert.assertTrue(rdr.getSymbolMapReader(columnIndex).isCached());
                },
                1,
                0,
                "alter+table+x+alter+column+s+cache");
    }

    @Test
    public void testAlterTableFailsToUpgradeConcurrently() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndexQuiet("y");
                    int columnIndex2 = metadata.getColumnIndexQuiet("x");

                    Assert.assertTrue(columnIndex > -1 || columnIndex2 > -1);
                    Assert.assertTrue(columnIndex == -1 || columnIndex2 == -1);
                },
                2,
                1,
                "alter+table+x+rename+column+s+to+y",
                "alter+table+x+rename+column+s+to+x");
    }

    @Test
    public void testCanReuseSameJsonContextForMultipleAlterRuns() throws Exception {
        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndexQuiet("y");
                    int columnIndex2 = metadata.getColumnIndexQuiet("x");

                    Assert.assertTrue(columnIndex > -1 && columnIndex2 > -1);
                    Assert.assertEquals(-1, metadata.getColumnIndexQuiet("s"));
                },
                1,
                0,
                "alter+table+x+add+y+long256,x+timestamp",
                "alter+table+x+drop+column+s");
    }

    @Test
    public void testShouldFailWithTimeout() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(10)
                .run((engine) -> {
            setupSql(engine);
            try {
                compiler.compile("create table x as (" +
                        " select rnd_symbol('a', 'b', 'c') as s" +
                        " from long_sequence(10)" +
                        " )", sqlExecutionContext);

                try (TableWriter ignored = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "test lock")) {
                    // Writer locked, supposed to return timeout error
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=alter+table+x+add+column+y+int HTTP/1.1\r\n",
                            "80\r\n" +
                                    "{\"query\":\"alter table x add column y int\",\"error\":\"Timeout expired on waiting for the ALTER TABLE execution start\",\"position\":0}\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
                }
            } finally {
                compiler.close();
            }
        });
    }

    @Test
    public void testAlterTableAddDisconnect() throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(500_000)
                .withAlterTableMaxtWaitTimeout(20_000_000)
                .withFilesFacade(new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "x/default/s.v")) {
                            try {
                                Thread.sleep(600);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return super.openRW(name);
                    }
                });

        runAlterOnBusyTable((writer, rdr) -> {
                    // Wait command execution
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    for (int i = 0; i < 100 && !writer.getMetadata().isColumnIndexed(columnIndex); i++) {
                        writer.tick(true);
                        Os.sleep(100);
                    }
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                2,
                0,
                queryTestBuilder,
                true,
                "alter+table+x+alter+column+s+add+index");
    }

    @Test
    public void testAlterTableAddIndexContinuesAfterStartTimeoutExpired() throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(500_000)
                .withAlterTableMaxtWaitTimeout(20_000_000)
                .withFilesFacade(new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "x/default/s.v")) {
                            try {
                                Thread.sleep(600);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return super.openRW(name);
                    }
                });

        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                1,
                0,
                queryTestBuilder,
                false,
                "alter+table+x+alter+column+s+add+index");
    }

    @Test
    public void testAlterTableAddIndexContinuesAfterStartTimeoutExpiredAndTimeout() throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(500_000)
                .withAlterTableMaxtWaitTimeout(600_000)
                .withFilesFacade(new FilesFacadeImpl() {
                    @Override
                    public long openRW(LPSZ name) {
                        if (Chars.endsWith(name, "x/default/s.v") || Chars.endsWith(name, "x\\default\\s.v")) {
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        return super.openRW(name);
                    }
                });

        runAlterOnBusyTable((writer, rdr) -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("s");
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex));
                },
                1,
                1,
                queryTestBuilder,
                false,
                "alter+table+x+alter+column+s+add+index");
    }

    private void runAlterOnBusyTable(
            final AlterVerifyAction alterVerifyAction,
            int httpWorkers,
            int errorsExpected,
            final String... httpAlterQueries
    ) throws Exception {
        HttpQueryTestBuilder queryTestBuilder = new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(httpWorkers)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder().withReceiveBufferSize(50)
                )
                .withAlterTableStartWaitTimeout(2_000_000);

        runAlterOnBusyTable(alterVerifyAction, httpWorkers, errorsExpected, queryTestBuilder, false, httpAlterQueries);
    }

    private void runAlterOnBusyTable(
            AlterVerifyAction alterVerifyAction,
            int httpWorkers,
            int errorsExpected,
            HttpQueryTestBuilder queryTestBuilder,
            boolean noWait,
            final String... httpAlterQueries
    ) throws Exception {
        queryTestBuilder.run((engine) -> {
            setupSql(engine);
            compiler.compile("create table IF NOT EXISTS x as (" +
                    " select rnd_symbol('a', 'b', 'c') as s," +
                    " cast(x as timestamp) ts" +
                    " from long_sequence(10)" +
                    " )", sqlExecutionContext);
            TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "test lock");
            SOCountDownLatch finished = new SOCountDownLatch(httpAlterQueries.length);
            AtomicInteger errors = new AtomicInteger();
            CyclicBarrier barrier = new CyclicBarrier(httpAlterQueries.length);

            for (int i = 0; i < httpAlterQueries.length; i++) {
                String httpAlterQuery = httpAlterQueries[i];
                Thread thread = new Thread(() -> {
                    try {
                        barrier.await();
                        if (noWait) {
                            new SendAndReceiveRequestBuilder()
                                    .withPauseBetweenSendAndReceive(100)
                                    .execute(
                                    "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n"
                                            + SendAndReceiveRequestBuilder.RequestHeaders,
                                    ""
                            );
                        } else {
                            new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                    "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n",
                                    "0d\r\n" +
                                            "{\"ddl\":\"OK\"}\n\r\n" +
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

            if (httpAlterQueries.length > 1 && httpAlterQueries.length <= httpWorkers) {
                // Allow all queries to trigger commands before start processing them
                MPSequence tableWriterCommandPubSeq = engine.getMessageBus().getTableWriterCommandPubSeq();
                while (tableWriterCommandPubSeq.current() < httpAlterQueries.length - 1) {
                    LockSupport.parkNanos(10_000_000L);
                }
            }

            for (int i = 0; i < 100 && finished.getCount() > 0 && errors.get() <= errorsExpected; i++) {
                writer.tick(true);
                finished.await(1_000_000);
            }
            if (error != null) {
                throw error;
            }

            Assert.assertEquals(errorsExpected, errors.get());
            Assert.assertEquals(0, finished.getCount());
            engine.releaseAllReaders();
            try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "x")) {
                alterVerifyAction.run(writer, rdr);
            }
            writer.close();
            compiler.close();
        });
    }

    @FunctionalInterface
    interface AlterVerifyAction {
        void run(TableWriter writer, TableReader rdr) throws InterruptedException;
    }
}
