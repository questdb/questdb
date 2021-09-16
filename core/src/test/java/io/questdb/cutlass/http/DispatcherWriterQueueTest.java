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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.mp.MPSequence;
import io.questdb.mp.SOCountDownLatch;
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
    public void testAlterTableAddIndex() throws Exception {
        runAlterOnBusyTable(writer -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(1, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                0,
                "alter+table+x+add+column+y+int");
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        runAlterOnBusyTable(writer -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertEquals(1, columnIndex);
                    Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
                },
                0,
                "alter+table+x+add+column+y+int");
    }

    @Test
    public void testAlterTableAddRenameColumn() throws Exception {
        runAlterOnBusyTable(writer -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndex("y");
                    Assert.assertTrue("Column y must exist", columnIndex > 0);
                    int columnIndex2 = metadata.getColumnIndex("s2");
                    Assert.assertTrue("Column s2 must exist", columnIndex2 > 0);
                    Assert.assertTrue(metadata.isColumnIndexed(columnIndex2));
                },
                0,
                "alter+table+x+add+column+y+int",
                "alter+table+x+add+column+s2+symbol+capacity+512+nocache+index");
    }

    @Test
    public void testAlterTableFailsToUpgradeConcurrently() throws Exception {
        runAlterOnBusyTable(writer -> {
                    TableWriterMetadata metadata = writer.getMetadata();
                    int columnIndex = metadata.getColumnIndexQuiet("y");
                    int columnIndex2 = metadata.getColumnIndexQuiet("x");

                    Assert.assertTrue(columnIndex > -1 || columnIndex2 > -1);
                    Assert.assertTrue(columnIndex == -1 || columnIndex2 == -1);
                },
                1,
                "alter+table+x+rename+column+s+to+y",
                "alter+table+x+rename+column+s+to+x");
    }

    private void runAlterOnBusyTable(final AlterVerifyAction alterVerifyAction, int errorsExpected, final String... httpAlterQueries) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(httpAlterQueries.length)
                .withHttpServerConfigBuilder(
                        new HttpServerConfigurationBuilder()
                                .withReceiveBufferSize(50)
                ).run((engine) -> {
            setupSql(engine);
            compiler.compile("create table x as (" +
                    " select rnd_symbol('a', 'b', 'c') as s" +
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
                        new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                                "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n",
                                "0d\r\n" +
                                        "{\"ddl\":\"OK\"}\n\r\n" +
                                        "00\r\n" +
                                        "\r\n"
                        );
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

            if (httpAlterQueries.length > 1) {
                // Allow all queries to trigger commands before start processing them
                MPSequence tableWriterCommandPubSeq = engine.getMessageBus().getTableWriterCommandPubSeq();
                while (tableWriterCommandPubSeq.current() < httpAlterQueries.length - 1) {
                    Thread.sleep(5);
                }
            }

            for(int i = 0; i < 100 && finished.getCount() > 0 && errors.get() <= errorsExpected; i++) {
                writer.tick();
                finished.await(1_000_000);
            }
            if (error != null) {
                throw error;
            }

            Assert.assertEquals(0, finished.getCount());
            Assert.assertEquals(errorsExpected, errors.get());
            alterVerifyAction.run(writer);
            writer.close();
            compiler.close();
        });
    }

    @FunctionalInterface
    interface AlterVerifyAction {
        void run(TableWriter writer);
    }
}
