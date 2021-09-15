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
import io.questdb.mp.SOCountDownLatch;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.atomic.AtomicInteger;

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
        runAlterOnBusyTable("alter+table+x+alter+column+s+add+index", writer -> {
            int columnIndex = writer.getMetadata().getColumnIndex("s");
            Assert.assertTrue(writer.getMetadata().isColumnIndexed(columnIndex));
        });
    }

    @Test
    public void testAlterTableAddColumn() throws Exception {
        runAlterOnBusyTable("alter+table+x+add+column+y+int", writer -> {
            TableWriterMetadata metadata = writer.getMetadata();
            int columnIndex = metadata.getColumnIndex("y");
            Assert.assertEquals(1, columnIndex);
            Assert.assertEquals(ColumnType.INT, metadata.getColumnType(columnIndex));
        });
    }

    private void runAlterOnBusyTable(final String httpAlterQuery, final AlterVerifyAction alterVerifyAction) throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(temp)
                .withWorkerCount(1)
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
            SOCountDownLatch finished = new SOCountDownLatch(1);
            AtomicInteger errors = new AtomicInteger();

            Thread thread = new Thread(() -> {
                try {
                    new SendAndReceiveRequestBuilder().executeWithStandardHeaders(
                            "GET /query?query=" + httpAlterQuery + " HTTP/1.1\r\n",
                            "0d\r\n" +
                                    "{\"ddl\":\"OK\"}\n\r\n" +
                                    "00\r\n" +
                                    "\r\n"
                    );
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

            for(int i = 0; i < 100 && finished.getCount() == 1 && error == null; i++) {
                writer.tick();
                finished.await(1_000_000);
            }
            if (error != null) {
                throw error;
            }
            alterVerifyAction.run(writer);
            thread.join();
            writer.close();

            Assert.assertEquals(0, finished.getCount());
            Assert.assertEquals(0, errors.get());
            compiler.close();
        });
    }

    @FunctionalInterface
    interface AlterVerifyAction {
        void run(TableWriter writer);
    }
}
