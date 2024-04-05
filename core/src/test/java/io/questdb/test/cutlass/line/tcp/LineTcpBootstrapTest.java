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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.Connection;

public class LineTcpBootstrapTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testDisconnectOnErrorWithWAL() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlCompiler sqlCompiler = engine.getSqlCompiler();
                     SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    sqlCompiler.compile("create table x (ts timestamp, a int) timestamp(ts) partition by day wal", sqlExecutionContext);
                }

                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindPort();
                try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(port).build()) {
                    for (int i = 0; i < 1_000_000; i++) {
                        sender.table("x").stringColumn("a", "42").atNow();
                    }
                    Assert.fail();
                } catch (LineSenderException expected) {
                }
            }
        });
    }

    @Ignore// update statements don't time out anymore but can be cancelled manually
    @Test
    public void testUpdateTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (final ServerMain serverMain = startWithEnvVariables(
                    PropertyKey.QUERY_TIMEOUT_SEC.getEnvVarName(), "0.015"
            )) {
                serverMain.start();
                CairoEngine engine = serverMain.getEngine();
                try (SqlCompiler sqlCompiler = engine.getSqlCompiler();
                     SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)) {
                    sqlCompiler.compile("create table abc as " +
                            "(select rnd_symbol('a', 'b', 'c', null) sym1," +
                            " rnd_symbol('a', 'b', 'c', null) sym2," +
                            " rnd_symbol('a', 'b', 'c', null) sym3," +
                            " timestamp_sequence('2022-02-24T04', 1000000L) ts" +
                            " from long_sequence(100 * 60 * 60)), " +
                            "index(sym1), index(sym2), index(sym3) timestamp(ts) partition by HOUR BYPASS WAL", sqlExecutionContext);
                }

                int port = serverMain.getConfiguration().getLineTcpReceiverConfiguration().getDispatcherConfiguration().getBindPort();
                Thread th = new Thread(() -> {
                    try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost").port(port).build()) {
                        for (int i = 0; i < 100; i++) {
                            sender.table("abc").symbol("sym1", "10").atNow();
                        }
                    }
                });
                th.start();

                int pgPort = serverMain.getConfiguration().getPGWireConfiguration().getDispatcherConfiguration().getBindPort();
                try (Connection conn = getConnection("admin", "quest", pgPort)) {
                    conn.createStatement().execute("update abc set sym1 = '0', sym2='2', sym3='3'");
                } catch (PSQLException e) {
                    if (!Chars.contains(e.getMessage(), "timeout")) {
                        // No timeout, shame, will happen another time
                        return;
                    }
                }

                LOG.info().$("HURRAY! TIMEOUT REPRODUCED").$();

                // The update should be rolled back and no rows with sym1 = '0' should be present
                TestUtils.assertSql(serverMain.getEngine(),
                        new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration())),
                        "select count() from abc where sym1 = '0'",
                        new StringSink(),
                        "count\n" +
                                "0\n"
                );
                th.join();
            }
        });
    }
}
