/*+*****************************************************************************
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

package io.questdb.test;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ServerMainCleanStartupTest extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testServerMainCleanStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            // create two tables:
            // 1. empty
            // 2. non-empty with a couple of translations

            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                serverMain.start();
                serverMain.getEngine().execute("create table x (a int, t timestamp) timestamp(t) partition by day wal", sqlExecutionContext);
                serverMain.getEngine().execute("create table y (b int, t timestamp) timestamp(t) partition by day wal", sqlExecutionContext);

                CairoEngine cairoEngine1 = serverMain.getEngine();
                cairoEngine1.execute("insert into y values(100, 1)", sqlExecutionContext);
                CairoEngine cairoEngine = serverMain.getEngine();
                cairoEngine.execute("insert into y values(200, 2)", sqlExecutionContext);

                // wait for txns to be written
                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {}, "select wait_wal_table('y')")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                wait_wal_table('y')
                                true
                                """);

                // ensure transactions
                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {}, "select * from wal_tables order by 1")
                        .noLeakCheck()
                        .returns("""
                                name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure
                                x\tfalse\t0\t0\t0\t\t\t0
                                y\tfalse\t2\t0\t2\t\t\t0
                                """);


                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {}, "select table_name, ownership_reason from writer_pool where table_name in ('x','y') order by 1")
                        .noLeakCheck()
                        .returns("""
                                table_name\townership_reason
                                y\t
                                """);

            }

            // start a new server; it should not attempt to open new writers
            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                serverMain.start();

                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {}, "select table_name, ownership_reason from writer_pool where table_name in ('x','y') order by 1")
                        .noLeakCheck()
                        .returns("table_name\townership_reason\n");
            }
        });
    }
}
