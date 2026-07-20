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
                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {
                }, "select wait_wal_table('y')")
                        .noLeakCheck()
                        .expectSize()
                        .noMemoryUsageCheck()
                        .returns("""
                                wait_wal_table('y')
                                true
                                """);

                // ensure transactions. Project only the deterministic columns: under the
                // default ADAPTIVE commit mode a real ServerMain fires durable epochs on a
                // wall-clock schedule, so durableEpochSeqTxn/walRetentionTxn/lastEpochTs are
                // timing-dependent (lastEpochTs is a wall-clock instant). The epoch columns
                // are covered deterministically by the adaptive-epoch suites; this clean-start
                // smoke check asserts the stable frontier instead.
                new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {
                }, "select name, suspended, writerTxn, bufferedTxnSize, sequencerTxn, errorTag, errorMessage, memoryPressure, commitMode, recoveryIncarnation, localDurableSeqTxn from wal_tables order by 1")
                        .noLeakCheck()
                        .returns("""
                                name\tsuspended\twriterTxn\tbufferedTxnSize\tsequencerTxn\terrorTag\terrorMessage\tmemoryPressure\tcommitMode\trecoveryIncarnation\tlocalDurableSeqTxn
                                x\tfalse\t0\t0\t0\t\t\t0\tadaptive\t0\t-1
                                y\tfalse\t2\t0\t2\t\t\t0\tadaptive\t0\t2
                                """);


                // Under the default ADAPTIVE commit mode the background apply job holds y's
                // writer through the durable-epoch advance a beat longer than NOSYNC did, so
                // right after apply the pooled ownership_reason is briefly "WAL Data
                // Application". Poll until the writer settles back into the pool (released).
                TestUtils.assertEventually(() ->
                        new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {
                        }, "select table_name, ownership_reason from writer_pool where table_name in ('x','y') order by 1")
                                .noLeakCheck()
                                .noMemoryUsageCheck()
                                .returns("""
                                        table_name\townership_reason
                                        y\t
                                        """), 10);

            }

            // start a new server; it should not attempt to open new writers
            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 1).with(AllowAllSecurityContext.INSTANCE)
            ) {
                serverMain.start();

                // ADAPTIVE restart recovery rolls the WAL tail forward (y's last commit is
                // past its last durable epoch), which opens y's writer and returns it to the
                // pool released. So y legitimately appears pooled-but-unowned after restart,
                // unlike NOSYNC where restart touched nothing. The invariant that still holds
                // -- and that this check verifies -- is that the restart leaves no writer
                // PERSISTENTLY OWNED; filter to owned rows and poll until the roll-forward
                // writer settles back to released.
                TestUtils.assertEventually(() ->
                        new QueryAssertion(serverMain.getEngine(), sqlExecutionContext, () -> {
                        }, "select table_name, ownership_reason from writer_pool where table_name in ('x','y') and ownership_reason is not null and ownership_reason != '' order by 1")
                                .noLeakCheck()
                                .noMemoryUsageCheck()
                                .returns("table_name\townership_reason\n"), 10);
            }
        });
    }
}
