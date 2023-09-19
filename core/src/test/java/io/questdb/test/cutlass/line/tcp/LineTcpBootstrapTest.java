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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
                try (Sender sender = Sender.builder().address("localhost").port(port).build()) {
                    for (int i = 0; i < 1_000_000; i++) {
                        sender.table("x").stringColumn("a", "42").atNow();
                    }
                    Assert.fail();
                } catch (LineSenderException expected) {
                }
            }
        });
    }
}
