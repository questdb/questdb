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

package io.questdb.test.griffin;

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.AlterTableSetTypeTest.NON_WAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AlterTableSetTypeSuspendedTest extends AbstractAlterTableSetTypeRestartTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractBootstrapTest.setUpStatic();
        TestUtils.unchecked(() -> createDummyConfiguration(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true"));
    }

    @Test
    public void testWalSuspendedToNonWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.containsAscii(name, "x.d.1") && attempt.getAndIncrement() == 0) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final ServerMain questdb = new TestServerMain(bootstrap)) {
                questdb.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = questdb.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                try (final ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 1, 1)) {
                    insertInto(tableName);
                    walApplyJob.drain(0);

                    // WAL table
                    assertTrue(engine.isWalTable(token));
                    assertNumOfRows(engine, tableName, 1);
                    assertConvertFileDoesNotExist(engine, token);

                    // suspend table
                    // below should fail
                    runSqlViaPG("update " + tableName + " set x = 1111");
                    walApplyJob.drain(0);
                    assertTrue(engine.getTableSequencerAPI().isSuspended(token));
                    checkSuspended(tableName);

                    insertInto(tableName);
                    walApplyJob.drain(0);

                    // WAL table suspended, insert not applied
                    assertTrue(engine.isWalTable(token));
                    assertNumOfRows(engine, tableName, 1);
                    assertConvertFileDoesNotExist(engine, token);

                    // schedule table conversion to non-WAL
                    setType(tableName, "BYPASS WAL");
                    final Path path = assertConvertFileExists(engine, token);
                    assertConvertFileContent(path, NON_WAL);
                }
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain questdb = new TestServerMain(getServerMainArgs())) {
                questdb.start();

                final CairoEngine engine = questdb.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                assertFalse(engine.isWalTable(token));

                // insert works now
                insertInto(tableName);
                assertNumOfRows(engine, tableName, 2);

                dropTable(tableName);
            }
        });
    }
}
