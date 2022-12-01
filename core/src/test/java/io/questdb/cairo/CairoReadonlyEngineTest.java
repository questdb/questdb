/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CairoReadonlyEngineTest extends AbstractCairoTest {

    private CairoConfiguration roConfig;

    @Before
    public void setUp() {
        currentMicros = 0;
        roConfig = new DefaultTestCairoConfiguration(root) {
            @Override
            public boolean getAllowTableRegistrySharedWrite() {
                return false;
            }

            @Override
            public MillisecondClock getMillisecondClock() {
                return () -> testMicrosClock.getTicks() / 1000L;
            }

            public long getTableRegistryAutoReloadTimeout() {
                return 1000;
            }

            @Override
            public boolean isReadOnlyInstance() {
                return true;
            }
        };
        super.setUp();
    }

    @Test
    public void testCannotCreateSecondWriteInsance() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine ignore = new CairoEngine(new DefaultTestCairoConfiguration(root) {
                @Override
                public boolean getAllowTableRegistrySharedWrite() {
                    return false;
                }
            })) {
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot lock table name registry file");
            }
        });
    }

    @Test
    public void testRoEngineCannotCreateTables() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            try (CairoEngine roEngine = new CairoEngine(roConfig)) {

                try {
                    createTable(tableName, roEngine);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals("instance is read only", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testRoEngineCannotDropTables() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            try (CairoEngine roEngine = new CairoEngine(roConfig)) {
                createTable(tableName, engine);

                roEngine.reloadTableNames();
                try {
                    roEngine.drop(
                            AllowAllCairoSecurityContext.INSTANCE,
                            Path.getThreadLocal(root),
                            tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals("instance is read only", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testRoEngineCannotRenameTables() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            try (CairoEngine roEngine = new CairoEngine(roConfig)) {
                createTable(tableName, engine);

                roEngine.reloadTableNames();
                try {
                    roEngine.rename(
                            AllowAllCairoSecurityContext.INSTANCE,
                            Path.getThreadLocal(root),
                            tableName,
                            Path.getThreadLocal2(root),
                            tableName + "_renamed"
                    );
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals("instance is read only", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testTableNameLoad() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            try (CairoEngine roEngine = new CairoEngine(roConfig)) {
                createTable(tableName, engine);
                roEngine.reloadTableNames();
                Assert.assertEquals(
                        engine.getTableToken(tableName),
                        roEngine.getTableToken(tableName)
                );
            }
        });
    }

    @Test
    public void testTableNameTimeoutLoad() throws Exception {
        assertMemoryLeak(() -> {
            String tableName = testName.getMethodName();
            try (CairoEngine roEngine = new CairoEngine(roConfig)) {
                createTable(tableName, engine);

                try {
                    roEngine.getTableToken(tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals(
                            "table does not exist [table=testTableNameTimeoutLoad]",
                            e.getFlyweightMessage()
                    );
                }

                currentMicros += 1_100_000L;
                Assert.assertEquals(
                        engine.getTableToken(tableName),
                        roEngine.getTableToken(tableName)
                );
            }
        });
    }

    private static void createTable(String tableName, CairoEngine cairoEngine) {
        try (TableModel table1 = new TableModel(
                configuration,
                tableName,
                PartitionBy.NONE
        )) {
            table1.timestamp("ts")
                    .col("x", ColumnType.INT)
                    .col("y", ColumnType.STRING);
            CairoTestUtils.create(table1, cairoEngine);
        }
    }
}
