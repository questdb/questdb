/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CairoReadonlyEngineTest extends AbstractCairoTest {

    private CairoConfiguration roConfig;

    @Before
    public void setUp() {
        super.setUp();
        setCurrentMicros(0);
        roConfig = new DefaultTestCairoConfiguration(root) {
            @Override
            public boolean getAllowTableRegistrySharedWrite() {
                return false;
            }

            @Override
            public @NotNull MillisecondClock getMillisecondClock() {
                return () -> testMicrosClock.getTicks() / 1000L;
            }

            public long getTableRegistryAutoReloadFrequency() {
                return 1000;
            }

            @Override
            public boolean isReadOnlyInstance() {
                return true;
            }
        };
    }

    @Test
    public void testCannotCreateSecondWriteInstance() throws Exception {
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
                TableToken token = createTable(tableName, engine);

                roEngine.reloadTableNames();
                try {
                    roEngine.dropTableOrViewOrMatView(
                            Path.getThreadLocal(root),
                            token
                    );
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
                try (MemoryMARW mem = Vm.getCMARWInstance()) {
                    roEngine.rename(
                            AllowAllSecurityContext.INSTANCE,
                            Path.getThreadLocal(root),
                            mem,
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
                        engine.verifyTableName(tableName),
                        roEngine.verifyTableName(tableName)
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
                    roEngine.verifyTableName(tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertEquals(
                            "table does not exist [table=testTableNameTimeoutLoad]",
                            e.getFlyweightMessage()
                    );
                }

                setCurrentMicros(1_100_000L);
                Assert.assertEquals(
                        engine.verifyTableName(tableName),
                        roEngine.verifyTableName(tableName)
                );
            }
        });
    }

    private static TableToken createTable(String tableName, CairoEngine cairoEngine) {
        TableModel table1 = new TableModel(
                configuration,
                tableName,
                PartitionBy.NONE
        );
        table1.timestamp("ts")
                .col("x", ColumnType.INT)
                .col("y", ColumnType.STRING);
        return TestUtils.createTable(cairoEngine, table1);
    }
}
