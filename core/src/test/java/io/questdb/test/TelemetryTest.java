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

package io.questdb.test;

import io.questdb.*;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.std.str.Path;
import io.questdb.tasks.TelemetryTask;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

public class TelemetryTest extends AbstractCairoTest {
    private final static FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private final static String TELEMETRY = TelemetryTask.TABLE_NAME;

    @Test
    public void testTelemetryCanDeleteTableWhenDisabled() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public TelemetryConfiguration getTelemetryConfiguration() {
                return new DefaultTelemetryConfiguration() {
                    @Override
                    public boolean getEnabled() {
                        return false;
                    }
                };
            }
        };

        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine);
                    TelemetryJob ignored = new TelemetryJob(engine);
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                try {
                    compiler.compile("drop table " + TELEMETRY, sqlExecutionContext);
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table="+TELEMETRY+"]");
                }
            }
        });
    }

    @Test
    public void testTelemetryConfigUpgrade() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                compiler.compile(
                        "CREATE TABLE " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " (id long256, enabled boolean)",
                        sqlExecutionContext);
                InsertOperation ist = compiler.compile(
                        "INSERT INTO " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " values(CAST('0x01' AS LONG256), true)",
                        sqlExecutionContext).getInsertOperation();
                InsertMethod im = ist.createMethod(sqlExecutionContext);
                im.execute();
                im.commit();
                im.close();
                TelemetryJob telemetryJob = new TelemetryJob(engine);
                String expectedSql = "column	type	indexed	indexBlockCapacity	symbolCached	symbolCapacity	designated\n" +
                        "id	LONG256	false	0	false	0	false\n" +
                        "enabled	BOOLEAN	false	0	false	0	false\n" +
                        "version	SYMBOL	false	256	true	128	false\n" +
                        "os	SYMBOL	false	256	true	128	false\n" +
                        "package	SYMBOL	false	256	true	128	false\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SHOW COLUMNS FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                expectedSql = "id\tversion\n" +
                        "0x01\t\n" +
                        "0x01\tUnknown Version\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT id, version FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink,
                        expectedSql);
                Misc.free(telemetryJob);
            }

        });
    }

    @Test
    public void testTelemetryCreatesTablesWhenEnabled() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                final TelemetryJob telemetryJob = new TelemetryJob(engine);

                try (Path path = new Path()) {
                    TableToken telemetry = engine.verifyTableName(TELEMETRY);
                    TableToken telemetry_config = engine.verifyTableName(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME);
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, telemetry.getDirName()));
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, telemetry_config.getDirName()));
                }

                Misc.free(telemetryJob);
            }
        });
    }

    @Test
    public void testTelemetryDisabledByDefault() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(FF, path, root, TELEMETRY));
                Assert.assertEquals(TableUtils.TABLE_DOES_NOT_EXIST, TableUtils.exists(FF, path, root, TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME));
            }
        });
    }

    @Test
    public void testTelemetryStoresUpAndDownEvents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TelemetryJob telemetryJob = new TelemetryJob(engine);
                Misc.free(telemetryJob);
                refreshTablesInBaseEngine();

                final String expectedEvent = "100\n" +
                        "101\n";
                assertColumn(expectedEvent, 1);

                final String expectedOrigin = "1\n" +
                        "1\n";
                assertColumn(expectedOrigin, 2);
            }
        });
    }

    @Test
    public void testTelemetryUpdatesVersion() throws Exception {
        final AtomicReference<String> refVersion = new AtomicReference<>();
        BuildInformation buildInformation = new BuildInformation() {
            @Override
            public CharSequence getCommitHash() {
                return null;
            }

            @Override
            public CharSequence getJdkVersion() {
                return null;
            }

            @Override
            public CharSequence getQuestDbVersion() {
                return refVersion.get();
            }
        };
        CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public BuildInformation getBuildInformation() {
                return buildInformation;
            }
        };
        TestUtils.assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = new SqlCompiler(engine);
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                refVersion.set("1.0");
                TelemetryJob telemetryJob = new TelemetryJob(engine);
                String os = System.getProperty(TelemetryConfigLogger.OS_NAME);

                String expectedSql = "count\n1\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                expectedSql = "version\tos\n" +
                        "1.0\t" + os + "\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT version, os FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                Misc.free(telemetryJob);

                telemetryJob = new TelemetryJob(engine);
                expectedSql = "count\n1\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                Misc.free(telemetryJob);

                refVersion.set("1.1");
                telemetryJob = new TelemetryJob(engine);
                expectedSql = "count\n2\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                expectedSql = "version\tos\n" +
                        "1.1\t" + os + "\n";
                TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT version, os FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " LIMIT -1", sink, expectedSql);
                Misc.free(telemetryJob);
            }
        });
    }

    protected void assertColumn(CharSequence expected, int index) {
        try (TableReader reader = newTableReader(configuration, TELEMETRY)) {
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false, sink);
            TestUtils.assertEquals(expected, sink);
            reader.getCursor().toTop();
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false, sink);
            TestUtils.assertEquals(expected, sink);
        }
    }
}
