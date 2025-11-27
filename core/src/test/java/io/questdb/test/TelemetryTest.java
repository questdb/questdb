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

package io.questdb.test;

import io.questdb.BuildInformation;
import io.questdb.DefaultTelemetryConfiguration;
import io.questdb.Telemetry;
import io.questdb.TelemetryConfigLogger;
import io.questdb.TelemetryConfiguration;
import io.questdb.TelemetryJob;
import io.questdb.TelemetrySystemEvent;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.tasks.AbstractTelemetryTask;
import io.questdb.tasks.TelemetryTask;
import io.questdb.tasks.TelemetryWalTask;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.cairo.TestTableReaderRecordCursor;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE;
import static io.questdb.TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_UNKNOWN;
import static io.questdb.test.TelemetryTest.DBSizeTestType.*;

public class TelemetryTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;
    private static final String TELEMETRY = TelemetryTask.TABLE_NAME;

    @Test
    public void testTelemetryConfigUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " (id long256, enabled boolean)");
            execute("INSERT INTO " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " values(CAST('0x01' AS LONG256), true)");

            try (TelemetryJob ignore = new TelemetryJob(engine)) {
                String expected = """
                        column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey
                        id\tLONG256\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                        enabled\tBOOLEAN\tfalse\t0\tfalse\t0\t0\tfalse\tfalse
                        version\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                        os\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                        package\tSYMBOL\tfalse\t256\ttrue\t128\t0\tfalse\tfalse
                        instance_name\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                        instance_type\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                        instance_desc\tSYMBOL\tfalse\t256\ttrue\t128\t1\tfalse\tfalse
                        """;
                assertSql(expected, "SHOW COLUMNS FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME);
                expected = """
                        id\tversion
                        0x01\t
                        0x01\t[DEVELOPMENT]
                        """;
                assertSql(expected, "SELECT id, version FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME);
            }
        });
    }

    @Test
    public void testTelemetryCreatesTablesWhenEnabled() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                try (
                        TelemetryJob ignore = new TelemetryJob(engine);
                        Path path = new Path()
                ) {
                    TableToken telemetry = engine.verifyTableName(TELEMETRY);
                    TableToken telemetry_config = engine.verifyTableName(TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME);
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, telemetry.getDirName()));
                    Assert.assertEquals(TableUtils.TABLE_EXISTS, TableUtils.exists(FF, path, root, telemetry_config.getDirName()));
                }
            }
        });
    }

    @Test
    public void testTelemetryDBSize() throws Exception {
        testTelemetryDBSize(SUCCESS);
    }

    @Test
    public void testTelemetryDBSizeError() throws Exception {
        testTelemetryDBSize(ERROR);
    }

    @Test
    public void testTelemetryDBSizeTimeout() throws Exception {
        testTelemetryDBSize(TIMEOUT);
    }

    @Test
    public void testTelemetryDoesntCreateTableWhenDisabled() throws Exception {
        final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull TelemetryConfiguration getTelemetryConfiguration() {
                return new DefaultTelemetryConfiguration() {
                    @Override
                    public boolean getEnabled() {
                        return false;
                    }
                };
            }
        };

        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    TelemetryJob ignored = new TelemetryJob(engine)
            ) {
                assertException(
                        "drop table telemetry",
                        11,
                        "table does not exist [table=" + TELEMETRY + "]"
                );
            }
        });
    }

    @Test
    public void testTelemetryStoresNonEvents() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TelemetryJob telemetryJob = new TelemetryJob(engine);
                Misc.free(telemetryJob);
                refreshTablesInBaseEngine();

                HashSet<Short> expectedClasses = new HashSet<>();
                expectedClasses.add(TelemetrySystemEvent.SYSTEM_OS_CLASS_BASE);
                expectedClasses.add(TelemetrySystemEvent.SYSTEM_ENV_TYPE_BASE);
                expectedClasses.add(TelemetrySystemEvent.SYSTEM_CPU_CLASS_BASE);
                expectedClasses.add(TelemetrySystemEvent.SYSTEM_DB_SIZE_CLASS_BASE);
                expectedClasses.add(TelemetrySystemEvent.SYSTEM_TABLE_COUNT_CLASS_BASE);

                HashSet<Short> actualClasses = new HashSet<>();
                try (
                        TableReader reader = newOffPoolReader(configuration, TELEMETRY);
                        TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
                ) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        final short event = record.getShort(1);
                        if (event >= 0) {
                            continue; // skip event entries
                        }
                        actualClasses.add((short) (event - event % 10));
                    }
                }

                Assert.assertEquals(expectedClasses, actualClasses);
            }
        });
    }

    @Test
    public void testTelemetryStoresUpAndDownEvents() throws Exception {
        assertMemoryLeak(() -> {
            try (CairoEngine engine = new CairoEngine(configuration)) {
                TelemetryJob telemetryJob = new TelemetryJob(engine);
                Misc.free(telemetryJob);
                refreshTablesInBaseEngine();

                final String expectedEvent = """
                        100\t1
                        101\t1
                        """;
                assertEventAndOrigin(expectedEvent);
            }
        });
    }

    @Test
    public void testTelemetryTableUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + TelemetryTask.TABLE_NAME + " (" +
                    "created TIMESTAMP, " +
                    "event SHORT, " +
                    "origin SHORT" +
                    ") TIMESTAMP(created)");

            String showCreateTable = "SHOW CREATE TABLE " + TelemetryTask.TABLE_NAME;
            String start = "ddl\n" +
                    "CREATE TABLE '" + TelemetryTask.TABLE_NAME + "' ( \n" +
                    "\tcreated TIMESTAMP,\n" +
                    "\tevent SHORT,\n" +
                    "\torigin SHORT\n" +
                    ") timestamp(created)";
            String middle = " PARTITION BY NONE";
            String end = " BYPASS WAL\nWITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n";

            assertSql(start + middle + end, showCreateTable);
            try (TelemetryJob ignore = new TelemetryJob(engine)) {
                assertSql(start + " PARTITION BY DAY TTL 1 WEEK" + end, showCreateTable);
            }
        });
    }

    @Test
    public void testTelemetryUpdatesVersion() throws Exception {
        final AtomicReference<String> refVersion = new AtomicReference<>();
        final CairoConfiguration configuration = getCairoConfiguration(refVersion);

        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine)
            ) {
                String os = System.getProperty(TelemetryConfigLogger.OS_NAME);
                refVersion.set("1.0");

                try (TelemetryJob ignore = new TelemetryJob(engine)) {
                    String expectedSql = "count\n1\n";
                    TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                    expectedSql = "version\tos\n" +
                            "1.0\t" + os + "\n";
                    TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT version, os FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                }

                try (TelemetryJob ignore = new TelemetryJob(engine)) {
                    String expectedSql = "count\n1\n";
                    TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                }

                refVersion.set("1.1");
                try (TelemetryJob ignore = new TelemetryJob(engine)) {
                    String expectedSql = "count\n2\n";
                    TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT count(*) FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME, sink, expectedSql);
                    expectedSql = "version\tos\n" +
                            "1.1\t" + os + "\n";
                    TestUtils.assertSql(compiler, sqlExecutionContext, "SELECT version, os FROM " + TelemetryConfigLogger.TELEMETRY_CONFIG_TABLE_NAME + " LIMIT -1", sink, expectedSql);
                }
            }
        });
    }

    @Test
    public void testTelemetryWalTableUpgrade() throws Exception {
        String tableName = configuration.getSystemTableNamePrefix() + TelemetryWalTask.TABLE_NAME;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE '" + tableName + "' (" +
                    "created TIMESTAMP, " +
                    "event SHORT, " +
                    "tableId INT, " +
                    "walId INT, " +
                    "seqTxn LONG, " +
                    "rowCount LONG," +
                    "physicalRowCount LONG," +
                    "latency FLOAT" +
                    ") TIMESTAMP(created) PARTITION BY MONTH BYPASS WAL");

            String showCreateTable = "SHOW CREATE TABLE '" + tableName + "'";
            String start = "ddl\n" +
                    "CREATE TABLE '" + tableName + "' ( \n" +
                    "\tcreated TIMESTAMP,\n" +
                    "\tevent SHORT,\n" +
                    "\ttableId INT,\n" +
                    "\twalId INT,\n" +
                    "\tseqTxn LONG,\n" +
                    "\trowCount LONG,\n" +
                    "\tphysicalRowCount LONG,\n" +
                    "\tlatency FLOAT\n" +
                    ") timestamp(created)";
            String midOld = " PARTITION BY MONTH";
            String midNew = " PARTITION BY DAY TTL 1 WEEK";
            String end = " BYPASS WAL\nWITH maxUncommittedRows=1000, o3MaxLag=300000000us;\n";

            assertSql(start + midOld + end, showCreateTable);
            try (TelemetryJob ignore = new TelemetryJob(engine)) {
                assertSql(start + midNew + end, showCreateTable);
            }
        });
    }

    private static @NotNull CairoConfiguration getCairoConfiguration(AtomicReference<String> refVersion) {
        final BuildInformation buildInformation = new BuildInformation() {
            @Override
            public String getCommitHash() {
                return null;
            }

            @Override
            public String getJdkVersion() {
                return null;
            }

            @Override
            public String getSwName() {
                return null;
            }

            @Override
            public String getSwVersion() {
                return refVersion.get();
            }
        };
        return new DefaultTestCairoConfiguration(root) {
            @Override
            public @NotNull BuildInformation getBuildInformation() {
                return buildInformation;
            }
        };
    }

    @SuppressWarnings("SameParameterValue")
    private void assertEventAndOrigin(CharSequence expected) {
        try (
                TableReader reader = newOffPoolReader(configuration, TELEMETRY);
                TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
        ) {
            sink.clear();
            printEventAndOrigin(cursor, reader.getMetadata());
            TestUtils.assertEquals(expected, sink);
            cursor.toTop();
            sink.clear();
            printEventAndOrigin(cursor, reader.getMetadata());
            TestUtils.assertEquals(expected, sink);
        }
    }

    private void printEventAndOrigin(RecordCursor cursor, RecordMetadata metadata) {
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            final short event = record.getShort(1);
            if (event < 0) {
                continue; // skip non-event entries
            }
            CursorPrinter.printColumn(record, metadata, 1, sink, false);
            sink.put('\t');
            CursorPrinter.printColumn(record, metadata, 2, sink, false);
            sink.put('\n');
        }
    }

    private void testTelemetryDBSize(DBSizeTestType type) throws Exception {
        assertMemoryLeak(() -> {
            try (
                    CairoEngine engine = new CairoEngine(configuration) {
                        @Override
                        protected @NotNull <T extends AbstractTelemetryTask> Telemetry<T> createTelemetry(
                                Telemetry.TelemetryTypeBuilder<T> builder, CairoConfiguration configuration
                        ) {
                            return new Telemetry<>(builder, configuration) {
                                @Override
                                protected boolean hasTimedOut() {
                                    if (type == ERROR) {
                                        throw new RuntimeException("test");
                                    } else {
                                        return type == TIMEOUT;
                                    }
                                }
                            };
                        }
                    };
                    TelemetryJob ignored = new TelemetryJob(engine);
                    SqlCompiler compiler = engine.getSqlCompiler();
                    SqlExecutionContext context = new SqlExecutionContextImpl(engine, 1)
            ) {
                TestUtils.printSql(compiler, context, "select event, origin from " + TELEMETRY, sink);
                TestUtils.assertContains(
                        sink,
                        (type == SUCCESS ? SYSTEM_DB_SIZE_CLASS_BASE : SYSTEM_DB_SIZE_CLASS_UNKNOWN) + "\t1\n"
                );
            }
        });
    }

    enum DBSizeTestType {
        SUCCESS, TIMEOUT, ERROR
    }
}
