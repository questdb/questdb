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

package io.questdb.test.griffin;

import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.QueryAssertion;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.util.HashMap;
import java.util.function.Consumer;

import static io.questdb.test.griffin.AlterTableSetTypeTest.NON_WAL;
import static io.questdb.test.griffin.AlterTableSetTypeTest.WAL;
import static org.junit.Assert.*;

public class AlterTableSetTypeRestartTest extends AbstractAlterTableSetTypeRestartTest {

    @Override
    @Before
    public void setUp() {
        TestUtils.unchecked(() -> createDummyConfiguration(PropertyKey.CAIRO_WAL_SUPPORTED.getPropertyPath() + "=true"));
    }

    @Test
    @Ignore
    public void testConvertLoop2() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);

                // WAL table
                assertTrue(engine.isWalTable(token));
                setType(tableName, "BYPASS WAL");
                assertNumOfRows(engine, tableName, 3);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to non-WAL
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 3);
                setType(tableName, "WAL");
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 3);

                insertInto(tableName);
                drainWalQueue(engine);
                assertNumOfRows(engine, tableName, 4);

                dropTable(tableName);
            }
            validateShutdown(tableName);
        });
    }

    @Test
    public void testNonPartitionedToWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
                put(PropertyKey.CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createNonPartitionedTable(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // table conversion to WAL is not allowed
                try {
                    setType(tableName, "WAL");
                    fail("Expected exception is missing");
                } catch (PSQLException e) {
                    TestUtils.assertContains(e.getMessage(), "Cannot convert non-partitioned table");
                }
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // WAL table
                assertTrue(engine.isWalTable(token));
                assertSeqTxn(engine, token, 0);
                assertNumOfRows(engine, tableName, 2);
                assertConvertFileDoesNotExist(engine, token);

                dropTable(tableName);
            }
        });
    }

    @Test
    public void testNonWalToWalKeepsMetaHeaderFields() throws Exception {
        // SET TYPE WAL on a table that was never WAL, the usual way to move an old table to WAL. The conversion
        // resets the metadata version in _meta, and the TTL and Parquet encoding config that ALTERs set while
        // the table was non-WAL must survive it. Dedup and FORMAT PARQUET need a WAL table, so the test sets
        // neither before the conversion.
        final String walDdl = """
                ddl
                CREATE TABLE 'n' (\s
                \tts TIMESTAMP,
                \tv LONG PARQUET(delta_binary_packed, zstd(3))
                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS;
                """;
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                serverMain.execute("CREATE TABLE n (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                serverMain.execute("ALTER TABLE n SET TTL 2 DAYS");
                serverMain.execute("ALTER TABLE n ALTER COLUMN v SET PARQUET(delta_binary_packed, zstd(3))");
                serverMain.execute("INSERT INTO n VALUES ('2024-01-01T00:00:00.000000Z', 10)");
                assertShowCreateTable(
                        serverMain.getEngine(),
                        "n",
                        """
                                ddl
                                CREATE TABLE 'n' (\s
                                \tts TIMESTAMP,
                                \tv LONG PARQUET(delta_binary_packed, zstd(3))
                                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS BYPASS WAL;
                                """
                );
                serverMain.execute("ALTER TABLE n SET TYPE WAL");
            }

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                final TableToken tableToken = engine.verifyTableName("n");
                assertTrue(tableToken.isWal());
                assertShowCreateTable(engine, "n", walDdl);
                final int encodingConfig;
                try (TableReader reader = engine.getReader(tableToken)) {
                    encodingConfig = reader.getMetadata().getColumnMetadata(1).getParquetEncodingConfig();
                }
                assertNotEquals(0, encodingConfig);
                try (TableReaderMetadata initialMetadata = openInitialMetadata(engine, tableToken)) {
                    assertEquals(48, initialMetadata.getTtlHoursOrMonths());
                    assertEquals(encodingConfig, initialMetadata.getColumnMetadata(1).getParquetEncodingConfig());
                }
                serverMain.execute("INSERT INTO n VALUES ('2024-01-02T00:00:00.000000Z', 20)");
                drainWalQueue(engine);
                assertRows(
                        engine,
                        "n",
                        """
                                ts\tv
                                2024-01-01T00:00:00.000000Z\t10
                                2024-01-02T00:00:00.000000Z\t20
                                """
                );
            }

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                assertShowCreateTable(serverMain.getEngine(), "n", walDdl);
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWalWithDropTable() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);

                // drop table
                dropTable(tableName);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                try {
                    engine.verifyTableName(tableName);
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + tableName + ']');
                }
            }
        });
    }

    @Test
    @Ignore
    public void testNonWalToWalWithTxn() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                setSeqTxn(engine, token);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertSeqTxn(engine, token, 12345L);
                assertNumOfRows(engine, tableName, 3);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                assertSeqTxn(engine, token, 0L);

                drainWalQueue(engine);
                assertNumOfRows(engine, tableName, 6);
                assertSeqTxn(engine, token, 3L);
            }
        });
    }

    @Test
    @Ignore
    public void testSetType() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "BYPASS WAL");
                insertInto(tableName);

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // non-WAL table
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 4);

                // schedule table conversion back to non-WAL
                setType(tableName, "BYPASS WAL");
                drainWalQueue(engine);
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);

                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 5);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to non-WAL
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 5);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);

                // schedule table conversion to non-WAL again
                setType(tableName, "BYPASS WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // no conversion happened, table was already non-WAL type
                assertFalse(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to WAL
                setType(tableName, "WAL");
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, WAL);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                // table has been converted to WAL
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 6);
                assertConvertFileDoesNotExist(engine, token);

                insertInto(tableName);
                insertInto(tableName);
                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 9);

                dropTable(tableName);
                drainWalQueue(engine);
            }
            validateShutdown(tableName);
        });
    }

    @Test
    public void testWalConversionCompletesInterruptedMetaStamp() throws Exception {
        // The conversion to WAL resets the metadata version in _meta: it writes the checksum for the new version,
        // then the version. A process that dies between the two writes leaves the old version next to the new
        // checksum, which readers do not trust. The conversion runs again at the next startup and must complete
        // the stamp before it registers the table with a sequencer, or txn_seq/_meta.0, which replicas rebuild
        // _meta from, gets the defaults in place of the header fields.
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                serverMain.execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, k)");
                serverMain.execute("ALTER TABLE t SET TTL 2 DAYS");
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 10)");
                drainWalQueue(serverMain.getEngine());
                serverMain.execute("ALTER TABLE t SET TYPE BYPASS WAL");
            }

            final String dbRoot;
            final String dirName;
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                dbRoot = engine.getConfiguration().getDbRoot();
                dirName = engine.verifyTableName("t").getDirName();
                serverMain.execute("ALTER TABLE t SET TYPE WAL");
            }

            // the state that a process death between the checksum and the version write leaves behind
            editMeta(dbRoot, dirName, metaMem -> {
                metaMem.putInt(
                        TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                        Numbers.encodeLowHighShorts(
                                TableUtils.checksumForMetaFormatMinorVersionField(0, metaMem.getInt(TableUtils.META_OFFSET_COUNT)),
                                Numbers.decodeHighShort(metaMem.getInt(TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION))
                        )
                );
                assertFalse(TableUtils.isMetaFormatUpToDate(metaMem));
            });

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                final TableToken tableToken = engine.verifyTableName("t");
                assertTrue(tableToken.isWal());
                assertShowCreateTable(
                        engine,
                        "t",
                        """
                                ddl
                                CREATE TABLE 't' (\s
                                \tts TIMESTAMP,
                                \tk INT,
                                \tv LONG
                                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS
                                DEDUP SKIP KEYS(ts,k);
                                """
                );
                try (TableReaderMetadata initialMetadata = openInitialMetadata(engine, tableToken)) {
                    assertEquals(TableUtils.DEDUP_MODE_SKIP, initialMetadata.getDedupMode());
                    assertEquals(48, initialMetadata.getTtlHoursOrMonths());
                }
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 30)");
                drainWalQueue(engine);
                assertRows(
                        engine,
                        "t",
                        """
                                ts\tk\tv
                                2024-01-01T00:00:00.000000Z\t1\t10
                                """
                );
            }
        });
    }

    @Test
    public void testWalConversionKeepsOlderMetaFormatTrust() throws Exception {
        // A release that predates the dedup mode field writes minor version 2 into _meta and leaves the bytes of
        // the later dedup mode field as they were, here holding a stale SKIP. The conversion to WAL keeps the
        // stored minor version, so readers keep ignoring those bytes and the table stays UPSERT. It also keeps
        // the TTL, which minor version 2 covers.
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                serverMain.execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE t DEDUP ENABLE UPSERT KEYS(ts, k)");
                serverMain.execute("ALTER TABLE t SET TTL 2 DAYS");
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 10)");
                drainWalQueue(serverMain.getEngine());
                serverMain.execute("ALTER TABLE t SET TYPE BYPASS WAL");
            }

            final String dbRoot;
            final String dirName;
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                dbRoot = engine.getConfiguration().getDbRoot();
                dirName = engine.verifyTableName("t").getDirName();
                serverMain.execute("ALTER TABLE t SET TYPE WAL");
            }

            editMeta(dbRoot, dirName, metaMem -> {
                final short olderMinorVersion = TableUtils.META_FORMAT_MINOR_VERSION_DEDUP_MODE - 1;
                metaMem.putInt(
                        TableUtils.META_OFFSET_META_FORMAT_MINOR_VERSION,
                        Numbers.encodeLowHighShorts(
                                TableUtils.checksumForMetaFormatMinorVersionField(
                                        metaMem.getLong(TableUtils.META_OFFSET_METADATA_VERSION),
                                        metaMem.getInt(TableUtils.META_OFFSET_COUNT)
                                ),
                                olderMinorVersion
                        )
                );
                metaMem.putInt(TableUtils.META_OFFSET_DEDUP_MODE, TableUtils.DEDUP_MODE_SKIP);
                assertTrue(TableUtils.hasParquetEncodingConfig(metaMem));
                assertFalse(TableUtils.isMetaFormatUpToDate(metaMem));
            });

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                assertTrue(engine.verifyTableName("t").isWal());
                assertShowCreateTable(
                        engine,
                        "t",
                        """
                                ddl
                                CREATE TABLE 't' (\s
                                \tts TIMESTAMP,
                                \tk INT,
                                \tv LONG
                                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS
                                DEDUP UPSERT KEYS(ts,k);
                                """
                );
                // UPSERT replaces the stored row
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 30)");
                drainWalQueue(engine);
                assertRows(
                        engine,
                        "t",
                        """
                                ts\tk\tv
                                2024-01-01T00:00:00.000000Z\t1\t30
                                """
                );
            }
        });
    }

    @Test
    public void testWalRoundTripKeepsMetaHeaderFields() throws Exception {
        // SET TYPE BYPASS WAL followed by SET TYPE WAL is the way out of a WAL transaction that keeps
        // failing. Converting back to WAL resets the metadata version in _meta. The header fields that the
        // Metadata Format Minor Version checksum guards (dedup mode, TTL, table format, Parquet encoding
        // config) must survive the reset. The ALTERs on t and p take the metadata version above zero, so a
        // reset that left the checksum as it was made readers fall back to the defaults. Table c gets DEDUP
        // SKIP from CREATE TABLE and no ALTER touches it, so it stays at metadata version zero. TableConverter
        // runs at startup, so each conversion step needs a real restart.
        final String tDdl = """
                ddl
                CREATE TABLE 't' (\s
                \tts TIMESTAMP,
                \tk INT,
                \tv LONG PARQUET(delta_binary_packed, zstd(3))
                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS
                DEDUP SKIP KEYS(ts,k);
                """;
        final String pDdl = """
                ddl
                CREATE TABLE 'p' (\s
                \tts TIMESTAMP,
                \tv LONG
                ) timestamp(ts) PARTITION BY DAY FORMAT PARQUET;
                """;
        final String cDdl = """
                ddl
                CREATE TABLE 'c' (\s
                \tts TIMESTAMP,
                \tk INT,
                \tv LONG
                ) timestamp(ts) PARTITION BY DAY
                DEDUP SKIP KEYS(ts,k);
                """;
        assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                serverMain.execute("CREATE TABLE t (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE t DEDUP ENABLE SKIP KEYS(ts, k)");
                serverMain.execute("ALTER TABLE t SET TTL 2 DAYS");
                serverMain.execute("ALTER TABLE t ALTER COLUMN v SET PARQUET(delta_binary_packed, zstd(3))");
                serverMain.execute("CREATE TABLE p (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("ALTER TABLE p SET FORMAT PARQUET");
                serverMain.execute("CREATE TABLE c (ts TIMESTAMP, k INT, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP SKIP KEYS(ts, k)");
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 10)");
                serverMain.execute("INSERT INTO c VALUES ('2024-01-01T00:00:00.000000Z', 1, 10)");
                final CairoEngine engine = serverMain.getEngine();
                drainWalQueue(engine);
                assertShowCreateTable(engine, "t", tDdl);
                assertShowCreateTable(engine, "p", pDdl);
                assertShowCreateTable(engine, "c", cDdl);
                serverMain.execute("ALTER TABLE t SET TYPE BYPASS WAL");
                serverMain.execute("ALTER TABLE p SET TYPE BYPASS WAL");
                serverMain.execute("ALTER TABLE c SET TYPE BYPASS WAL");
            }

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                assertFalse(engine.verifyTableName("t").isWal());
                assertFalse(engine.verifyTableName("p").isWal());
                assertFalse(engine.verifyTableName("c").isWal());
                // the conversion to non-WAL leaves the metadata version as it is
                assertShowCreateTable(
                        engine,
                        "t",
                        """
                                ddl
                                CREATE TABLE 't' (\s
                                \tts TIMESTAMP,
                                \tk INT,
                                \tv LONG PARQUET(delta_binary_packed, zstd(3))
                                ) timestamp(ts) PARTITION BY DAY TTL 2 DAYS BYPASS WAL
                                DEDUP SKIP KEYS(ts,k);
                                """
                );
                assertShowCreateTable(
                        engine,
                        "p",
                        """
                                ddl
                                CREATE TABLE 'p' (\s
                                \tts TIMESTAMP,
                                \tv LONG
                                ) timestamp(ts) PARTITION BY DAY FORMAT PARQUET BYPASS WAL;
                                """
                );
                serverMain.execute("ALTER TABLE t SET TYPE WAL");
                serverMain.execute("ALTER TABLE p SET TYPE WAL");
                serverMain.execute("ALTER TABLE c SET TYPE WAL");
            }

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                assertTrue(engine.verifyTableName("t").isWal());
                assertTrue(engine.verifyTableName("p").isWal());
                assertTrue(engine.verifyTableName("c").isWal());
                assertShowCreateTable(engine, "t", tDdl);
                assertShowCreateTable(engine, "p", pDdl);
                assertShowCreateTable(engine, "c", cDdl);

                // SKIP keeps the stored row when a row with the same dedup key values arrives again
                serverMain.execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1, 30)");
                serverMain.execute("INSERT INTO c VALUES ('2024-01-01T00:00:00.000000Z', 1, 30)");
                drainWalQueue(engine);
                final String expectedRows = """
                        ts\tk\tv
                        2024-01-01T00:00:00.000000Z\t1\t10
                        """;
                assertRows(engine, "t", expectedRows);
                assertRows(engine, "c", expectedRows);
            }

            try (TestServerMain serverMain = startWithWalApplyDisabled()) {
                final CairoEngine engine = serverMain.getEngine();
                assertShowCreateTable(engine, "t", tDdl);
                assertShowCreateTable(engine, "p", pDdl);
                assertShowCreateTable(engine, "c", cDdl);
            }
        });
    }

    @Test
    @Ignore
    public void testWalToNonWal() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                insertInto(tableName);
                drainWalQueue(engine);

                // WAL table
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to non-WAL
                setType(tableName, "BYPASS WAL");
                drainWalQueue(engine);
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);

                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);
                assertFalse(engine.isWalTable(token));
                assertSeqTxn(engine, token, 0);
                insertInto(tableName);
                assertNumOfRows(engine, tableName, 3);

                dropTable(tableName);
            }
        });
    }

    @Test
    @Ignore
    public void testWalToNonWalWithDropTable() throws Exception {
        final String tableName = testName.getMethodName();
        TestUtils.assertMemoryLeak(() -> {
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();
                createTable(tableName, "WAL");

                final CairoEngine engine = serverMain.getEngine();
                final TableToken token = engine.verifyTableName(tableName);

                insertInto(tableName);
                drainWalQueue(engine);

                // WAL table
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 1);
                assertConvertFileDoesNotExist(engine, token);

                // schedule table conversion to non-WAL
                setType(tableName, "BYPASS WAL");
                drainWalQueue(engine);
                final Path path = assertConvertFileExists(engine, token);
                assertConvertFileContent(path, NON_WAL);

                insertInto(tableName);
                drainWalQueue(engine);
                assertTrue(engine.isWalTable(token));
                assertNumOfRows(engine, tableName, 2);

                // drop table
                dropTable(tableName);
                drainWalQueue(engine);
            }
            validateShutdown(tableName);

            // restart
            try (final ServerMain serverMain = ServerMain.create(root, new HashMap<>() {{
                put(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
            }})) {
                serverMain.start();

                final CairoEngine engine = serverMain.getEngine();
                try {
                    engine.verifyTableName(tableName);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "table does not exist [table=" + tableName + ']');
                }
            }
        });
    }

    // The memory usage check reads process-wide counters per memory tag, which the threads of a running
    // server change on their own (e.g. NATIVE_HTTP_CONN when the HTTP server sets up connections), so
    // the assertions against a live server skip it.
    private static void assertRows(CairoEngine engine, String tableName, String expected) throws Exception {
        try (SqlExecutionContext context = createSqlExecutionContext(engine)) {
            new QueryAssertion(engine, context, () -> {
            }, tableName)
                    .noLeakCheck()
                    .noMemoryUsageCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(expected);
        }
    }

    private static void assertShowCreateTable(CairoEngine engine, String tableName, String expectedDdl) throws Exception {
        try (SqlExecutionContext context = createSqlExecutionContext(engine)) {
            new QueryAssertion(engine, context, () -> {
            }, "SHOW CREATE TABLE " + tableName)
                    .noLeakCheck()
                    .noMemoryUsageCheck()
                    .noRandomAccess()
                    .returns(expectedDdl);
        }
    }

    // Opens the _meta file of a table while no server runs and lets the editor change it in place.
    private static void editMeta(CharSequence dbRoot, CharSequence dirName, Consumer<MemoryCMARW> editor) {
        try (
                Path path = new Path();
                MemoryCMARW metaMem = Vm.getCMARWInstance()
        ) {
            metaMem.smallFile(
                    TestFilesFacadeImpl.INSTANCE,
                    path.of(dbRoot).concat(dirName).concat(TableUtils.META_FILE_NAME).$(),
                    MemoryTag.MMAP_DEFAULT
            );
            editor.accept(metaMem);
        }
    }

    // Opens txn_seq/_meta.0, the sequencer's initial metadata that a replica rebuilds the table's _meta from.
    private static TableReaderMetadata openInitialMetadata(CairoEngine engine, TableToken tableToken) {
        final CairoConfiguration configuration = engine.getConfiguration();
        final TableReaderMetadata metadata = new TableReaderMetadata(configuration);
        try (Path path = new Path()) {
            metadata.loadMetadata(
                    path.of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat(WalUtils.SEQ_DIR)
                            .concat(WalUtils.INITIAL_META_FILE_NAME)
                            .$()
            );
            return metadata;
        } catch (Throwable th) {
            Misc.free(metadata);
            throw th;
        }
    }

    private static TestServerMain startWithWalApplyDisabled() {
        return startWithEnvVariables(PropertyKey.CAIRO_WAL_APPLY_ENABLED.getEnvVarName(), "false");
    }
}
