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

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.questdb.test.tools.TestUtils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertTrue;

public class ServerMainTest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testPgWirePort() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {

                try {
                    serverMain.getPgWireServerPort();
                    Assert.fail();
                } catch (CairoException ex) {
                    TestUtils.assertContains(ex.getFlyweightMessage(), "pgwire server is not running");
                }

                serverMain.start();
                int port = serverMain.getPgWireServerPort();
                Assert.assertTrue(port > 0);
            }
        });
    }

    @Test
    public void testServerMainNoRestart() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();
                serverMain.start(); // <== no effect
                serverMain.close();
                try {
                    serverMain.getEngine();
                } catch (IllegalStateException ex) {
                    assertContains(ex.getMessage(), "close was called");
                }
                try {
                    serverMain.getWorkerPoolManager();
                } catch (IllegalStateException ex) {
                    assertContains(ex.getMessage(), "close was called");
                }
                serverMain.start(); // <== no effect
                serverMain.close(); // <== no effect
                serverMain.start(); // <== no effect
            }
        });
    }

    @Test
    public void testServerMainNoStart() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain ignore = new ServerMain(getServerMainArgs())) {
                Os.pause();
            }
        });
    }

    @Test
    public void testServerMainPgWire() throws Exception {
        try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
            serverMain.start();
            try (Connection ignored = DriverManager.getConnection(PG_CONNECTION_URI, PG_CONNECTION_PROPERTIES)) {
                Os.pause();
            }
        }
    }

    @Test
    public void testServerMainStart() throws Exception {
        assertMemoryLeak(() -> {
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                Assert.assertNotNull(serverMain.getConfiguration());
                final CairoConfiguration cairoConf = serverMain.getConfiguration().getCairoConfiguration();
                Assert.assertEquals(cairoConf.getInstallRoot(), root);
                Assert.assertEquals(cairoConf.getDbRoot(), new java.io.File(root, "db").getAbsolutePath());
                Assert.assertNotNull(serverMain.getEngine());
                Assert.assertNull(serverMain.getWorkerPoolManager());
                Assert.assertFalse(serverMain.hasStarted());
                Assert.assertFalse(serverMain.hasBeenClosed());
                serverMain.start();
                Assert.assertNotNull(serverMain.getWorkerPoolManager());
            }
        });
    }

    @Test
    public void testServerMainStartHttpDisabled() throws Exception {
        assertMemoryLeak(() -> {
            Map<String, String> env = new HashMap<>(System.getenv());
            env.put(PropertyKey.HTTP_ENABLED.getEnvVarName(), "false");
            Bootstrap bootstrap = new Bootstrap(
                    new DefaultBootstrapConfiguration() {
                        @Override
                        public Map<String, String> getEnv() {
                            return env;
                        }
                    },
                    getServerMainArgs()
            );
            try (final ServerMain serverMain = new ServerMain(bootstrap)) {
                Assert.assertFalse(serverMain.getConfiguration().getHttpServerConfiguration().isEnabled());
                serverMain.start();
            }
        });
    }

    @Test
    public void testServerUpgradeDoesNotOverrideWebConsoleConfig() throws Exception {
        assertMemoryLeak(() -> {
            String nonDefaultConfig = """
                    {
                      "ctaBanner": true,
                      "readOnly": true,
                      "savedQueries": []
                    }
                    """;

            File publicDir = new File(root, "public");
            File consoleFile = new File(new File(publicDir, "assets"), "console-configuration.json");
            File versionFile = new File(publicDir, "version.txt");
            File indexFile = new File(publicDir, "index.html");
            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                // test sanity check: our new config differs from the default one
                Assert.assertNotEquals(nonDefaultConfig, readStringFromFile(consoleFile));
            }

            // mangle the version file -> this is to force web console upgrade on the next server start
            writeStringToFile(versionFile, "0"); // timestamp 0 -> everything is newer than this

            // change console config to a non-default value
            writeStringToFile(consoleFile, nonDefaultConfig);

            // delete index file - after the rest we uses the index file absence or presence
            // to determine if the web console upgrade procedure was triggered
            Assert.assertTrue(indexFile.delete());

            try (final ServerMain serverMain = new ServerMain(getServerMainArgs())) {
                serverMain.start();

                // sanity check: we deleted the index file in a previous step
                // if it exists then it means the web console upgrade procedure was triggered
                Assert.assertTrue(consoleFile.exists());

                // check that our config is still there and was not overridden by the upgrade procedure
                Assert.assertEquals(nonDefaultConfig, readStringFromFile(consoleFile));
            }
        });
    }

    @Test
    public void testShowParameters() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    final ServerMain serverMain = new ServerMain(getServerMainArgs());
                    final SqlExecutionContext executionContext = new SqlExecutionContextImpl(serverMain.getEngine(), 0)
            ) {
                serverMain.start();

                try (SqlCompiler compiler = serverMain.getEngine().getSqlCompiler()) {
                    final StringSink actualSink = new StringSink();
                    printSql(
                            compiler,
                            executionContext,
                            "(show parameters) where property_path not in (" +
                                    "'cairo.root', 'cairo.sql.backup.root', 'cairo.sql.copy.root', 'cairo.sql.copy.work.root', " +
                                    "'cairo.writer.misc.append.page.size', 'line.tcp.io.worker.count', 'cairo.sql.copy.export.root', " +
                                    "'wal.apply.worker.count', 'export.worker.count', 'mat.view.refresh.worker.count'," +
                                    "'http.export.connection.limit'" +
                                    ") order by 1",
                            actualSink
                    );
                    final Set<String> actualProps = new HashSet<>(asList(actualSink.toString().split("\n")));

                    final String[] expectedProps = (
                            "property_path\tenv_var_name\tvalue\tvalue_source\tsensitive\treloadable\n" +
                                    "binarydata.encoding.maxlength\tQDB_BINARYDATA_ENCODING_MAXLENGTH\t32768\tdefault\tfalse\tfalse\n" +
                                    "cairo.attach.partition.copy\tQDB_CAIRO_ATTACH_PARTITION_COPY\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.attach.partition.suffix\tQDB_CAIRO_ATTACH_PARTITION_SUFFIX\t.attachable\tdefault\tfalse\tfalse\n" +
                                    "cairo.character.store.capacity\tQDB_CAIRO_CHARACTER_STORE_CAPACITY\t1024\tdefault\tfalse\tfalse\n" +
                                    "cairo.character.store.sequence.pool.capacity\tQDB_CAIRO_CHARACTER_STORE_SEQUENCE_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.column.indexer.queue.capacity\tQDB_CAIRO_COLUMN_INDEXER_QUEUE_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.column.pool.capacity\tQDB_CAIRO_COLUMN_POOL_CAPACITY\t4096\tdefault\tfalse\tfalse\n" +
                                    "cairo.commit.lag\tQDB_CAIRO_COMMIT_LAG\t600000\tdefault\tfalse\tfalse\n" +
                                    "cairo.commit.mode\tQDB_CAIRO_COMMIT_MODE\tnosync\tdefault\tfalse\tfalse\n" +
                                    "cairo.create.as.select.retry.count\tQDB_CAIRO_CREATE_AS_SELECT_RETRY_COUNT\t5\tdefault\tfalse\tfalse\n" +
                                    "cairo.date.locale\tQDB_CAIRO_DATE_LOCALE\ten\tdefault\tfalse\tfalse\n" +
                                    "cairo.default.sequencer.part.txn.count\tQDB_CAIRO_DEFAULT_SEQUENCER_PART_TXN_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "cairo.default.symbol.cache.flag\tQDB_CAIRO_DEFAULT_SYMBOL_CACHE_FLAG\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.default.symbol.capacity\tQDB_CAIRO_DEFAULT_SYMBOL_CAPACITY\t256\tdefault\tfalse\tfalse\n" +
                                    "cairo.detached.mkdir.mode\tQDB_CAIRO_DETACHED_MKDIR_MODE\t509\tdefault\tfalse\tfalse\n" +
                                    "dev.mode.enabled\tQDB_DEV_MODE_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.expression.pool.capacity\tQDB_CAIRO_EXPRESSION_POOL_CAPACITY\t8192\tdefault\tfalse\tfalse\n" +
                                    "cairo.fast.map.load.factor\tQDB_CAIRO_FAST_MAP_LOAD_FACTOR\t0.7\tdefault\tfalse\tfalse\n" +
                                    "cairo.file.operation.retry.count\tQDB_CAIRO_FILE_OPERATION_RETRY_COUNT\t30\tdefault\tfalse\tfalse\n" +
                                    "cairo.id.generator.batch.step\tQDB_CAIRO_ID_GENERATOR_BATCH_STEP\t512\tdefault\tfalse\tfalse\n" +
                                    "cairo.idle.check.interval\tQDB_CAIRO_IDLE_CHECK_INTERVAL\t300000\tdefault\tfalse\tfalse\n" +
                                    "cairo.inactive.reader.max.open.partitions\tQDB_CAIRO_INACTIVE_READER_MAX_OPEN_PARTITIONS\t10000\tdefault\tfalse\tfalse\n" +
                                    "cairo.inactive.reader.ttl\tQDB_CAIRO_INACTIVE_READER_TTL\t120000\tdefault\tfalse\tfalse\n" +
                                    "cairo.inactive.writer.ttl\tQDB_CAIRO_INACTIVE_WRITER_TTL\t600000\tdefault\tfalse\tfalse\n" +
                                    "cairo.index.value.block.size\tQDB_CAIRO_INDEX_VALUE_BLOCK_SIZE\t256\tdefault\tfalse\tfalse\n" +
                                    "cairo.iouring.enabled\tQDB_CAIRO_IOURING_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.latestby.queue.capacity\tQDB_CAIRO_LATESTBY_QUEUE_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.legacy.string.column.type.default\tQDB_CAIRO_LEGACY_STRING_COLUMN_TYPE_DEFAULT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.lexer.pool.capacity\tQDB_CAIRO_LEXER_POOL_CAPACITY\t2048\tdefault\tfalse\tfalse\n" +
                                    "cairo.max.crash.files\tQDB_CAIRO_MAX_CRASH_FILES\t100\tdefault\tfalse\tfalse\n" +
                                    "cairo.max.file.name.length\tQDB_CAIRO_MAX_FILE_NAME_LENGTH\t127\tdefault\tfalse\tfalse\n" +
                                    "cairo.max.swap.file.count\tQDB_CAIRO_MAX_SWAP_FILE_COUNT\t30\tdefault\tfalse\tfalse\n" +
                                    "cairo.max.uncommitted.rows\tQDB_CAIRO_MAX_UNCOMMITTED_ROWS\t500000\tdefault\tfalse\tfalse\n" +
                                    "cairo.mkdir.mode\tQDB_CAIRO_MKDIR_MODE\t509\tdefault\tfalse\tfalse\n" +
                                    "cairo.model.pool.capacity\tQDB_CAIRO_MODEL_POOL_CAPACITY\t1024\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.callback.queue.capacity\tQDB_CAIRO_O3_CALLBACK_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.column.memory.size\tQDB_CAIRO_O3_COLUMN_MEMORY_SIZE\t8388608\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.copy.queue.capacity\tQDB_CAIRO_O3_COPY_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.lag.calculation.windows.size\tQDB_CAIRO_O3_LAG_CALCULATION_WINDOWS_SIZE\t4\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.last.partition.max.splits\tQDB_CAIRO_O3_LAST_PARTITION_MAX_SPLITS\t20\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.max.lag\tQDB_CAIRO_O3_MAX_LAG\t600000\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.min.lag\tQDB_CAIRO_O3_MIN_LAG\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.open.column.queue.capacity\tQDB_CAIRO_O3_OPEN_COLUMN_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.partition.purge.list.initial.capacity\tQDB_CAIRO_O3_PARTITION_PURGE_LIST_INITIAL_CAPACITY\t1\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.partition.queue.capacity\tQDB_CAIRO_O3_PARTITION_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.partition.split.min.size\tQDB_CAIRO_O3_PARTITION_SPLIT_MIN_SIZE\t52428800\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.purge.discovery.queue.capacity\tQDB_CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.quicksort.enabled\tQDB_CAIRO_O3_QUICKSORT_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.txn.scoreboard.entry.count\tQDB_CAIRO_O3_TXN_SCOREBOARD_ENTRY_COUNT\t16384\tdefault\tfalse\tfalse\n" +
                                    "cairo.page.frame.column.list.capacity\tQDB_CAIRO_PAGE_FRAME_COLUMN_LIST_CAPACITY\t16\tdefault\tfalse\tfalse\n" +
                                    "cairo.page.frame.reduce.queue.capacity\tQDB_CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY\t4\tdefault\tfalse\tfalse\n" +
                                    "cairo.page.frame.rowid.list.capacity\tQDB_CAIRO_PAGE_FRAME_ROWID_LIST_CAPACITY\t256\tdefault\tfalse\tfalse\n" +
                                    "cairo.page.frame.shard.count\tQDB_CAIRO_PAGE_FRAME_SHARD_COUNT\t2\tdefault\tfalse\tfalse\n" +
                                    "cairo.parallel.index.threshold\tQDB_CAIRO_PARALLEL_INDEX_THRESHOLD\t100000\tdefault\tfalse\tfalse\n" +
                                    "cairo.parallel.indexing.enabled\tQDB_CAIRO_PARALLEL_INDEXING_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.query.cache.event.queue.capacity\tQDB_CAIRO_QUERY_CACHE_EVENT_QUEUE_CAPACITY\t4\tdefault\tfalse\tfalse\n" +
                                    "cairo.reader.pool.max.segments\tQDB_CAIRO_READER_POOL_MAX_SEGMENTS\t10\tdefault\tfalse\tfalse\n" +
                                    "cairo.repeat.migration.from.version\tQDB_CAIRO_REPEAT_MIGRATION_FROM_VERSION\t426\tdefault\tfalse\tfalse\n" +
                                    "cairo.rnd.memory.max.pages\tQDB_CAIRO_RND_MEMORY_MAX_PAGES\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.rnd.memory.page.size\tQDB_CAIRO_RND_MEMORY_PAGE_SIZE\t8192\tdefault\tfalse\tfalse\n" +
                                    "cairo.snapshot.instance.id\tQDB_CAIRO_SNAPSHOT_INSTANCE_ID\t\tdefault\tfalse\tfalse\n" +
                                    "cairo.snapshot.recovery.enabled\tQDB_CAIRO_SNAPSHOT_RECOVERY_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.spin.lock.timeout\tQDB_CAIRO_SPIN_LOCK_TIMEOUT\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.column.pool.capacity\tQDB_CAIRO_SQL_ANALYTIC_COLUMN_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.initial.range.buffer.size\tQDB_CAIRO_SQL_ANALYTIC_INITIAL_RANGE_BUFFER_SIZE\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.rowid.max.pages\tQDB_CAIRO_SQL_ANALYTIC_ROWID_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.rowid.page.size\tQDB_CAIRO_SQL_ANALYTIC_ROWID_PAGE_SIZE\t524288\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.store.max.pages\tQDB_CAIRO_SQL_ANALYTIC_STORE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.store.page.size\tQDB_CAIRO_SQL_ANALYTIC_STORE_PAGE_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.tree.max.pages\tQDB_CAIRO_SQL_ANALYTIC_TREE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.analytic.tree.page.size\tQDB_CAIRO_SQL_ANALYTIC_TREE_PAGE_SIZE\t524288\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.backup.dir.datetime.format\tQDB_CAIRO_SQL_BACKUP_DIR_DATETIME_FORMAT\tyyyy-MM-dd\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.backup.dir.tmp.name\tQDB_CAIRO_SQL_BACKUP_DIR_TMP_NAME\ttmp\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.backup.mkdir.mode\tQDB_CAIRO_SQL_BACKUP_MKDIR_MODE\t509\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.bind.variable.pool.size\tQDB_CAIRO_SQL_BIND_VARIABLE_POOL_SIZE\t8\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.query.registry.pool.size\tQDB_CAIRO_SQL_QUERY_REGISTRY_POOL_SIZE\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.purge.queue.capacity\tQDB_CAIRO_SQL_COLUMN_PURGE_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.purge.retry.delay\tQDB_CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY\t10000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.purge.retry.delay.limit\tQDB_CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT\t60000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.purge.retry.delay.multiplier\tQDB_CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_MULTIPLIER\t10.0\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.purge.task.pool.capacity\tQDB_CAIRO_SQL_COLUMN_PURGE_TASK_POOL_CAPACITY\t256\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.buffer.size\tQDB_CAIRO_SQL_COPY_BUFFER_SIZE\t2097152\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.formats.file\tQDB_CAIRO_SQL_COPY_FORMATS_FILE\t/text_loader.json\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.id.supplier\tQDB_CAIRO_SQL_COPY_ID_SUPPLIER\trandom\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.log.retention.days\tQDB_CAIRO_SQL_COPY_LOG_RETENTION_DAYS\t3\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.max.index.chunk.size\tQDB_CAIRO_SQL_COPY_MAX_INDEX_CHUNK_SIZE\t104857600\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.model.pool.capacity\tQDB_CAIRO_SQL_COPY_MODEL_POOL_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.queue.capacity\tQDB_CAIRO_SQL_COPY_QUEUE_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.copy.export.queue.capacity\tQDB_CAIRO_SQL_COPY_EXPORT_QUEUE_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.count.distinct.capacity\tQDB_CAIRO_SQL_COUNT_DISTINCT_CAPACITY\t3\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.count.distinct.load.factor\tQDB_CAIRO_SQL_COUNT_DISTINCT_LOAD_FACTOR\t0.5\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.create.table.model.batch.size\tQDB_CAIRO_SQL_CREATE_TABLE_MODEL_BATCH_SIZE\t1000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.distinct.timestamp.key.capacity\tQDB_CAIRO_SQL_DISTINCT_TIMESTAMP_KEY_CAPACITY\t512\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.distinct.timestamp.load.factor\tQDB_CAIRO_SQL_DISTINCT_TIMESTAMP_LOAD_FACTOR\t0.5\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.explain.model.pool.capacity\tQDB_CAIRO_SQL_EXPLAIN_MODEL_POOL_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.groupby.map.capacity\tQDB_CAIRO_SQL_GROUPBY_MAP_CAPACITY\t1024\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.groupby.pool.capacity\tQDB_CAIRO_SQL_GROUPBY_POOL_CAPACITY\t1024\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.groupby.allocator.default.chunk.size\tQDB_CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.groupby.allocator.max.chunk.size\tQDB_CAIRO_SQL_GROUPBY_ALLOCATOR_MAX_CHUNK_SIZE\t4294967296\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.hash.join.light.value.max.pages\tQDB_CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.hash.join.light.value.page.size\tQDB_CAIRO_SQL_HASH_JOIN_LIGHT_VALUE_PAGE_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.hash.join.value.max.pages\tQDB_CAIRO_SQL_HASH_JOIN_VALUE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.hash.join.value.page.size\tQDB_CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE\t16777216\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.asof.join.lookahead\tQDB_CAIRO_SQL_ASOF_JOIN_LOOKAHEAD\t100\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.asof.join.short.circuit.cache.capacity\tQDB_CAIRO_SQL_ASOF_JOIN_SHORT_CIRCUIT_CACHE_CAPACITY\t10000000\tdefault\tfalse\ttrue\n" +
                                    "cairo.sql.asof.join.evacuation.threshold\tQDB_CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD\t10000000\tdefault\tfalse\ttrue\n" +
                                    "cairo.sql.insert.model.pool.capacity\tQDB_CAIRO_SQL_INSERT_MODEL_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.insert.model.batch.size\tQDB_CAIRO_SQL_INSERT_MODEL_BATCH_SIZE\t1000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.bind.vars.memory.max.pages\tQDB_CAIRO_SQL_JIT_BIND_VARS_MEMORY_MAX_PAGES\t8\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.bind.vars.memory.page.size\tQDB_CAIRO_SQL_JIT_BIND_VARS_MEMORY_PAGE_SIZE\t4096\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.debug.enabled\tQDB_CAIRO_SQL_JIT_DEBUG_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.ir.memory.max.pages\tQDB_CAIRO_SQL_JIT_IR_MEMORY_MAX_PAGES\t8\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.ir.memory.page.size\tQDB_CAIRO_SQL_JIT_IR_MEMORY_PAGE_SIZE\t8192\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.mode\tQDB_CAIRO_SQL_JIT_MODE\ton\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.page.address.cache.threshold\tQDB_CAIRO_SQL_JIT_PAGE_ADDRESS_CACHE_THRESHOLD\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.join.context.pool.capacity\tQDB_CAIRO_SQL_JOIN_CONTEXT_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.join.metadata.max.resizes\tQDB_CAIRO_SQL_JOIN_METADATA_MAX_RESIZES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.join.metadata.page.size\tQDB_CAIRO_SQL_JOIN_METADATA_PAGE_SIZE\t16384\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.latest.by.row.count\tQDB_CAIRO_SQL_LATEST_BY_ROW_COUNT\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.map.max.pages\tQDB_CAIRO_SQL_MAP_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.map.max.resizes\tQDB_CAIRO_SQL_MAP_MAX_RESIZES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.unordered.map.max.entry.size\tQDB_CAIRO_SQL_UNORDERED_MAP_MAX_ENTRY_SIZE\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.max.negative.limit\tQDB_CAIRO_SQL_MAX_NEGATIVE_LIMIT\t10000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.max.recompile.attempts\tQDB_CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS\t10\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.max.symbol.not.equals.count\tQDB_CAIRO_SQL_MAX_SYMBOL_NOT_EQUALS_COUNT\t100\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.page.frame.max.rows\tQDB_CAIRO_SQL_PAGE_FRAME_MAX_ROWS\t1000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.page.frame.min.rows\tQDB_CAIRO_SQL_PAGE_FRAME_MIN_ROWS\t100000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.filter.enabled\tQDB_CAIRO_SQL_PARALLEL_FILTER_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.filter.pretouch.threshold\tQDB_CAIRO_SQL_PARALLEL_FILTER_PRETOUCH_THRESHOLD\t0.05\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.topk.enabled\tQDB_CAIRO_SQL_PARALLEL_TOPK_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.enabled\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.merge.shard.queue.capacity\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_MERGE_SHARD_QUEUE_CAPACITY\t4\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.sharding.threshold\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD\t100000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.presize.enabled\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.presize.max.capacity\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_MAX_CAPACITY\t100000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.groupby.presize.max.heap.size\tQDB_CAIRO_SQL_PARALLEL_GROUPBY_PRESIZE_MAX_HEAP_SIZE\t1073741824\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.work.stealing.threshold\tQDB_CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD\t16\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parallel.read.parquet.enabled\tQDB_CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.parquet.frame.cache.capacity\tQDB_CAIRO_SQL_PARQUET_FRAME_CACHE_CAPACITY\t3\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.rename.table.model.pool.capacity\tQDB_CAIRO_SQL_RENAME_TABLE_MODEL_POOL_CAPACITY\t16\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sampleby.page.size\tQDB_CAIRO_SQL_SAMPLEBY_PAGE_SIZE\t0\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sampleby.default.alignment.calendar\tQDB_CAIRO_SQL_SAMPLEBY_DEFAULT_ALIGNMENT_CALENDAR\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.unsupported.sampleby.validate.fill.type\tQDB_CAIRO_SQL_UNSUPPORTED_SAMPLEBY_VALIDATE_FILL_TYPE\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.small.map.key.capacity\tQDB_CAIRO_SQL_SMALL_MAP_KEY_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.small.map.page.size\tQDB_CAIRO_SQL_SMALL_MAP_PAGE_SIZE\t32768\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.key.max.pages\tQDB_CAIRO_SQL_SORT_KEY_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.key.page.size\tQDB_CAIRO_SQL_SORT_KEY_PAGE_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.light.value.max.pages\tQDB_CAIRO_SQL_SORT_LIGHT_VALUE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.light.value.page.size\tQDB_CAIRO_SQL_SORT_LIGHT_VALUE_PAGE_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.value.max.pages\tQDB_CAIRO_SQL_SORT_VALUE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.sort.value.page.size\tQDB_CAIRO_SQL_SORT_VALUE_PAGE_SIZE\t16777216\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.string.function.buffer.max.size\tQDB_CAIRO_SQL_STRING_FUNCTION_BUFFER_MAX_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.column.pool.capacity\tQDB_CAIRO_SQL_WINDOW_COLUMN_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.max.recursion\tQDB_CAIRO_SQL_WINDOW_MAX_RECURSION\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.rowid.max.pages\tQDB_CAIRO_SQL_WINDOW_ROWID_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.rowid.page.size\tQDB_CAIRO_SQL_WINDOW_ROWID_PAGE_SIZE\t524288\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.store.max.pages\tQDB_CAIRO_SQL_WINDOW_STORE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.store.page.size\tQDB_CAIRO_SQL_WINDOW_STORE_PAGE_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.tree.max.pages\tQDB_CAIRO_SQL_WINDOW_TREE_MAX_PAGES\t2147483647\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.legacy.operator.precedence\tQDB_CAIRO_SQL_LEGACY_OPERATOR_PRECEDENCE\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.window.tree.page.size\tQDB_CAIRO_SQL_WINDOW_TREE_PAGE_SIZE\t524288\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.with.clause.model.pool.capacity\tQDB_CAIRO_SQL_WITH_CLAUSE_MODEL_POOL_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.orderby.sort.enabled\tQDB_CAIRO_SQL_ORDERBY_SORT_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.orderby.radix.sort.threshold\tQDB_CAIRO_SQL_ORDERBY_RADIX_SORT_THRESHOLD\t600\tdefault\tfalse\tfalse\n" +
                                    "cairo.system.o3.column.memory.size\tQDB_CAIRO_SYSTEM_O3_COLUMN_MEMORY_SIZE\t262144\tdefault\tfalse\tfalse\n" +
                                    "cairo.system.table.prefix\tQDB_CAIRO_SYSTEM_TABLE_PREFIX\tsys.\tdefault\tfalse\tfalse\n" +
                                    "cairo.system.wal.writer.data.append.page.size\tQDB_CAIRO_SYSTEM_WAL_WRITER_DATA_APPEND_PAGE_SIZE\t262144\tdefault\tfalse\tfalse\n" +
                                    "cairo.system.wal.writer.event.append.page.size\tQDB_CAIRO_SYSTEM_WAL_WRITER_EVENT_APPEND_PAGE_SIZE\t16384\tdefault\tfalse\tfalse\n" +
                                    "cairo.system.writer.data.append.page.size\tQDB_CAIRO_SYSTEM_WRITER_DATA_APPEND_PAGE_SIZE\t262144\tdefault\tfalse\tfalse\n" +
                                    "cairo.symbol.table.min.allocation.page.size\tQDB_CAIRO_SYMBOL_TABLE_MIN_ALLOCATION_PAGE_SIZE\t" + Files.PAGE_SIZE + "\tdefault\tfalse\tfalse\n" +
                                    "cairo.symbol.table.max.allocation.page.size\tQDB_CAIRO_SYMBOL_TABLE_MAX_ALLOCATION_PAGE_SIZE\t8388608\tdefault\tfalse\tfalse\n" +
                                    "cairo.table.registry.auto.reload.frequency\tQDB_CAIRO_TABLE_REGISTRY_AUTO_RELOAD_FREQUENCY\t500\tdefault\tfalse\tfalse\n" +
                                    "cairo.table.registry.compaction.threshold\tQDB_CAIRO_TABLE_REGISTRY_COMPACTION_THRESHOLD\t30\tdefault\tfalse\tfalse\n" +
                                    "cairo.vector.aggregate.queue.capacity\tQDB_CAIRO_VECTOR_AGGREGATE_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "cairo.volumes\tQDB_CAIRO_VOLUMES\t\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.apply.enabled\tQDB_CAIRO_WAL_APPLY_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.apply.look.ahead.txn.count\tQDB_CAIRO_WAL_APPLY_LOOK_AHEAD_TXN_COUNT\t200\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.apply.table.time.quota\tQDB_CAIRO_WAL_APPLY_TABLE_TIME_QUOTA\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.apply.parallel.sql.enabled\tQDB_CAIRO_WAL_APPLY_PARALLEL_SQL_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.enabled.default\tQDB_CAIRO_WAL_ENABLED_DEFAULT\tfalse\tconf\tfalse\tfalse\n" +
                                    "cairo.wal.inactive.writer.ttl\tQDB_CAIRO_WAL_INACTIVE_WRITER_TTL\t120000\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.max.lag.txn.count\tQDB_CAIRO_WAL_MAX_LAG_TXN_COUNT\t-1\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.max.segment.file.descriptors.cache\tQDB_CAIRO_WAL_MAX_SEGMENT_FILE_DESCRIPTORS_CACHE\t30\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.max.lag.size\tQDB_CAIRO_WAL_MAX_LAG_SIZE\t78643200\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.purge.interval\tQDB_CAIRO_WAL_PURGE_INTERVAL\t30000\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.recreate.distressed.sequencer.attempts\tQDB_CAIRO_WAL_RECREATE_DISTRESSED_SEQUENCER_ATTEMPTS\t3\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.segment.rollover.row.count\tQDB_CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT\t200000\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.segment.rollover.size\tQDB_CAIRO_WAL_SEGMENT_ROLLOVER_SIZE\t52428800\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.squash.uncommitted.rows.multiplier\tQDB_CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER\t20.0\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.supported\tQDB_CAIRO_WAL_SUPPORTED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.temp.pending.rename.table.prefix\tQDB_CAIRO_WAL_TEMP_PENDING_RENAME_TABLE_PREFIX\ttemp_5822f658-31f6-11ee-be56-0242ac120002\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.txn.notification.queue.capacity\tQDB_CAIRO_WAL_TXN_NOTIFICATION_QUEUE_CAPACITY\t4096\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.writer.data.append.page.size\tQDB_CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.writer.pool.max.segments\tQDB_CAIRO_WAL_WRITER_POOL_MAX_SEGMENTS\t10\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.sequencer.check.interval\tQDB_CAIRO_WAL_SEQUENCER_CHECK_INTERVAL\t10000\tdefault\tfalse\tfalse\n" +
                                    "cairo.wal.writer.event.append.page.size\tQDB_CAIRO_WAL_WRITER_EVENT_APPEND_PAGE_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "cairo.work.steal.timeout.nanos\tQDB_CAIRO_WORK_STEAL_TIMEOUT_NANOS\t10000\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.alter.busy.wait.timeout\tQDB_CAIRO_WRITER_ALTER_BUSY_WAIT_TIMEOUT\t500\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.alter.max.wait.timeout\tQDB_CAIRO_WRITER_ALTER_MAX_WAIT_TIMEOUT\t30000\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.command.queue.capacity\tQDB_CAIRO_WRITER_COMMAND_QUEUE_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.command.queue.slot.size\tQDB_CAIRO_WRITER_COMMAND_QUEUE_SLOT_SIZE\t2048\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.data.append.page.size\tQDB_CAIRO_WRITER_DATA_APPEND_PAGE_SIZE\t16777216\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.data.index.key.append.page.size\tQDB_CAIRO_WRITER_DATA_INDEX_KEY_APPEND_PAGE_SIZE\t524288\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.data.index.value.append.page.size\tQDB_CAIRO_WRITER_DATA_INDEX_VALUE_APPEND_PAGE_SIZE\t16777216\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.fo_opts\tQDB_CAIRO_WRITER_FO_OPTS\to_none\tdefault\tfalse\tfalse\n" +
                                    "cairo.writer.tick.rows.count\tQDB_CAIRO_WRITER_TICK_ROWS_COUNT\t1024\tdefault\tfalse\tfalse\n" +
                                    "cairo.txn.scoreboard.format\tQDB_CAIRO_TXN_SCOREBOARD_FORMAT\t2\tdefault\tfalse\tfalse\n" +
                                    "circuit.breaker.buffer.size\tQDB_CIRCUIT_BREAKER_BUFFER_SIZE\t64\tdefault\tfalse\tfalse\n" +
                                    "circuit.breaker.throttle\tQDB_CIRCUIT_BREAKER_THROTTLE\t2000000\tdefault\tfalse\tfalse\n" +
                                    "config.reload.enabled\tQDB_CONFIG_RELOAD_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "config.validation.strict\tQDB_CONFIG_VALIDATION_STRICT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.allow.deflate.before.send\tQDB_HTTP_ALLOW_DEFLATE_BEFORE_SEND\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.bind.to\tQDB_HTTP_BIND_TO\t0.0.0.0:" + HTTP_PORT + "\tconf\tfalse\tfalse\n" +
                                    "http.user\tQDB_HTTP_USER\t\tdefault\tfalse\tfalse\n" +
                                    "http.password\tQDB_HTTP_PASSWORD\t****\tdefault\ttrue\tfalse\n" +
                                    "http.busy.retry.exponential.wait.multiplier\tQDB_HTTP_BUSY_RETRY_EXPONENTIAL_WAIT_MULTIPLIER\t2.0\tdefault\tfalse\tfalse\n" +
                                    "http.busy.retry.initialWaitQueueSize\tQDB_HTTP_BUSY_RETRY_INITIALWAITQUEUESIZE\t64\tdefault\tfalse\tfalse\n" +
                                    "http.busy.retry.maxProcessingQueueSize\tQDB_HTTP_BUSY_RETRY_MAXPROCESSINGQUEUESIZE\t4096\tdefault\tfalse\tfalse\n" +
                                    "http.busy.retry.maximum.wait.before.retry\tQDB_HTTP_BUSY_RETRY_MAXIMUM_WAIT_BEFORE_RETRY\t1000\tdefault\tfalse\tfalse\n" +
                                    "http.connection.pool.initial.capacity\tQDB_HTTP_CONNECTION_POOL_INITIAL_CAPACITY\t4\tdefault\tfalse\tfalse\n" +
                                    "http.connection.string.pool.capacity\tQDB_HTTP_CONNECTION_STRING_POOL_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "http.net.accept.loop.timeout\tQDB_HTTP_NET_ACCEPT_LOOP_TIMEOUT\t500\tdefault\tfalse\tfalse\n" +
                                    "http.export.timeout\tQDB_HTTP_EXPORT_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "http.enabled\tQDB_HTTP_ENABLED\ttrue\tconf\tfalse\tfalse\n" +
                                    "http.frozen.clock\tQDB_HTTP_FROZEN_CLOCK\ttrue\tconf\tfalse\tfalse\n" +
                                    "http.health.check.authentication.required\tQDB_HTTP_HEALTH_CHECK_AUTHENTICATION_REQUIRED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "http.json.query.connection.check.frequency\tQDB_HTTP_JSON_QUERY_CONNECTION_CHECK_FREQUENCY\t1000000\tdefault\tfalse\tfalse\n" +
                                    "http.keep-alive.max\tQDB_HTTP_KEEP-ALIVE_MAX\t10000\tdefault\tfalse\tfalse\n" +
                                    "http.keep-alive.timeout\tQDB_HTTP_KEEP-ALIVE_TIMEOUT\t5\tdefault\tfalse\tfalse\n" +
                                    "http.min.bind.to\tQDB_HTTP_MIN_BIND_TO\t0.0.0.0:9003\tdefault\tfalse\tfalse\n" +
                                    "http.min.enabled\tQDB_HTTP_MIN_ENABLED\ttrue\tconf\tfalse\tfalse\n" +
                                    "http.min.net.bind.to\tQDB_HTTP_MIN_NET_BIND_TO\t0.0.0.0:" + HTTP_MIN_PORT + "\tconf\tfalse\tfalse\n" +
                                    "http.min.net.connection.hint\tQDB_HTTP_MIN_NET_CONNECTION_HINT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.connection.limit\tQDB_HTTP_MIN_NET_CONNECTION_LIMIT\t64\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.connection.sndbuf\tQDB_HTTP_MIN_NET_CONNECTION_SNDBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.snd.buf.size\tQDB_HTTP_MIN_NET_SND_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.min.send.buffer.size\tQDB_HTTP_MIN_SEND_BUFFER_SIZE\t1024\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.connection.rcvbuf\tQDB_HTTP_MIN_NET_CONNECTION_RCVBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.min.receive.buffer.size\tQDB_HTTP_MIN_RECEIVE_BUFFER_SIZE\t1024\tdefault\tfalse\tfalse\n" +
                                    "http.min.recv.buffer.size\tQDB_HTTP_MIN_RECV_BUFFER_SIZE\t1024\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.connection.queue.timeout\tQDB_HTTP_MIN_NET_CONNECTION_QUEUE_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.connection.timeout\tQDB_HTTP_MIN_NET_CONNECTION_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.idle.connection.timeout\tQDB_HTTP_MIN_NET_IDLE_CONNECTION_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.queued.connection.timeout\tQDB_HTTP_MIN_NET_QUEUED_CONNECTION_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "http.min.net.accept.loop.timeout\tQDB_HTTP_MIN_NET_ACCEPT_LOOP_TIMEOUT\t500\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.affinity\tQDB_HTTP_MIN_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.count\tQDB_HTTP_MIN_WORKER_COUNT\t1\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.haltOnError\tQDB_HTTP_MIN_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.sleep.threshold\tQDB_HTTP_MIN_WORKER_SLEEP_THRESHOLD\t100\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.sleep.timeout\tQDB_HTTP_MIN_WORKER_SLEEP_TIMEOUT\t50\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.nap.threshold\tQDB_HTTP_MIN_WORKER_NAP_THRESHOLD\t100\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.yield.threshold\tQDB_HTTP_MIN_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "http.multipart.header.buffer.size\tQDB_HTTP_MULTIPART_HEADER_BUFFER_SIZE\t512\tdefault\tfalse\ttrue\n" +
                                    "http.multipart.idle.spin.count\tQDB_HTTP_MULTIPART_IDLE_SPIN_COUNT\t10000\tdefault\tfalse\tfalse\n" +
                                    "http.net.snd.buf.size\tQDB_HTTP_NET_SND_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.net.connection.sndbuf\tQDB_HTTP_NET_CONNECTION_SNDBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.send.buffer.size\tQDB_HTTP_SEND_BUFFER_SIZE\t2097152\tdefault\tfalse\ttrue\n" +
                                    "http.net.rcv.buf.size\tQDB_HTTP_NET_RCV_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.net.connection.rcvbuf\tQDB_HTTP_NET_CONNECTION_RCVBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "http.receive.buffer.size\tQDB_HTTP_RECEIVE_BUFFER_SIZE\t2097152\tdefault\tfalse\tfalse\n" +
                                    "http.recv.buffer.size\tQDB_HTTP_RECV_BUFFER_SIZE\t2097152\tdefault\tfalse\ttrue\n" +
                                    "http.net.active.connection.limit\tQDB_HTTP_NET_ACTIVE_CONNECTION_LIMIT\t256\tdefault\tfalse\tfalse\n" +
                                    "http.net.bind.to\tQDB_HTTP_NET_BIND_TO\t0.0.0.0:" + HTTP_PORT + "\tdefault\tfalse\tfalse\n" +
                                    "http.net.connection.hint\tQDB_HTTP_NET_CONNECTION_HINT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.net.connection.limit\tQDB_HTTP_NET_CONNECTION_LIMIT\t256\tdefault\tfalse\ttrue\n" +
                                    "http.net.connection.queue.timeout\tQDB_HTTP_NET_CONNECTION_QUEUE_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "http.net.connection.timeout\tQDB_HTTP_NET_CONNECTION_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "http.net.idle.connection.timeout\tQDB_HTTP_NET_IDLE_CONNECTION_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "http.net.queued.connection.timeout\tQDB_HTTP_NET_QUEUED_CONNECTION_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "http.pessimistic.health.check.enabled\tQDB_HTTP_PESSIMISTIC_HEALTH_CHECK_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.query.cache.block.count\tQDB_HTTP_QUERY_CACHE_BLOCK_COUNT\t32\tdefault\tfalse\tfalse\n" +
                                    "http.query.cache.enabled\tQDB_HTTP_QUERY_CACHE_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "http.query.cache.row.count\tQDB_HTTP_QUERY_CACHE_ROW_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "http.request.header.buffer.size\tQDB_HTTP_REQUEST_HEADER_BUFFER_SIZE\t64448\tdefault\tfalse\ttrue\n" +
                                    "http.security.interrupt.on.closed.connection\tQDB_HTTP_SECURITY_INTERRUPT_ON_CLOSED_CONNECTION\ttrue\tdefault\tfalse\tfalse\n" +
                                    "http.security.max.response.rows\tQDB_HTTP_SECURITY_MAX_RESPONSE_ROWS\t9223372036854775807\tdefault\tfalse\tfalse\n" +
                                    "http.security.readonly\tQDB_HTTP_SECURITY_READONLY\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.server.keep.alive\tQDB_HTTP_SERVER_KEEP_ALIVE\ttrue\tdefault\tfalse\tfalse\n" +
                                    "http.static.public.directory\tQDB_HTTP_STATIC_PUBLIC_DIRECTORY\tpublic\tdefault\tfalse\tfalse\n" +
                                    "http.text.analysis.max.lines\tQDB_HTTP_TEXT_ANALYSIS_MAX_LINES\t1000\tdefault\tfalse\tfalse\n" +
                                    "http.text.date.adapter.pool.capacity\tQDB_HTTP_TEXT_DATE_ADAPTER_POOL_CAPACITY\t16\tdefault\tfalse\tfalse\n" +
                                    "http.text.json.cache.limit\tQDB_HTTP_TEXT_JSON_CACHE_LIMIT\t16384\tdefault\tfalse\tfalse\n" +
                                    "http.text.json.cache.size\tQDB_HTTP_TEXT_JSON_CACHE_SIZE\t8192\tdefault\tfalse\tfalse\n" +
                                    "http.text.lexer.string.pool.capacity\tQDB_HTTP_TEXT_LEXER_STRING_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "http.text.max.required.delimiter.stddev\tQDB_HTTP_TEXT_MAX_REQUIRED_DELIMITER_STDDEV\t0.1222\tdefault\tfalse\tfalse\n" +
                                    "http.text.max.required.line.length.stddev\tQDB_HTTP_TEXT_MAX_REQUIRED_LINE_LENGTH_STDDEV\t0.8\tdefault\tfalse\tfalse\n" +
                                    "http.text.metadata.string.pool.capacity\tQDB_HTTP_TEXT_METADATA_STRING_POOL_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "http.text.roll.buffer.limit\tQDB_HTTP_TEXT_ROLL_BUFFER_LIMIT\t4194304\tdefault\tfalse\tfalse\n" +
                                    "http.text.roll.buffer.size\tQDB_HTTP_TEXT_ROLL_BUFFER_SIZE\t1024\tdefault\tfalse\tfalse\n" +
                                    "http.text.timestamp.adapter.pool.capacity\tQDB_HTTP_TEXT_TIMESTAMP_ADAPTER_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "http.text.utf8.sink.size\tQDB_HTTP_TEXT_UTF8_SINK_SIZE\t4096\tdefault\tfalse\tfalse\n" +
                                    "http.version\tQDB_HTTP_VERSION\tHTTP/1.1\tdefault\tfalse\tfalse\n" +
                                    "http.worker.affinity\tQDB_HTTP_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "http.worker.count\tQDB_HTTP_WORKER_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "http.worker.haltOnError\tQDB_HTTP_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.worker.sleep.threshold\tQDB_HTTP_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "http.worker.sleep.timeout\tQDB_HTTP_WORKER_SLEEP_TIMEOUT\t10\tdefault\tfalse\tfalse\n" +
                                    "http.worker.nap.threshold\tQDB_HTTP_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "http.worker.yield.threshold\tQDB_HTTP_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "line.log.message.on.error\tQDB_LINE_LOG_MESSAGE_ON_ERROR\ttrue\tdefault\tfalse\tfalse\n" +
                                    "line.auto.create.new.columns\tQDB_LINE_AUTO_CREATE_NEW_COLUMNS\ttrue\tdefault\tfalse\tfalse\n" +
                                    "line.auto.create.new.tables\tQDB_LINE_AUTO_CREATE_NEW_TABLES\ttrue\tdefault\tfalse\tfalse\n" +
                                    "line.default.partition.by\tQDB_LINE_DEFAULT_PARTITION_BY\tDAY\tdefault\tfalse\tfalse\n" +
                                    "line.float.default.column.type\tQDB_LINE_FLOAT_DEFAULT_COLUMN_TYPE\tDOUBLE\tdefault\tfalse\tfalse\n" +
                                    "line.http.enabled\tQDB_LINE_HTTP_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "line.http.max.recv.buffer.size\tQDB_LINE_HTTP_MAX_RECV_BUFFER_SIZE\t1073741824\tdefault\tfalse\ttrue\n" +
                                    "line.http.ping.version\tQDB_LINE_HTTP_PING_VERSION\tv2.7.4\tdefault\tfalse\tfalse\n" +
                                    "line.integer.default.column.type\tQDB_LINE_INTEGER_DEFAULT_COLUMN_TYPE\tLONG\tdefault\tfalse\tfalse\n" +
                                    "line.timestamp.default.column.type\tQDB_LINE_TIMESTAMP_DEFAULT_COLUMN_TYPE\tTIMESTAMP\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.auth.db.path\tQDB_LINE_TCP_AUTH_DB_PATH\t\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.commit.interval.default\tQDB_LINE_TCP_COMMIT_INTERVAL_DEFAULT\t2000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.commit.interval.fraction\tQDB_LINE_TCP_COMMIT_INTERVAL_FRACTION\t0.5\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.connection.pool.capacity\tQDB_LINE_TCP_CONNECTION_POOL_CAPACITY\t8\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.default.partition.by\tQDB_LINE_TCP_DEFAULT_PARTITION_BY\tDAY\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.disconnect.on.error\tQDB_LINE_TCP_DISCONNECT_ON_ERROR\ttrue\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.enabled\tQDB_LINE_TCP_ENABLED\ttrue\tconf\tfalse\tfalse\n" +
                                    "line.tcp.io.halt.on.error\tQDB_LINE_TCP_IO_HALT_ON_ERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.io.worker.affinity\tQDB_LINE_TCP_IO_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.io.worker.sleep.threshold\tQDB_LINE_TCP_IO_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.io.worker.nap.threshold\tQDB_LINE_TCP_IO_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.io.worker.yield.threshold\tQDB_LINE_TCP_IO_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.maintenance.job.interval\tQDB_LINE_TCP_MAINTENANCE_JOB_INTERVAL\t1000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.max.measurement.size\tQDB_LINE_TCP_MAX_MEASUREMENT_SIZE\t32768\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.min.idle.ms.before.writer.release\tQDB_LINE_TCP_MIN_IDLE_MS_BEFORE_WRITER_RELEASE\t500\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.msg.buffer.size\tQDB_LINE_TCP_MSG_BUFFER_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.recv.buffer.size\tQDB_LINE_TCP_RECV_BUFFER_SIZE\t131072\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.max.recv.buffer.size\tQDB_LINE_TCP_MAX_RECV_BUFFER_SIZE\t1073741824\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.recv.buf.size\tQDB_LINE_TCP_NET_RECV_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.connection.rcvbuf\tQDB_LINE_TCP_NET_CONNECTION_RCVBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.active.connection.limit\tQDB_LINE_TCP_NET_ACTIVE_CONNECTION_LIMIT\t256\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.bind.to\tQDB_LINE_TCP_NET_BIND_TO\t0.0.0.0:" + ILP_PORT + "\tconf\tfalse\tfalse\n" +
                                    "line.tcp.net.connection.heartbeat.interval\tQDB_LINE_TCP_NET_CONNECTION_HEARTBEAT_INTERVAL\t500\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.connection.hint\tQDB_LINE_TCP_NET_CONNECTION_HINT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.connection.limit\tQDB_LINE_TCP_NET_CONNECTION_LIMIT\t256\tdefault\tfalse\ttrue\n" +
                                    "line.tcp.net.connection.queue.timeout\tQDB_LINE_TCP_NET_CONNECTION_QUEUE_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.connection.timeout\tQDB_LINE_TCP_NET_CONNECTION_TIMEOUT\t0\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.idle.timeout\tQDB_LINE_TCP_NET_IDLE_TIMEOUT\t0\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.queued.timeout\tQDB_LINE_TCP_NET_QUEUED_TIMEOUT\t5000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.net.accept.loop.timeout\tQDB_LINE_TCP_NET_ACCEPT_LOOP_TIMEOUT\t500\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.symbol.cache.wait.before.reload\tQDB_LINE_TCP_SYMBOL_CACHE_WAIT_BEFORE_RELOAD\t500000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.timestamp\tQDB_LINE_TCP_TIMESTAMP\tn\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.undocumented.string.to.char.cast.allowed\tQDB_LINE_TCP_UNDOCUMENTED_STRING_TO_CHAR_CAST_ALLOWED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.halt.on.error\tQDB_LINE_TCP_WRITER_HALT_ON_ERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.queue.capacity\tQDB_LINE_TCP_WRITER_QUEUE_CAPACITY\t128\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.worker.affinity\tQDB_LINE_TCP_WRITER_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.worker.count\tQDB_LINE_TCP_WRITER_WORKER_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.worker.sleep.threshold\tQDB_LINE_TCP_WRITER_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.worker.nap.threshold\tQDB_LINE_TCP_WRITER_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "line.tcp.writer.worker.yield.threshold\tQDB_LINE_TCP_WRITER_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "line.udp.bind.to\tQDB_LINE_UDP_BIND_TO\t0.0.0.0:" + ILP_PORT + "\tconf\tfalse\tfalse\n" +
                                    "line.udp.commit.mode\tQDB_LINE_UDP_COMMIT_MODE\tnosync\tdefault\tfalse\tfalse\n" +
                                    "line.udp.commit.rate\tQDB_LINE_UDP_COMMIT_RATE\t1000000\tdefault\tfalse\tfalse\n" +
                                    "line.udp.enabled\tQDB_LINE_UDP_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.udp.join\tQDB_LINE_UDP_JOIN\t232.1.2.3\tdefault\tfalse\tfalse\n" +
                                    "line.udp.msg.buffer.size\tQDB_LINE_UDP_MSG_BUFFER_SIZE\t2048\tdefault\tfalse\tfalse\n" +
                                    "line.udp.msg.count\tQDB_LINE_UDP_MSG_COUNT\t10000\tdefault\tfalse\tfalse\n" +
                                    "line.udp.own.thread\tQDB_LINE_UDP_OWN_THREAD\tfalse\tdefault\tfalse\tfalse\n" +
                                    "line.udp.own.thread.affinity\tQDB_LINE_UDP_OWN_THREAD_AFFINITY\t-1\tdefault\tfalse\tfalse\n" +
                                    "line.udp.receive.buffer.size\tQDB_LINE_UDP_RECEIVE_BUFFER_SIZE\t4096\tconf\tfalse\tfalse\n" +
                                    "line.udp.timestamp\tQDB_LINE_UDP_TIMESTAMP\tn\tdefault\tfalse\tfalse\n" +
                                    "line.udp.unicast\tQDB_LINE_UDP_UNICAST\tfalse\tdefault\tfalse\tfalse\n" +
                                    "metrics.enabled\tQDB_METRICS_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "cairo.mat.view.enabled\tQDB_CAIRO_MAT_VIEW_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.mat.view.max.refresh.retries\tQDB_CAIRO_MAT_VIEW_MAX_REFRESH_RETRIES\t10\tdefault\tfalse\ttrue\n" +
                                    "cairo.mat.view.refresh.oom.retry.timeout\tQDB_CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT\t200\tdefault\tfalse\tfalse\n" +
                                    "cairo.mat.view.insert.as.select.batch.size\tQDB_CAIRO_MAT_VIEW_INSERT_AS_SELECT_BATCH_SIZE\t1000000\tdefault\tfalse\ttrue\n" +
                                    "cairo.mat.view.rows.per.query.estimate\tQDB_CAIRO_MAT_VIEW_ROWS_PER_QUERY_ESTIMATE\t1000000\tdefault\tfalse\ttrue\n" +
                                    "cairo.mat.view.parallel.sql.enabled\tQDB_CAIRO_MAT_VIEW_PARALLEL_SQL_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.mat.view.max.refresh.intervals\tQDB_CAIRO_MAT_VIEW_MAX_REFRESH_INTERVALS\t100\tdefault\tfalse\ttrue\n" +
                                    "cairo.mat.view.max.refresh.step\tQDB_CAIRO_MAT_VIEW_MAX_REFRESH_STEP\t31536000000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.mat.view.refresh.intervals.update.period\tQDB_CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD\t15000\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.nap.threshold\tQDB_MAT_VIEW_REFRESH_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.affinity\tQDB_MAT_VIEW_REFRESH_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.sleep.timeout\tQDB_MAT_VIEW_REFRESH_WORKER_SLEEP_TIMEOUT\t10\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.haltOnError\tQDB_MAT_VIEW_REFRESH_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.yield.threshold\tQDB_MAT_VIEW_REFRESH_WORKER_YIELD_THRESHOLD\t1000\tdefault\tfalse\tfalse\n" +
                                    "mat.view.refresh.worker.sleep.threshold\tQDB_MAT_VIEW_REFRESH_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "export.worker.nap.threshold\tQDB_EXPORT_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "export.worker.affinity\tQDB_EXPORT_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "export.worker.sleep.timeout\tQDB_EXPORT_WORKER_SLEEP_TIMEOUT\t10\tdefault\tfalse\tfalse\n" +
                                    "export.worker.haltOnError\tQDB_EXPORT_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "export.worker.yield.threshold\tQDB_EXPORT_WORKER_YIELD_THRESHOLD\t1000\tdefault\tfalse\tfalse\n" +
                                    "export.worker.sleep.threshold\tQDB_EXPORT_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "net.test.connection.buffer.size\tQDB_NET_TEST_CONNECTION_BUFFER_SIZE\t64\tdefault\tfalse\tfalse\n" +
                                    "pg.binary.param.count.capacity\tQDB_PG_BINARY_PARAM_COUNT_CAPACITY\t2\tdefault\tfalse\tfalse\n" +
                                    "pg.character.store.capacity\tQDB_PG_CHARACTER_STORE_CAPACITY\t4096\tdefault\tfalse\tfalse\n" +
                                    "pg.character.store.pool.capacity\tQDB_PG_CHARACTER_STORE_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "pg.connection.pool.capacity\tQDB_PG_CONNECTION_POOL_CAPACITY\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.daemon.pool\tQDB_PG_DAEMON_POOL\ttrue\tdefault\tfalse\tfalse\n" +
                                    "pg.date.locale\tQDB_PG_DATE_LOCALE\ten\tdefault\tfalse\tfalse\n" +
                                    "pg.enabled\tQDB_PG_ENABLED\ttrue\tconf\tfalse\tfalse\n" +
                                    "pg.halt.on.error\tQDB_PG_HALT_ON_ERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "pg.insert.cache.block.count\tQDB_PG_INSERT_CACHE_BLOCK_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.insert.cache.enabled\tQDB_PG_INSERT_CACHE_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "pg.insert.cache.row.count\tQDB_PG_INSERT_CACHE_ROW_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.max.blob.size.on.query\tQDB_PG_MAX_BLOB_SIZE_ON_QUERY\t524288\tdefault\tfalse\tfalse\n" +
                                    "pg.named.statement.cache.capacity\tQDB_PG_NAMED_STATEMENT_CACHE_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "pg.named.statement.pool.capacity\tQDB_PG_NAMED_STATEMENT_POOL_CAPACITY\t32\tdefault\tfalse\tfalse\n" +
                                    "pg.net.active.connection.limit\tQDB_PG_NET_ACTIVE_CONNECTION_LIMIT\t64\tdefault\tfalse\tfalse\n" +
                                    "pg.net.bind.to\tQDB_PG_NET_BIND_TO\t0.0.0.0:" + PG_PORT + "\tconf\tfalse\tfalse\n" +
                                    "pg.net.connection.hint\tQDB_PG_NET_CONNECTION_HINT\tfalse\tdefault\tfalse\tfalse\n" +
                                    "pg.net.connection.limit\tQDB_PG_NET_CONNECTION_LIMIT\t64\tdefault\tfalse\ttrue\n" +
                                    "pg.net.connection.queue.timeout\tQDB_PG_NET_CONNECTION_QUEUE_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "pg.net.connection.timeout\tQDB_PG_NET_CONNECTION_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "pg.net.idle.timeout\tQDB_PG_NET_IDLE_TIMEOUT\t300000\tdefault\tfalse\tfalse\n" +
                                    "pg.net.connection.sndbuf\tQDB_PG_NET_CONNECTION_SNDBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "pg.net.accept.loop.timeout\tQDB_PG_NET_ACCEPT_LOOP_TIMEOUT\t500\tdefault\tfalse\tfalse\n" +
                                    "pg.net.send.buf.size\tQDB_PG_NET_SEND_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "pg.net.connection.rcvbuf\tQDB_PG_NET_CONNECTION_RCVBUF\t-1\tdefault\tfalse\tfalse\n" +
                                    "pg.net.recv.buf.size\tQDB_PG_NET_RECV_BUF_SIZE\t-1\tdefault\tfalse\tfalse\n" +
                                    "pg.password\tQDB_PG_PASSWORD\t****\tdefault\ttrue\ttrue\n" +
                                    "pg.pending.writers.cache.capacity\tQDB_PG_PENDING_WRITERS_CACHE_CAPACITY\t16\tdefault\tfalse\tfalse\n" +
                                    "pg.readonly.password\tQDB_PG_READONLY_PASSWORD\t****\tdefault\ttrue\ttrue\n" +
                                    "pg.readonly.user\tQDB_PG_READONLY_USER\tuser\tdefault\tfalse\ttrue\n" +
                                    "pg.readonly.user.enabled\tQDB_PG_READONLY_USER_ENABLED\tfalse\tdefault\tfalse\ttrue\n" +
                                    "pg.recv.buffer.size\tQDB_PG_RECV_BUFFER_SIZE\t1048576\tdefault\tfalse\ttrue\n" +
                                    "pg.security.readonly\tQDB_PG_SECURITY_READONLY\tfalse\tdefault\tfalse\tfalse\n" +
                                    "pg.select.cache.block.count\tQDB_PG_SELECT_CACHE_BLOCK_COUNT\t32\tdefault\tfalse\tfalse\n" +
                                    "pg.select.cache.enabled\tQDB_PG_SELECT_CACHE_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "pg.select.cache.row.count\tQDB_PG_SELECT_CACHE_ROW_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.send.buffer.size\tQDB_PG_SEND_BUFFER_SIZE\t1048576\tdefault\tfalse\ttrue\n" +
                                    "pg.update.cache.block.count\tQDB_PG_UPDATE_CACHE_BLOCK_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.update.cache.enabled\tQDB_PG_UPDATE_CACHE_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "pg.update.cache.row.count\tQDB_PG_UPDATE_CACHE_ROW_COUNT\t4\tdefault\tfalse\tfalse\n" +
                                    "pg.user\tQDB_PG_USER\tadmin\tdefault\tfalse\ttrue\n" +
                                    "pg.worker.affinity\tQDB_PG_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "pg.worker.count\tQDB_PG_WORKER_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "pg.worker.sleep.threshold\tQDB_PG_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "pg.worker.nap.threshold\tQDB_PG_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "pg.worker.yield.threshold\tQDB_PG_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "pg.named.statement.limit\tQDB_PG_NAMED_STATEMENT_LIMIT\t10000\tdefault\tfalse\ttrue\n" +
                                    "posthog.enabled\tQDB_POSTHOG_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "posthog.api.key\tQDB_POSTHOG_API_KEY\t\tdefault\tfalse\tfalse\n" +
                                    "query.timeout.sec\tQDB_QUERY_TIMEOUT_SEC\t60\tdefault\tfalse\tfalse\n" +
                                    "ram.usage.limit.bytes\tQDB_RAM_USAGE_LIMIT_BYTES\t0\tdefault\tfalse\tfalse\n" +
                                    "ram.usage.limit.percent\tQDB_RAM_USAGE_LIMIT_PERCENT\t90\tdefault\tfalse\tfalse\n" +
                                    "readonly\tQDB_READONLY\tfalse\tdefault\tfalse\tfalse\n" +
                                    "shared.worker.count\tQDB_SHARED_WORKER_COUNT\t2\tconf\tfalse\tfalse\n" +
                                    "shared.worker.haltOnError\tQDB_SHARED_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "shared.worker.sleep.threshold\tQDB_SHARED_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "shared.worker.sleep.timeout\tQDB_SHARED_WORKER_SLEEP_TIMEOUT\t10\tdefault\tfalse\tfalse\n" +
                                    "shared.worker.nap.threshold\tQDB_SHARED_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "shared.worker.yield.threshold\tQDB_SHARED_WORKER_YIELD_THRESHOLD\t10\tdefault\tfalse\tfalse\n" +
                                    "shared.network.worker.affinity\tQDB_SHARED_NETWORK_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "shared.network.worker.count\tQDB_SHARED_NETWORK_WORKER_COUNT\t2\tdefault\tfalse\tfalse\n" +
                                    "shared.query.worker.affinity\tQDB_SHARED_QUERY_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "shared.query.worker.count\tQDB_SHARED_QUERY_WORKER_COUNT\t2\tdefault\tfalse\tfalse\n" +
                                    "shared.write.worker.affinity\tQDB_SHARED_WRITE_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "shared.write.worker.count\tQDB_SHARED_WRITE_WORKER_COUNT\t2\tdefault\tfalse\tfalse\n" +
                                    "table.type.conversion.enabled\tQDB_TABLE_TYPE_CONVERSION_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "telemetry.disable.completely\tQDB_TELEMETRY_DISABLE_COMPLETELY\ttrue\tconf\tfalse\tfalse\n" +
                                    "telemetry.enabled\tQDB_TELEMETRY_ENABLED\tfalse\tconf\tfalse\tfalse\n" +
                                    "telemetry.hide.tables\tQDB_TELEMETRY_HIDE_TABLES\ttrue\tdefault\tfalse\tfalse\n" +
                                    "telemetry.queue.capacity\tQDB_TELEMETRY_QUEUE_CAPACITY\t512\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.affinity\tQDB_WAL_APPLY_WORKER_AFFINITY\t\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.haltOnError\tQDB_WAL_APPLY_WORKER_HALTONERROR\tfalse\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.sleep.threshold\tQDB_WAL_APPLY_WORKER_SLEEP_THRESHOLD\t10000\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.sleep.timeout\tQDB_WAL_APPLY_WORKER_SLEEP_TIMEOUT\t10\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.nap.threshold\tQDB_WAL_APPLY_WORKER_NAP_THRESHOLD\t7000\tdefault\tfalse\tfalse\n" +
                                    "wal.apply.worker.yield.threshold\tQDB_WAL_APPLY_WORKER_YIELD_THRESHOLD\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.checkpoint.recovery.enabled\tQDB_CAIRO_CHECKPOINT_RECOVERY_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "log.sql.query.progress.exe\tQDB_LOG_SQL_QUERY_PROGRESS_EXE\ttrue\tdefault\tfalse\tfalse\n" +
                                    "log.level.verbose\tQDB_LOG_LEVEL_VERBOSE\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.statistics.enabled\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.raw.array.encoding.enabled\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.version\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_VERSION\t1\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.row.group.size\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE\t100000\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.data.page.size\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.compression.codec\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_CODEC\tZSTD\tdefault\tfalse\tfalse\n" +
                                    "cairo.partition.encoder.parquet.compression.level\tQDB_CAIRO_PARTITION_ENCODER_PARQUET_COMPRESSION_LEVEL\t9\tdefault\tfalse\tfalse\n" +
                                    "http.min.request.header.buffer.size\tQDB_HTTP_MIN_REQUEST_HEADER_BUFFER_SIZE\t4096\tdefault\tfalse\tfalse\n" +
                                    "http.min.allow.deflate.before.send\tQDB_HTTP_MIN_ALLOW_DEFLATE_BEFORE_SEND\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.min.multipart.header.buffer.size\tQDB_HTTP_MIN_MULTIPART_HEADER_BUFFER_SIZE\t512\tdefault\tfalse\tfalse\n" +
                                    "http.min.server.keep.alive\tQDB_HTTP_MIN_SERVER_KEEP_ALIVE\ttrue\tdefault\tfalse\tfalse\n" +
                                    "http.min.connection.string.pool.capacity\tQDB_HTTP_MIN_CONNECTION_STRING_POOL_CAPACITY\t2\tdefault\tfalse\tfalse\n" +
                                    "http.min.connection.pool.initial.capacity\tQDB_HTTP_MIN_CONNECTION_POOL_INITIAL_CAPACITY\t2\tdefault\tfalse\tfalse\n" +
                                    "http.min.multipart.idle.spin.count\tQDB_HTTP_MIN_MULTIPART_IDLE_SPIN_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "cairo.o3.partition.overwrite.control.enabled\tQDB_CAIRO_O3_PARTITION_OVERWRITE_CONTROL_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "http.min.worker.priority\tQDB_HTTP_MIN_WORKER_PRIORITY\t8\tdefault\tfalse\tfalse\n" +
                                    "cairo.commit.latency\tQDB_CAIRO_COMMIT_LATENCY\t30000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.create.table.column.model.pool.capacity\tQDB_CAIRO_CREATE_TABLE_COLUMN_MODEL_POOL_CAPACITY\t16\tdefault\tfalse\tfalse\n" +
                                    "log.timestamp.format\tQDB_LOG_TIMESTAMP_FORMAT\tyyyy-MM-ddTHH:mm:ss.SSSUUUz\tdefault\tfalse\tfalse\n" +
                                    "log.timestamp.locale\tQDB_LOG_TIMESTAMP_LOCALE\ten\tdefault\tfalse\tfalse\n" +
                                    "log.timestamp.timezone\tQDB_LOG_TIMESTAMP_TIMEZONE\tZ\tdefault\tfalse\tfalse\n" +
                                    "query.tracing.enabled\tQDB_QUERY_TRACING_ENABLED\tfalse\tdefault\tfalse\ttrue\n" +
                                    "http.json.query.connection.limit\tQDB_HTTP_JSON_QUERY_CONNECTION_LIMIT\t-1\tdefault\tfalse\ttrue\n" +
                                    "http.ilp.connection.limit\tQDB_HTTP_ILP_CONNECTION_LIMIT\t-1\tdefault\tfalse\ttrue\n" +
                                    "query.timeout\tQDB_QUERY_TIMEOUT\t60000\tdefault\tfalse\tfalse\n" +
                                    "http.context.table.status\tQDB_HTTP_CONTEXT_TABLE_STATUS\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.execute\tQDB_HTTP_CONTEXT_EXECUTE\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.ilp\tQDB_HTTP_CONTEXT_ILP\t\tdefault\tfalse\tfalse\n" +
                                    "http.redirect.count\tQDB_HTTP_REDIRECT_COUNT\t0\tdefault\tfalse\tfalse\n" +
                                    "http.context.import\tQDB_HTTP_CONTEXT_IMPORT\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.web.console\tQDB_HTTP_CONTEXT_WEB_CONSOLE\t/\tdefault\tfalse\tfalse\n" +
                                    "http.context.export\tQDB_HTTP_CONTEXT_EXPORT\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.ilp.ping\tQDB_HTTP_CONTEXT_ILP_PING\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.settings\tQDB_HTTP_CONTEXT_SETTINGS\t\tdefault\tfalse\tfalse\n" +
                                    "http.context.warnings\tQDB_HTTP_CONTEXT_WARNINGS\t\tdefault\tfalse\tfalse\n" +
                                    "http.server.cookies.enabled\tQDB_HTTP_SERVER_COOKIES_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "http.session.timeout\tQDB_HTTP_SESSION_TIMEOUT\t1800000000\tdefault\tfalse\tfalse\n" +
                                    "cairo.max.array.element.count\tQDB_CAIRO_MAX_ARRAY_ELEMENT_COUNT\t10000000\tdefault\tfalse\tfalse\n" +
                                    "telemetry.db.size.estimate.timeout\tQDB_TELEMETRY_DB_SIZE_ESTIMATE_TIMEOUT\t1000\tdefault\tfalse\tfalse\n" +
                                    "cairo.write.back.off.timeout.on.mem.pressure\tQDB_CAIRO_WRITE_BACK_OFF_TIMEOUT_ON_MEM_PRESSURE\t4000\tdefault\tfalse\tfalse\n" +
                                    "pg.pipeline.capacity\tQDB_PG_PIPELINE_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "query.within.latest.by.optimisation.enabled\tQDB_QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.preferences.string.pool.capacity\tQDB_CAIRO_PREFERENCES_STRING_POOL_CAPACITY\t64\tdefault\tfalse\tfalse\n" +
                                    "http.settings.readonly\tQDB_HTTP_SETTINGS_READONLY\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.alias.expression.enabled\tQDB_CAIRO_SQL_COLUMN_ALIAS_EXPRESSION_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.column.alias.generated.max.size\tQDB_CAIRO_SQL_COLUMN_ALIAS_GENERATED_MAX_SIZE\t64\tdefault\tfalse\tfalse\n" +
                                    "cairo.file.descriptor.cache.enabled\tQDB_CAIRO_FILE_DESCRIPTOR_CACHE_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.sql.jit.max.in.list.size.threshold\tQDB_CAIRO_SQL_JIT_MAX_IN_LIST_SIZE_THRESHOLD\t10\tdefault\tfalse\ttrue\n" +
                                    "cairo.auto.scale.symbol.capacity\tQDB_CAIRO_AUTO_SCALE_SYMBOL_CAPACITY\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.auto.scale.symbol.capacity.threshold\tQDB_CAIRO_AUTO_SCALE_SYMBOL_CAPACITY_THRESHOLD\t0.8\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.table.prefix\tQDB_CAIRO_PARQUET_EXPORT_TABLE_PREFIX\tzzz.copy.\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.statistics.enabled\tQDB_CAIRO_PARQUET_EXPORT_STATISTICS_ENABLED\ttrue\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.raw.array.encoding.enabled\tQDB_CAIRO_PARQUET_EXPORT_RAW_ARRAY_ENCODING_ENABLED\tfalse\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.version\tQDB_CAIRO_PARQUET_EXPORT_VERSION\t1\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.row.group.size\tQDB_CAIRO_PARQUET_EXPORT_ROW_GROUP_SIZE\t100000\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.data.page.size\tQDB_CAIRO_PARQUET_EXPORT_DATA_PAGE_SIZE\t1048576\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.compression.codec\tQDB_CAIRO_PARQUET_EXPORT_COMPRESSION_CODEC\tZSTD\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.compression.level\tQDB_CAIRO_PARQUET_EXPORT_COMPRESSION_LEVEL\t9\tdefault\tfalse\tfalse\n" +
                                    "cairo.parquet.export.copy.report.frequency.lines\tQDB_CAIRO_PARQUET_EXPORT_COPY_REPORT_FREQUENCY_LINES\t500000\tdefault\tfalse\ttrue\n" +
                                    "cairo.resource.pool.tracing.enabled\tQDB_CAIRO_RESOURCE_POOL_TRACING_ENABLED\tfalse\tdefault\tfalse\tfalse\n"
                    )
                            .split("\n");

                    final Set<String> missingProps = new HashSet<>();
                    for (String property : expectedProps) {
                        if (!actualProps.remove(property)) {
                            missingProps.add(property);
                        }
                    }
                    assertTrue(
                            String.format("Missing properties: %s\nExtra properties: %s", missingProps, actualProps),
                            missingProps.isEmpty() && actualProps.isEmpty()
                    );
                }
            }
        });
    }
}
