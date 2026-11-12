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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MetadataCacheReader;
import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterMetrics;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.cutlass.text.CopyExportContext;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.table.parquet.ParquetEncoding;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.ParquetTestUtils;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;


public class CopyExportTest extends AbstractCairoTest {

    // the copy_export_log rows of a streaming export that fails
    private static final String FAILED_STREAMING_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            stream_sending_data\tstarted
            stream_sending_data\tfailed
            """;
    // the 10_000-char symbol of sym_src, which createSymbolExportTables() creates
    private static final String LONG_SYMBOL = "L" + "z".repeat(9_999);
    // metric, c, total and ts of the tables createTableWithParquetSettings*() create
    private static final String PARQUET_SETTINGS_TABLE_ROWS = """
            (10, 1, 1000, '2024-01-01T00:00:00.000000Z'),
            (20, 2, 2000, '2024-01-01T08:00:00.000000Z'),
            (NULL, 3, NULL, '2024-01-01T16:00:00.000000Z'),
            (10, 4, 1000, '2024-01-02T00:00:00.000000Z'),
            (30, 5, 3000, '2024-01-02T08:00:00.000000Z'),
            (NULL, 6, NULL, '2024-01-02T16:00:00.000000Z'),
            (20, 7, 2000, '2024-01-03T00:00:00.000000Z'),
            (40, 8, 4000, '2024-01-03T08:00:00.000000Z'),
            (NULL, 9, NULL, '2024-01-03T16:00:00.000000Z')
            """;
    // the copy_export_log rows of a streaming export whose select, compiled again on the worker, needs a temp table
    private static final String REROUTED_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            stream_sending_data\tstarted
            populating_data_to_temp_table\tstarted
            populating_data_to_temp_table\tfinished
            converting_partitions\tstarted
            converting_partitions\tfinished
            move_files\tstarted
            move_files\tfinished
            dropping_temp_table\tstarted
            dropping_temp_table\tfinished
            success\tfinished
            """;
    // the rows of r and nr, which createRetypedTables() creates: two symbols alternate inside each of three days, and
    // other_ts holds NULL in two rows
    private static final String RETYPED_TABLE_ROWS = """
            ('a', 'p', 1, '2023-06-05T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
            ('b', 'q', 2, NULL, '2024-01-01T01:00:00.000000Z'),
            ('a', 'q', 3, '2023-06-03T00:00:00.000000Z', '2024-01-01T02:00:00.000000Z'),
            ('b', 'p', 4, '2023-06-02T00:00:00.000000Z', '2024-01-01T03:00:00.000000Z'),
            ('a', 'p', 5, '2023-06-09T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z'),
            ('b', 'q', 6, NULL, '2024-01-02T01:00:00.000000Z'),
            ('a', 'q', 7, '2023-06-07T00:00:00.000000Z', '2024-01-02T02:00:00.000000Z'),
            ('b', 'p', 8, '2023-06-06T00:00:00.000000Z', '2024-01-02T03:00:00.000000Z'),
            ('a', 'p', 9, '2023-06-10T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z'),
            ('b', 'q', 10, '2023-06-11T00:00:00.000000Z', '2024-01-03T01:00:00.000000Z')
            """;
    // the streaming exports of the table that createStaleExportTable() creates
    private static final StaleExportShape[] STALE_EXPORT_SHAPES = {
            new StaleExportShape(
                    "SELECT * FROM t",
                    ParquetExportMode.DIRECT_PAGE_FRAME,
                    "ts",
                    0,
                    """
                            x\ts\tts
                            3\ta\t2024-01-01T00:00:00.000000Z
                            2\tb\t2024-01-02T00:00:00.000000Z
                            1\ta\t2024-01-03T00:00:00.000000Z
                            """,
                    """
                            x\ts\tts\textra
                            3\ta\t2024-01-01T00:00:00.000000Z\tnull
                            2\tb\t2024-01-02T00:00:00.000000Z\tnull
                            1\ta\t2024-01-03T00:00:00.000000Z\tnull
                            """
            ),
            new StaleExportShape(
                    "SELECT x + 1 AS y, * FROM t",
                    ParquetExportMode.PAGE_FRAME_BACKED,
                    "ts",
                    1,
                    """
                            y\tx\ts\tts
                            4\t3\ta\t2024-01-01T00:00:00.000000Z
                            3\t2\tb\t2024-01-02T00:00:00.000000Z
                            2\t1\ta\t2024-01-03T00:00:00.000000Z
                            """,
                    """
                            y\tx\ts\tts\textra
                            4\t3\ta\t2024-01-01T00:00:00.000000Z\tnull
                            3\t2\tb\t2024-01-02T00:00:00.000000Z\tnull
                            2\t1\ta\t2024-01-03T00:00:00.000000Z\tnull
                            """
            ),
            new StaleExportShape(
                    "SELECT * FROM t ORDER BY x",
                    ParquetExportMode.CURSOR_BASED,
                    null,
                    0,
                    """
                            x\ts\tts
                            1\ta\t2024-01-03T00:00:00.000000Z
                            2\tb\t2024-01-02T00:00:00.000000Z
                            3\ta\t2024-01-01T00:00:00.000000Z
                            """,
                    """
                            x\ts\tts\textra
                            1\ta\t2024-01-03T00:00:00.000000Z\tnull
                            2\tb\t2024-01-02T00:00:00.000000Z\tnull
                            3\ta\t2024-01-01T00:00:00.000000Z\tnull
                            """
            ),
            new StaleExportShape(
                    "WITH c AS (SELECT * FROM t) SELECT * FROM c",
                    ParquetExportMode.DIRECT_PAGE_FRAME,
                    "ts",
                    0,
                    """
                            x\ts\tts
                            3\ta\t2024-01-01T00:00:00.000000Z
                            2\tb\t2024-01-02T00:00:00.000000Z
                            1\ta\t2024-01-03T00:00:00.000000Z
                            """,
                    """
                            x\ts\tts\textra
                            3\ta\t2024-01-01T00:00:00.000000Z\tnull
                            2\tb\t2024-01-02T00:00:00.000000Z\tnull
                            1\ta\t2024-01-03T00:00:00.000000Z\tnull
                            """
            ),
            // a projection over a filter, which has no page frames to pass through
            new StaleExportShape(
                    "SELECT x + 1 AS y, * FROM t WHERE x > 0",
                    ParquetExportMode.CURSOR_BASED,
                    "ts",
                    1,
                    """
                            y\tx\ts\tts
                            4\t3\ta\t2024-01-01T00:00:00.000000Z
                            3\t2\tb\t2024-01-02T00:00:00.000000Z
                            2\t1\ta\t2024-01-03T00:00:00.000000Z
                            """,
                    """
                            y\tx\ts\tts\textra
                            4\t3\ta\t2024-01-01T00:00:00.000000Z\tnull
                            3\t2\tb\t2024-01-02T00:00:00.000000Z\tnull
                            2\t1\ta\t2024-01-03T00:00:00.000000Z\tnull
                            """
            )
    };
    // the copy_export_log rows of a streaming export that succeeds
    private static final String STREAMING_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            stream_sending_data\tstarted
            stream_sending_data\tfinished
            move_files\tstarted
            move_files\tfinished
            success\tfinished
            """;
    // the copy_export_log rows of an export that COPY routes through a temp table when it queues it
    private static final String TEMP_TABLE_EXPORT_LOG = """
            phase\tstatus
            wait_to_run\tstarted
            wait_to_run\tfinished
            populating_data_to_temp_table\tstarted
            populating_data_to_temp_table\tfinished
            converting_partitions\tstarted
            converting_partitions\tfinished
            move_files\tstarted
            move_files\tfinished
            dropping_temp_table\tstarted
            dropping_temp_table\tfinished
            success\tfinished
            """;
    static HashSet<Class<?>> exceptionTypesToCatch = new HashSet<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        exportRoot = TestUtils.unchecked(() -> temp.newFolder("export").getAbsolutePath());
        inputRoot = exportRoot;
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_ROOT, exportRoot);
        staticOverrides.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        AbstractCairoTest.setUpStatic();
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, exportRoot);
        FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).$();
            if (ff.exists(path.$())) {
                ff.rmdir(path);
            }
        }
    }

    @Test
    public void testConcurrentInsertAndCopyPartitionFuzz() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table fuzz_table (ts timestamp, id long, value double, name string) timestamp(ts) partition by DAY WAL");

            StringBuilder initialInsert = new StringBuilder("insert into fuzz_table values ");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) initialInsert.append(", ");
                initialInsert.append("(")
                        .append(1000 + i)
                        .append(", ")
                        .append(i)
                        .append(", ")
                        .append(i * 1.5)
                        .append(", 'name")
                        .append(i)
                        .append("')");
            }
            execute(initialInsert);
            drainWalQueue();

            Thread insertThread = new Thread(() -> {
                try (SqlExecutionContext threadCtx = TestUtils.createSqlExecutionCtx(engine)) {
                    for (int batch = 0; batch < 50; batch++) {
                        StringBuilder batchInsert = new StringBuilder("insert into fuzz_table values (");
                        for (int i = 0; i < 100; i++) {
                            if (i > 0) batchInsert.append("), (");
                            long id = 20000 + (batch * 100) + i;
                            batchInsert.append(10000 + i)
                                    .append(", ")
                                    .append(id)
                                    .append(", ")
                                    .append(id * 2.0)
                                    .append(", 'concurrent")
                                    .append(id)
                                    .append('\'');
                        }

                        batchInsert.append(')');
                        engine.execute(batchInsert, threadCtx);
                        drainWalQueue();
//                        Os.sleep(10);
                    }
                } catch (Exception e) {
                    LOG.error().$("Unexpected error in test insert thread: ").$(e).$();
                } finally {
                    Path.clearThreadLocals();
                    Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
                }
            });

            CopyExportRunnable stmt = () -> {
                insertThread.start();
                Os.sleep(50);
                runAndFetchCopyExportID("copy fuzz_table to 'fuzz_output' with format parquet partition_by MONTH", sqlExecutionContext);
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        try {
                            insertThread.join();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "fuzz_output.parquet" + "\t1\tfinished\n");

                        // Verify exported data integrity - should have at least initial 10k records
                        String countQuery = "select count(*) from read_parquet('" + exportRoot + File.separator + "fuzz_output.parquet')";
                        try (
                                RecordCursorFactory factory = select(countQuery);
                                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                        ) {
                            assertTrue(cursor.hasNext());
                            long count = cursor.getRecord().getLong(0);
                            assertTrue(count >= 1000);
                        }

                        assertQuery("select id, value, name from read_parquet('" + exportRoot + File.separator + "fuzz_output.parquet') where id = 0")
                                .noLeakCheck()
                                .returns("""
                                        id\tvalue\tname
                                        0\t0.0\tname0
                                        """);
                        assertQuery("select id, value, name from read_parquet('" + exportRoot + File.separator + "fuzz_output.parquet') where id = 999")
                                .noLeakCheck()
                                .returns("""
                                        id\tvalue\tname
                                        999\t1498.5\tname999
                                        """);

                        assertQuery("SELECT path from export_files() order by modifiedTime")
                                .noLeakCheck()
                                .returns("""
                                        path
                                        fuzz_output.parquet
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyCancelSyntaxError() throws Exception {
        assertQuery("copy 'foobar' cancel aw beans;")
                .fails(21, "unexpected token [aw]");
    }

    @Test
    public void testCopyConvertedColumnExportAcrossExecutionModes() throws Exception {
        // The planner picks other factories for the same query under other settings, and the streamed export of each
        // writes the rows of the same query over nr.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            final FilesFacade ff = configuration.getFilesFacade();
            final String filterQuery = "SELECT * FROM %s WHERE x > 1";
            final String topKQuery = "SELECT * FROM %s WHERE x > 1 ORDER BY ts DESC, x LIMIT 3";
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                assertStreamedExportOfMode(filterQuery, "ts", "jit_filter", "Async JIT Filter");
                assertStreamedExportOfMode(topKQuery, null, "jit_top_k", "Async JIT Top K");
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertStreamedExportOfMode(filterQuery, "ts", "filter", "Async Filter");
                assertStreamedExportOfMode(topKQuery, null, "top_k", "Async Top K");
                sqlExecutionContext.setParallelFilterEnabled(false);
                sqlExecutionContext.setParallelTopKEnabled(false);
                assertStreamedExportOfMode(filterQuery, "ts", "serial_filter", "Filter filter: 1<x");
                assertStreamedExportOfMode(topKQuery, null, "sort_limited", "Encode sort light lo: 3");
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, false);
                assertStreamedExportOfMode(topKQuery, null, "sort_limited_light", "Sort light lo: 3");
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
                sqlExecutionContext.setParallelFilterEnabled(true);
                sqlExecutionContext.setParallelTopKEnabled(true);
                node1.setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, true);
            }

            // a filter on an indexed and on a plain SYMBOL column
            assertStreamedExportOfMode("SELECT * FROM %s WHERE s = 'b'", "ts", "indexed_symbol", "Index forward scan on: s");
            assertStreamedExportOfMode("SELECT * FROM %s WHERE s2 = 'q'", "ts", "plain_symbol", "filter: s2='q'");

            // rc covers x in its posting index, so the covering scan reads no retyped column, and the scan without it
            // reads y in its old type from the Parquet partition
            for (String tableName : new String[]{"rc", "nrc"}) {
                execute("CREATE TABLE " + tableName + " (s SYMBOL INDEX TYPE POSTING INCLUDE (x), x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + tableName + " SELECT s, x::INT, x::INT, ts FROM nr");
            }
            drainWalQueue();
            execute("ALTER TABLE rc CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE rc ALTER COLUMN y TYPE LONG");
            execute("ALTER TABLE nrc ALTER COLUMN y TYPE LONG");
            drainWalQueue();
            assertStreamedExportOfMode("SELECT s, x, ts FROM %s WHERE s = 'a'", "ts", "covering", "CoveringIndex on: s", "rc", "nrc");
            assertStreamedExportOfMode("SELECT /*+ no_covering */ s, x, y, ts FROM %s WHERE s = 'a'", "ts", "no_covering", "Index forward scan on: s", "rc", "nrc");

            // rows that land inside the Parquet partition after the retype, and a column added after the rows
            execute("INSERT INTO r VALUES ('a', 'q', 11, NULL, '2024-01-01T00:30:00.000000Z')");
            execute("INSERT INTO nr VALUES ('a', 'q', 11, NULL, '2024-01-01T00:30:00.000000Z')");
            drainWalQueue();
            assertStreamedExportOfMode("SELECT * FROM %s", "ts", "out_of_order", "Frame forward scan on: r");
            execute("ALTER TABLE r ADD COLUMN z INT");
            execute("ALTER TABLE nr ADD COLUMN z INT");
            execute("INSERT INTO r VALUES ('b', 'p', 12, NULL, '2024-01-03T02:00:00.000000Z', 7)");
            execute("INSERT INTO nr VALUES ('b', 'p', 12, NULL, '2024-01-03T02:00:00.000000Z', 7)");
            drainWalQueue();
            assertStreamedExportOfMode("SELECT * FROM %s", "ts", "added_column", "Frame forward scan on: r");
            assertQuery("SELECT count() FROM read_parquet('" + parquetPath("added_column") + "') WHERE z IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n11\n");

            // a backward scan of page frames of two rows each descends across the frame boundaries
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
            setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
            assertStreamedExportOfMode("SELECT * FROM %s ORDER BY ts DESC", null, "small_frames", "Frame backward scan on: r");
            ParquetTestUtils.assertColumnMaxDefLevel(parquetPath("small_frames"), ff, 4, 0);
        });
    }

    @Test
    public void testCopyConvertedColumnExportCancelled() throws Exception {
        // The user cancels a streamed export of a retyped table while the export compiles its stale select again: the
        // export stops as cancelled, leaves neither a file nor a temp directory behind, and the next export runs.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            final String exportId = queueCopy("COPY (SELECT * FROM r) TO 'cancelled' WITH FORMAT PARQUET");
            execute("ALTER TABLE r ADD COLUMN extra INT");
            execute("ALTER TABLE nr ADD COLUMN extra INT");
            drainWalQueue();
            final CopyExportContext copyContext = engine.getCopyExportContext();
            Assert.assertEquals(1, runExportJobCountingCompiles(
                    _ -> Assert.assertTrue(copyContext.cancel(copyContext.getActiveExportId(), null))
            ));
            assertExportLog(exportId, """
                    phase\tstatus
                    wait_to_run\tstarted
                    wait_to_run\tfinished
                    stream_sending_data\tstarted
                    stream_sending_data\tcancelled
                    """);
            assertExportFailure(exportId, "cancelled", "cancelled by user");
            Assert.assertFalse(exportFileExists("cancelled"));
            Assert.assertNull(engine.getTableTokenIfExists("copy." + exportId));
            assertNoTempDirOf(exportId);

            assertCopyStreamsRows("SELECT * FROM r", "after_cancel");
            assertParquetMatchesControl("SELECT * FROM nr", "after_cancel", "ts");
        });
    }

    @Test
    public void testCopyConvertedColumnExportFailedWriteCleansUp() throws Exception {
        // The second write of a streamed export of a retyped table fails: the export fails, leaves neither a file nor a
        // temp directory behind, and runs again.
        final AtomicInteger exportFileWriteCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            private volatile long exportFd = -1;

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, "tmp_") && Utf8s.endsWithAscii(name, "export.parquet")) {
                    exportFd = fd;
                }
                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == exportFd && exportFileWriteCount.incrementAndGet() == 2) {
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        };
        assertMemoryLeak(ff, () -> {
            createRetypedTables("WAL");
            final String copySql = "COPY (SELECT * FROM r) TO 'failed_write' WITH FORMAT PARQUET ROW_GROUP_SIZE 2";
            final String exportId = queueCopy(copySql);
            runExportJob();
            assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
            assertExportFailure(exportId, "failed", "failed to write parquet data to temp file");
            Assert.assertEquals(2, exportFileWriteCount.get());
            assertNoTempDirOf(exportId);
            Assert.assertFalse(exportFileExists("failed_write"));

            copyToParquet(copySql);
            assertParquetMatchesControl("SELECT * FROM nr", "failed_write", "ts");
            ParquetTestUtils.assertRowGroupCount(parquetPath("failed_write"), configuration.getFilesFacade(), 5);
        });
    }

    @Test
    public void testCopyConvertedColumnExportHitsQueryMemoryLimit() throws Exception {
        // A streamed export of a retyped table counts its buffers against the query memory limit: over the limit it
        // fails and leaves no temp directory behind, and without a limit it writes every row.
        assertMemoryLeak(() -> {
            for (String tableName : new String[]{"big", "nbig"}) {
                execute("CREATE TABLE " + tableName + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 100_000 rows on 2024-01-01 and one on 2024-01-02
                execute("INSERT INTO " + tableName + " SELECT x::VARCHAR, timestamp_sequence('2024-01-01', 864_000) FROM long_sequence(100_001)");
            }
            drainWalQueue();
            execute("ALTER TABLE big CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE big ALTER COLUMN v TYPE LONG");
            execute("ALTER TABLE nbig ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            final String query = "SELECT * FROM big";
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            final String exportId = queueCopy("COPY (" + query + ") TO 'over_limit' WITH FORMAT PARQUET");
            // the export's worker takes a tracker of this limit for the export
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 65_536);
            try {
                runExportJob();
            } finally {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
            }
            assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
            printSql("SELECT message FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "' AND status = 'failed'");
            Assert.assertTrue(sink.toString(), Chars.contains(sink, "query memory limit exceeded"));
            Assert.assertFalse(exportFileExists("over_limit"));
            assertNoTempDirOf(exportId);

            assertCopyStreamsRows(query, "no_limit");
            assertQueriesPrintAlike(
                    "SELECT count(), sum(v), min(v), max(v), min(ts), max(ts) FROM nbig",
                    "SELECT count(), sum(v), min(v), max(v), min(ts), max(ts) FROM read_parquet('" + parquetPath("no_limit") + "')"
            );
        });
    }

    @Test
    public void testCopyConvertedColumnExportOverManyRowGroups() throws Exception {
        // More rows than one page frame and one row group, two days of them in Parquet partitions of several row groups:
        // the streamed export writes all of them, ascending or descending, in row groups of ROW_GROUP_SIZE rows, or, with
        // the default, in a row group of 100_000 rows.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            for (String tableName : new String[]{"mg", "nmg"}) {
                execute("CREATE TABLE " + tableName + " (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 10_000 rows a day for 3 days; c holds 1 to 30_000
                execute("INSERT INTO " + tableName + " SELECT x::INT, timestamp_sequence('2024-01-01', 8_640_000) FROM long_sequence(30_000)");
            }
            drainWalQueue();
            execute("ALTER TABLE mg CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            execute("ALTER TABLE mg ALTER COLUMN c TYPE LONG");
            execute("ALTER TABLE nmg ALTER COLUMN c TYPE LONG");
            drainWalQueue();

            // the sum of 1 to 30_000 is 30_000 * 30_001 / 2; the last row lands 29_999 steps of 8.64 s after midnight
            final String aggregates = """
                    count\tsum\tmin\tmax\tmin1\tmax1
                    30000\t450015000\t1\t30000\t2024-01-01T00:00:00.000000Z\t2024-01-03T23:59:51.360000Z
                    """;
            assertCopyStreamsRows("SELECT * FROM mg", "row_groups", " ROW_GROUP_SIZE 1000");
            ParquetTestUtils.assertRowGroupCount(parquetPath("row_groups"), ff, 30);
            assertParquetMatchesControl("SELECT * FROM nmg", "row_groups", "ts");
            assertQuery("SELECT count(), sum(c), min(c), max(c), min(ts), max(ts) FROM read_parquet('" + parquetPath("row_groups") + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(aggregates);

            assertCopyStreamsRows("SELECT * FROM mg", "default_row_group", " ROW_GROUP_SIZE 0");
            ParquetTestUtils.assertRowGroupCount(parquetPath("default_row_group"), ff, 1);
            assertParquetMatchesControl("SELECT * FROM nmg", "default_row_group", "ts");

            assertCopyStreamsRows("SELECT * FROM mg ORDER BY ts DESC", "descending_row_groups", " ROW_GROUP_SIZE 1000");
            ParquetTestUtils.assertRowGroupCount(parquetPath("descending_row_groups"), ff, 30);
            assertParquetMatchesControl("SELECT * FROM nmg ORDER BY ts DESC", "descending_row_groups", null);
            assertQuery("SELECT count(), sum(c), min(c), max(c), min(ts), max(ts) FROM read_parquet('" + parquetPath("descending_row_groups") + "')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(aggregates);
        });
    }

    @Test
    public void testCopyCursorExportKeepsMemoizedSymbolAlias() throws Exception {
        // With function memoization, as in production, the outer projection reads the alias through a memoizer over the
        // inner projection's column, which passes the table's keys through.
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            createSymbolExportTables("BYPASS WAL");
            final String query = "SELECT a, a || 'x' c FROM (SELECT sym AS a, v + 1 AS vp FROM sym_src CROSS JOIN long_sequence(1))";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("memoize(a)");
            assertCursorExportOfSymbols(query, "memoized", new int[]{0}, new int[]{});
        });
    }

    @Test
    public void testCopyCursorExportKeepsSymbolValues() throws Exception {
        // A CURSOR_BASED export keeps a table's SYMBOL columns as SYMBOL and encodes them as the DIRECT export of the
        // same columns does: the same values, and the same encodings in every column.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createSymbolExportTables(walMode);
                final String query = "SELECT t.* FROM sym_src t CROSS JOIN long_sequence(1)";
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                assertExportMode("SELECT * FROM sym_src", ParquetExportMode.DIRECT_PAGE_FRAME);
                copyToParquet("COPY (SELECT * FROM sym_src) TO 'sym_direct' WITH FORMAT PARQUET");
                copyToParquet("COPY (" + query + ") TO 'sym_cursor' WITH FORMAT PARQUET");

                final String parquetPath = parquetPath("sym_cursor");
                assertParquetMatchesQuery(query, parquetPath);
                assertQuery("SELECT sym, sym IS NULL sym_is_null, k, k IS NULL k_is_null, v, ts FROM read_parquet('" + parquetPath + "')")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                sym\tsym_is_null\tk\tk_is_null\tv\tts
                                a\tfalse\tk1\tfalse\t1\t2024-01-01T00:00:00.000000Z
                                a\tfalse\tk2\tfalse\t9\t2024-01-01T00:30:00.000000Z
                                \ttrue\tk2\tfalse\t2\t2024-01-01T01:00:00.000000Z
                                \tfalse\tk1\tfalse\t3\t2024-01-01T02:00:00.000000Z
                                é\tfalse\tk2\tfalse\t4\t2024-01-02T00:00:00.000000Z
                                Ж\tfalse\t\ttrue\t5\t2024-01-02T01:00:00.000000Z
                                €\tfalse\tk1\tfalse\t6\t2024-01-02T02:00:00.000000Z
                                😀\tfalse\tk2\tfalse\t7\t2024-01-03T00:00:00.000000Z
                                """ + LONG_SYMBOL + "\tfalse\tk1\tfalse\t8\t2024-01-03T01:00:00.000000Z\n");
                assertSymbolColumnsKept(parquetPath, 0, 1);
                ParquetTestUtils.assertColumnEncodingsMatch(parquetPath("sym_direct"), parquetPath, ff);
                ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, ff, 0, 1, 2);
                execute("DROP TABLE sym_src");
                execute("DROP TABLE sym_dim");
            }
        });
    }

    @Test
    public void testCopyCursorExportOfDescendingPlanMarksNoAscendingTimestamp() throws Exception {
        // COPY gives the cursor writer the direction of the plan it streams, as /exp does: rows that descend by the
        // timestamp are not marked as ascending, so read_parquet() designates no timestamp and sorts by it on ORDER BY.
        // A plan whose rows ascend keeps its designated timestamp, and so does a plan that promises no order.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createRetypedTables(walMode);
                final String[] descendingTemplates = {
                        "SELECT * FROM %s WHERE s = 'a' ORDER BY ts DESC",
                        "SELECT * FROM %s WHERE x > 1 ORDER BY ts DESC LIMIT 3",
                        "SELECT * FROM %s WHERE s IN ('a', 'b') ORDER BY ts DESC",
                        "SELECT * FROM %s ORDER BY ts DESC LIMIT -2",
                        "SELECT a.* FROM (SELECT * FROM %s ORDER BY ts DESC) a CROSS JOIN long_sequence(1)",
                };
                final String[] ascendingTemplates = {
                        "SELECT * FROM %s WHERE s = 'a'",
                        "SELECT * FROM %s WHERE x > 1 LIMIT 3",
                        "SELECT * FROM %s WHERE s IN ('a', 'b')",
                };
                for (String tableName : new String[]{"nr", "r"}) {
                    for (int i = 0; i < descendingTemplates.length; i++) {
                        final String query = descendingTemplates[i].replace("%s", tableName);
                        final String controlQuery = descendingTemplates[i].replace("%s", "nr");
                        final String fileName = "descending_" + tableName + '_' + i;
                        try {
                            assertCopyStreamsRows(query, fileName);
                            assertParquetMatchesControl(controlQuery, fileName, null);
                            assertQueriesPrintAlike(
                                    "SELECT ts FROM (" + controlQuery + ") ORDER BY ts",
                                    "SELECT ts FROM read_parquet('" + parquetPath(fileName) + "') ORDER BY ts"
                            );
                        } catch (AssertionError e) {
                            throw new AssertionError(walMode + ": " + query, e);
                        }
                    }
                    for (int i = 0; i < ascendingTemplates.length; i++) {
                        final String query = ascendingTemplates[i].replace("%s", tableName);
                        final String fileName = "ascending_" + tableName + '_' + i;
                        try {
                            assertCopyStreamsRows(query, fileName);
                            assertParquetMatchesControl(ascendingTemplates[i].replace("%s", "nr"), fileName, "ts");
                        } catch (AssertionError e) {
                            throw new AssertionError(walMode + ": " + query, e);
                        }
                    }
                }
                dropRetypedTables();
            }

            // The step of generate_series() comes from a bind variable, so the plan promises no order; the rows
            // ascend, and the file designates the timestamp, as before.
            bindVariableService.clear();
            bindVariableService.setLong(0, 86_400_000_000L);
            final String seriesQuery = "SELECT * FROM generate_series('2024-01-01'::timestamp, '2024-01-03'::timestamp, $1)";
            try (RecordCursorFactory factory = select(seriesQuery)) {
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_OTHER, factory.getScanDirection());
            }
            assertCopyStreamsRows(seriesQuery, "series");
            assertParquetRows("series", "generate_series", """
                    generate_series
                    2024-01-01T00:00:00.000000Z
                    2024-01-02T00:00:00.000000Z
                    2024-01-03T00:00:00.000000Z
                    """);
            bindVariableService.clear();
        });
    }

    @Test
    public void testCopyCursorExportOfMatViewKeepsSymbol() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolExportTables("WAL");
            execute("CREATE MATERIALIZED VIEW sym_mv AS (SELECT ts, sym, count() c FROM sym_src SAMPLE BY 1d) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertCursorExportOfSymbols("SELECT m.* FROM sym_mv m CROSS JOIN long_sequence(1)", "mat_view", new int[]{1}, new int[]{});
        });
    }

    @Test
    public void testCopyCursorExportSymbolBloomFilter() throws Exception {
        // The file holds 'a' and 'c' of a symbol map that also holds 'b', so its min and max cannot rule out 'b', and
        // only a bloom filter of the symbols the row group uses lets a reader skip the row group. read_parquet() reads
        // a SYMBOL column of a file as VARCHAR and pushes no filter down to it, so the test asks the row group
        // pruning that a table's Parquet partitions run for a SYMBOL column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bl (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO bl VALUES
                        ('a', 1, '2024-01-01T00:00:00.000000Z'),
                        ('b', 2, '2024-01-01T01:00:00.000000Z'),
                        ('c', 3, '2024-01-01T02:00:00.000000Z')
                    """);
            final String query = "SELECT b.sym, b.v FROM bl b CROSS JOIN long_sequence(1) WHERE b.v <> 2";
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            for (boolean hasBloomFilter : new boolean[]{true, false}) {
                final String fileName = hasBloomFilter ? "bloom" : "no_bloom";
                copyToParquet("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + (hasBloomFilter ? " BLOOM_FILTER_COLUMNS 'sym'" : ""));
                final String parquetPath = parquetPath(fileName);
                assertParquetMatchesQuery(query, parquetPath);
                assertSymbolColumnsKept(parquetPath, 0);

                final FilesFacade ff = configuration.getFilesFacade();
                Assert.assertEquals(hasBloomFilter, ParquetTestUtils.canSkipRowGroupOnSymbolEquals(parquetPath, ff, 0, "sym", "b"));
                // The bloom filter holds the symbols the row group uses.
                Assert.assertFalse(ParquetTestUtils.canSkipRowGroupOnSymbolEquals(parquetPath, ff, 0, "sym", "a"));
                Assert.assertFalse(ParquetTestUtils.canSkipRowGroupOnSymbolEquals(parquetPath, ff, 0, "sym", "c"));
                assertQuery("SELECT sym FROM read_parquet('" + parquetPath + "') WHERE sym = 'b'")
                        .noLeakCheck()
                        .returns("sym\n");
                assertQuery("SELECT sym, v FROM read_parquet('" + parquetPath + "') WHERE sym = 'a'")
                        .noLeakCheck()
                        .returns("sym\tv\na\t1\n");
            }
        });
    }

    @Test
    public void testCopyCursorExportSymbolCancel() throws Exception {
        // A user cancels the export of a kept SYMBOL column once it has streamed rows: the export stops in the phase
        // that streams its file, leaves neither a file nor a temp directory behind, and the next export of the
        // column runs.
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            CopyExportRunnable stmt = () -> runAndFetchCopyExportID(
                    "COPY (SELECT t.sym, t.k, x.x FROM sym_src t CROSS JOIN long_sequence(1_000_000_000) x) TO 'cancelled' WITH FORMAT PARQUET ROW_GROUP_SIZE 1000",
                    sqlExecutionContext
            );
            final AtomicReference<String> cancelledExportId = new AtomicReference<>();
            CopyExportRunnable test = () -> {
                try {
                    final long copyID = ParquetTestUtils.awaitExportStreamedRows(engine);
                    final StringSink sink = new StringSink();
                    Numbers.appendHex(sink, copyID, true);
                    final String copyIDStr = sink.toString();
                    cancelledExportId.set(copyIDStr);
                    assertQuery("COPY '" + copyIDStr + "' CANCEL")
                            .noLeakCheck()
                            .returnsOnce("id\tstatus\n" + copyIDStr + "\tcancelled\n");
                    awaitExportsDrained();
                } finally {
                    Path.clearThreadLocals();
                }
            };
            testCopyExport(stmt, test, false, 1);
            final String exportId = cancelledExportId.get();
            assertExportLog(exportId, """
                    phase\tstatus
                    wait_to_run\tstarted
                    wait_to_run\tfinished
                    stream_sending_data\tstarted
                    stream_sending_data\tcancelled
                    """);
            assertExportFailure(exportId, "cancelled", "cancelled by user");
            Assert.assertFalse(exportFileExists("cancelled"));
            assertNoTempDirOf(exportId);

            assertCursorExportOfSymbols("SELECT t.sym, t.k, x.x FROM sym_src t CROSS JOIN long_sequence(3) x", "after_cancel", new int[]{0, 1}, new int[]{});
        });
    }

    @Test
    public void testCopyCursorExportSymbolDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");

            // An empty result writes a file without row groups that still types the column.
            final String emptyQuery = "SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1) WHERE t.v > 100";
            assertCursorExportOfSymbols(emptyQuery, "empty", new int[]{0}, new int[]{});
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("empty") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tv\n");

            final String singleRowQuery = "SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1) WHERE t.v = 7";
            assertCursorExportOfSymbols(singleRowQuery, "single_row", new int[]{0}, new int[]{});
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("single_row") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tv\n😀\t7\n");

            // A column of NULLs over an empty symbol map: late is added after the rows and never set.
            execute("ALTER TABLE sym_src ADD COLUMN late SYMBOL");
            final String emptyMapQuery = "SELECT t.v, t.late FROM sym_src t CROSS JOIN long_sequence(1) WHERE t.v < 4";
            assertCursorExportOfSymbols(emptyMapQuery, "empty_map", new int[]{1}, new int[]{});
            assertQuery("SELECT v, late, late IS NULL late_is_null FROM read_parquet('" + parquetPath("empty_map") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tlate\tlate_is_null
                            1\t\ttrue
                            2\t\ttrue
                            3\t\ttrue
                            """);

            // A column of NULLs over a symbol map that holds symbols
            final String nullRowsQuery = "SELECT t.v, t.sym FROM sym_src t CROSS JOIN long_sequence(1) WHERE t.sym IS NULL";
            assertCursorExportOfSymbols(nullRowsQuery, "null_rows", new int[]{1}, new int[]{});
            assertQuery("SELECT v, sym, sym IS NULL sym_is_null FROM read_parquet('" + parquetPath("null_rows") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tsym\tsym_is_null
                            2\t\ttrue
                            """);
        });
    }

    @Test
    public void testCopyCursorExportSymbolDictionaryHoldsOnlyUsedSymbols() throws Exception {
        // A kept SYMBOL column's dictionary holds the symbols its row group uses, so the file of a few rows stays small
        // whatever keys they have. hs holds 1_000_000 symbols, s1 to s1000000 with keys 0 to 999_999; a dictionary of
        // every key up to the largest used one writes at least 4 bytes per key, 4_000_000 bytes for s1000000.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE hs (s SYMBOL CAPACITY 1_048_576 NOCACHE, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO hs SELECT 's' || x, x, (x * 1000)::TIMESTAMP FROM long_sequence(1_000_000)");
            execute("CREATE TABLE one (v LONG)");
            execute("INSERT INTO one VALUES (1_000_000)");
            final String[] queries = {
                    "SELECT * FROM hs ORDER BY ts DESC LIMIT 1",
                    "SELECT * FROM hs WHERE v = 1_000_000",
                    "SELECT s, count() c FROM hs WHERE v > 999_998",
                    "SELECT h.s, h.v FROM hs h JOIN one o ON h.v = o.v"
            };
            for (int i = 0; i < queries.length; i++) {
                assertCursorExportOfSymbols(queries[i], "few_rows_" + i, new int[]{0}, new int[]{}, " COMPRESSION_CODEC UNCOMPRESSED");
                final long fileSize = parquetFileSize("few_rows_" + i);
                Assert.assertTrue(queries[i] + ", file size: " + fileSize, fileSize < 10_000);
            }
            // ORDER BY ts DESC scans backward, so the file designates no timestamp
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("few_rows_0") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv\tts
                            s1000000\t1000000\t1970-01-01T00:16:40.000000Z
                            """);

            // Three row groups of two rows, each with its own dictionary
            final String rowGroupsQuery = "SELECT * FROM hs WHERE v IN (1, 2, 3, 500_000, 999_998, 999_999) ORDER BY v DESC";
            assertCursorExportOfSymbols(rowGroupsQuery, "row_groups", new int[]{0}, new int[]{}, " COMPRESSION_CODEC UNCOMPRESSED ROW_GROUP_SIZE 2");
            ParquetTestUtils.assertRowGroupCount(parquetPath("row_groups"), configuration.getFilesFacade(), 3);
            final long fileSize = parquetFileSize("row_groups");
            Assert.assertTrue("file size: " + fileSize, fileSize < 10_000);
            assertQuery("SELECT s, v FROM read_parquet('" + parquetPath("row_groups") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tv
                            s999999\t999999
                            s999998\t999998
                            s500000\t500000
                            s3\t3
                            s2\t2
                            s1\t1
                            """);
        });
    }

    @Test
    public void testCopyCursorExportSymbolExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            final FilesFacade ff = configuration.getFilesFacade();

            // Parallel and serial GROUP BY keep the keys and their PARQUET(...) settings.
            for (boolean isParallel : new boolean[]{true, false}) {
                sqlExecutionContext.setParallelGroupByEnabled(isParallel);
                try {
                    final String suffix = isParallel ? "parallel" : "serial";
                    assertCursorExportOfSymbols("SELECT sym, k, count() c FROM sym_src", "keys_" + suffix, new int[]{0, 1}, new int[]{});
                    assertCursorExportOfSymbols("SELECT sym, count() c FROM sym_src", "key_" + suffix, new int[]{0}, new int[]{});
                    copyToParquet("COPY (SELECT v, count() c FROM sym_src) TO 'int_key_" + suffix + "' WITH FORMAT PARQUET");
                    assertParquetMatchesQueryInAnyOrder("SELECT v, count() c FROM sym_src", parquetPath("int_key_" + suffix));
                    ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath("int_key_" + suffix), ff, 0);
                } finally {
                    sqlExecutionContext.setParallelGroupByEnabled(true);
                }
            }

            // A table without partitions
            execute("CREATE TABLE np (sym SYMBOL, v INT)");
            execute("INSERT INTO np VALUES ('p', 1), (NULL, 2), ('q', 3), ('p', 4)");
            assertCursorExportOfSymbols("SELECT n.* FROM np n CROSS JOIN long_sequence(2)", "no_partitions", new int[]{0}, new int[]{});

            // A column added after data exists holds NULL above its column top.
            execute("CREATE TABLE added (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO added VALUES (1, '2024-01-01T00:00:00.000000Z'), (2, '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE added ADD COLUMN late SYMBOL");
            execute("INSERT INTO added VALUES (3, '2024-01-02T01:00:00.000000Z', 'l1'), (4, '2024-01-03T00:00:00.000000Z', NULL), (5, '2024-01-03T01:00:00.000000Z', 'l2')");
            assertCursorExportOfSymbols("SELECT a.* FROM added a CROSS JOIN long_sequence(1)", "added_column", new int[]{2}, new int[]{});
            assertQuery("SELECT v, late, late IS NULL late_is_null FROM read_parquet('" + parquetPath("added_column") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tlate\tlate_is_null
                            1\t\ttrue
                            2\t\ttrue
                            3\tl1\tfalse
                            4\t\ttrue
                            5\tl2\tfalse
                            """);

            // Parquet partitions decode the table's own symbol keys.
            execute("ALTER TABLE sym_src CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
            drainWalQueue();
            assertCursorExportOfSymbols("SELECT t.* FROM sym_src t CROSS JOIN long_sequence(1)", "parquet_cross_join", new int[]{0, 1}, new int[]{});
            assertCursorExportOfSymbols("SELECT sym, k, count() c FROM sym_src", "parquet_group_by", new int[]{0, 1}, new int[]{});
            assertCursorExportOfSymbols("SELECT * FROM sym_src ORDER BY v DESC", "parquet_order_by", new int[]{0, 1}, new int[]{});
        });
    }

    @Test
    public void testCopyCursorExportSymbolFailedWriteCleansUp() throws Exception {
        // The second write of the export file fails: the export fails, leaves no temp directory behind, and the same
        // export runs again.
        final AtomicInteger exportFileWriteCount = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            private volatile long exportFd = -1;

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, "tmp_") && Utf8s.endsWithAscii(name, "export.parquet")) {
                    exportFd = fd;
                }
                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == exportFd && exportFileWriteCount.incrementAndGet() == 2) {
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        };
        assertMemoryLeak(ff, () -> {
            createSymbolExportTables("BYPASS WAL");
            final String query = "SELECT t.sym, t.k, t.v FROM sym_src t CROSS JOIN long_sequence(1)";
            final String copySql = "COPY (" + query + ") TO 'failed_write' WITH FORMAT PARQUET ROW_GROUP_SIZE 2";
            runAndFetchCopyExportID(copySql, sqlExecutionContext);
            runExportJob();
            assertQuery("SELECT status, message FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\tmessage\nfailed\tfailed to write parquet data to temp file\n");
            Assert.assertEquals(2, exportFileWriteCount.get());
            final File[] tmpDirs = new File(exportRoot).listFiles(f -> f.isDirectory() && f.getName().startsWith("tmp_"));
            Assert.assertTrue("temp directory must be removed", tmpDirs == null || tmpDirs.length == 0);
            Assert.assertFalse(exportFileExists("failed_write"));

            copyToParquet(copySql);
            assertParquetMatchesQuery(query, parquetPath("failed_write"));
            assertSymbolColumnsKept(parquetPath("failed_write"), 0, 1);
            ParquetTestUtils.assertRowGroupCount(parquetPath("failed_write"), configuration.getFilesFacade(), 5);
        });
    }

    @Test
    public void testCopyCursorExportSymbolFilterWithJitOnAndOff() throws Exception {
        // A filter on a non-indexed symbol, compiled by the JIT or not. The JIT does not compile a comparison with an
        // empty symbol, so the filter selects the row of '' by v.
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            final String query = "SELECT * FROM sym_src WHERE sym = '😀' OR v = 3";
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (boolean isJitEnabled : new boolean[]{true, false}) {
                    sqlExecutionContext.setJitMode(isJitEnabled ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    assertQuery(query)
                            .noLeakCheck()
                            .assertsPlanContaining(isJitEnabled ? "Async JIT Filter" : "Async Filter");
                    final String fileName = isJitEnabled ? "jit_filter" : "filter";
                    assertCursorExportOfSymbols(query, fileName, new int[]{0, 1}, new int[]{});
                    assertQuery("SELECT sym, sym IS NULL sym_is_null, k, v FROM read_parquet('" + parquetPath(fileName) + "')")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    sym\tsym_is_null\tk\tv
                                    \tfalse\tk1\t3
                                    😀\tfalse\tk2\t7
                                    """);
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    @Test
    public void testCopyCursorExportSymbolKeepsParquetCompression() throws Exception {
        // A kept SYMBOL column takes the compression of its PARQUET(...) setting, and so does a GROUP BY or DISTINCT
        // key over it. The 100_000 distinct 66-byte symbols take more than 6 MB as an uncompressed dictionary, and the
        // ZSTD(3) setting of zsym compresses their common prefixes to well under 2 MB.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE zsym (sym SYMBOL CAPACITY 131_072 NOCACHE INDEX TYPE POSTING PARQUET(default, ZSTD(3)), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO zsym SELECT 'symbol_' || lpad(x::STRING, 59, '0'), timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(100_000)");
            execute("CREATE TABLE psym (sym SYMBOL CAPACITY 131_072 NOCACHE INDEX TYPE POSTING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO psym SELECT sym, ts FROM zsym");
            final String virtualProjectionTemplate = "SELECT t.sym, true b FROM %s t CROSS JOIN long_sequence(1)";
            try (RecordCursorFactory factory = select(virtualProjectionTemplate.replace("%s", "zsym"))) {
                Assert.assertTrue(
                        "the export must set up a virtual factory with setUpCursorBacked()",
                        ParquetExportMode.unwrapFactory(factory) instanceof VirtualRecordCursorFactory
                );
            }
            final String[] queryTemplates = {
                    // HybridColumnMaterializer.setUp()
                    "SELECT t.sym FROM %s t CROSS JOIN long_sequence(1)",
                    // HybridColumnMaterializer.setUpCursorBacked(), where a kept column takes the setting of the base
                    // column it references
                    virtualProjectionTemplate,
                    // a key of the vectorized GROUP BY
                    "SELECT sym, count() c FROM %s",
                    // a key of the async GROUP BY, over 100_000 distinct symbols
                    "SELECT sym, first(ts) f FROM %s",
                    // a key of the posting index DISTINCT
                    "SELECT DISTINCT sym FROM %s"
            };
            for (int i = 0; i < queryTemplates.length; i++) {
                for (String tableName : new String[]{"zsym", "psym"}) {
                    final String fileName = tableName + "_" + i;
                    assertCursorExportOfSymbols(queryTemplates[i].replace("%s", tableName), fileName, new int[]{0}, new int[]{}, " COMPRESSION_CODEC UNCOMPRESSED");
                }
                final long compressedSize = parquetFileSize("zsym_" + i);
                Assert.assertTrue(queryTemplates[i] + ": sym must be compressed, file size: " + compressedSize, compressedSize < 2_000_000);
                final long uncompressedSize = parquetFileSize("psym_" + i);
                Assert.assertTrue(queryTemplates[i] + ": sym without a setting must not be compressed, file size: " + uncompressedSize, uncompressedSize >= 6_000_000);
            }
        });
    }

    @Test
    public void testCopyCursorExportSymbolLegacySampleByFirstLast() throws Exception {
        // The legacy SAMPLE BY of first() and last() over an indexed symbol filter reads the stored keys of the bucket's
        // first and last rows, and serves the table's symbol map, so the export keeps them as SYMBOL. The same query
        // without ALIGN TO FIRST OBSERVATION runs as GROUP BY, whose first() and last() compute their symbols.
        assertMemoryLeak(() -> {
            createFirstLastTable();
            final String query = "SELECT ts, first(s) f, last(s) l FROM fl WHERE k = 'x' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            assertQuery(query)
                    .noLeakCheck()
                    .assertsPlanContaining("SampleByFirstLast");
            assertCursorExportOfSymbols(query, "legacy_first_last", new int[]{1, 2}, new int[]{});
            assertQuery("SELECT ts, f, f IS NULL f_is_null, l, l IS NULL l_is_null FROM read_parquet('" + parquetPath("legacy_first_last") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tf\tf_is_null\tl\tl_is_null
                            2024-01-01T00:00:00.000000Z\ta\tfalse\t\ttrue
                            2024-01-01T01:00:00.000000Z\tc\tfalse\tc\tfalse
                            2024-01-02T00:00:00.000000Z\td\tfalse\td\tfalse
                            2024-01-02T03:00:00.000000Z\t\ttrue\t\ttrue
                            2024-01-02T04:00:00.000000Z\tf\tfalse\tg\tfalse
                            """);

            // With a key and a column added after the rows
            final String keyedQuery = "SELECT ts, k, first(s) f, last(s) l, last(late) ll FROM fl WHERE k = 'x' SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            assertQuery(keyedQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("SampleByFirstLast");
            assertCursorExportOfSymbols(keyedQuery, "legacy_first_last_keyed", new int[]{1, 2, 3, 4}, new int[]{});

            final String groupByQuery = "SELECT ts, first(s) f, last(s) l FROM fl WHERE k = 'x' SAMPLE BY 1h";
            assertCursorExportOfSymbols(groupByQuery, "group_by_first_last", new int[]{}, new int[]{1, 2});
        });
    }

    @Test
    public void testCopyCursorExportSymbolNullsWithoutNullInMap() throws Exception {
        // sym_dim's symbol map holds no NULL, yet the outer joins return NULL for the rows they do not match.
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            final String leftJoinQuery = "SELECT t.v, t.sym, d.sym dsym, d.w FROM sym_src t LEFT JOIN sym_dim d ON t.sym = d.sym ORDER BY t.v";
            assertCursorExportOfSymbols(leftJoinQuery, "left_join", new int[]{1, 2}, new int[]{});
            assertQuery("SELECT v, sym, dsym, dsym IS NULL dsym_is_null, w FROM read_parquet('" + parquetPath("left_join") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tsym\tdsym\tdsym_is_null\tw
                            1\ta\ta\tfalse\t10
                            2\t\t\ttrue\tnull
                            3\t\t\ttrue\tnull
                            4\té\té\tfalse\t40
                            5\tЖ\t\ttrue\tnull
                            6\t€\t\ttrue\tnull
                            7\t😀\t😀\tfalse\t70
                            """ + "8\t" + LONG_SYMBOL + "\t\ttrue\tnull\n" + """
                            9\ta\ta\tfalse\t10
                            """);

            final String rightJoinQuery = "SELECT t.v, t.sym, d.sym dsym, d.w FROM sym_src t RIGHT JOIN sym_dim d ON t.sym = d.sym ORDER BY d.w, t.v";
            assertCursorExportOfSymbols(rightJoinQuery, "right_join", new int[]{1, 2}, new int[]{});
            assertQuery("SELECT v, sym, sym IS NULL sym_is_null, dsym, w FROM read_parquet('" + parquetPath("right_join") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tsym\tsym_is_null\tdsym\tw
                            1\ta\tfalse\ta\t10
                            9\ta\tfalse\ta\t10
                            4\té\tfalse\té\t40
                            7\t😀\tfalse\t😀\t70
                            null\t\ttrue\tzz\t99
                            """);

            final String fullJoinQuery = "SELECT t.v, t.sym, d.sym dsym, d.w FROM sym_src t FULL JOIN sym_dim d ON t.sym = d.sym ORDER BY t.v, d.w";
            assertCursorExportOfSymbols(fullJoinQuery, "full_join", new int[]{1, 2}, new int[]{});
            assertQuery("SELECT v, sym, sym IS NULL sym_is_null, dsym, dsym IS NULL dsym_is_null, w FROM read_parquet('" + parquetPath("full_join") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\tsym\tsym_is_null\tdsym\tdsym_is_null\tw
                            null\t\ttrue\tzz\tfalse\t99
                            1\ta\tfalse\ta\tfalse\t10
                            2\t\ttrue\t\ttrue\tnull
                            3\t\tfalse\t\ttrue\tnull
                            4\té\tfalse\té\tfalse\t40
                            5\tЖ\tfalse\t\ttrue\tnull
                            6\t€\tfalse\t\ttrue\tnull
                            7\t😀\tfalse\t😀\tfalse\t70
                            """ + "8\t" + LONG_SYMBOL + "\tfalse\t\ttrue\tnull\n" + """
                            9\ta\tfalse\ta\tfalse\t10
                            """);
        });
    }

    @Test
    public void testCopyCursorExportSymbolReusesExporterAcrossExports() throws Exception {
        // One export job runs four exports in turn: a kept SYMBOL column at column 0, a PAGE_FRAME_BACKED export, an INT
        // at column 0 and a kept SYMBOL column at column 2 of another table, and no SYMBOL column. Each export sets the
        // columns up afresh.
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            final String[] queries = {
                    "SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1)",
                    "SELECT sym, v + 1 vp FROM sym_src",
                    "SELECT d.w, d.ts, d.sym FROM sym_dim d CROSS JOIN long_sequence(1)",
                    "SELECT t.v, t.ts FROM sym_src t CROSS JOIN long_sequence(1)"
            };
            assertExportMode(queries[0], ParquetExportMode.CURSOR_BASED);
            assertExportMode(queries[1], ParquetExportMode.PAGE_FRAME_BACKED);
            assertExportMode(queries[2], ParquetExportMode.CURSOR_BASED);
            assertExportMode(queries[3], ParquetExportMode.CURSOR_BASED);
            for (int i = 0; i < queries.length; i++) {
                runAndFetchCopyExportID("COPY (" + queries[i] + ") TO 'reuse_" + i + "' WITH FORMAT PARQUET", sqlExecutionContext);
            }
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                for (int i = 0; i < queries.length; i++) {
                    Assert.assertTrue("the queued export must be picked up", job.run());
                }
            }
            for (int i = 0; i < queries.length; i++) {
                assertParquetMatchesQuery(queries[i], parquetPath("reuse_" + i));
            }
            assertSymbolColumnsKept(parquetPath("reuse_0"), 0);
            assertSymbolColumnsKept(parquetPath("reuse_1"), 0);
            assertSymbolColumnsKept(parquetPath("reuse_2"), 2);
            ParquetTestUtils.assertColumnTypes(parquetPath("reuse_2"), configuration.getFilesFacade(), ColumnType.INT, 0);
            ParquetTestUtils.assertColumnTypes(parquetPath("reuse_3"), configuration.getFilesFacade(), ColumnType.INT, 0);
        });
    }

    @Test
    public void testCopyCursorExportSymbolRowGroupsAndPages() throws Exception {
        // 10_000 rows of 50 symbols, NULL in every 7th row, and 'only_rg2' in a few rows of the second of 1000-row row
        // groups. Every row group builds its own dictionary.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE rg (sym SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO rg SELECT
                        CASE
                            WHEN x % 7 = 0 THEN NULL
                            WHEN x BETWEEN 1_001 AND 2_000 AND x % 100 = 0 THEN 'only_rg2'
                            ELSE 'sym' || (x % 50)
                        END,
                        x,
                        timestamp_sequence('2024-01-01', 10_000_000L)
                    FROM long_sequence(10_000)
                    """);
            final FilesFacade ff = configuration.getFilesFacade();
            final String query = "SELECT r.* FROM rg r CROSS JOIN long_sequence(1)";
            final String[] copyOptions = {" ROW_GROUP_SIZE 1000", " DATA_PAGE_SIZE 1024", ""};
            for (int i = 0; i < copyOptions.length; i++) {
                assertCursorExportOfSymbols(query, "pages_" + i, new int[]{0}, new int[]{}, copyOptions[i]);
                assertParquetMatchesQuery(query, parquetPath("pages_" + i));
                copyToParquet("COPY (SELECT * FROM rg) TO 'pages_direct_" + i + "' WITH FORMAT PARQUET" + copyOptions[i]);
                ParquetTestUtils.assertColumnEncodingsMatch(parquetPath("pages_direct_" + i), parquetPath("pages_" + i), ff);
            }
            ParquetTestUtils.assertRowGroupCount(parquetPath("pages_0"), ff, 10);
            ParquetTestUtils.assertRowGroupCount(parquetPath("pages_2"), ff, 1);
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("pages_0") + "') WHERE sym = 'only_rg2'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            sym\tv\tts
                            only_rg2\t1100\t2024-01-01T03:03:10.000000Z
                            only_rg2\t1200\t2024-01-01T03:19:50.000000Z
                            only_rg2\t1300\t2024-01-01T03:36:30.000000Z
                            only_rg2\t1500\t2024-01-01T04:09:50.000000Z
                            only_rg2\t1600\t2024-01-01T04:26:30.000000Z
                            only_rg2\t1700\t2024-01-01T04:43:10.000000Z
                            only_rg2\t1800\t2024-01-01T04:59:50.000000Z
                            only_rg2\t1900\t2024-01-01T05:16:30.000000Z
                            only_rg2\t2000\t2024-01-01T05:33:10.000000Z
                            """);
            assertQueriesPrintAlike("SELECT * FROM rg WHERE sym = 'only_rg2'", "SELECT * FROM read_parquet('" + parquetPath("pages_0") + "') WHERE sym = 'only_rg2'");

            // One row per row group
            final String twentyRowsQuery = "SELECT r.* FROM rg r CROSS JOIN long_sequence(1) WHERE r.v <= 20";
            assertCursorExportOfSymbols(twentyRowsQuery, "row_per_row_group", new int[]{0}, new int[]{}, " ROW_GROUP_SIZE 1");
            ParquetTestUtils.assertRowGroupCount(parquetPath("row_per_row_group"), ff, 20);
        });
    }

    @Test
    public void testCopyCursorExportSymbolShapes() throws Exception {
        // A CURSOR_BASED export keeps a SYMBOL column whose cursor passes a table column's symbol keys through, and
        // writes a SYMBOL that a function computes, a cast or a set operation produces as STRING, as before.
        assertMemoryLeak(() -> {
            createSymbolExportTables("BYPASS WAL");
            createFirstLastTable();
            execute("CREATE TABLE pidx (p SYMBOL INDEX TYPE POSTING, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO pidx VALUES ('p1', 1, '2024-01-01T00:00:00.000000Z'), ('p2', 2, '2024-01-01T01:00:00.000000Z'), (NULL, 3, '2024-01-02T00:00:00.000000Z'), ('p1', 4, '2024-01-02T01:00:00.000000Z')");
            execute("CREATE TABLE idx (isym SYMBOL INDEX, s2 SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO idx VALUES ('x', 'p', 1, '2024-01-01T00:00:00.000000Z'), ('y', 'q', 2, '2024-01-01T01:00:00.000000Z'), ('x', NULL, 3, '2024-01-02T00:00:00.000000Z'), ('x', 'r', 4, '2024-01-02T01:00:00.000000Z')");
            execute("CREATE VIEW sym_v AS (SELECT * FROM sym_src)");
            drainWalAndViewQueues();

            final SymbolExportShape[] shapes = {
                    // joins, both sides
                    new SymbolExportShape("SELECT t.sym, t.k, x.x FROM sym_src t CROSS JOIN long_sequence(2) x", 0, 1),
                    new SymbolExportShape("SELECT t.sym, d.sym dsym, t.v, d.w FROM sym_src t JOIN sym_dim d ON t.sym = d.sym", 0, 1),
                    new SymbolExportShape("SELECT t.sym, d.sym dsym, t.v, d.w FROM sym_src t LEFT JOIN sym_dim d ON t.sym = d.sym", 0, 1),
                    new SymbolExportShape("SELECT t.sym, d.sym dsym, t.v, d.w FROM sym_src t RIGHT JOIN sym_dim d ON t.sym = d.sym", 0, 1),
                    new SymbolExportShape("SELECT t.sym, d.sym dsym, t.v, d.w FROM sym_src t FULL JOIN sym_dim d ON t.sym = d.sym", 0, 1),
                    new SymbolExportShape("SELECT t.ts, t.sym, d.sym dsym FROM sym_src t ASOF JOIN sym_dim d", 1, 2),
                    new SymbolExportShape("SELECT t.ts, t.sym, d.sym dsym FROM sym_src t ASOF JOIN sym_dim d ON (sym)", 1, 2),
                    new SymbolExportShape("SELECT t.ts, t.sym, d.sym dsym FROM sym_src t LT JOIN sym_dim d ON (sym)", 1, 2),
                    new SymbolExportShape("SELECT t.ts, t.sym, d.sym dsym FROM sym_src t SPLICE JOIN sym_dim d", 1, 2),
                    new SymbolExportShape("SELECT a.sym, b.sym bsym, a.v FROM sym_src a JOIN sym_src b ON a.v = b.v", 0, 1),
                    new SymbolExportShape("SELECT t.sym, d.sym dsym, t.v FROM sym_src t JOIN sym_dim d ON t.sym = d.sym WHERE t.v > 1", 0, 1),
                    // filters, sorts and limits
                    new SymbolExportShape("SELECT * FROM sym_src WHERE v > 1", 0, 1),
                    new SymbolExportShape("SELECT * FROM sym_src ORDER BY v DESC", 0, 1),
                    new SymbolExportShape("SELECT * FROM sym_src ORDER BY v LIMIT 2", 0, 1),
                    new SymbolExportShape("SELECT * FROM sym_src LIMIT -2", 0, 1),
                    new SymbolExportShape("SELECT * FROM sym_src LATEST ON ts PARTITION BY k", 0, 1),
                    new SymbolExportShape("SELECT * FROM idx WHERE isym = 'x' ORDER BY v DESC", 0, 1),
                    // window functions pass the column through
                    new SymbolExportShape("SELECT sym, k, row_number() OVER (PARTITION BY k ORDER BY ts) rn, ts FROM sym_src", 0, 1),
                    new SymbolExportShape("SELECT sym, sum(v) OVER (PARTITION BY k ORDER BY ts DESC) sv, ts FROM sym_src", 0),
                    // set operations that return rows of their first query
                    new SymbolExportShape("SELECT sym FROM sym_src EXCEPT SELECT sym FROM sym_dim", 0),
                    new SymbolExportShape("SELECT sym FROM sym_src INTERSECT SELECT sym FROM sym_dim", 0),
                    // GROUP BY, DISTINCT and SAMPLE BY keys
                    new SymbolExportShape("SELECT sym, k, count() c FROM sym_src", 0, 1),
                    new SymbolExportShape("SELECT sym, count() c FROM sym_src", 0),
                    new SymbolExportShape("SELECT sym AS s, count() c FROM sym_src", 0),
                    new SymbolExportShape("SELECT d.sym dsym, count() c FROM sym_src t ASOF JOIN sym_dim d ON (sym)", 0),
                    new SymbolExportShape("SELECT DISTINCT sym FROM sym_src", 0),
                    new SymbolExportShape("SELECT DISTINCT sym, k FROM sym_src", 0, 1),
                    new SymbolExportShape("SELECT DISTINCT p FROM pidx", 0),
                    new SymbolExportShape("SELECT sym, k, count() c FROM sym_src GROUP BY GROUPING SETS ((sym), (k), ())", 0, 1),
                    new SymbolExportShape("SELECT sym, k, count() c FROM sym_src GROUP BY ROLLUP (sym, k)", 0, 1),
                    new SymbolExportShape("SELECT ts, sym, count() c FROM sym_src SAMPLE BY 1d", 1),
                    new SymbolExportShape("SELECT ts, sym, count() c FROM sym_src SAMPLE BY 12h FILL(NULL)", 1),
                    new SymbolExportShape("SELECT ts, sym, count() c FROM sym_src SAMPLE BY 12h FILL(PREV)", 1),
                    new SymbolExportShape("SELECT ts, sym, avg(v) a FROM sym_src SAMPLE BY 12h FILL(LINEAR)", 1),
                    new SymbolExportShape("SELECT ts, sym, count() c FROM sym_src SAMPLE BY 12h ALIGN TO FIRST OBSERVATION", 1),
                    new SymbolExportShape("SELECT h.offset, t.sym, count() c FROM sym_src t HORIZON JOIN sym_dim p LIST (0s, 1s) AS h", 1),
                    new SymbolExportShape("SELECT t.ts, t.sym, first(p.w) fw FROM sym_src t WINDOW JOIN sym_dim p RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING", 1),
                    new SymbolExportShape("SELECT sym, max(ts) mts FROM sym_src GROUP BY sym ORDER BY 2 DESC LIMIT 2", 0),
                    // sub-query, CTE, view and a cast to the column's own type
                    new SymbolExportShape("SELECT * FROM (SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1)) WHERE v > 2", 0),
                    new SymbolExportShape("WITH c AS (SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1)) SELECT * FROM c", 0),
                    new SymbolExportShape("SELECT * FROM sym_v ORDER BY v DESC", 0, 1),
                    new SymbolExportShape("SELECT sym::SYMBOL cs, ts FROM sym_src CROSS JOIN long_sequence(1)", 0),

                    // STRING: set operations that mix symbol maps
                    SymbolExportShape.string("SELECT sym, ts FROM sym_src UNION ALL SELECT sym, ts FROM sym_dim", 0),
                    SymbolExportShape.string("SELECT sym FROM sym_src UNION SELECT sym FROM sym_dim", 0),
                    SymbolExportShape.string("SELECT sym, count() c FROM (SELECT sym FROM sym_src UNION ALL SELECT sym FROM sym_dim)", 0),
                    SymbolExportShape.string("SELECT u.sym, u.sym || 'x' c FROM (SELECT sym FROM sym_src UNION ALL SELECT sym FROM sym_dim) u", 0),
                    // STRING: casts that compute a symbol
                    SymbolExportShape.string("SELECT (t.sym || 'x')::SYMBOL cs, t.v FROM sym_src t CROSS JOIN long_sequence(1)", 0),
                    SymbolExportShape.string("SELECT t.v::SYMBOL vs, t.v FROM sym_src t CROSS JOIN long_sequence(1)", 0),
                    // STRING: aggregate and window functions
                    SymbolExportShape.string("SELECT first(sym) f, last(sym) l, mode(sym) m FROM sym_src", 0, 1, 2),
                    new SymbolExportShape("SELECT k, first(sym) f, last(sym) l FROM sym_src", new int[]{0}, new int[]{1, 2}),
                    SymbolExportShape.string("SELECT ts, first(sym) fs, count() c FROM sym_src SAMPLE BY 12h FILL(NULL)", 1),
                    SymbolExportShape.string("SELECT ts, first(sym) fs, count() c FROM sym_src SAMPLE BY 12h FILL(PREV)", 1),
                    SymbolExportShape.string("SELECT lag(sym) OVER (ORDER BY ts) l, ts FROM sym_src", 0),
                    SymbolExportShape.string("SELECT lag(sym, 0) OVER (ORDER BY ts) l, ts FROM sym_src", 0),
                    new SymbolExportShape("SELECT t.ts, t.sym, first(p.sym) fp FROM sym_src t WINDOW JOIN sym_dim p RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING", new int[]{1}, new int[]{2}),
                    SymbolExportShape.string("SELECT fs, count() c FROM (SELECT ts, first(sym) fs FROM sym_src SAMPLE BY 12h FILL(NULL))", 0),
                    // STRING: a join side that the optimizer made an empty table, whose symbol map has no memory
                    new SymbolExportShape("SELECT t.sym, e.sym esym FROM sym_src t LEFT JOIN (SELECT * FROM sym_dim WHERE 1 = 2) e ON t.v = e.w", new int[]{0}, new int[]{1})
            };
            for (int i = 0; i < shapes.length; i++) {
                final SymbolExportShape shape = shapes[i];
                assertCursorExportOfSymbols(shape.query(), "shape_" + i, shape.keptColumns(), shape.stringColumns());
            }

            // STRING: the fused hash join GROUP BY resolves its keys through copies of the symbol maps.
            final String fusedQuery = "SELECT t.sym, count() c FROM sym_src t JOIN sym_dim d ON t.sym = d.sym GROUP BY t.sym";
            assertQuery(fusedQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Hash Join Group By");
            assertCursorExportOfSymbols(fusedQuery, "fused", new int[]{}, new int[]{0});

            // A bind variable, with two values
            final String bindQuery = "SELECT t.sym, t.v FROM sym_src t CROSS JOIN long_sequence(1) WHERE t.v > $1";
            bindVariableService.clear();
            bindVariableService.setInt(0, 5);
            assertCursorExportOfSymbols(bindQuery, "bind_5", new int[]{0}, new int[]{});
            bindVariableService.setInt(0, 7);
            assertCursorExportOfSymbols(bindQuery, "bind_7", new int[]{0}, new int[]{});
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("bind_7") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tv\na\t9\n" + LONG_SYMBOL + "\t8\n");
        });
    }

    @Test
    public void testCopyCursorExportSymbolStorageVariants() throws Exception {
        // Symbol storage options do not change what the export keeps.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE sv (
                        c SYMBOL CACHE,
                        nc SYMBOL NOCACHE,
                        small SYMBOL CAPACITY 4,
                        bi SYMBOL INDEX,
                        pi SYMBOL INDEX TYPE POSTING INCLUDE (v),
                        bf SYMBOL PARQUET(BLOOM_FILTER),
                        v INT,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("""
                    INSERT INTO sv SELECT
                        CASE WHEN x % 11 = 0 THEN NULL ELSE 'c' || (x % 20) END,
                        'n' || (x % 20),
                        CASE WHEN x % 13 = 0 THEN NULL ELSE 's' || (x % 20) END,
                        'b' || (x % 7),
                        CASE WHEN x % 9 = 0 THEN NULL ELSE 'p' || (x % 5) END,
                        'f' || (x % 3),
                        x::INT,
                        timestamp_sequence('2024-01-01', 3_600_000_000L)
                    FROM long_sequence(60)
                    """);
            final String query = "SELECT s.* FROM sv s CROSS JOIN long_sequence(1)";
            assertCursorExportOfSymbols(query, "storage_variants", new int[]{0, 1, 2, 3, 4, 5}, new int[]{});
            assertParquetMatchesQuery(query, parquetPath("storage_variants"));
            copyToParquet("COPY (SELECT * FROM sv) TO 'storage_variants_direct' WITH FORMAT PARQUET");
            ParquetTestUtils.assertColumnEncodingsMatch(parquetPath("storage_variants_direct"), parquetPath("storage_variants"), configuration.getFilesFacade());
        });
    }

    @Test
    public void testCopyCursorExportSymbolWithLoneSurrogateFailsAsDirectExport() throws Exception {
        // A symbol of a lone UTF-16 surrogate has no UTF-8 form, so the export of a row group that uses it fails, as the
        // DIRECT export of the column does. A row group that does not use it exports.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ls (sym SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO ls VALUES ('ok', '2024-01-01T00:00:00.000000Z'), ('\uD800', '2024-01-01T01:00:00.000000Z')");
            assertQuery("SELECT length(sym) len FROM ls")
                    .noLeakCheck()
                    .expectSize()
                    .returns("len\n2\n1\n");
            final String directMessage = failedExportMessage("COPY (SELECT * FROM ls) TO 'surrogate_direct' WITH FORMAT PARQUET");
            final String cursorMessage = failedExportMessage("COPY (SELECT l.* FROM ls l CROSS JOIN long_sequence(1)) TO 'surrogate_cursor' WITH FORMAT PARQUET");
            Assert.assertEquals(directMessage, cursorMessage);
            TestUtils.assertContains(cursorMessage, "unpaired surrogate found: d800");
            Assert.assertFalse(exportFileExists("surrogate_cursor"));
            assertCursorExportOfSymbols("SELECT l.* FROM ls l CROSS JOIN long_sequence(1) WHERE l.sym = 'ok'", "surrogate_unused", new int[]{0}, new int[]{});
        });
    }

    @Test
    public void testCopyCursorExportsOnOneJobFollowEachPlanDirection() throws Exception {
        // One export job streams a descending, an ascending and a descending plan: the direction of one export never
        // reaches the next one.
        assertMemoryLeak(() -> {
            createRetypedTables("BYPASS WAL");
            final String descendingQuery = "SELECT * FROM nr WHERE s = 'a' ORDER BY ts DESC";
            final String ascendingQuery = "SELECT * FROM nr WHERE s = 'a'";
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                final String[] queries = {descendingQuery, ascendingQuery, descendingQuery};
                for (int i = 0; i < queries.length; i++) {
                    assertExportMode(queries[i], ParquetExportMode.CURSOR_BASED);
                    final String exportId = queueCopy("COPY (" + queries[i] + ") TO 'one_job_" + i + "' WITH FORMAT PARQUET");
                    Assert.assertTrue(job.run());
                    assertExportLog(exportId, STREAMING_EXPORT_LOG);
                }
            }
            assertParquetMatchesControl(descendingQuery, "one_job_0", null);
            assertParquetMatchesControl(ascendingQuery, "one_job_1", "ts");
            assertParquetMatchesControl(descendingQuery, "one_job_2", null);
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyExportCancel() throws Exception {
        assertMemoryLeak(() -> {
            CopyExportRunnable stmt = () -> {
                try {
                    runAndFetchCopyExportID("copy (generate_series(0, '9999-01-01', '1U')) TO 'very_large_table' WITH FORMAT PARQUET;", sqlExecutionContext);
                } catch (SqlException e) {
                    throw new RuntimeException(e);
                } finally {
                    Path.clearThreadLocals();
                }
            };
            CopyExportRunnable test = () -> {
                try {
                    long copyID;
                    do {
                        copyID = engine.getCopyExportContext().getActiveExportId();
                    } while (copyID == -1);

                    StringSink sink = new StringSink();
                    Numbers.appendHex(sink, copyID, true);
                    String copyIDStr = sink.toString();
                    sink.clear();
                    sink.put("COPY '").put(copyIDStr).put("' CANCEL;");
                    try {
                        assertQuery(sink)
                                .noLeakCheck()
                                .returnsOnce("id\tstatus\n" + copyIDStr + "\tcancelled\n");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    // wait cancel finish
                    do {
                        copyID = engine.getCopyExportContext().getActiveExportId();
                    } while (copyID != -1);
                } finally {
                    Path.clearThreadLocals();
                }
            };
            testCopyExport(stmt, test, false, 1);
        });
    }

    @Test
    public void testCopyFactoryRunAgainWhileExportQueuedIsDuplicate() throws Exception {
        // A run of one compiled COPY while its previous export is still queued is rejected as a new compile of the same
        // COPY is: the duplicate check keys on the source that the COPY names.
        assertMemoryLeak(() -> {
            createTableToCopyAgain("BYPASS WAL");
            assertCopyFactoryRunWhileQueuedIsDuplicate("COPY (SELECT * FROM t) TO 'again' WITH FORMAT PARQUET", "SELECT * FROM t");
            assertCopyFactoryRunWhileQueuedIsDuplicate("COPY t TO 'again' WITH FORMAT PARQUET PARTITION_BY MONTH", "t");
        });
    }

    @Test
    public void testCopyFactoryRunsAgain() throws Exception {
        // A server-side PostgreSQL wire prepared statement keeps its compiled COPY and runs it on every execution, so
        // every run of one compiled COPY exports the source that the COPY names.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"BYPASS WAL", "WAL"}) {
                // streams the select
                assertCopyFactoryRunsAgain(walMode, "COPY (SELECT * FROM t) TO 'again' WITH FORMAT PARQUET", true);
                // materializes the select in a temp table
                assertCopyFactoryRunsAgain(walMode, "COPY (SELECT * FROM t) TO 'again' WITH FORMAT PARQUET PARTITION_BY MONTH", true);
                // repartitions the table through a select of it
                assertCopyFactoryRunsAgain(walMode, "COPY t TO 'again' WITH FORMAT PARQUET PARTITION_BY MONTH", true);
                // keeps the partitioning of the table, so it reads the table
                assertCopyFactoryRunsAgain(walMode, "COPY t TO 'again' WITH FORMAT PARQUET PARTITION_BY DAY", false);
                // reads the table and checks the bloom filter columns against the table
                assertCopyFactoryRunsAgain(walMode, "COPY t TO 'again' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'x'", false);
            }
        });
    }

    @Test
    public void testCopyFactoryRunsAgainAfterBloomFilterError() throws Exception {
        // A run that fails validation releases its export, so the next run of the same compiled COPY fails the same way.
        assertMemoryLeak(() -> {
            createTableToCopyAgain("BYPASS WAL");
            try (RecordCursorFactory factory = select("COPY t TO 'again' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'nope'")) {
                assertCopyFails(factory, 60, "bloom_filter_columns contains non-existent column: nope");
                assertCopyFails(factory, 60, "bloom_filter_columns contains non-existent column: nope");
            }
            try (RecordCursorFactory factory = select("COPY (SELECT * FROM t) TO 'again' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'nope'")) {
                assertCopyFails(factory, 76, "bloom_filter_columns contains non-existent column: nope");
                assertCopyFails(factory, 76, "bloom_filter_columns contains non-existent column: nope");
            }
        });
    }

    @Test
    public void testCopyFactoryRunsAgainAfterSourceTableDropped() throws Exception {
        // Every run of one compiled COPY looks its source up by name, and a run that fails releases its export.
        assertMemoryLeak(() -> {
            createTableToCopyAgain("BYPASS WAL");
            try (RecordCursorFactory factory = select("COPY t TO 'again' WITH FORMAT PARQUET PARTITION_BY MONTH")) {
                assertCopyFactoryExports(factory, """
                        x\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-01T01:00:00.000000Z
                        """);
                execute("DROP TABLE t");
                assertCopyFails(factory, 5, "table does not exist [table=t]");
                execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO t VALUES (10, '2024-01-01T00:00:00.000000Z')");
                assertCopyFactoryExports(factory, """
                        x\tts
                        10\t2024-01-01T00:00:00.000000Z
                        """);
            }

            execute("DROP TABLE t");
            // COPY (SELECT ...) compiles its select on each run, so it compiles while t does not exist.
            try (RecordCursorFactory factory = select("COPY (SELECT * FROM t) TO 'again' WITH FORMAT PARQUET")) {
                assertCopyFails(factory, 20, "table does not exist [table=t]");
                execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO t VALUES (20, '2024-01-01T00:00:00.000000Z')");
                assertCopyFactoryExports(factory, """
                        x\tts
                        20\t2024-01-01T00:00:00.000000Z
                        """);
            }
        });
    }

    @Test
    public void testCopyFactoryRunsAgainAfterSourceTablePartitioningChanged() throws Exception {
        // Every run of one compiled COPY picks its route from the source table of that run, as a new compile of the
        // same COPY does: a PARTITION_BY other than the table's exports through a select of the table, the table's own
        // PARTITION_BY reads the table.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY MONTH BYPASS WAL");
            execute("""
                    INSERT INTO t VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-01T01:00:00.000000Z')
                    """);
            try (RecordCursorFactory factory = select("COPY t TO 'again' WITH FORMAT PARQUET PARTITION_BY DAY")) {
                final String firstId = assertCopyFactoryExports(factory, """
                        x\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-01T01:00:00.000000Z
                        """);
                assertExportLogTableName(firstId, "copy." + firstId);

                execute("DROP TABLE t");
                execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                execute("INSERT INTO t VALUES (10, '2024-01-01T00:00:00.000000Z')");
                final String secondId = assertCopyFactoryExports(factory, """
                        x\tts
                        10\t2024-01-01T00:00:00.000000Z
                        """);
                assertExportLogTableName(secondId, "t");
            }
        });
    }

    @Test
    public void testCopyFactoryRunsAgainOnCompositeTable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        // A table with a composite partition exports through a select of the table, which every run of one compiled
        // COPY builds from the table again.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ct AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(2000))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2024-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO ct VALUES (999_999, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();
            // Lands only inside 2024-01-01 and makes it composite.
            execute("INSERT INTO ct SELECT x::INT + 70_000 i," +
                    " timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L) ts FROM long_sequence(40)");
            drainWalQueue();
            assertFirstPartitionOfCtComposite();

            try (
                    RecordCursorFactory factory = select("COPY ct TO 'again' WITH FORMAT PARQUET");
                    RecordCursorFactory dayFactory = select("COPY ct TO 'again_day' WITH FORMAT PARQUET PARTITION_BY DAY")
            ) {
                final String lastDayRows = """
                        i\tts
                        999999\t2024-01-03T00:00:00.000000Z
                        """;
                assertCompositeCopyFactoryExports(factory, "again", lastDayRows);
                assertCompositeCopyFactoryExports(dayFactory, "again_day", lastDayRows);

                execute("INSERT INTO ct VALUES (4242, '2024-01-03T01:00:00.000000Z')");
                drainWalQueue();
                assertFirstPartitionOfCtComposite();
                final String lastDayRowsWithInsert = """
                        i\tts
                        999999\t2024-01-03T00:00:00.000000Z
                        4242\t2024-01-03T01:00:00.000000Z
                        """;
                assertCompositeCopyFactoryExports(factory, "again", lastDayRowsWithInsert);
                assertCompositeCopyFactoryExports(dayFactory, "again_day", lastDayRowsWithInsert);
            }
        });
    }

    @Test
    public void testCopyFactoryRunsAgainWithNewBindValue() throws Exception {
        // Every run of one compiled COPY compiles its select with the bind values of that run.
        assertMemoryLeak(() -> {
            createTableToCopyAgain("BYPASS WAL");
            execute("INSERT INTO t VALUES (3, '2024-01-01T02:00:00.000000Z')");
            for (String copySql : new String[]{
                    // streams the select
                    "COPY (SELECT * FROM t WHERE x <= $1) TO 'again' WITH FORMAT PARQUET",
                    // materializes the select in a temp table
                    "COPY (SELECT * FROM t WHERE x <= $1) TO 'again' WITH FORMAT PARQUET PARTITION_BY MONTH"
            }) {
                bindVariableService.clear();
                bindVariableService.setInt(0, 1);
                try (RecordCursorFactory factory = select(copySql)) {
                    assertCopyFactoryExports(factory, """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            """);
                    bindVariableService.setInt(0, 2);
                    assertCopyFactoryExports(factory, """
                            x\tts
                            1\t2024-01-01T00:00:00.000000Z
                            2\t2024-01-01T01:00:00.000000Z
                            """);
                }
            }
        });
    }

    @Test
    public void testCopyFilteredQuerySurvivesPartitionFormatChangeAfterQueue() throws Exception {
        // The callback converts the first day after COPY queues the export and before the export starts, to Parquet and
        // back, which makes the queued plan stale: the export compiles the select again and writes the same rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES (1, '2024-01-01'), (-1, '2024-01-01T01:00:00'), (2, '2024-01-02')");

            CopyExportRunnable statement = () -> runAndFetchCopyExportID(
                    "COPY (SELECT * FROM x WHERE val > 0) TO 'format_change' WITH FORMAT PARQUET",
                    sqlExecutionContext
            );
            Callable<Exception> callback = () -> {
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                return null;
            };
            CopyExportRunnable test = () -> assertEventually(() -> {
                assertQuery("SELECT num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("num_exported_files\tstatus\n1\tfinished\n");
                assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "format_change.parquet') ORDER BY ts")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                val\tts
                                1\t2024-01-01T00:00:00.000000Z
                                2\t2024-01-02T00:00:00.000000Z
                                """);
            });

            testCopyExport(statement, test, callback);

            execute("CREATE TABLE y (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO y VALUES (1, '2024-01-01'), (-1, '2024-01-01T01:00:00'), (2, '2024-01-02')");
            execute("ALTER TABLE y CONVERT PARTITION TO PARQUET LIST '2024-01-01'");

            statement = () -> runAndFetchCopyExportID(
                    "COPY (SELECT * FROM y WHERE val > 0) TO 'format_change_reverse' WITH FORMAT PARQUET",
                    sqlExecutionContext
            );
            callback = () -> {
                execute("ALTER TABLE y CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                return null;
            };
            test = () -> assertEventually(() -> {
                assertQuery("SELECT num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("num_exported_files\tstatus\n1\tfinished\n");
                assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "format_change_reverse.parquet') ORDER BY ts")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                val\tts
                                1\t2024-01-01T00:00:00.000000Z
                                2\t2024-01-02T00:00:00.000000Z
                                """);
            });

            testCopyExport(statement, test, callback);
        });
    }

    @Test
    public void testCopyFusedInnerJoinGroupBySurvivesBuildTableGrowthAfterQueue() throws Exception {
        assertMemoryLeak(() -> assertCopyOfFusedJoinGroupByAfterBuildTableGrowth("INT", "(x % 5)::INT", "", "fused_inner_growth"));
    }

    @Test
    public void testCopyFusedLeftJoinGroupBySurvivesBuildTableGrowthAfterQueue() throws Exception {
        assertMemoryLeak(() -> assertCopyOfFusedJoinGroupByAfterBuildTableGrowth("INT", "(x % 5)::INT", "LEFT ", "fused_left_growth"));
    }

    @Test
    public void testCopyFusedSymbolKeyJoinGroupBySurvivesBuildTableGrowthAfterQueue() throws Exception {
        assertMemoryLeak(() -> assertCopyOfFusedJoinGroupByAfterBuildTableGrowth("SYMBOL", "('k' || (x % 5))::SYMBOL", "", "fused_symbol_growth"));
    }

    @Test
    public void testCopyGroupByKeysKeepParquetSettingsOfEveryType() throws Exception {
        // A GROUP BY key passes its column through, so it keeps the column's PARQUET(...) setting, whatever its type.
        // The DIRECT export of the same columns is the reference. BINARY and arrays cannot be GROUP BY keys.
        assertMemoryLeak(() -> {
            ParquetTestUtils.createTableWithParquetSettingPerType(engine, sqlExecutionContext, "ty");
            final String columns = "b, sh, ch, i, l, dt, t, tn, f, d, u, l256, ip, g1, g2, g4, g8, d8, d16, d32, d64, d128, d256, s, vc, sym, bo, bf, bfe";
            final String query = "SELECT DISTINCT " + columns + " FROM ty";
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            copyToParquet("COPY (SELECT " + columns + " FROM ty) TO 'ty_columns' WITH FORMAT PARQUET");
            copyToParquet("COPY (" + query + ") TO 'ty_distinct' WITH FORMAT PARQUET");

            final String parquetPath = parquetPath("ty_distinct");
            final FilesFacade ff = configuration.getFilesFacade();
            assertParquetMatchesQueryInAnyOrder(query, parquetPath);
            ParquetTestUtils.assertColumnEncodingsMatch(parquetPath("ty_columns"), parquetPath, ff);
            // b, sh, ch, l, dt, t, ip, g1, g2, g4 and g8 default to PLAIN
            ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0, 1, 2, 4, 5, 6, 12, 13, 14, 15, 16);
            // i, tn, f, d, u, l256, the six decimals and bfe default to PLAIN, s and vc to DELTA_LENGTH_BYTE_ARRAY
            ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_RLE_DICTIONARY, 3, 7, 8, 9, 10, 11, 17, 18, 19, 20, 21, 22, 23, 24, 28);
            assertSymbolColumnsKept(parquetPath, 25);
        });
    }

    @Test
    public void testCopyGroupByOutputKeepsParquetSettingsOfKeys() throws Exception {
        // GROUP BY, SAMPLE BY and the joins that aggregate keep the PARQUET(...) setting of a key column they pass
        // through, under any name. Aggregates, function keys and the bucket timestamp of SAMPLE BY compute their values,
        // so they keep their type's default.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE kt (v INT PARQUET(RLE_DICTIONARY), w INT, ts TIMESTAMP PARQUET(RLE_DICTIONARY)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO kt VALUES
                        (1, 10, '2024-01-01T00:00:00.000000Z'),
                        (2, 20, '2024-01-01T05:00:00.000000Z'),
                        (1, 30, '2024-01-02T00:00:00.000000Z'),
                        (NULL, 40, '2024-01-02T07:00:00.000000Z'),
                        (2, 50, '2024-01-03T00:00:00.000000Z')
                    """);
            execute("CREATE TABLE f (k INT, x LONG)");
            execute("INSERT INTO f VALUES (1, 100), (2, 200), (3, 300), (1, 400)");
            execute("CREATE TABLE d (k INT, gi INT PARQUET(RLE_DICTIONARY))");
            execute("INSERT INTO d VALUES (1, 7), (2, 8), (3, 7)");
            final FilesFacade ff = configuration.getFilesFacade();

            // the vectorized GROUP BY, its key under its own name and under an alias
            assertCursorExportEncodesKeys("SELECT v, count() c FROM kt", "rosti", 0);
            assertCursorExportEncodesKeys("SELECT v AS vk, count() c FROM kt", "rosti_alias", 0);
            // the async GROUP BY
            assertCursorExportEncodesKeys("SELECT v, first(w) fw FROM kt", "async", 0);
            assertCursorExportEncodesKeys("SELECT v AS vk, first(w) fw FROM kt", "async_alias", 0);
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("async_alias"), ff, 1);
            // a key on the designated timestamp passes ts through, unlike the bucket timestamp of SAMPLE BY below
            final String timestampKeyQuery = "SELECT ts, first(w) fw FROM kt";
            assertQuery(timestampKeyQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Group By");
            assertCursorExportEncodesKeys(timestampKeyQuery, "async_ts", 0);
            assertCursorExportEncodesKeys("SELECT DISTINCT v FROM kt", "distinct", 0);
            assertCursorExportEncodesKeys("SELECT v, count() c FROM kt GROUP BY ROLLUP (v)", "rollup", 0);
            // hour(ts) computes an INT key from ts
            assertCursorExportEncodesKeys("SELECT hour(ts) h, count() c FROM kt", "rosti_hour");
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("rosti_hour"), ff, 0);
            // SAMPLE BY computes its bucket timestamp, which takes the designated timestamp's default encoding
            for (String align : new String[]{" ALIGN TO FIRST OBSERVATION", ""}) {
                final String fileName = align.isEmpty() ? "sample_by" : "sample_by_first_observation";
                assertCursorExportEncodesKeys("SELECT ts, v, count() c FROM kt SAMPLE BY 1d" + align, fileName, 1);
                ParquetTestUtils.assertColumnsUseEncoding(parquetPath(fileName), ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0);
                ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath(fileName), ff, 0);
            }
            // the fused hash join GROUP BY
            final String fusedQuery = "SELECT d.gi, sum(f.x) s FROM f JOIN d ON f.k = d.k GROUP BY d.gi";
            assertQuery(fusedQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("Async Hash Join Group By");
            assertCursorExportEncodesKeys(fusedQuery, "fused", 0);
            // the master columns of WINDOW JOIN and the keys of HORIZON JOIN
            assertCursorExportEncodesKeys("SELECT t.ts, t.v, first(p.w) fw FROM kt t WINDOW JOIN kt p RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING", "window_join", 1);
            assertCursorExportEncodesKeys("SELECT h.offset, t.v, count() c FROM kt t HORIZON JOIN kt p LIST (0s, 1s) AS h", "horizon_join", 1);
        });
    }

    @Test
    public void testCopyOfConvertedColumnFollowsTableChangesOnEachExecution() throws Exception {
        // The same COPY runs after each change of the table and routes the select it compiles then: a retyped column of
        // a Parquet partition streams through the cursor exporter, a table without Parquet partitions streams its page
        // frames, and a truncated table exports its columns and no rows. A second run overwrites the file.
        assertMemoryLeak(() -> {
            for (String tableName : new String[]{"cc", "ncc"}) {
                execute("CREATE TABLE " + tableName + " (s SYMBOL, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("""
                        INSERT INTO %s VALUES
                            ('a', 1, '2024-01-01T00:00:00.000000Z'),
                            ('b', -1, '2024-01-01T12:00:00.000000Z'),
                            ('a', 2_147_483_647, '2024-01-02T00:00:00.000000Z'),
                            (NULL, NULL, '2024-01-03T00:00:00.000000Z')
                        """.formatted(tableName));
            }
            drainWalQueue();
            final String query = "SELECT * FROM cc";
            final String copySql = "COPY (" + query + ") TO 'changes' WITH FORMAT PARQUET";

            execute("ALTER TABLE cc CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE cc ALTER COLUMN c TYPE LONG");
            execute("ALTER TABLE ncc ALTER COLUMN c TYPE LONG");
            drainWalQueue();
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            copyToParquet(copySql);
            assertParquetMatchesControl("SELECT * FROM ncc", "changes", "ts");
            copyToParquet(copySql);
            assertParquetMatchesControl("SELECT * FROM ncc", "changes", "ts");

            execute("ALTER TABLE cc CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
            drainWalQueue();
            assertExportMode(query, ParquetExportMode.DIRECT_PAGE_FRAME);
            copyToParquet(copySql);
            assertParquetMatchesControl("SELECT * FROM ncc", "changes", "ts");

            execute("ALTER TABLE cc CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            execute("ALTER TABLE cc ALTER COLUMN c TYPE DOUBLE");
            execute("ALTER TABLE ncc ALTER COLUMN c TYPE DOUBLE");
            drainWalQueue();
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            copyToParquet(copySql);
            assertParquetMatchesControl("SELECT * FROM ncc", "changes", "ts");
            ParquetTestUtils.assertColumnTypes(parquetPath("changes"), configuration.getFilesFacade(), ColumnType.DOUBLE, 1);

            execute("TRUNCATE TABLE cc");
            drainWalQueue();
            copyToParquet(copySql);
            assertParquetRows("changes", "ts", "s\tc\tts\n");
        });
    }

    @Test
    public void testCopyOptionError() throws Exception {
        assertQuery("copy test_table to 'test_table'  with format parquet1;")
                .fails(45, "unsupported format, only 'parquet' is supported");
        assertQuery("copy test_table to 'test_table'  with format parquet1;")
                .fails(45, "unsupported format, only 'parquet' is supported");

        execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
        assertQuery("copy test_table to 'test_table'  with partition_by Day;")
                .fails(0, "export format must be specified, supported formats: 'parquet'");

        assertQuery("copy test_table to 'test_table'  with partition_by Day1;")
                .fails(51, "invalid partition by option: Day1");

        assertQuery("copy test_table to 'test_table'  with partition_by1 Day1;")
                .fails(38, "unrecognised option [option=partition_by1]");

        assertQuery("copy test_table to 'test_table'  with partition_by1 Day1;")
                .fails(38, "unrecognised option [option=partition_by1]");

        assertQuery("copy test_table to 'test_table'  with size_limit aa;")
                .fails(38, "size limit is not yet supported");

        assertQuery("copy test_table to 'test_table'  with compression_codec aa;")
                .fails(56, "invalid compression codec[aa], expected one of: uncompressed, snappy, gzip, brotli, zstd, lz4_raw");

        assertQuery("copy test_table to 'test_table'  with format parquet compression_codec uncompressed compression_level aa;")
                .fails(102, "found [tok='aa', len=2] bad integer");

        assertQuery("copy test_table to 'test_table'  with format parquet compression_codec zstd compression_level 120;")
                .fails(94, "ZSTD compression level must be between 1 and 22");

        assertQuery("copy test_table to 'test_table'  with format parquet compression_codec GZIP compression_level 120;")
                .fails(94, "GZIP compression level must be between 0 and 9");

        assertQuery("copy test_table to 'test_table'  with format parquet compression_codec BROTLI compression_level 120;")
                .fails(96, "Brotli compression level must be between 0 and 11");
        assertQuery("copy test_table to 'test_table'  with format parquet parquet_version 3;")
                .fails(69, "invalid parquet version: 3, expected 1 or 2");

        assertQuery("copy test_table to 'test_table'  with format parquet bloom_filter_fpp 0;")
                .fails(70, "bloom_filter_fpp must be between 0 and 1 (exclusive)");

        assertQuery("copy test_table to 'test_table'  with format parquet bloom_filter_fpp 1;")
                .fails(70, "bloom_filter_fpp must be between 0 and 1 (exclusive)");

        assertQuery("copy test_table to 'test_table'  with format parquet bloom_filter_fpp '1.5';")
                .fails(70, "bloom_filter_fpp must be between 0 and 1 (exclusive)");

        assertQuery("copy test_table to 'test_table'  with format parquet bloom_filter_fpp '-0.1';")
                .fails(70, "bloom_filter_fpp must be between 0 and 1 (exclusive)");

        assertQuery("copy test_table to 'test_table'  with format parquet bloom_filter_fpp abc;")
                .fails(70, "bad number");
    }

    @Test
    public void testCopyParquetBoundaryValuesMaxRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size 2147483647", sqlExecutionContext);

            CopyExportRunnable test = () -> {
                assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("export_path\tnum_exported_files\tstatus\n" +
                                exportRoot + File.separator + "test_table.parquet" + "\t1\tfinished\n"));
                assertQuery("select path, diskSizeHuman from export_files()  order by modifiedTime")
                        .noLeakCheck()
                        .returns("""
                                path\tdiskSizeHuman
                                test_table.parquet\t641.0 B
                                """);
            };
            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetBoundaryValuesMinRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size 1", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("export_path\tnum_exported_files\tstatus\n" +
                                    exportRoot + File.separator + "test_table.parquet" + "\t1\tfinished\n"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetBoundaryValuesNegativeRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size -1", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("export_path\tnum_exported_files\tstatus\n" +
                                    exportRoot + File.separator + "test_table.parquet" + "\t1\tfinished\n"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetBoundaryValuesZeroRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size 0", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("export_path\tnum_exported_files\tstatus\n" +
                                    exportRoot + File.separator + "test_table.parquet" + "\t1\tfinished\n"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetDesignatedTimestampAfterDropColumn() throws Exception {
        // Regression test for designated timestamp detection in the batch
        // parquet encoder (create_partition_descriptor in jni.rs).
        //
        // populateFromTableReader() passes the reader timestamp index to Rust,
        // but the Rust code compares it with col_id (writer index). After a
        // DROP COLUMN the two diverge, causing the wrong column to be flagged
        // as the designated timestamp. If that column is not a TIMESTAMP type,
        // into_designated_with_order() fails and the export errors out.
        //
        //   dummy (0, INT) — dropped
        //   val   (1, DOUBLE)
        //   ts    (2, TIMESTAMP designated)
        //   extra (3, LONG)
        //
        // After DROP dummy: val reader=0(writer=1), ts reader=1(writer=2)
        // timestamp_index = 1 (reader index of ts).
        // Bug: col_id(val)=1 == timestamp_index → val (DOUBLE) flagged
        //      as designated → into_designated_with_order() fails.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        dummy INT,
                        val DOUBLE,
                        ts TIMESTAMP,
                        extra LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO t VALUES
                    (1, 1.0, '2024-01-01T00:00:00.000000Z', 10),
                    (2, 2.0, '2024-01-01T01:00:00.000000Z', 20),
                    (3, 3.0, '2024-01-02T00:00:00.000000Z', 30)
                    """);

            execute("ALTER TABLE t DROP COLUMN dummy");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("COPY t TO 'drop_col_test' WITH FORMAT parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        // The export must complete successfully — the encoder
                        // must correctly identify the designated timestamp
                        // using the writer index, not the reader index.
                        // Before the fix, the export failed with status=failed
                        // because into_designated_with_order() errored on the
                        // DOUBLE column that was incorrectly flagged.
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        status
                                        finished
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetEmptyPartitionAfterDropColumn() throws Exception {
        // Regression test for populateEmptyPartition() timestamp index fix.
        // When the table is empty, the exporter calls populateEmptyPartition
        // instead of populateFromTableReader. After DROP COLUMN, the reader
        // timestamp index diverges from the writer index. Without the fix,
        // the Rust encoder misidentifies the designated timestamp column.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t (
                        dummy INT,
                        val DOUBLE,
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);

            execute("ALTER TABLE t DROP COLUMN dummy");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("COPY t TO 'empty_drop_col' WITH FORMAT parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("status\nfinished\n"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table all_types_empty (" +
                    "bool_col boolean, " +
                    "byte_col byte, " +
                    "short_col short, " +
                    "int_col int, " +
                    "long_col long, " +
                    "float_col float, " +
                    "double_col double, " +
                    "string_col string, " +
                    "symbol_col symbol, " +
                    "t_ns timestamp_ns, " +
                    "d_array DOUBLE[], " +
                    "ts timestamp" +
                    ") timestamp(ts)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy all_types_empty to 'all_types_empty' with format parquet", sqlExecutionContext);
            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "all_types_empty.parquet" + "\t1\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "all_types_empty" + ".parquet')")
                                .noLeakCheck()
                                .timestamp("ts")
                                .returns("""
                                        bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tt_ns\td_array\tts
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetExportBinaryColumnKeepsParquetEncodings() throws Exception {
        // A BINARY column in a select without page frames routes COPY through a temp table.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bt (metric INT PARQUET(RLE_DICTIONARY), bin BINARY PARQUET(PLAIN), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO bt VALUES
                        (10, rnd_bin(2, 2, 0), '2024-01-01T00:00:00.000000Z'),
                        (10, rnd_bin(2, 2, 0), '2024-01-01T01:00:00.000000Z'),
                        (20, rnd_bin(2, 2, 0), '2024-01-02T00:00:00.000000Z'),
                        (NULL, NULL, '2024-01-02T01:00:00.000000Z')
                    """);
            final String query = "SELECT * FROM bt ORDER BY metric, ts";
            assertExportMode(query, ParquetExportMode.TEMP_TABLE);

            copyToParquet("COPY (" + query + ") TO 'binary_settings' WITH FORMAT PARQUET");
            final String parquetPath = parquetPath("binary_settings");
            assertParquetMatchesQuery(query, parquetPath);
            final FilesFacade ff = configuration.getFilesFacade();
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, ff, 0);
            // BINARY defaults to DELTA_LENGTH_BYTE_ARRAY
            ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_PLAIN, 1);
            ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_LENGTH_BYTE_ARRAY, 1);
        });
    }

    @Test
    public void testCopyParquetExportCompositeTableKeepsParquetEncodings() throws Exception {
        // COPY <table> materializes a table with a composite partition through a temp table.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            createTableNeedingQuotes();
            execute("ALTER TABLE \"src ct\" ALTER COLUMN i SET PARQUET(RLE_DICTIONARY)");
            drainWalQueue();
            makeFirstPartitionComposite();

            copyToParquet("COPY \"src ct\" TO 'composite_settings' WITH FORMAT PARQUET");
            assertCompositeExportKeepsParquetSetting("composite_settings");
        });
    }

    @Test
    public void testCopyParquetExportFixedToVarConvertedColumn() throws Exception {
        // fixed->var lazy conversion (INT->STRING) over a parquet partition, exported via the
        // DIRECT_PAGE_FRAME path. The exported file must hold the formatted strings, not the
        // raw INT bytes reinterpreted as a var column.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("INT", "(x * 3)::INT", "STRING", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """));
    }

    @Test
    public void testCopyParquetExportFixedToVarConvertedColumnThroughView() throws Exception {
        // Regression test for #7807. The plan of a query over a view does not show the pending
        // fixed->var conversion (INT->VARCHAR) to the export mode check unless the view's wrapper
        // asks its own base. The DIRECT_PAGE_FRAME path then handed the encoder the raw INT
        // bytes as VARCHAR, and the encoder panicked and aborted the JVM.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("INT", "(x * 3)::INT", "VARCHAR", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE));
    }

    @Test
    public void testCopyParquetExportFixedToVarConvertedColumnThroughViewCreatedBeforeTypeChange() throws Exception {
        // Same as above with INT->STRING, through a view that exists before the type change.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("INT", "(x * 3)::INT", "STRING", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """, ConvertedColumnExportSource.VIEW_CREATED_BEFORE_TYPE_CHANGE));
    }

    @Test
    public void testCopyParquetExportOfConvertedColumnAcceptsAliasTheTempTableRejects() throws Exception {
        // A temp table rejects a column name such as "x-1", so the export of a retyped column under that alias failed
        // when it went through one. It streams now, and the file holds the column under its alias.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            final String query = "SELECT x AS \"x-1\", ts FROM r";
            assertCopyStreamsRows(query, "alias");
            assertParquetMatchesControl("SELECT x AS \"x-1\", ts FROM nr", "alias", "ts");
            ParquetTestUtils.assertColumnTypes(parquetPath("alias"), configuration.getFilesFacade(), ColumnType.LONG, 0);
        });
    }

    @Test
    public void testCopyParquetExportOfConvertedColumnEveryConversionClass() throws Exception {
        // A streamed export of a retyped column writes the values that the lazy conversion of the Parquet partition
        // gives, in the column's new type, for every class of conversion that converts lazily: the rows of nt, which
        // converts the column eagerly while every partition is native. The row before the column was added holds NULL.
        assertMemoryLeak(() -> {
            final String[] ints = {"NULL", "-2_147_483_647", "-1", "0", "1", "2_147_483_647"};
            final String[] longs = {"NULL", "-9_223_372_036_854_775_807", "-1", "0", "1", "9_223_372_036_854_775_807", "2_147_483_648"};
            final String[] doubles = {"NULL", "'-0.0'::DOUBLE", "'Infinity'::DOUBLE", "'-Infinity'::DOUBLE", "4.9E-324", "1.7976931348623157E308", "-1.5", "2.5"};
            final String[] floats = {"NULL", "'-0.0'::FLOAT", "'Infinity'::FLOAT", "'-Infinity'::FLOAT", "1.401298464324817E-45::FLOAT", "-1.5::FLOAT", "2.5::FLOAT"};
            final String[] timestamps = {"NULL", "'1969-12-31T23:59:59.999999Z'", "'1970-01-01T00:00:00.000000Z'", "'2024-02-29T12:00:00.000000Z'", "'9999-12-31T23:59:59.999999Z'"};
            final String[] dates = {"NULL", "'1969-12-31T23:59:59.999Z'", "'1970-01-01T00:00:00.000Z'", "'2024-02-29T12:00:00.000Z'", "'9999-12-31T23:59:59.999Z'"};
            final String[] texts = {"NULL", "''", "'a'", "'€'", "'😀'", "'" + "z".repeat(300) + "'", "'2147483648'", "'abc'", "'42'", "'-1'"};
            final String[] numericTexts = {"NULL", "''", "'42'", "'0'", "'-1'", "'1.5'", "'abc'", "'2147483648'"};
            final ConvertedColumnConversion[] conversions = {
                    // fixed to wider and narrower fixed types
                    new ConvertedColumnConversion("INT", "LONG", ints),
                    new ConvertedColumnConversion("LONG", "INT", longs),
                    new ConvertedColumnConversion("FLOAT", "DOUBLE", floats),
                    new ConvertedColumnConversion("DOUBLE", "FLOAT", doubles),
                    new ConvertedColumnConversion("DATE", "TIMESTAMP", dates),
                    new ConvertedColumnConversion("TIMESTAMP", "TIMESTAMP_NS", timestamps),
                    new ConvertedColumnConversion("INT", "DECIMAL(18, 0)", ints),
                    new ConvertedColumnConversion("LONG", "DECIMAL(38, 0)", longs),
                    // fixed to text
                    new ConvertedColumnConversion("INT", "VARCHAR", ints),
                    new ConvertedColumnConversion("INT", "STRING", ints),
                    new ConvertedColumnConversion("DOUBLE", "VARCHAR", doubles),
                    new ConvertedColumnConversion("UUID", "VARCHAR", "NULL", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'11111111-1111-1111-1111-111111111111'"),
                    new ConvertedColumnConversion("IPv4", "VARCHAR", "NULL", "'0.0.0.1'", "'192.168.1.1'", "'255.255.255.255'"),
                    new ConvertedColumnConversion("BOOLEAN", "VARCHAR", "true", "false"),
                    new ConvertedColumnConversion("BOOLEAN", "STRING", "true", "false"),
                    new ConvertedColumnConversion("BYTE", "VARCHAR", "0", "-128", "127"),
                    new ConvertedColumnConversion("SHORT", "STRING", "0", "-32_768", "32_767"),
                    new ConvertedColumnConversion("CHAR", "VARCHAR", "NULL", "'a'", "'ж'"),
                    new ConvertedColumnConversion("CHAR", "STRING", "NULL", "'a'", "'ж'"),
                    new ConvertedColumnConversion("DATE", "VARCHAR", dates),
                    new ConvertedColumnConversion("DATE", "STRING", dates),
                    // text to fixed
                    new ConvertedColumnConversion("STRING", "LONG", numericTexts),
                    new ConvertedColumnConversion("VARCHAR", "INT", numericTexts),
                    new ConvertedColumnConversion("VARCHAR", "DOUBLE", numericTexts),
                    new ConvertedColumnConversion("VARCHAR", "BOOLEAN", "NULL", "''", "'true'", "'TRUE'", "'false'", "'1'"),
                    new ConvertedColumnConversion("STRING", "BOOLEAN", "NULL", "''", "'true'", "'TrUe'", "'false'", "'0'"),
                    new ConvertedColumnConversion("VARCHAR", "BYTE", numericTexts),
                    new ConvertedColumnConversion("STRING", "SHORT", numericTexts),
                    new ConvertedColumnConversion("VARCHAR", "CHAR", "NULL", "'a'", "'z'"),
                    new ConvertedColumnConversion("STRING", "DATE", "NULL", "'2020-06-15T12:00:00.000Z'", "'1970-01-01T00:00:00.000Z'", "'abc'"),
                    new ConvertedColumnConversion("VARCHAR", "TIMESTAMP_NS", "NULL", "'2020-06-15T12:30:00.123456789Z'", "'1970-01-01T00:00:00.000000001Z'", "'abc'"),
                    new ConvertedColumnConversion("VARCHAR", "IPv4", "NULL", "'192.168.1.1'", "'255.255.255.255'", "'abc'"),
                    new ConvertedColumnConversion("STRING", "UUID", "NULL", "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'not-a-uuid'", "''", "'zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz'"),
                    new ConvertedColumnConversion("VARCHAR", "DECIMAL(2, 1)", "NULL", "'1.2'", "'0'", "'-1.5'", "'1.25'", "'999'"),
                    new ConvertedColumnConversion("STRING", "DECIMAL(4, 2)", "NULL", "'12.34'", "'-9.99'", "'1.005'", "'99999'"),
                    new ConvertedColumnConversion("VARCHAR", "DECIMAL(9, 3)", "NULL", "'123456.789'", "'-999.999'", "'9999999999'"),
                    new ConvertedColumnConversion(
                            "STRING",
                            "DECIMAL(76, 4)",
                            "NULL",
                            "'123456789012345678901234567890123456789012345678901234567890123456789012.5678'",
                            "'-99.9999'",
                            "'99999999999999999999999999999999999999999999999999999999999999999999999999999'"
                    ),
                    // text to text, and the symbol of a table to fixed and text
                    new ConvertedColumnConversion("STRING", "VARCHAR", texts),
                    new ConvertedColumnConversion("VARCHAR", "STRING", texts),
                    new ConvertedColumnConversion("VARCHAR", "STRING", "rpad('', 1_000_000, 'y')"),
                    new ConvertedColumnConversion("SYMBOL", "LONG", "NULL", "'42'", "'-1'", "'abc'"),
                    new ConvertedColumnConversion("SYMBOL", "VARCHAR", texts),
                    new ConvertedColumnConversion("SYMBOL", "STRING", texts),
            };
            assertStreamedExportOfConversions(conversions);
        });
    }

    @Test
    public void testCopyParquetExportOfConvertedColumnKeepsParquetEncodings() throws Exception {
        // A column retyped since its partition went Parquet streams its rows through the cursor exporter, whether the
        // select reaches the table directly or through a view, and whether it reads the Parquet partition or not, and
        // each column keeps its PARQUET(...) setting.
        assertMemoryLeak(() -> {
            final String[] filters = {"", " WHERE ts >= '2024-01-02'"};
            final String[] expectedRows = {
                    """
                    metric\tc\ttotal\tts
                    10\t1\t1000\t2024-01-01T00:00:00.000000Z
                    20\t2\t2000\t2024-01-01T08:00:00.000000Z
                    null\t3\tnull\t2024-01-01T16:00:00.000000Z
                    10\t4\t1000\t2024-01-02T00:00:00.000000Z
                    30\t5\t3000\t2024-01-02T08:00:00.000000Z
                    null\t6\tnull\t2024-01-02T16:00:00.000000Z
                    20\t7\t2000\t2024-01-03T00:00:00.000000Z
                    40\t8\t4000\t2024-01-03T08:00:00.000000Z
                    null\t9\tnull\t2024-01-03T16:00:00.000000Z
                    """,
                    """
                    metric\tc\ttotal\tts
                    10\t4\t1000\t2024-01-02T00:00:00.000000Z
                    30\t5\t3000\t2024-01-02T08:00:00.000000Z
                    null\t6\tnull\t2024-01-02T16:00:00.000000Z
                    20\t7\t2000\t2024-01-03T00:00:00.000000Z
                    40\t8\t4000\t2024-01-03T08:00:00.000000Z
                    null\t9\tnull\t2024-01-03T16:00:00.000000Z
                    """
            };
            for (boolean isWal : new boolean[]{true, false}) {
                for (ConvertedColumnExportSource source : ConvertedColumnExportSource.values()) {
                    final String tableName = (isWal ? "converted_wal_" : "converted_") + source.ordinal();
                    createTableWithParquetSettingsAndConvertedColumn(tableName, isWal, source);
                    final String from = source == ConvertedColumnExportSource.TABLE ? tableName : tableName + "_v";
                    for (int i = 0; i < filters.length; i++) {
                        final String query = "SELECT * FROM " + from + filters[i];
                        final String fileName = tableName + "_" + i;
                        assertCopyStreamsRows(query, fileName);
                        assertQuery("SELECT * FROM read_parquet('" + parquetPath(fileName) + "')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns(expectedRows[i]);
                        assertParquetSettingsKept(parquetPath(fileName));
                    }
                }
            }
        });
    }

    @Test
    public void testCopyParquetExportOfConvertedColumnWithBinaryUsesTempTable() throws Exception {
        // The cursor exporter cannot write BINARY, so the export of a retyped table goes through a temp table when the
        // result holds a BINARY column anywhere, from the table or computed, and streams without one.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                execute("CREATE TABLE rb (x INT, b BINARY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("""
                        INSERT INTO rb VALUES
                            (1, rnd_bin(2, 4, 0), '2024-01-01T00:00:00.000000Z'),
                            (2, NULL, '2024-01-01T12:00:00.000000Z'),
                            (3, rnd_bin(2, 4, 0), '2024-01-02T00:00:00.000000Z'),
                            (4, rnd_bin(2, 4, 0), '2024-01-03T00:00:00.000000Z')
                        """);
                drainWalQueue();
                execute("ALTER TABLE rb CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                execute("ALTER TABLE rb ALTER COLUMN x TYPE LONG");
                drainWalQueue();
                final String[] binaryQueries = {
                        "SELECT * FROM rb",
                        "SELECT b, x, ts FROM rb",
                        "SELECT x, ts, b FROM rb",
                        "SELECT x, ts, 'x'::BINARY c FROM rb",
                };
                for (int i = 0; i < binaryQueries.length; i++) {
                    final String fileName = "binary_" + i;
                    try {
                        assertCopyMaterializesRows(binaryQueries[i], fileName);
                        assertParquetMatchesQuery(binaryQueries[i], parquetPath(fileName));
                        ParquetTestUtils.assertColumnTypes(parquetPath(fileName), configuration.getFilesFacade(), ColumnType.LONG, i == 1 ? 1 : 0);
                    } catch (AssertionError e) {
                        throw new AssertionError(walMode + ": " + binaryQueries[i], e);
                    }
                }
                assertCopyStreamsRows("SELECT x, ts FROM rb", "no_binary");
                assertParquetRows("no_binary", "ts", """
                        x\tts
                        1\t2024-01-01T00:00:00.000000Z
                        2\t2024-01-01T12:00:00.000000Z
                        3\t2024-01-02T00:00:00.000000Z
                        4\t2024-01-03T00:00:00.000000Z
                        """);
                ParquetTestUtils.assertColumnTypes(parquetPath("no_binary"), configuration.getFilesFacade(), ColumnType.LONG, 0);
                execute("DROP TABLE rb");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testCopyParquetExportOfRetypedTableKeepsEveryTypeAndSetting() throws Exception {
        // A streamed export of a retyped table writes a column of every type the Parquet encoder writes, each with its
        // PARQUET(...) setting, and keeps the SYMBOL column as SYMBOL. With the BINARY column it goes through a temp
        // table, which keeps the settings too.
        assertMemoryLeak(() -> {
            ParquetTestUtils.createTableWithParquetSettingPerType(engine, sqlExecutionContext, "ty");
            execute("INSERT INTO ty (i, ts) VALUES (5, '2024-01-02T00:00:00.000000Z')");
            execute("ALTER TABLE ty CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            // ALTER COLUMN TYPE drops the PARQUET(...) setting of the column, so the retyped column is bf, whose setting
            // names no encoding
            execute("ALTER TABLE ty ALTER COLUMN bf TYPE LONG");
            final FilesFacade ff = configuration.getFilesFacade();

            final String query = "SELECT b, sh, ch, i, l, dt, t, tn, f, d, u, l256, ip, g1, g2, g4, g8, d8, d16, d32, d64, d128, d256, s, vc,"
                    + " sym, bo, arr, bf, bfe, ts FROM ty";
            assertCopyStreamsRows(query, "every_type");
            assertParquetMatchesQuery(query, parquetPath("every_type"));
            ParquetTestUtils.assertColumnTypes(parquetPath("every_type"), ff, ColumnType.LONG, 28);
            ParquetTestUtils.assertColumnsUseParquetSettingPerType(parquetPath("every_type"), ff, false);
            assertSymbolColumnsKept(parquetPath("every_type"), 25);

            assertCopyMaterializesRows("SELECT * FROM ty", "every_type_binary");
            assertParquetMatchesQuery("SELECT * FROM ty", parquetPath("every_type_binary"));
            ParquetTestUtils.assertColumnTypes(parquetPath("every_type_binary"), ff, ColumnType.LONG, 29);
            ParquetTestUtils.assertColumnsUseParquetSettingPerType(parquetPath("every_type_binary"), ff);
        });
    }

    @Test
    public void testCopyParquetExportPartitionByKeepsParquetEncodings() throws Exception {
        // Re-partitioning materializes through a temp table, for a SELECT and for a table alike.
        assertMemoryLeak(() -> {
            createTableWithParquetSettings("repartitioned");

            copyToParquet("COPY (SELECT * FROM repartitioned) TO 'by_day' WITH FORMAT PARQUET PARTITION_BY DAY");
            for (String day : new String[]{"2024-01-01", "2024-01-02", "2024-01-03"}) {
                final String parquetPath = exportRoot + File.separator + "by_day" + File.separator + day + ".parquet";
                assertParquetMatchesQuery("SELECT * FROM repartitioned WHERE ts IN '" + day + "'", parquetPath);
                assertParquetSettingsKept(parquetPath);
            }

            copyToParquet("COPY repartitioned TO 'by_month' WITH FORMAT PARQUET PARTITION_BY MONTH");
            assertParquetMatchesQuery("SELECT * FROM repartitioned", parquetPath("by_month"));
            assertParquetSettingsKept(parquetPath("by_month"));
        });
    }

    @Test
    public void testCopyParquetExportSymbolToFixedConvertedColumn() throws Exception {
        // symbol->fixed lazy conversion (SYMBOL->LONG; parquet stores the symbol as a UTF-8
        // BYTE_ARRAY decoded as VARCHAR_SLICE) over a parquet partition, exported via the
        // DIRECT_PAGE_FRAME path. (A symbol->VARCHAR export is layout-compatible and is fine;
        // only conversions that change the fixed/var storage class are affected.)
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("SYMBOL", "(x * 3)::STRING", "LONG", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """));
    }

    @Test
    public void testCopyParquetExportSymbolToFixedConvertedColumnThroughView() throws Exception {
        // Regression test for #7807: SYMBOL->LONG through a view. The DIRECT_PAGE_FRAME path
        // failed the export, since the parquet partition stores the column as a symbol.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("SYMBOL", "(x * 3)::STRING", "LONG", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE));
    }

    @Test
    public void testCopyParquetExportTableTurnedCompositeAfterEnqueueKeepsParquetEncodings() throws Exception {
        // When a table turns composite after COPY was queued, the export worker materializes it through a temp table
        // of its own.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            createTableNeedingQuotes();
            execute("ALTER TABLE \"src ct\" ALTER COLUMN i SET PARQUET(RLE_DICTIONARY)");
            drainWalQueue();

            // No export job runs yet, so the export, queued as a direct table read, waits until the table turns
            // composite.
            runAndFetchCopyExportID("COPY \"src ct\" TO 'turned_composite_settings' WITH FORMAT PARQUET", sqlExecutionContext);
            makeFirstPartitionComposite();
            runExportJob();
            assertExportFinished();
            assertCompositeExportKeepsParquetSetting("turned_composite_settings");
        });
    }

    @Test
    public void testCopyParquetExportTempTableEdgeShapesKeepParquetEncodings() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithParquetSettingsAndConvertedColumn("converted", true, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE);
            createTableWithParquetSettings("streamed");
            final FilesFacade ff = configuration.getFilesFacade();

            // An empty result still writes a file, one without row groups, whether it streams or goes through a temp
            // table, which its BINARY column needs.
            final String emptyQuery = "SELECT * FROM converted_v WHERE ts < '2000-01-01'";
            assertCopyStreamsRows(emptyQuery, "empty");
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("empty") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("metric\tc\ttotal\tts\n");
            final String emptyBinaryQuery = "SELECT *, 'x'::BINARY bin FROM converted_v WHERE ts < '2000-01-01'";
            assertCopyMaterializesRows(emptyBinaryQuery, "empty_binary");
            assertQuery("SELECT * FROM read_parquet('" + parquetPath("empty_binary") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("metric\tc\ttotal\tts\tbin\n");

            // How the encoder writes a lone row or a column of NULLs is not specified, so the export of the same
            // query over a table without a retype is the oracle, for the export that streams and for the one that
            // PARTITION_BY routes through a temp table.
            for (String template : new String[]{"SELECT * FROM %s WHERE ts = '2024-01-02T08:00:00.000000Z'", "SELECT * FROM %s WHERE metric IS NULL"}) {
                final String fileName = template.contains("IS NULL") ? "null_metric" : "single_row";
                assertCopyStreamsLikeStreamedTable(template, fileName);
                assertCopyThroughTempTableEncodesLikeStreaming(template, " PARTITION_BY YEAR", fileName + "_temp_table");
            }
            for (String fileName : new String[]{"null_metric", "null_metric_temp_table"}) {
                assertQuery("SELECT metric, total FROM read_parquet('" + parquetPath(fileName) + "')")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                metric\ttotal
                                null\tnull
                                null\tnull
                                null\tnull
                                """);
            }

            // Every row group keeps the setting.
            execute("CREATE TABLE row_groups (metric INT PARQUET(RLE_DICTIONARY), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO row_groups SELECT (x % 7)::INT, timestamp_sequence('2024-01-01', 1_000_000L) FROM long_sequence(5_000)");
            copyToParquet("COPY (SELECT * FROM row_groups) TO 'row_groups' WITH FORMAT PARQUET PARTITION_BY YEAR ROW_GROUP_SIZE 1000");
            assertParquetMatchesQuery("SELECT * FROM row_groups", parquetPath("row_groups"));
            ParquetTestUtils.assertRowGroupCount(parquetPath("row_groups"), ff, 5);
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath("row_groups"), ff, 0);

            // A column added after data exists holds NULL above its column top.
            execute("ALTER TABLE converted ADD COLUMN extra INT");
            drainWalAndViewQueues();
            execute("INSERT INTO converted VALUES (50, 10, 5000, '2024-01-04T00:00:00.000000Z', 7), (50, 11, 5000, '2024-01-04T08:00:00.000000Z', 7)");
            drainWalAndViewQueues();
            final String addedColumnQuery = "SELECT * FROM converted";
            assertCopyStreamsRows(addedColumnQuery, "added_column");
            copyThroughTempTable("COPY (" + addedColumnQuery + ") TO 'added_column_temp_table' WITH FORMAT PARQUET PARTITION_BY YEAR");
            for (String fileName : new String[]{"added_column", "added_column_temp_table"}) {
                assertQuery("SELECT * FROM read_parquet('" + parquetPath(fileName) + "')")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                metric\tc\ttotal\tts\textra
                                10\t1\t1000\t2024-01-01T00:00:00.000000Z\tnull
                                20\t2\t2000\t2024-01-01T08:00:00.000000Z\tnull
                                null\t3\tnull\t2024-01-01T16:00:00.000000Z\tnull
                                10\t4\t1000\t2024-01-02T00:00:00.000000Z\tnull
                                30\t5\t3000\t2024-01-02T08:00:00.000000Z\tnull
                                null\t6\tnull\t2024-01-02T16:00:00.000000Z\tnull
                                20\t7\t2000\t2024-01-03T00:00:00.000000Z\tnull
                                40\t8\t4000\t2024-01-03T08:00:00.000000Z\tnull
                                null\t9\tnull\t2024-01-03T16:00:00.000000Z\tnull
                                50\t10\t5000\t2024-01-04T00:00:00.000000Z\t7
                                50\t11\t5000\t2024-01-04T08:00:00.000000Z\t7
                                """);
                assertParquetSettingsKept(parquetPath(fileName));
                // extra has no setting, so it keeps the INT default, PLAIN
                ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath(fileName), ff, 4);
                ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath(fileName), ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 4);
            }
        });
    }

    @Test
    public void testCopyParquetExportTempTableEncodingFollowsAlterSetParquet() throws Exception {
        // A setting that changes after COPY is queued makes the queued plan stale, so the export worker compiles the
        // select again, once, and encodes metric by the setting the select carries then: the export that streams the
        // rows of the retyped column, and the one that PARTITION_BY routes through a temp table, whose CREATE TABLE AS
        // SELECT compiles the select on the worker. A setting that changes between two exports applies to the next file.
        assertMemoryLeak(() -> {
            createTableWithParquetSettingsAndConvertedColumn("converted", true, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE);
            final String query = "SELECT * FROM converted_v";
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            final FilesFacade ff = configuration.getFilesFacade();
            final String[] copyOptions = {"", " PARTITION_BY YEAR"};
            final String[] expectedLogs = {STREAMING_EXPORT_LOG, TEMP_TABLE_EXPORT_LOG};
            for (int i = 0; i < copyOptions.length; i++) {
                final String queuedFileName = "queued_" + i;
                final String resetFileName = "reset_" + i;
                // No export job runs yet, so the setting changes after COPY is queued and before the worker runs it.
                final String exportId = queueCopy("COPY (" + query + ") TO '" + queuedFileName + "' WITH FORMAT PARQUET" + copyOptions[i]);
                execute("ALTER TABLE converted ALTER COLUMN metric SET PARQUET(DELTA_BINARY_PACKED)");
                drainWalAndViewQueues();
                // A pooled reader keeps the PARQUET(...) setting it loaded before the ALTER, so the test releases the
                // readers for the export to compile its select over the new setting.
                engine.releaseAllReaders();
                Assert.assertEquals(1, runExportJobCountingCompiles());
                assertExportLog(exportId, expectedLogs[i]);
                assertParquetMatchesQuery(query, parquetPath(queuedFileName));
                ParquetTestUtils.assertColumnsUseEncoding(parquetPath(queuedFileName), ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0);
                ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath(queuedFileName), ff, 0);

                execute("ALTER TABLE converted ALTER COLUMN metric SET PARQUET(default)");
                drainWalAndViewQueues();
                engine.releaseAllReaders();
                copyToParquet("COPY (" + query + ") TO '" + resetFileName + "' WITH FORMAT PARQUET" + copyOptions[i]);
                assertParquetMatchesQuery(query, parquetPath(resetFileName));
                // metric has no setting now, so it takes the INT default, PLAIN
                ParquetTestUtils.assertColumnsUseEncoding(parquetPath(resetFileName), ff, ParquetEncoding.ENCODING_PLAIN, 0);
                ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath(resetFileName), ff, 0);
                ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath(resetFileName), ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 0);
            }
        });
    }

    @Test
    public void testCopyParquetExportTempTableKeepsParquetCompression() throws Exception {
        // COMPRESSION_CODEC sets the codec of the file and a column's PARQUET(..., ZSTD(3)) setting overrides it.
        // 100_000 LONG values take 800_000 bytes as uncompressed PLAIN, so only a compressed v fits a smaller file.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE compressed (v LONG PARQUET(default, ZSTD(3)), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO compressed SELECT 7, timestamp_sequence('2024-01-01', 100_000) FROM long_sequence(100_000)");
            execute("CREATE TABLE uncompressed (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO uncompressed SELECT v, ts FROM compressed");

            for (String tableName : new String[]{"compressed", "uncompressed"}) {
                copyToParquet("COPY (SELECT * FROM " + tableName + ") TO '" + tableName + "' WITH FORMAT PARQUET COMPRESSION_CODEC UNCOMPRESSED PARTITION_BY YEAR");
                assertQuery("SELECT count(), sum(v), min(ts), max(ts) FROM read_parquet('" + parquetPath(tableName) + "')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tsum\tmin\tmax
                                100000\t700000\t2024-01-01T00:00:00.000000Z\t2024-01-01T02:46:39.900000Z
                                """);
            }
            final long compressedSize = parquetFileSize("compressed");
            Assert.assertTrue("v must be compressed, file size: " + compressedSize, compressedSize < 800_000);
            final long uncompressedSize = parquetFileSize("uncompressed");
            Assert.assertTrue("v without a setting must not be compressed, file size: " + uncompressedSize, uncompressedSize >= 800_000);
        });
    }

    @Test
    public void testCopyParquetExportTempTableKeepsParquetEncodingsOfEveryType() throws Exception {
        // COPY <table> encodes the table partition by partition and applies its PARQUET(...) settings, so its file
        // is the reference for the temp table that PARTITION_BY materializes.
        assertMemoryLeak(() -> {
            ParquetTestUtils.createTableWithParquetSettingPerType(engine, sqlExecutionContext, "ty");
            copyToParquet("COPY ty TO 'ty_reference' WITH FORMAT PARQUET");
            copyToParquet("COPY (SELECT * FROM ty) TO 'ty_temp_table' WITH FORMAT PARQUET PARTITION_BY YEAR");

            final String parquetPath = parquetPath("ty_temp_table");
            final FilesFacade ff = configuration.getFilesFacade();
            assertParquetMatchesQuery("SELECT * FROM ty", parquetPath);
            ParquetTestUtils.assertColumnsUseParquetSettingPerType(parquetPath, ff);
            ParquetTestUtils.assertColumnEncodingsMatch(parquetPath("ty_reference"), parquetPath, ff);

            // An export writes bloom filters for the columns BLOOM_FILTER_COLUMNS names, as the streaming paths do,
            // and ignores the BLOOM_FILTER setting of bf.
            assertBloomFilterOnBf(parquetPath, false);
            copyToParquet("COPY (SELECT * FROM ty) TO 'ty_bloom_filter' WITH FORMAT PARQUET PARTITION_BY YEAR BLOOM_FILTER_COLUMNS 'bf'");
            assertBloomFilterOnBf(parquetPath("ty_bloom_filter"), true);
        });
    }

    @Test
    public void testCopyParquetExportTempTableQueryShapesKeepParquetEncodings() throws Exception {
        assertMemoryLeak(() -> {
            createTableWithParquetSettingsAndConvertedColumn("converted", true, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE);
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_153_600_000_000L); // 2024-01-02T00:00:00Z

            // A column the select passes through keeps its setting under any name, and a computed one takes its
            // type's default. A scan of the retyped column streams its rows (testRetypedExportStreamsTableScanShapes),
            // and a join does not see the retyped column, so PARTITION_BY, or a BINARY column for a query without an
            // ascending designated timestamp, routes these to the temp table.
            assertCopyThroughTempTableEncodesFirstColumn("SELECT metric AS m, ts FROM converted_v", " PARTITION_BY YEAR", "alias", true);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT metric + 1 AS mp, ts FROM converted_v", " PARTITION_BY YEAR", "computed", false);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT * FROM (SELECT * FROM converted_v)", " PARTITION_BY YEAR", "subquery", true);
            assertCopyThroughTempTableEncodesFirstColumn("WITH w AS (SELECT * FROM converted_v) SELECT * FROM w", " PARTITION_BY YEAR", "cte", true);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT converted_v.* FROM converted_v CROSS JOIN long_sequence(1)", " PARTITION_BY YEAR", "cross_join", true);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT *, 'x'::BINARY bin FROM converted_v ORDER BY ts DESC LIMIT 5", "", "order_by_desc_limit", true);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT * FROM converted_v LIMIT -3", " PARTITION_BY YEAR", "negative_limit", true);
            assertCopyThroughTempTableEncodesFirstColumn("SELECT * FROM converted_v WHERE ts >= $1", " PARTITION_BY YEAR", "bind_variable", true);
            // A GROUP BY or SAMPLE BY key keeps the setting of its column, and an aggregate takes its type's default.
            assertCopyThroughTempTableEncodesFirstColumn("SELECT metric, count() c, 'x'::BINARY bin FROM converted_v ORDER BY metric", "", "group_by", true);
            // count() has no setting, so it takes the LONG default, PLAIN
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("group_by"), configuration.getFilesFacade(), 1);
            ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath("group_by"), configuration.getFilesFacade(), ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 1);

            // The select metadata decides which columns of these shapes carry a setting, and the streaming paths
            // read the same metadata, so the export of the same query that streams is the oracle. A plan whose
            // timestamp a table scan does not order keeps the temp table, and the set operations do not see the
            // retyped column, so PARTITION_BY routes them to the temp table.
            createTableWithParquetSettings("streamed");
            final FilesFacade ff = configuration.getFilesFacade();
            assertCopyThroughTempTableEncodesLikeStreaming("SELECT ts, last(metric) lm, sum(total) st FROM %s SAMPLE BY 1d", "", "sample_by");
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("sample_by"), ff, 1);
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("sample_by_streamed"), ff, 1);
            assertCopyThroughTempTableEncodesLikeStreaming("SELECT ts, metric, count() c FROM %s SAMPLE BY 1d", "", "sample_by_key");
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath("sample_by_key"), ff, 1);
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath("sample_by_key_streamed"), ff, 1);
            // LATEST ON over the view keeps no designated timestamp, so it streams, and a BINARY column routes it here
            assertCopyThroughTempTableEncodesFirstColumn("SELECT *, 'x'::BINARY bin FROM converted_v LATEST ON ts PARTITION BY metric", "", "latest_on", true);
            assertParquetSettingsKept(parquetPath("latest_on"));
            assertCopyThroughTempTableEncodesLikeStreaming("SELECT metric, total, ts, row_number() OVER (PARTITION BY metric ORDER BY ts) rn FROM %s", "", "window");
            assertCopyThroughTempTableEncodesLikeStreaming(
                    "SELECT * FROM (SELECT * FROM %s WHERE ts < '2024-01-02' UNION ALL SELECT * FROM %s WHERE ts >= '2024-01-02') TIMESTAMP(ts)",
                    " PARTITION_BY YEAR",
                    "union_all"
            );
            assertCopyThroughTempTableEncodesLikeStreaming("SELECT * FROM %s INTERSECT SELECT * FROM %s WHERE ts >= '2024-01-02'", " PARTITION_BY YEAR", "intersect");
            assertCopyThroughTempTableEncodesLikeStreaming("SELECT * FROM %s EXCEPT SELECT * FROM %s WHERE ts < '2024-01-02'", " PARTITION_BY YEAR", "except");
        });
    }

    @Test
    public void testCopyParquetExportVarToFixedConvertedColumnSurvivesEmptyMetadataCacheWindow() throws Exception {
        // var->fixed lazy conversion (STRING->LONG) over a parquet partition, exported via the
        // CURSOR_BASED path. The empty cache forces determineExportMode() to hydrate the table
        // metadata before it can detect the pending conversion.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("STRING", "(x * 3)::STRING", "LONG", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """, true));
    }

    @Test
    public void testCopyParquetExportVarToFixedConvertedColumnThroughView() throws Exception {
        // Regression test for #7807: STRING->LONG through a view. The DIRECT_PAGE_FRAME path
        // exported the raw STRING bytes, length prefix included, as LONG values.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("STRING", "(x * 1_000_003)::STRING", "LONG", """
                s\tts
                1000003\t2024-01-01T00:00:00.000000Z
                2000006\t2024-01-01T00:10:00.000000Z
                3000009\t2024-01-01T00:20:00.000000Z
                4000012\t2024-01-01T00:30:00.000000Z
                5000015\t2024-01-01T00:40:00.000000Z
                6000018\t2024-01-01T00:50:00.000000Z
                """, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE));
    }

    @Test
    public void testCopyParquetExportVarcharToIntConvertedColumnThroughViewCreatedBeforeTypeChange() throws Exception {
        // Regression test for #7807: VARCHAR->INT through a view that exists before the type
        // change. The DIRECT_PAGE_FRAME path exported the ASCII digits as INT values.
        assertMemoryLeak(() -> assertParquetExportOfConvertedColumn("VARCHAR", "lpad((x * 3)::VARCHAR, 12, '0')", "INT", """
                s\tts
                3\t2024-01-01T00:00:00.000000Z
                6\t2024-01-01T00:10:00.000000Z
                9\t2024-01-01T00:20:00.000000Z
                12\t2024-01-01T00:30:00.000000Z
                15\t2024-01-01T00:40:00.000000Z
                18\t2024-01-01T00:50:00.000000Z
                """, ConvertedColumnExportSource.VIEW_CREATED_BEFORE_TYPE_CHANGE));
    }

    @Test
    public void testCopyParquetFailsWithIllegalSql() throws Exception {
        assertQuery("copy (select x from non_existing_table) to 'tmp' with format parquet")
                .fails(20, "table does not exist [table=non_existing_table]");
        assertQuery("copy (select a+1 from1 v) to 'tmp' with format parquet")
                .fails(23, "found [tok='v', len=1] ',', 'from' or 'over' expected");
        assertQuery("copy (select 1) to 'tmp' with format csv")
                .fails(37, "unsupported format, only 'parquet' is supported");
    }

    @Test
    public void testCopyParquetFailsWithMmapErrorCleansTempTable() throws Exception {
        final LongHashSet tempTableColumnFds = new LongHashSet();
        final AtomicBoolean failed = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean close(long fd) {
                tempTableColumnFds.remove(fd);
                return super.close(fd);
            }

            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (tempTableColumnFds.contains(fd) && failed.compareAndSet(false, true)) {
                    return -1;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, File.separator + "copy.") && Utf8s.endsWithAscii(name, ".d")) {
                    tempTableColumnFds.add(fd);
                }
                return fd;
            }
        };

        assertMemoryLeak(ff, () -> {
            // BINARY column forces the TEMP_TABLE export path
            execute("CREATE TABLE test_table (x INT, y LONG, z BINARY)");
            execute("INSERT INTO test_table VALUES (1, 100, NULL), (2, 200, NULL)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (test_table where x > 0) TO 'mmap_fail_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        status
                                        failed
                                        """);

                        ObjHashSet<TableToken> bucket = new ObjHashSet<>();
                        engine.getTableTokens(bucket, false);
                        for (int i = 0, n = bucket.size(); i < n; i++) {
                            TableToken token = bucket.get(i);
                            Assert.assertFalse(
                                    "temp table should have been cleaned up: " + token.getTableName(),
                                    token.getTableName().startsWith("copy.")
                            );
                        }
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFailsWithNonExistentTable() throws Exception {
        assertQuery("copy test_table to 'blah blah blah' with format parquet")
                .fails(0, "table does not exist [table=test_table]");
    }

    @Test
    public void testCopyParquetFailsWithReadOnlyPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            final FilesFacade ff = configuration.getFilesFacade();

            // Create a file where a directory is expected to force a failure
            try (Path readOnlyPath = new Path()) {
                readOnlyPath.of(exportRoot).concat("readonly").$();
                ff.touch(readOnlyPath.$()); // now 'readonly' is a file, not a directory
            }

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'readonly/output' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        // This export should fail due to permission issues
                        try {
                            String status;
                            try (
                                    RecordCursorFactory factory = select("SELECT status FROM \"sys.copy_export_log\" LIMIT -1");
                                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                            ) {
                                if (cursor.hasNext()) {
                                    CharSequence value = cursor.getRecord().getStrA(0);
                                    status = value != null ? value.toString() : null;
                                } else {
                                    status = null;
                                }
                            }
                            assertTrue("Export should fail", status != null && (status.contains("failed") || status.contains("error")));
                        } catch (Exception e) {
                            // Expected failure due to permissions or path issues
                            assertTrue(e.getMessage().contains("could not") || e instanceof CairoException);
                        }
                    });

            try {
                testCopyExport(stmt, test);
            } catch (Exception e) {
                // Expected failure due to permissions or path issues
                assertTrue(e.getMessage().contains("could not") || e instanceof CairoException);
            }
        });
    }

    @Test
    public void testCopyParquetFailsWithSpecifyPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts timestamp, x int) timestamp(ts) partition by DAY");
            execute("insert into test_table values ('2023-01-01T10:00:00.000Z', 1), ('2023-01-02T10:00:00.000Z', 2), ('2023-02-01T10:00:00.000Z', 3), ('2023-02-02T10:00:00.000Z', 4)");
            CopyExportRunnable stmt = () -> runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet partition_by MONTH", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files,phase,status,message,errors FROM sys.copy_export_log")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tphase\tstatus\tmessage\terrors\n" +
                                        "\tnull\twait_to_run\tstarted\tqueued\t0\n" +
                                        "\tnull\twait_to_run\tfinished\t\t0\n" +
                                        "\tnull\tpopulating_data_to_temp_table\tstarted\t\t0\n" +
                                        "\tnull\tpopulating_data_to_temp_table\tfinished\t\t0\n" +
                                        "\tnull\tconverting_partitions\tstarted\t\t0\n" +
                                        "\tnull\tconverting_partitions\tfinished\t\t0\n" +
                                        "\tnull\tmove_files\tstarted\t\t0\n" +
                                        "\tnull\tmove_files\tfinished\t\t0\n" +
                                        "\tnull\tdropping_temp_table\tstarted\t\t0\n" +
                                        "\tnull\tdropping_temp_table\tfinished\t\t0\n" +
                                        exportRoot + File.separator + "test_table" + File.separator + "\t2\tsuccess\tfinished\t\t0\n");
                        // Verify count and sample data
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2023-01.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-01-01T10:00:00.000000Z\t1
                                        2023-01-02T10:00:00.000000Z\t2
                                        """);
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2023-02.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-02-01T10:00:00.000000Z\t3
                                        2023-02-02T10:00:00.000000Z\t4
                                        """);
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("path\tdiskSizeHuman\n" +
                                        "test_table" + File.separator + "2023-01.parquet\t672.0 B\n" +
                                        "test_table" + File.separator + "2023-02.parquet\t672.0 B\n");
                    });
            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFromParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1)");
            drainWalQueue();

            execute("alter table test_table convert partition to parquet where ts < '2020-01-02T00:00:00.000000Z'");
            drainWalQueue();
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("export_path\tnum_exported_files\tstatus\n" +
                                    exportRoot + File.separator + "test_table" + File.separator + "\t2\tfinished\n"));
            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFromParquet1() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");

            drainWalQueue();
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    export_path\tnum_exported_files\tstatus
                                    \t0\tfinished
                                    """));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFromParquet2() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values ('2020-01-01T00:00:00.000000Z', 0), ('2020-01-02T00:00:00.000000Z', 1)");
            drainWalQueue();
            execute("alter table test_table convert partition to parquet where ts < '2020-01-02T00:00:00.000000Z'");
            drainWalQueue();
            execute("insert into test_table values ('2020-01-01T00:00:01.000000Z', 10)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status, message FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\tmessage\n" +
                                        exportRoot + File.separator + "test_table" + File.separator + "\t2\tfinished\t\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2020-01-01.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2020-01-01T00:00:00.000000Z\t0
                                        2020-01-01T00:00:01.000000Z\t10
                                        """);
                        // The single-pass conversion leaves the table name out of the file schema's root, so
                        // the converted 2020-01-01 is ten bytes - "test_table" - short of a named one.
                        assertQuery("select path, diskSizeHuman from export_files()  order by path")
                                .noLeakCheck()
                                .returns("path\tdiskSizeHuman\n" +
                                        "test_table" + File.separator + "2020-01-01.parquet\t656.0 B\n" +
                                        "test_table" + File.separator + "2020-01-02.parquet\t648.0 B\n");
                    });
            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetLargeTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table large_table (id int, value string)");

            // Insert multiple rows to test larger datasets
            StringBuilder insertQuery = new StringBuilder("insert into large_table values ");
            for (int i = 0; i < 10000; i++) {
                if (i > 0) insertQuery.append(", ");
                insertQuery.append("(").append(i).append(", 'value").append(i).append("')");
            }
            execute(insertQuery.toString());

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy large_table to 'output_large' with format parquet row_group_size 100", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output_large.parquet" + "\t1\tfinished\n");
                        // Verify count and sample data
                        assertQuery("select count(*) from read_parquet('" + exportRoot + File.separator + "output_large.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n10000\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_large.parquet') where id = 0")
                                .noLeakCheck()
                                .returns("id\tvalue\n0\tvalue0\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_large.parquet') where id = 999")
                                .noLeakCheck()
                                .returns("id\tvalue\n999\tvalue999\n");
                        assertQuery("select path, diskSizeHuman from export_files()  order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        output_large.parquet\t126.2 KiB
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetOnMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table base_price (" +
                            "  sym symbol, price double, ts timestamp_ns" +
                            ") timestamp(ts) partition by DAY WAL"
            );
            execute(
                    "insert into base_price(sym, price, ts) values ('gbpusd', 1.320, '2023-09-10T12:01')" +
                            ",('gbpusd', 1.323, '2023-09-10T12:02')" +
                            ",('jpyusd', 103.21, '2023-11-10T12:02')" +
                            ",('jpyusd', 1.321, '2023-11-10T12:03')"
            );
            drainWalQueue();

            execute(
                    "create materialized view price_1h as " +
                            "select sym, last(price) as price, ts from base_price sample by 1h;"
            );

            currentMicros = parseFloorPartialTimestamp("2024-01-01T01:01:01.000000Z");
            drainWalAndMatViewQueues();
            drainPurgeJob();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy price_1h to 'price_1h' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "price_1h" + File.separator + "\t2\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "price_1h" + File.separator + "2023-09.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        sym\tprice\tts
                                        gbpusd\t1.323\t2023-09-10T12:00:00.000000000Z
                                        """);
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "price_1h" + File.separator + "2023-11.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        sym\tprice\tts
                                        jpyusd\t1.321\t2023-11-10T12:00:00.000000000Z
                                        """);
                        assertQuery("select path, diskSizeHuman from export_files()  order by path")
                                .noLeakCheck()
                                .returns("path\tdiskSizeHuman\n" +
                                        "price_1h" + File.separator + "2023-09.parquet\t1022.0 B\n" +
                                        "price_1h" + File.separator + "2023-11.parquet\t1.0 KiB\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetOnMatViewWithRowExpiry() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-05T01:00:00.000000Z'),
                    ('CCC', 0.5, '2024-01-05T02:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // The policy hides the two expired rows from every read of the view.
            assertQuery("SELECT sym, v FROM mv")
                    .noLeakCheck()
                    .returns("""
                            sym\tv
                            BBB\t5.0
                            """);

            // The bare-table spelling of COPY must export the same rows the view returns.
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy mv to 'mv_expiry' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("status\nfinished\n");
                        assertQuery("select sym, v from read_parquet('" + exportRoot + File.separator + "mv_expiry.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        sym\tv
                                        BBB\t5.0
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetOnMatViewWithRowExpiryHyphenatedName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_hyphen (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base_hyphen VALUES
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-05T01:00:00.000000Z'),
                    ('CCC', 0.5, '2024-01-05T02:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW \"orders-daily\" AS (SELECT * FROM base_hyphen) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // The hyphen makes this name an expression when it reaches the compiler unquoted, so the
            // policy route has to serialize it as one identifier.
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy \"orders-daily\" to 'orders_daily' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("status\nfinished\n");
                        assertQuery("select sym, v from read_parquet('" + exportRoot + File.separator + "orders_daily.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        sym\tv
                                        BBB\t5.0
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetOnMatViewWithRowExpiryNameCollision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_collision (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base_collision VALUES
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-05T01:00:00.000000Z'),
                    ('CCC', 0.5, '2024-01-05T02:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW \"my view\" AS (SELECT * FROM base_collision) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // A table named after the first word of the view. Unquoted, "my view" is a valid query that
            // reads this table under the alias "view", so a lost pair of quotes exports the wrong rows
            // and still reports success.
            execute("CREATE TABLE my (decoy SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO my VALUES ('DECOY', '2024-01-05T00:00:00.000000Z')");
            drainWalQueue();

            // COPY takes the source in three quoting forms and hands the factory the same semantic name
            // for each, so every spelling must reach the view.
            assertMyViewExportKeepsIdentity("'my view'", "my_view_single");
            assertMyViewExportKeepsIdentity("\"my view\"", "my_view_double");
            assertMyViewExportKeepsIdentity("`my view`", "my_view_backtick");
        });
    }

    @Test
    public void testCopyParquetRepartitionKeepsQuotedTableName() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE \"repart table\" (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO "repart table" VALUES
                    ('2023-01-01T10:00:00.000000Z', 1),
                    ('2023-02-01T10:00:00.000000Z', 2)
                    """);

            // Re-partitioning routes through the compiler as well, so it shares the serialization site.
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy 'repart table' to 'repart_out' with format parquet partition_by MONTH", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("status\nfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "repart_out" + File.separator + "2023-01.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-01-01T10:00:00.000000Z\t1
                                        """);
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "repart_out" + File.separator + "2023-02.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-02-01T10:00:00.000000Z\t2
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetReusedFactoryOnMatViewWithRowExpiry() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_reuse (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base_reuse VALUES
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-05T01:00:00.000000Z')
                    """);
            drainWalQueue();
            execute("CREATE MATERIALIZED VIEW \"reuse view\" AS (SELECT * FROM base_reuse) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // One factory, two executions: the second one must resolve the view again rather than the
            // staging table the first one generated.
            CopyExportRunnable stmt = () -> {
                try (RecordCursorFactory factory = select("copy 'reuse view' to 'reuse_out' with format parquet")) {
                    fetchCopyExportID(factory);
                    awaitExportsDrained();
                    fetchCopyExportID(factory);
                }
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" WHERE status = 'finished' AND phase = 'success'")
                                .noLeakCheck()
                                .returns("""
                                        status
                                        finished
                                        finished
                                        """);
                        assertQuery("select sym, v from read_parquet('" + exportRoot + File.separator + "reuse_out.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        sym\tv
                                        BBB\t5.0
                                        """);
                    });

            testCopyExport(stmt, test, true, 2);
        });
    }

    @Test
    public void testCopyParquetReusedFactoryOnQuerySource() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base_reuse_query (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base_reuse_query VALUES
                    ('AAA', 1.0, '2024-01-05T00:00:00.000000Z'),
                    ('BBB', 5.0, '2024-01-05T01:00:00.000000Z')
                    """);
            drainWalQueue();

            // The query source carries the same hazard: the staging name must not take the place of the
            // COPY source between executions.
            CopyExportRunnable stmt = () -> {
                try (RecordCursorFactory factory = select("copy (select sym, v from base_reuse_query where v > 2.0) to 'reuse_query_out' with format parquet")) {
                    fetchCopyExportID(factory);
                    awaitExportsDrained();
                    fetchCopyExportID(factory);
                }
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" WHERE status = 'finished' AND phase = 'success'")
                                .noLeakCheck()
                                .returns("""
                                        status
                                        finished
                                        finished
                                        """);
                        assertQuery("select sym, v from read_parquet('" + exportRoot + File.separator + "reuse_query_out.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        sym\tv
                                        BBB\t5.0
                                        """);
                    });

            testCopyExport(stmt, test, true, 2);
        });
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidCompressionLevel() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet compression_level 'invalid'")
                .fails(66, "found [tok=''invalid'', len=9] bad integer");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidDataPageSize() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet data_page_size 'invalid'")
                .fails(63, "found [tok=''invalid'', len=9] bad integer");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidFormat() throws Exception {
        assertQuery("copy test_table to 'output' with format invalid")
                .fails(40, "unsupported format, only 'parquet' is supported");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidOptionName() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet invalid_option 'value'")
                .fails(48, "unrecognised option [option=invalid_option]");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidParquetVersion() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet parquet_version 'invalid'")
                .fails(64, "found [tok=''invalid'', len=9] bad integer");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidRowGroupSize() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet row_group_size 'invalid'")
                .fails(63, "found [tok=''invalid'', len=9] bad integer");
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidStatisticsValue() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet statistics_enabled 'invalid'")
                .fails(67, "unexpected token ['invalid']");
    }

    @Test
    public void testCopyParquetSyntaxErrorMissingFormat() throws Exception {
        assertQuery("copy test_table to 'output' with parquet")
                .fails(33, "unrecognised option [option=parquet]");
    }

    @Test
    public void testCopyParquetSyntaxErrorMissingOptionValue() throws Exception {
        assertQuery("copy test_table to 'output' with format parquet compression_codec unknown")
                .fails(66, "invalid compression codec[unknown], expected one of: uncompressed, snappy, gzip, brotli, zstd, lz4_raw");
    }

    @Test
    public void testCopyParquetWithAllDataTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table all_types (" +
                    "bool_col boolean, " +
                    "byte_col byte, " +
                    "short_col short, " +
                    "int_col int, " +
                    "long_col long, " +
                    "float_col float, " +
                    "double_col double, " +
                    "string_col string, " +
                    "symbol_col symbol, " +
                    "t_ns timestamp_ns, " +
                    "d_array DOUBLE[], " +
                    "ts timestamp" +
                    ") timestamp(ts)");

            execute("insert into all_types values (" +
                    "true, 1, 100, 1000, 10000L, 1.5f, 2.5, 'test', 'sym1', '2023-01-01T10:00:00.123456789Z', ARRAY[1.0, 2, 3],'2023-01-01T10:00:00.000Z'" +
                    ")");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy all_types to 'output_all_types' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output_all_types.parquet" + "\t1\tfinished\n");
                        // Verify all data types are preserved correctly
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_all_types" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tt_ns\td_array\tts
                                        true\t1\t100\t1000\t10000\t1.5\t2.5\ttest\tsym1\t2023-01-01T10:00:00.123456789Z\t[1.0,2.0,3.0]\t2023-01-01T10:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetWithAsyncMonitoringAllDataTypes() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table comprehensive_types (" +
                    "bool_col boolean, " +
                    "byte_col byte, " +
                    "short_col short, " +
                    "int_col int, " +
                    "long_col long, " +
                    "float_col float, " +
                    "double_col double, " +
                    "string_col string, " +
                    "symbol_col symbol, " +
                    "ts timestamp" +
                    ") timestamp(ts)");

            execute("insert into comprehensive_types values (" +
                    "true, 42, 1000, 100000, 1000000L, 3.14f, 2.718, 'hello world', 'symbol1', '2023-06-15T14:30:00.000Z'" +
                    ")");

            runAndFetchCopyExportID("copy comprehensive_types to 'async_types' with format parquet " +
                    "parquet_version 2 data_page_size 2048", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportFileExists("async_types"));

            String query = "select status from \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" limit -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");

            assertQuery("select * from read_parquet('" + exportRoot + File.separator + "async_types" + ".parquet')")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts
                            true\t42\t1000\t100000\t1000000\t3.14\t2.718\thello world\tsymbol1\t2023-06-15T14:30:00.000000Z
                            """);
        };

        testCopyExport(statement, test);
    }

    @Test
    public void testCopyParquetWithAsyncMonitoringLargeDataset() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table large_dataset (id int, data string)");

            StringBuilder insertQuery = new StringBuilder("insert into large_dataset values ");
            for (int i = 0; i < 500; i++) {
                if (i > 0) insertQuery.append(", ");
                insertQuery.append("(").append(i).append(", 'data").append(i).append("')");
            }
            execute(insertQuery.toString());

            runAndFetchCopyExportID("copy large_dataset to 'async_large' with format parquet " +
                    "row_group_size 100 compression_codec snappy", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportFileExists("async_large"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");

            assertQuery("select count(*) from read_parquet('" + exportRoot + File.separator + "async_large" + ".parquet')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n500\n");
            assertQuery("select * from read_parquet('" + exportRoot + File.separator + "async_large" + ".parquet') where id = 0")
                    .noLeakCheck()
                    .returns("id\tdata\n0\tdata0\n");
            assertQuery("select * from read_parquet('" + exportRoot + File.separator + "async_large" + ".parquet') where id = 499")
                    .noLeakCheck()
                    .returns("id\tdata\n499\tdata499\n");
        };

        testCopyExport(statement, test);
    }

    // Additional tests using async export monitoring pattern
    @Test
    public void testCopyParquetWithAsyncMonitoringMultipleOptions() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table test_table (id int, name string, value double)");
            execute("insert into test_table values (1, 'alpha', 1.1), (2, 'beta', 2.2), (3, 'gamma', 3.3)");

            runAndFetchCopyExportID("copy test_table to 'async_output1' with format parquet " +
                    "compression_codec gzip row_group_size 2000 statistics_enabled true", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportFileExists("async_output1"));

            String query = "select status from \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" limit -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");

            assertQuery("select * from read_parquet('" + exportRoot + File.separator + "async_output1" + ".parquet') order by id")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tname\tvalue
                            1\talpha\t1.1
                            2\tbeta\t2.2
                            3\tgamma\t3.3
                            """);
        };

        testCopyExport(statement, test);
    }

    @Test
    public void testCopyParquetWithComplexQuery() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table orders (id int, customer_id int, amount double, order_date timestamp) timestamp(order_date)");
            execute("create table customers (id int, name string, country string)");

            execute("insert into customers values (1, 'John', 'USA'), (2, 'Jane', 'UK')");
            execute("insert into orders values (1, 1, 100.50, '2023-01-01T10:00:00.000Z'), (2, 2, 200.75, '2023-01-02T11:00:00.000Z')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy (" +
                            "select o.id, c.name, o.amount, o.order_date " +
                            "from orders o " +
                            "join customers c on o.customer_id = c.id " +
                            "where o.amount > 100" +
                            ") to 'output_complex' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output_complex.parquet" + "\t1\tfinished\n");
                        // Verify complex query results
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_complex" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("order_date")
                                .returns("""
                                        id\tname\tamount\torder_date
                                        1\tJohn\t100.5\t2023-01-01T10:00:00.000000Z
                                        2\tJane\t200.75\t2023-01-02T11:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (null, null, null), (3, 'world', 3.5)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output_nulls' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output_nulls.parquet" + "\t1\tfinished\n");
                        // Verify null values are handled correctly
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_nulls" + ".parquet') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty\tz
                                        null\t\tnull
                                        1\thello\t1.5
                                        3\tworld\t3.5
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetWithSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'hello\\nworld'), (2, 'tab\\there')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output_special' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output_special.parquet" + "\t1\tfinished\n");
                        // Verify special characters are preserved
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output_special" + ".parquet') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty
                                        1\thello\\nworld
                                        2\ttab\\there
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetWithTableSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table❤️ (x int, y string)");
            execute("insert into test_table❤️ values (1, 'hello\\nworld11'), (2, 'tab\\there')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy `test_table❤️` to '❤️🍺' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "❤️🍺.parquet" + "\t1\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "❤️🍺" + ".parquet') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty
                                        1\thello\\nworld11
                                        2\ttab\\there
                                        """);
                        assertQuery("select path, diskSizeHuman from export_files()  order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        ❤️🍺.parquet\t697.0 B
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryCursorBackedPreservesDictionaryEncodingAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_copy_cursor_src (
                        metric INT PARQUET(RLE_DICTIONARY),
                        total LONG PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_copy_cursor_src VALUES
                        (10, 1000, '2020-01-01T00:00:00.000000Z'),
                        (20, 2000, '2020-01-01T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-01T02:00:00.000000Z'),
                        (10, 1000, '2020-01-02T00:00:00.000000Z'),
                        (30, 3000, '2020-01-02T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-02T02:00:00.000000Z'),
                        (20, 2000, '2020-01-03T00:00:00.000000Z'),
                        (40, 4000, '2020-01-03T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-03T02:00:00.000000Z')
                    """);

            final String parquetPath = exportRoot + File.separator + "dict_copy_cursor_output.parquet";
            final String query = """
                    SELECT metric, total, ts, metric + 1 AS computed_metric
                    FROM dict_copy_cursor_src
                    CROSS JOIN long_sequence(1)
                    """;

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_copy_cursor_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        assertParquetMatchesQuery(query, parquetPath);
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath,
                                configuration.getFilesFacade(),
                                0,
                                1
                        );
                        ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(
                                parquetPath,
                                configuration.getFilesFacade(),
                                3
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryDirectPageFramePreservesDictionaryEncodingAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_copy_src (
                        metric INT PARQUET(RLE_DICTIONARY),
                        total LONG PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_copy_src VALUES
                        (10, 1000, '2020-01-01T00:00:00.000000Z'),
                        (20, 2000, '2020-01-01T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-01T02:00:00.000000Z'),
                        (10, 1000, '2020-01-02T00:00:00.000000Z'),
                        (30, 3000, '2020-01-02T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-02T02:00:00.000000Z'),
                        (20, 2000, '2020-01-03T00:00:00.000000Z'),
                        (40, 4000, '2020-01-03T01:00:00.000000Z'),
                        (NULL, NULL, '2020-01-03T02:00:00.000000Z')
                    """);

            final String parquetPath = exportRoot + File.separator + "dict_copy_output.parquet";
            final String query = "SELECT metric, total, ts FROM dict_copy_src";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_copy_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        assertParquetMatchesQuery(query, parquetPath);
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath,
                                configuration.getFilesFacade(),
                                0,
                                1
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryJoinPreservesDictionaryEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_join_master (
                        id LONG,
                        metric INT PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    CREATE TABLE dict_join_slave (
                        id LONG,
                        total LONG PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_join_master VALUES
                        (1, 10, '2020-01-01T00:00:00.000000Z'),
                        (1, 10, '2020-01-01T01:00:00.000000Z'),
                        (2, 20, '2020-01-02T00:00:00.000000Z'),
                        (2, 20, '2020-01-02T01:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO dict_join_slave VALUES
                        (1, 1000, '2020-01-01T00:00:00.000000Z'),
                        (1, 1000, '2020-01-01T01:00:00.000000Z'),
                        (2, 2000, '2020-01-02T00:00:00.000000Z'),
                        (2, 2000, '2020-01-02T01:00:00.000000Z')
                    """);

            final String query = """
                    SELECT *
                    FROM dict_join_master
                    ASOF JOIN dict_join_slave ON id
                    """;

            // Sanity-check that the test exercises the non-VRCF CURSOR_BASED path:
            // SelectedRecordCursorFactory(AsOfJoin*) is not a VirtualRecordCursorFactory.
            try (RecordCursorFactory f = select(query)) {
                RecordCursorFactory unwrapped = ParquetExportMode.unwrapFactory(f);
                Assert.assertFalse(
                        "expected non-VRCF factory; got " + unwrapped.getClass().getName(),
                        unwrapped instanceof VirtualRecordCursorFactory
                );
                Assert.assertFalse(
                        "expected CURSOR_BASED dispatch (no page-frame cursor support)",
                        f.supportsPageFrameCursor()
                );
            }

            final String parquetPath = exportRoot + File.separator + "dict_join_output.parquet";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_join_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        assertParquetMatchesQuery(query, parquetPath);
                        // Output column order: id, metric, ts, id1, total, ts1.
                        // metric is column 1 and total is column 4.
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath,
                                configuration.getFilesFacade(),
                                1,
                                4
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryMemoizedColumnPreservesDictionaryEncoding() throws Exception {
        // SqlCodeGenerator wraps a ColumnFunction in a *FunctionMemoizer when its projection
        // alias is referenced more than once. The hybrid materializer must still discover
        // the per-column parquet encoding override through the memoizer wrapper.
        assertMemoryLeak(() -> {
            allowFunctionMemoization();
            execute("""
                    CREATE TABLE dict_memo_src (
                        metric INT PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_memo_src VALUES
                        (10, '2020-01-01T00:00:00.000000Z'),
                        (20, '2020-01-01T01:00:00.000000Z'),
                        (10, '2020-01-02T00:00:00.000000Z')
                    """);

            // `metric` is referenced in two additional projections; the alias's ref count
            // forces SqlCodeGenerator to wrap the ColumnFunction in an IntFunctionMemoizer.
            final String query = """
                    SELECT metric, metric + 1 AS metric_plus, metric - 1 AS metric_minus, ts
                    FROM dict_memo_src
                    """;
            final String parquetPath = exportRoot + File.separator + "dict_memo_output.parquet";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_memo_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        // Source col 0 (metric) must still be dict-encoded despite the memoizer wrap.
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 0
                        );
                        // Computed cols 1, 2 are expressions; they must not inherit the override.
                        ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 1, 2
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryNestedProjectionPreservesDictionaryEncoding() throws Exception {
        // Nested virtual projections: the outer factory's base is another VRCF, not the
        // underlying reader. The per-column parquet encoding must ride through both levels
        // of generateSelectVirtualWithSubQuery.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_nested_src (
                        metric INT PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_nested_src VALUES
                        (10, '2020-01-01T00:00:00.000000Z'),
                        (20, '2020-01-01T01:00:00.000000Z'),
                        (10, '2020-01-02T00:00:00.000000Z')
                    """);

            // Both the inner and outer SELECTs add a computed column to ensure each level
            // generates its own VirtualRecordCursorFactory, exercising the nested path.
            final String query = """
                    SELECT metric, inner_expr + 1 AS outer_expr, ts
                    FROM (
                        SELECT metric, metric * 2 AS inner_expr, ts
                        FROM dict_nested_src
                    )
                    """;
            final String parquetPath = exportRoot + File.separator + "dict_nested_output.parquet";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_nested_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        // Col 0 (metric) is a pass-through through both nested VRCFs and
                        // must retain its RLE_DICTIONARY override.
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 0
                        );
                        // Col 1 (outer_expr) is a computed expression; must not inherit.
                        ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 1
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQuerySymbolKeepsDictionaryEncoding() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_sym_src (
                        label SYMBOL PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_sym_src VALUES
                        ('A', '2020-01-01T00:00:00.000000Z'),
                        ('B', '2020-01-01T01:00:00.000000Z'),
                        ('A', '2020-01-02T00:00:00.000000Z')
                    """);

            // Force CURSOR_BASED mode via CROSS JOIN (base no longer supports
            // page frame cursor). The cursor path keeps the table's SYMBOL
            // column as SYMBOL, with its dictionary encoding.
            final String query = """
                    SELECT label, ts, label || '!' AS computed_label
                    FROM dict_sym_src
                    CROSS JOIN long_sequence(1)
                    """;
            final String parquetPath = exportRoot + File.separator + "dict_sym_output.parquet";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_sym_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");
                        assertParquetMatchesQuery(query, parquetPath);

                        // Col 0 (label) stays SYMBOL, read back as VARCHAR, and uses RLE_DICTIONARY;
                        // col 2 (computed_label) is a computed expression and must not inherit it.
                        assertSymbolColumnsKept(parquetPath, 0);
                        ParquetTestUtils.assertColumnsUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 0
                        );
                        ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(
                                parquetPath, configuration.getFilesFacade(), 2
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table source_table (id int, value double, name string)");
            execute("insert into source_table values (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy (select id, value from source_table where id > 1) to 'output3' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(
                            () -> {
                                assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                        .noLeakCheck()
                                        .expectSize()
                                        .returns("export_path\tnum_exported_files\tstatus\n" +
                                                exportRoot + File.separator + "output3.parquet" + "\t1\tfinished\n");
                                // Verify only filtered data was exported
                                assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output3" + ".parquet') order by id")
                                        .noLeakCheck()
                                        .expectSize()
                                        .returns("""
                                                id\tvalue
                                                2\t2.5
                                                3\t3.5
                                                """);
                            }
                    );

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryVarcharSliceRoundtrip() throws Exception {
        // VARCHAR_SLICE is a transient type produced by read_parquet(). This test
        // verifies that VARCHAR_SLICE columns can flow through the parquet write path
        // (HybridColumnMaterializer) without errors.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_src (
                        id INT,
                        vc VARCHAR
                    )""");
            execute("""
                    INSERT INTO vc_src VALUES
                        (1, 'hello'),
                        (2, ''),
                        (3, NULL),
                        (4, 'café ☕'),
                        (5, 'world')""");

            // Encode the table to a parquet file so read_parquet() produces VARCHAR_SLICE columns.
            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("vc_src")
            ) {
                Files.mkdirs(path.of(exportRoot).slash(), configuration.getMkDirMode());
                path.of(exportRoot).concat("vc_input.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
            }

            // Round-trip: read_parquet() → VARCHAR_SLICE → COPY TO parquet
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_input.parquet')) TO 'vc_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_output.parquet') ORDER BY id")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        id\tvc
                                        1\thello
                                        2\t
                                        3\t
                                        4\tcafé ☕
                                        5\tworld
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithArithmeticExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x LONG, y DOUBLE, name STRING)");
            execute("INSERT INTO t1 VALUES (10, 1.5, 'a'), (20, 2.5, 'b'), (30, 3.5, 'c')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x + 1 AS x_plus, y * 2 AS y_doubled, name FROM t1) TO 'arith_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "arith_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "arith_output.parquet') ORDER BY x_plus")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x_plus\ty_doubled\tname
                                        11\t3.0\ta
                                        21\t5.0\tb
                                        31\t7.0\tc
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithBindVariable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ts_table (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO ts_table VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_153_600_000_000L); // 2024-01-02T00:00:00Z

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM ts_table WHERE ts <= $1) TO 'bind_output' WITH FORMAT parquet PARTITION_BY DAY",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "bind_output" + File.separator + "\t2\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bind_output" + File.separator + "2024-01-01.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        x\tts
                                        1\t2024-01-01T00:00:00.000000Z
                                        """);
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bind_output" + File.separator + "2024-01-02.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        x\tts
                                        2\t2024-01-02T00:00:00.000000Z
                                        """);
                        Assert.assertFalse("excluded partition should not exist",
                                exportFileExists("bind_output" + File.separator + "2024-01-03"));
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithBindVariableStreaming() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ts_table2 (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO ts_table2 VALUES
                    (1, '2024-01-01T00:00:00.000000Z'),
                    (2, '2024-01-02T00:00:00.000000Z'),
                    (3, '2024-01-03T00:00:00.000000Z')
                    """);

            bindVariableService.clear();
            bindVariableService.setInt(0, 2);

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM ts_table2 WHERE x <= $1) TO 'bind_streaming' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "bind_streaming.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bind_streaming.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\tts
                                        1\t2024-01-01T00:00:00.000000Z
                                        2\t2024-01-02T00:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithCastExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x LONG, y DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t1 VALUES
                    (100, 1.5, '2024-01-01T00:00:00.000000Z'),
                    (200, 2.5, '2024-01-02T00:00:00.000000Z'),
                    (300, 3.5, '2024-01-03T00:00:00.000000Z')
                    """);

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x::INT AS x_int, y::FLOAT AS y_float, ts FROM t1) TO 'cast_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "cast_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "cast_output.parquet') ORDER BY x_int")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x_int\ty_float\tts
                                        100\t1.5\t2024-01-01T00:00:00.000000Z
                                        200\t2.5\t2024-01-02T00:00:00.000000Z
                                        300\t3.5\t2024-01-03T00:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedAndPassthroughColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, y LONG, name STRING, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t1 VALUES
                    (1, 100, 'alpha', '2024-01-01T00:00:00.000000Z'),
                    (2, 200, 'beta', '2024-01-02T00:00:00.000000Z'),
                    (3, 300, 'gamma', '2024-01-03T00:00:00.000000Z')
                    """);

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x, x + y AS combined, name, ts FROM t1) TO 'mixed_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "mixed_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "mixed_output.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\tcombined\tname\tts
                                        1\t101\talpha\t2024-01-01T00:00:00.000000Z
                                        2\t202\tbeta\t2024-01-02T00:00:00.000000Z
                                        3\t303\tgamma\t2024-01-03T00:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedColumnsDescending() throws Exception {
        // Descending order + VirtualRecordCursorFactory with computed columns.
        // The COPY path uses ascending page frame order for the hybrid path,
        // so the result is in ascending order. This test verifies data correctness
        // with small row_group_size forcing multiple row group flushes.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE desc_comp AS (
                        SELECT x, x * 2.0 AS dbl_col,
                        timestamp_sequence('2024-01-01', 1_000_000L) AS ts
                        FROM long_sequence(100)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x + 1 AS cx, dbl_col, ts FROM desc_comp) TO 'desc_comp_output' WITH FORMAT parquet row_group_size 20",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "desc_comp_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "desc_comp_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n100\n");
                        // Verify computed column values at boundaries
                        assertQuery("SELECT cx, dbl_col FROM read_parquet('" + exportRoot + File.separator + "desc_comp_output.parquet') WHERE cx = 2")
                                .noLeakCheck()
                                .returns("""
                                        cx\tdbl_col
                                        2\t2.0
                                        """);
                        assertQuery("SELECT cx, dbl_col FROM read_parquet('" + exportRoot + File.separator + "desc_comp_output.parquet') WHERE cx = 101")
                                .noLeakCheck()
                                .returns("""
                                        cx\tdbl_col
                                        101\t200.0
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedColumnsMultipleRowGroups() throws Exception {
        // PAGE_FRAME_BACKED: computed columns + small row_group_size forces
        // multiple row group flushes and exercises buffer pinning.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE pfb_rg AS (
                        SELECT x, x * 2.0 AS dbl_col,
                        timestamp_sequence('2024-01-01', 100_000L) AS ts
                        FROM long_sequence(5000)
                    ) TIMESTAMP(ts) PARTITION BY HOUR""");

            // x::STRING produces a computed var-size STRING column, exercising buffer pinning
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x + 1 AS computed_x, x::STRING AS str_x, dbl_col, ts FROM pfb_rg) TO 'pfb_rg_output' WITH FORMAT parquet row_group_size 100",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "pfb_rg_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "pfb_rg_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n5000\n");
                        // Verify computed column values including the STRING column
                        assertQuery("SELECT computed_x, str_x, dbl_col FROM read_parquet('" + exportRoot + File.separator + "pfb_rg_output.parquet') WHERE computed_x = 2")
                                .noLeakCheck()
                                .returns("""
                                        computed_x\tstr_x\tdbl_col
                                        2\t1\t2.0
                                        """);
                        assertQuery("SELECT computed_x, str_x, dbl_col FROM read_parquet('" + exportRoot + File.separator + "pfb_rg_output.parquet') WHERE computed_x = 5001")
                                .noLeakCheck()
                                .returns("""
                                        computed_x\tstr_x\tdbl_col
                                        5001\t5000\t10000.0
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, y DOUBLE, name STRING)");
            execute("INSERT INTO t1 VALUES (1, 1.5, 'a'), (NULL, NULL, NULL), (3, 3.5, 'c')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x::LONG AS x_long, y::FLOAT AS y_float, name FROM t1) TO 'null_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "null_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "null_output.parquet') ORDER BY x_long")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x_long\ty_float\tname
                                        null\tnull\t
                                        1\t1.5\ta
                                        3\t3.5\tc
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedNullValuesMultipleRowGroups() throws Exception {
        // PAGE_FRAME_BACKED: NULL values in computed columns spanning multiple row group
        // boundaries exercise buffer pinning with NULL sentinel values across flushes.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE null_rg AS (
                        SELECT
                            CASE WHEN x % 3 = 0 THEN NULL::INT ELSE x::INT END AS val,
                            CASE WHEN x % 5 = 0 THEN NULL ELSE rnd_str(3, 8, 0) END AS name,
                            timestamp_sequence('2024-01-01', 100_000L) AS ts
                        FROM long_sequence(1000)
                    ) TIMESTAMP(ts) PARTITION BY HOUR""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT val::LONG AS val_long, name, ts FROM null_rg) TO 'null_rg_output' WITH FORMAT parquet row_group_size 50",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "null_rg_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "null_rg_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n1000\n");
                        // Verify NULL count matches original
                        assertQuery("SELECT count(*) AS null_count FROM read_parquet('" + exportRoot + File.separator + "null_rg_output.parquet') WHERE val_long IS NULL")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("null_count\n333\n");
                        // Verify non-NULL values preserved correctly
                        assertQuery("SELECT val_long FROM read_parquet('" + exportRoot + File.separator + "null_rg_output.parquet') WHERE val_long IS NOT NULL ORDER BY val_long LIMIT 2")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        val_long
                                        1
                                        2
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithComputedSymbolColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, name STRING)");
            execute("INSERT INTO t1 VALUES (1, 'hello'), (2, 'world'), (3, 'test')");

            // Cast STRING to SYMBOL - this creates a computed SYMBOL column
            // which should be exported as STRING in Parquet
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x, name::SYMBOL AS sym_name FROM t1) TO 'comp_sym_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "comp_sym_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "comp_sym_output.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\tsym_name
                                        1\thello
                                        2\tworld
                                        3\ttest
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithCursorBasedMultipleRowGroups() throws Exception {
        // CURSOR_BASED: CROSS JOIN produces a non-page-frame factory.
        // Small row_group_size forces multiple batches and exercises buffer pinning.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cb_t1 AS (SELECT x AS a FROM long_sequence(10))");
            execute("""
                    CREATE TABLE cb_t2 AS (
                        SELECT x AS b,
                        rnd_str(5, 10, 0) AS s,
                        rnd_varchar(5, 10, 0) AS vc,
                        rnd_double_array(1, 5) AS arr
                        FROM long_sequence(500)
                    )""");

            // All columns materialized through buffers in cursor mode:
            // STRING (s), VARCHAR (vc), and ARRAY (arr) exercise var-size buffer pinning
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT cb_t1.a + cb_t2.b AS sum_ab, cb_t2.s, cb_t2.vc, cb_t2.arr FROM cb_t1 CROSS JOIN cb_t2) TO 'cb_rg_output' WITH FORMAT parquet row_group_size 100",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "cb_rg_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "cb_rg_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n5000\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithCursorBasedNullsMultipleRowGroups() throws Exception {
        // CURSOR_BASED: NULL values across multiple row group boundaries in a
        // non-page-frame factory (CROSS JOIN). Exercises cursor-based buffer
        // pinning with NULL sentinels for both fixed-size and var-size columns.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE cb_null_t1 AS (
                        SELECT
                            CASE WHEN x % 4 = 0 THEN NULL::INT ELSE x::INT END AS a,
                            CASE WHEN x % 3 = 0 THEN NULL ELSE rnd_str(2, 6, 0) END AS s
                        FROM long_sequence(50)
                    )""");
            execute("CREATE TABLE cb_null_t2 AS (SELECT x AS b FROM long_sequence(10))");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT cb_null_t1.a, cb_null_t2.b, cb_null_t1.s FROM cb_null_t1 CROSS JOIN cb_null_t2) TO 'cb_null_rg_output' WITH FORMAT parquet row_group_size 50",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "cb_null_rg_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "cb_null_rg_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n500\n");
                        // Every 4th value of a is NULL: 50 rows * 10 = 500, 12 NULLs per set of 50 * 10 = 120
                        assertQuery("SELECT count(*) AS null_count FROM read_parquet('" + exportRoot + File.separator + "cb_null_rg_output.parquet') WHERE a IS NULL")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("null_count\n120\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithEmptyComputedResult() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, y DOUBLE)");
            execute("INSERT INTO t1 VALUES (1, 1.5), (2, 2.5), (3, 3.5)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x::LONG AS x_long, y FROM t1 WHERE x > 100) TO 'empty_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "empty_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "empty_output.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("""
                                        count
                                        0
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithFullMaterialization() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, name STRING)");
            execute("CREATE TABLE t2 (id INT, label STRING)");
            execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            execute("INSERT INTO t2 VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");

            // Cross join doesn't support page frame cursor, forces full materialization
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT t1.x, t2.label FROM t1 CROSS JOIN t2 WHERE t1.x = t2.id) TO 'full_mat_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "full_mat_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "full_mat_output.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\tlabel
                                        1\talpha
                                        2\tbeta
                                        3\tgamma
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithPassthroughSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t1 (x INT, sym SYMBOL, name STRING)");
            execute("INSERT INTO t1 VALUES (1, 'SYM_A', 'first'), (2, 'SYM_B', 'second'), (3, 'SYM_A', 'third')");

            // Pass-through symbol: sym is a direct column reference, should be preserved
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT x, sym, name FROM t1) TO 'sym_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "sym_output.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "sym_output.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\tsym\tname
                                        1\tSYM_A\tfirst
                                        2\tSYM_B\tsecond
                                        3\tSYM_A\tthird
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueryWithPivotToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table monthly_sales (empid int, amount int, month symbol)");
            execute("insert into monthly_sales values " +
                    "(1, 10000, 'JAN'), (1, 400, 'JAN'), (2, 4500, 'JAN'), (2, 35000, 'JAN'), " +
                    "(1, 5000, 'FEB'), (1, 3000, 'FEB'), (2, 200, 'FEB'), (2, 90500, 'FEB'), " +
                    "(1, 6000, 'MAR'), (1, 5000, 'MAR'), (2, 2500, 'MAR'), (2, 9500, 'MAR')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy (monthly_sales PIVOT (SUM(amount) FOR month IN ('JAN', 'FEB', 'MAR') GROUP BY empid) ORDER BY empid) to 'pivot_output' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(
                            () -> {
                                assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                        .noLeakCheck()
                                        .expectSize()
                                        .returns("export_path\tnum_exported_files\tstatus\n" +
                                                exportRoot + File.separator + "pivot_output.parquet" + "\t1\tfinished\n");
                                assertQuery("select * from read_parquet('" + exportRoot + File.separator + "pivot_output" + ".parquet') order by empid")
                                        .noLeakCheck()
                                        .expectSize()
                                        .returns("""
                                                empid\tJAN\tFEB\tMAR
                                                1\t10400\t8000\t11000
                                                2\t39500\t90700\t12000
                                                """);
                            }
                    );

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyQueuedExportCancelledAfterRecompile() throws Exception {
        // The user cancels the export while it compiles its stale select again: the export stops as cancelled, leaves no
        // file behind, and the next export of the table runs.
        assertMemoryLeak(() -> {
            createStaleExportTable("BYPASS WAL");
            final String exportId = queueCopy("COPY (SELECT * FROM t) TO 'cancelled' WITH FORMAT PARQUET");
            execute("ALTER TABLE t ADD COLUMN extra INT");
            final CopyExportContext copyContext = engine.getCopyExportContext();
            Assert.assertEquals(1, runExportJobCountingCompiles(
                    _ -> Assert.assertTrue(copyContext.cancel(copyContext.getActiveExportId(), null))
            ));
            assertExportLog(exportId, """
                    phase\tstatus
                    wait_to_run\tstarted
                    wait_to_run\tfinished
                    stream_sending_data\tstarted
                    stream_sending_data\tcancelled
                    """);
            assertExportFailure(exportId, "cancelled", "cancelled by user");
            Assert.assertFalse(exportFileExists("cancelled"));
            assertNoTempDirLeft();

            copyToParquet("COPY (SELECT * FROM t) TO 'after_cancel' WITH FORMAT PARQUET");
            assertParquetMatchesQuery("SELECT * FROM t", parquetPath("after_cancel"));
        });
    }

    @Test
    public void testCopyQueuedExportFailsWhenRecompiledSelectFails() throws Exception {
        // The export compiles its stale select again, and that compile, or the set-up of the new plan, fails as the same
        // COPY issued after the change fails. The export logs the error and leaves no file behind, and the next export of
        // the table runs. A table dropped and created again under its name is a stale plan, which compiles again over the
        // new table.
        assertMemoryLeak(() -> {
            createStaleExportTable("BYPASS WAL");
            String exportId = queueCopy("COPY (SELECT x, ts FROM t) TO 'renamed_column' WITH FORMAT PARQUET");
            execute("ALTER TABLE t RENAME COLUMN x TO x2");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
            assertExportFailure(exportId, "failed", "Invalid column: x");
            Assert.assertFalse(exportFileExists("renamed_column"));
            assertNoTempDirLeft();
            copyToParquet("COPY (SELECT x2, ts FROM t) TO 'after_renamed_column' WITH FORMAT PARQUET");
            assertParquetMatchesQuery("SELECT x2, ts FROM t", parquetPath("after_renamed_column"));

            execute("ALTER TABLE t ADD COLUMN extra INT");
            exportId = queueCopy("COPY (SELECT * FROM t) TO 'dropped_bloom_column' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'extra'");
            execute("ALTER TABLE t DROP COLUMN extra");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
            assertExportFailure(exportId, "failed", "bloom_filter_columns contains non-existent column: extra");
            Assert.assertFalse(exportFileExists("dropped_bloom_column"));
            assertNoTempDirLeft();
            copyToParquet("COPY (SELECT * FROM t) TO 'after_dropped_bloom_column' WITH FORMAT PARQUET BLOOM_FILTER_COLUMNS 'x2'");
            assertParquetMatchesQuery("SELECT * FROM t", parquetPath("after_dropped_bloom_column"));

            exportId = queueCopy("COPY (SELECT * FROM t) TO 'recreated_table' WITH FORMAT PARQUET");
            execute("DROP TABLE t");
            execute("CREATE TABLE t (x INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES (5, 'c', '2024-02-01T00:00:00.000000Z')");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("recreated_table", "ts", """
                    x\ts\tts
                    5\tc\t2024-02-01T00:00:00.000000Z
                    """);
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportFollowsParquetSettingChangedAfterQueue() throws Exception {
        // A PARQUET(...) setting that changes after COPY queues the export makes the queued plan stale. The export
        // compiles its select again and encodes x by the new setting, in each streaming mode.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            for (StaleExportShape shape : STALE_EXPORT_SHAPES) {
                try {
                    createStaleExportTable("WAL");
                    assertExportMode(shape.query(), shape.mode());
                    final String exportId = queueCopy("COPY (" + shape.query() + ") TO 'parquet_setting' WITH FORMAT PARQUET");
                    execute("ALTER TABLE t ALTER COLUMN x SET PARQUET(DELTA_BINARY_PACKED)");
                    drainWalAndViewQueues();
                    // A pooled reader keeps the PARQUET(...) setting it loaded before the ALTER (#8191), so the test
                    // releases the readers for the export to compile its select over the new setting.
                    engine.releaseAllReaders();
                    Assert.assertEquals(1, runExportJobCountingCompiles());
                    assertExportLog(exportId, STREAMING_EXPORT_LOG);
                    assertParquetRows("parquet_setting", shape.timestampColumn(), shape.rows());
                    ParquetTestUtils.assertColumnsUseEncoding(parquetPath("parquet_setting"), ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, shape.xColumnIndex());
                    ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath("parquet_setting"), ff, shape.xColumnIndex());
                    assertNoTempDirLeft();
                    execute("DROP TABLE t");
                    drainWalQueue();
                } catch (AssertionError e) {
                    throw new AssertionError(shape.query(), e);
                }
            }
        });
    }

    @Test
    public void testCopyQueuedExportMaterializesWhenRecompiledSelectNeedsTempTable() throws Exception {
        // The table gains a BINARY column after COPY queues the export, and the export of a sorted select cannot write a
        // BINARY column that it reads row by row. The export compiles its select again and, as COPY issued after the
        // change does, materializes the new plan through a temp table, which it drops at the end.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createStaleExportTable(walMode);
                final String query = "SELECT * FROM t ORDER BY x";
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                final String exportId = queueCopy("COPY (" + query + ") TO 'rerouted' WITH FORMAT PARQUET");
                execute("ALTER TABLE t ADD COLUMN b BINARY");
                drainWalQueue();
                assertExportMode(query, ParquetExportMode.TEMP_TABLE);
                // the compile of the select and the CREATE TABLE AS SELECT of the temp table
                Assert.assertEquals(walMode, 2, runExportJobCountingCompiles());
                assertExportLog(exportId, """
                        phase\tstatus
                        wait_to_run\tstarted
                        wait_to_run\tfinished
                        stream_sending_data\tstarted
                        populating_data_to_temp_table\tstarted
                        populating_data_to_temp_table\tfinished
                        converting_partitions\tstarted
                        converting_partitions\tfinished
                        move_files\tstarted
                        move_files\tfinished
                        dropping_temp_table\tstarted
                        dropping_temp_table\tfinished
                        success\tfinished
                        """);
                assertExportedFileCount(exportId, 1);
                assertParquetRows("rerouted", null, """
                        x\ts\tts\tb
                        1\ta\t2024-01-03T00:00:00.000000Z\t
                        2\tb\t2024-01-02T00:00:00.000000Z\t
                        3\ta\t2024-01-01T00:00:00.000000Z\t
                        """);
                assertParquetMatchesQuery(query, parquetPath("rerouted"));
                Assert.assertNull("the temp table must be dropped", engine.getTableTokenIfExists("copy." + exportId));
                // No check for a temp directory left behind: an export through a temp table that writes one file keeps
                // its empty tmp_<id> directory (a known bug of SQLSerialParquetExporter.process()).
                execute("DROP TABLE t");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testCopyQueuedExportOfColumnRetypedAfterQueueOverParquetPartition() throws Exception {
        // s turns from INT to LONG after COPY queues the export, while the first day of pt is a Parquet partition, which
        // keeps s as INT. The export compiles its select again and streams the rows of the new plan through the cursor
        // exporter, which reads s through the converting accessors, so it writes s as LONG, with the same values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pt (s INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO pt SELECT x::INT, timestamp_sequence('2024-01-01', 3_600_000_000) FROM long_sequence(30)");
            drainWalQueue();
            execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final String query = "SELECT * FROM pt";
            assertExportMode(query, ParquetExportMode.DIRECT_PAGE_FRAME);
            final String exportId = queueCopy("COPY (" + query + ") TO 'retyped_after_queue' WITH FORMAT PARQUET");
            execute("ALTER TABLE pt ALTER COLUMN s TYPE LONG");
            drainWalQueue();
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertNoTempDirOf(exportId);
            final String parquetPath = parquetPath("retyped_after_queue");
            ParquetTestUtils.assertColumnTypes(parquetPath, configuration.getFilesFacade(), ColumnType.LONG, 0);
            // s holds 1 to 30, one per hour from 2024-01-01T00:00
            assertQuery("SELECT count(), min(s), max(s), sum(s), min(ts), max(ts) FROM read_parquet('" + parquetPath + "')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            count\tmin\tmax\tsum\tmin1\tmax1
                            30\t1\t30\t465\t2024-01-01T00:00:00.000000Z\t2024-01-02T05:00:00.000000Z
                            """);
            assertParquetMatchesQuery(query, parquetPath);
        });
    }

    @Test
    public void testCopyQueuedExportOfEmptyTableRecompiles() throws Exception {
        // An empty, non-partitioned table whose plan goes stale in the queue exports a file of its columns, the new one
        // included, and no rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE e (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY NONE");
            assertExportMode("SELECT * FROM e", ParquetExportMode.DIRECT_PAGE_FRAME);
            final String exportId = queueCopy("COPY (SELECT * FROM e) TO 'empty_table' WITH FORMAT PARQUET");
            execute("ALTER TABLE e ADD COLUMN extra INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertExportedFileCount(exportId, 1);
            assertParquetRows("empty_table", "ts", "x\tts\textra\n");
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportOfTableConvertedToParquetAfterQueue() throws Exception {
        // c turned from INT to LONG while every partition was native. The first day goes to Parquet after COPY queues
        // the export of a filter, which makes the queued plan, whose Parquet row group filter knows no Parquet partition,
        // stale: the export compiles its select again and streams the new plan, which reads a retyped column of a
        // Parquet partition, through the cursor exporter rather than a temp table.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createRetypedTables(walMode);
                execute("CREATE TABLE q (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("INSERT INTO q SELECT x::INT, timestamp_sequence('2024-01-01', 21_600_000_000) FROM long_sequence(12)");
                drainWalQueue();
                execute("ALTER TABLE q ALTER COLUMN c TYPE LONG");
                drainWalQueue();
                final String query = "SELECT * FROM q WHERE c > 0";
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertFalse(factory.hasParquetConvertedColumns(sqlExecutionContext));
                }
                final String exportId = queueCopy("COPY (" + query + ") TO 'converted_after_queue' WITH FORMAT PARQUET");
                execute("ALTER TABLE q CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                try (RecordCursorFactory factory = select(query)) {
                    Assert.assertTrue(factory.hasParquetConvertedColumns(sqlExecutionContext));
                }
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                Assert.assertEquals(walMode, 1, runExportJobCountingCompiles());
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                Assert.assertNull(engine.getTableTokenIfExists("copy." + exportId));
                assertNoTempDirOf(exportId);
                // c holds 1 to 12, one every 6 hours from 2024-01-01T00:00
                assertQuery("SELECT count(), sum(c), min(ts), max(ts) FROM read_parquet('" + parquetPath("converted_after_queue") + "')")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count\tsum\tmin\tmax
                                12\t78\t2024-01-01T00:00:00.000000Z\t2024-01-03T18:00:00.000000Z
                                """);
                ParquetTestUtils.assertColumnTypes(parquetPath("converted_after_queue"), configuration.getFilesFacade(), ColumnType.LONG, 0);
                execute("DROP TABLE q");
                dropRetypedTables();
            }
        });
    }

    @Test
    public void testCopyQueuedExportOfTableDroppedRenamedOrTruncated() throws Exception {
        // A dropped or renamed table is no stale plan: the open of the plan fails with "table does not exist", and the
        // export fails without compiling its select again, whether the table goes before the export starts or after the
        // export compiled its stale select again. A truncated table exports its columns and no rows.
        assertMemoryLeak(() -> {
            for (StaleExportShape shape : new StaleExportShape[]{STALE_EXPORT_SHAPES[0], STALE_EXPORT_SHAPES[2]}) {
                final String query = shape.query();
                for (String change : new String[]{"DROP TABLE t", "RENAME TABLE t TO t2"}) {
                    try {
                        createStaleExportTable("BYPASS WAL");
                        String exportId = queueCopy("COPY (" + query + ") TO 'gone' WITH FORMAT PARQUET");
                        execute(change);
                        Assert.assertEquals(0, runExportJobCountingCompiles());
                        assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
                        assertExportFailure(exportId, "failed", "table does not exist [table=t]");
                        Assert.assertFalse(exportFileExists("gone"));
                        assertNoTempDirLeft();
                        execute("DROP TABLE IF EXISTS t2");

                        createStaleExportTable("BYPASS WAL");
                        exportId = queueCopy("COPY (" + query + ") TO 'gone_after_compile' WITH FORMAT PARQUET");
                        execute("ALTER TABLE t ADD COLUMN extra INT");
                        Assert.assertEquals(1, runExportJobCountingCompiles(_ -> execute(change)));
                        assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
                        assertExportFailure(exportId, "failed", "table does not exist [table=t]");
                        Assert.assertFalse(exportFileExists("gone_after_compile"));
                        assertNoTempDirLeft();

                        // the export queue still works, and so does a renamed table
                        final String remainingTable = change.startsWith("RENAME") ? "t2" : "t";
                        if (remainingTable.equals("t")) {
                            createStaleExportTable("BYPASS WAL");
                        }
                        copyToParquet("COPY (SELECT * FROM " + remainingTable + ") TO 'after_gone' WITH FORMAT PARQUET");
                        assertParquetMatchesQuery("SELECT * FROM " + remainingTable, parquetPath("after_gone"));
                        execute("DROP TABLE " + remainingTable);
                    } catch (AssertionError e) {
                        throw new AssertionError(query + ", " + change, e);
                    }
                }

                createStaleExportTable("BYPASS WAL");
                String exportId = queueCopy("COPY (" + query + ") TO 'truncated' WITH FORMAT PARQUET");
                execute("TRUNCATE TABLE t");
                Assert.assertEquals(0, runExportJobCountingCompiles());
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                assertExportedFileCount(exportId, 1);
                assertParquetRows("truncated", shape.timestampColumn(), "x\ts\tts\n");
                assertNoTempDirLeft();

                exportId = queueCopy("COPY (" + query + ") TO 'truncated_after_compile' WITH FORMAT PARQUET");
                execute("INSERT INTO t VALUES (4, 'd', '2024-01-04T00:00:00.000000Z')");
                execute("ALTER TABLE t ADD COLUMN extra INT");
                Assert.assertEquals(1, runExportJobCountingCompiles(_ -> execute("TRUNCATE TABLE t")));
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                assertExportedFileCount(exportId, 1);
                assertParquetRows("truncated_after_compile", shape.timestampColumn(), "x\ts\tts\textra\n");
                assertNoTempDirLeft();
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testCopyQueuedExportPrunesPartitionConvertedAfterQueue() throws Exception {
        // The first day of x turns to Parquet after COPY queues the export, which makes the queued plan, which has no
        // Parquet row group filter, stale. The export compiles its select again, and the new plan skips the one row group
        // of the first day, whose val values all fail the filter, as a fresh compile of the select does.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                execute("CREATE TABLE x (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("""
                        INSERT INTO x VALUES
                        (-1, '2024-01-01T00:00:00.000000Z'),
                        (-2, '2024-01-01T01:00:00.000000Z'),
                        (2, '2024-01-02T00:00:00.000000Z')
                        """);
                drainWalQueue();
                final String query = "SELECT * FROM x WHERE val > 0";
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                final String exportId = queueCopy("COPY (" + query + ") TO 'pruned' WITH FORMAT PARQUET");
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                drainWalQueue();
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                Assert.assertEquals(walMode, 1, runExportJobCountingCompiles());
                Assert.assertEquals(walMode, 1, ParquetRowGroupFilter.getRowGroupsSkipped());
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                assertParquetRows("pruned", "ts", """
                        val\tts
                        2\t2024-01-02T00:00:00.000000Z
                        """);
                assertNoTempDirLeft();
                execute("DROP TABLE x");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testCopyQueuedExportRecompileTakesBindVariables() throws Exception {
        // The export compiles its stale select again with the bind values that COPY took when it queued the export.
        assertMemoryLeak(() -> {
            createStaleExportTable("BYPASS WAL");
            final String query = "SELECT * FROM t WHERE x <= $1";
            bindVariableService.clear();
            bindVariableService.setInt(0, 2);
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            final String exportId = queueCopy("COPY (" + query + ") TO 'bind_variable' WITH FORMAT PARQUET");
            // a bind value set after COPY queued the export is not the export's
            bindVariableService.setInt(0, 3);
            execute("ALTER TABLE t ADD COLUMN extra INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            bindVariableService.setInt(0, 2);
            assertParquetMatchesQuery(query, parquetPath("bind_variable"));
            bindVariableService.clear();
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("bind_variable", "ts", """
                    x\ts\tts\textra
                    2\tb\t2024-01-02T00:00:00.000000Z\tnull
                    1\ta\t2024-01-03T00:00:00.000000Z\tnull
                    """);
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportRecompiledIntoBackwardScanStreams() throws Exception {
        // A view redefined, or a column added, after COPY queues an export makes the queued plan stale, so the export
        // compiles its select again. The worker reads the page frames of a new plan ascending, so a new plan that scans
        // backward streams its rows through the cursor exporter, or through a temp table when it holds a BINARY column,
        // which the cursor exporter cannot write. A new plan that scans forward keeps its page frames. A select over a
        // view never exports a projection over page frames, as the view's wrapper hides the projection, so those
        // selects read the tables.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            execute("CREATE TABLE tb (x INT, b BINARY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO tb VALUES
                        (1, rnd_bin(2, 4, 0), '2024-01-01T00:00:00.000000Z'),
                        (2, NULL, '2024-01-01T12:00:00.000000Z'),
                        (3, rnd_bin(2, 4, 0), '2024-01-02T00:00:00.000000Z'),
                        (4, rnd_bin(2, 4, 0), '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalQueue();
            final RecompiledExport[] exports = {
                    // a filter turns into a backward scan of page frames
                    new RecompiledExport(
                            "SELECT * FROM v", "CREATE OR REPLACE VIEW v AS (SELECT * FROM nr WHERE x > 0)", ParquetExportMode.CURSOR_BASED,
                            "CREATE OR REPLACE VIEW v AS (SELECT * FROM nr ORDER BY ts DESC)",
                            ParquetExportMode.DIRECT_PAGE_FRAME, ParquetExportMode.CURSOR_BASED, STREAMING_EXPORT_LOG, 1, null
                    ),
                    // a projection over a backward scan of page frames
                    new RecompiledExport(
                            "SELECT x + 1 AS y, ts FROM nr ORDER BY ts DESC", null, ParquetExportMode.PAGE_FRAME_BACKED,
                            "ALTER TABLE nr ADD COLUMN extra INT",
                            ParquetExportMode.PAGE_FRAME_BACKED, ParquetExportMode.CURSOR_BASED, STREAMING_EXPORT_LOG, 1, null
                    ),
                    // the same two with a BINARY column
                    new RecompiledExport(
                            "SELECT * FROM v", "CREATE OR REPLACE VIEW v AS (SELECT * FROM tb)", ParquetExportMode.DIRECT_PAGE_FRAME,
                            "CREATE OR REPLACE VIEW v AS (SELECT * FROM tb ORDER BY ts DESC)",
                            ParquetExportMode.DIRECT_PAGE_FRAME, ParquetExportMode.TEMP_TABLE, REROUTED_EXPORT_LOG, 2, null
                    ),
                    new RecompiledExport(
                            "SELECT b, x + 1 AS y, ts FROM tb ORDER BY ts DESC", null, ParquetExportMode.PAGE_FRAME_BACKED,
                            "ALTER TABLE tb ADD COLUMN extra INT",
                            ParquetExportMode.PAGE_FRAME_BACKED, ParquetExportMode.TEMP_TABLE, REROUTED_EXPORT_LOG, 2, null
                    ),
                    // a scan that stays forward keeps its page frames
                    new RecompiledExport(
                            "SELECT * FROM v", "CREATE OR REPLACE VIEW v AS (SELECT * FROM nr WHERE x > 0)", ParquetExportMode.CURSOR_BASED,
                            "CREATE OR REPLACE VIEW v AS (SELECT * FROM nr)",
                            ParquetExportMode.DIRECT_PAGE_FRAME, ParquetExportMode.DIRECT_PAGE_FRAME, STREAMING_EXPORT_LOG, 1, "ts"
                    ),
            };
            for (int i = 0; i < exports.length; i++) {
                final RecompiledExport export = exports[i];
                final String fileName = "recompiled_" + i;
                try {
                    if (export.setup() != null) {
                        execute(export.setup());
                        drainWalAndViewQueues();
                    }
                    assertExportMode(export.query(), export.mode());
                    final String exportId = queueCopy("COPY (" + export.query() + ") TO '" + fileName + "' WITH FORMAT PARQUET");
                    execute(export.change());
                    drainWalAndViewQueues();
                    // a fresh plan of the select exports its page frames, a recompiled one goes by its direction
                    try (RecordCursorFactory factory = select(export.query())) {
                        Assert.assertEquals(export.newMode(), ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext));
                        Assert.assertEquals(export.recompiledMode(), ParquetExportMode.determineRecompiledExportMode(factory, sqlExecutionContext));
                    }
                    Assert.assertEquals(export.compileCount(), runExportJobCountingCompiles());
                    assertExportLog(exportId, export.log());
                    if (export.log().equals(STREAMING_EXPORT_LOG)) {
                        assertNoTempDirOf(exportId);
                    }
                    // the select, after the change, is the oracle
                    assertParquetMatchesControl(export.query(), fileName, export.timestampColumn());
                } catch (AssertionError e) {
                    throw new AssertionError(export.query() + " after " + export.change(), e);
                }
            }
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesAfterAddColumn() throws Exception {
        // COPY compiles the select when it queues the export, and a column that the table gains before the export runs
        // makes that plan stale. The export compiles its select again, once, and writes the rows of the new plan, with s
        // kept as SYMBOL. The same export with no change in between compiles nothing.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                for (StaleExportShape shape : STALE_EXPORT_SHAPES) {
                    try {
                        createStaleExportTable(walMode);
                        assertExportMode(shape.query(), shape.mode());

                        final String freshId = queueCopy("COPY (" + shape.query() + ") TO 'fresh' WITH FORMAT PARQUET");
                        Assert.assertEquals("a current plan compiles nothing on the export's thread", 0, runExportJobCountingCompiles());
                        assertExportLog(freshId, STREAMING_EXPORT_LOG);
                        assertParquetRows("fresh", shape.timestampColumn(), shape.rows());

                        final String staleId = queueCopy("COPY (" + shape.query() + ") TO 'stale' WITH FORMAT PARQUET");
                        execute("ALTER TABLE t ADD COLUMN extra INT");
                        drainWalQueue();
                        Assert.assertEquals(1, runExportJobCountingCompiles());
                        assertExportLog(staleId, STREAMING_EXPORT_LOG);
                        assertParquetRows("stale", shape.timestampColumn(), shape.rowsWithExtra());
                        assertParquetMatchesQuery(shape.query(), parquetPath("stale"));
                        assertSymbolColumnsKept(parquetPath("stale"), shape.xColumnIndex() + 1);
                        assertNoTempDirLeft();
                        execute("DROP TABLE t");
                        drainWalQueue();
                    } catch (AssertionError e) {
                        throw new AssertionError(walMode + ": " + shape.query(), e);
                    }
                }
            }
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesIndexedSymbolFilter() throws Exception {
        // A filter on an indexed, or a non-indexed, symbol whose plan goes stale in the queue.
        assertMemoryLeak(() -> {
            for (boolean isSymbolIndexed : new boolean[]{true, false}) {
                createStaleExportTable("BYPASS WAL", isSymbolIndexed);
                final String query = "SELECT * FROM t WHERE s = 'a'";
                final String exportId = queueCopy("COPY (" + query + ") TO 'symbol_filter' WITH FORMAT PARQUET");
                execute("ALTER TABLE t ADD COLUMN extra INT");
                Assert.assertEquals(1, runExportJobCountingCompiles());
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                assertParquetRows("symbol_filter", "ts", """
                        x\ts\tts\textra
                        3\ta\t2024-01-01T00:00:00.000000Z\tnull
                        1\ta\t2024-01-03T00:00:00.000000Z\tnull
                        """);
                assertParquetMatchesQuery(query, parquetPath("symbol_filter"));
                assertNoTempDirLeft();
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesIntoAnotherMode() throws Exception {
        // A view redefined after COPY queues the export of a select over it makes the queued plan stale. The export
        // compiles its select again and writes the rows of the new definition, in the mode of the new plan. That holds
        // also for a plan whose set-up over page frames went stale before the plan that reads rows from a cursor.
        assertMemoryLeak(() -> {
            createStaleExportTable("WAL");
            execute("CREATE VIEW v AS (SELECT * FROM t)");
            drainWalAndViewQueues();
            final String query = "SELECT * FROM v";
            assertExportMode(query, ParquetExportMode.DIRECT_PAGE_FRAME);
            String exportId = queueCopy("COPY (" + query + ") TO 'direct_to_cursor' WITH FORMAT PARQUET");
            execute("ALTER VIEW v AS (SELECT * FROM t ORDER BY x)");
            drainWalAndViewQueues();
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("direct_to_cursor", null, """
                    x\ts\tts
                    1\ta\t2024-01-03T00:00:00.000000Z
                    2\tb\t2024-01-02T00:00:00.000000Z
                    3\ta\t2024-01-01T00:00:00.000000Z
                    """);

            exportId = queueCopy("COPY (" + query + ") TO 'cursor_to_direct' WITH FORMAT PARQUET");
            execute("ALTER VIEW v AS (SELECT * FROM t)");
            drainWalAndViewQueues();
            assertExportMode(query, ParquetExportMode.DIRECT_PAGE_FRAME);
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("cursor_to_direct", "ts", """
                    x\ts\tts
                    3\ta\t2024-01-01T00:00:00.000000Z
                    2\tb\t2024-01-02T00:00:00.000000Z
                    1\ta\t2024-01-03T00:00:00.000000Z
                    """);

            // The sub-query of the projection over the table p reads u, which gains a column after COPY queues the
            // export, so the set-up of the projection over the page frames of p fails. Right after the export compiled
            // its select again, a view that sorts t takes the place of p, so the export compiles the select once more
            // and reads the rows from a cursor.
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (1), (2)");
            execute("CREATE TABLE p (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO p VALUES (7, '2024-02-01T00:00:00.000000Z')");
            final String projectionQuery = "SELECT x > (SELECT max(y) FROM u) b, ts FROM p";
            assertExportMode(projectionQuery, ParquetExportMode.PAGE_FRAME_BACKED);
            exportId = queueCopy("COPY (" + projectionQuery + ") TO 'page_frame_backed_to_cursor' WITH FORMAT PARQUET");
            execute("ALTER TABLE u ADD COLUMN z INT");
            Assert.assertEquals(2, runExportJobCountingCompiles(compileCount -> {
                if (compileCount == 1) {
                    execute("DROP TABLE p");
                    execute("CREATE VIEW p AS (SELECT * FROM t ORDER BY x)");
                    drainWalAndViewQueues();
                }
            }));
            assertExportMode(projectionQuery, ParquetExportMode.CURSOR_BASED);
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("page_frame_backed_to_cursor", null, """
                    b\tts
                    false\t2024-01-03T00:00:00.000000Z
                    false\t2024-01-02T00:00:00.000000Z
                    true\t2024-01-01T00:00:00.000000Z
                    """);
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesOverManyRowGroups() throws Exception {
        // More rows than one page frame and one row group: the export of a plan that it compiled again streams all of
        // them, in row groups of ROW_GROUP_SIZE rows, or, with ROW_GROUP_SIZE 0, in batches and a row group of the default
        // size, 100_000 rows.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            // 1_000 rows a day for 3 days
            execute("CREATE TABLE t2 AS (SELECT x::INT x, timestamp_sequence('2024-01-01', 86_400_000L) ts FROM long_sequence(3_000)) TIMESTAMP(ts) PARTITION BY DAY");
            final String directQuery = "SELECT * FROM t2";
            assertExportMode(directQuery, ParquetExportMode.DIRECT_PAGE_FRAME);
            String exportId = queueCopy("COPY (" + directQuery + ") TO 'direct_row_groups' WITH FORMAT PARQUET ROW_GROUP_SIZE 100");
            execute("ALTER TABLE t2 ADD COLUMN extra INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetMatchesQuery(directQuery, parquetPath("direct_row_groups"));
            assertQuery("SELECT count(), sum(x), count(extra) FROM read_parquet('" + parquetPath("direct_row_groups") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\tcount1\n3000\t4501500\t0\n");
            ParquetTestUtils.assertRowGroupCount(parquetPath("direct_row_groups"), ff, 30);

            final String cursorQuery = "SELECT * FROM t2 ORDER BY x DESC";
            assertExportMode(cursorQuery, ParquetExportMode.CURSOR_BASED);
            exportId = queueCopy("COPY (" + cursorQuery + ") TO 'cursor_default_row_group' WITH FORMAT PARQUET ROW_GROUP_SIZE 0");
            execute("ALTER TABLE t2 ADD COLUMN extra2 INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetMatchesQuery(cursorQuery, parquetPath("cursor_default_row_group"));
            assertQuery("SELECT count(), sum(x), count(extra2) FROM read_parquet('" + parquetPath("cursor_default_row_group") + "')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\tsum\tcount1\n3000\t4501500\t0\n");
            ParquetTestUtils.assertRowGroupCount(parquetPath("cursor_default_row_group"), ff, 1);
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesStaleProjectionSubQuery() throws Exception {
        // The projection's sub-query reads u, which gains a column after COPY queues the export, so the queued plan goes
        // stale when the export sets the projection up over the page frames of t. The export compiles its select again
        // and sets the new plan up from scratch, also over more than one page frame and row group.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (1), (2)");
            createStaleExportTable("BYPASS WAL");
            final String query = "SELECT x > (SELECT max(y) FROM u) b, ts FROM t";
            assertExportMode(query, ParquetExportMode.PAGE_FRAME_BACKED);
            String exportId = queueCopy("COPY (" + query + ") TO 'sub_query' WITH FORMAT PARQUET");
            execute("ALTER TABLE u ADD COLUMN z INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("sub_query", "ts", """
                    b\tts
                    true\t2024-01-01T00:00:00.000000Z
                    false\t2024-01-02T00:00:00.000000Z
                    false\t2024-01-03T00:00:00.000000Z
                    """);
            assertParquetMatchesQuery(query, parquetPath("sub_query"));
            assertNoTempDirLeft();

            // 1_000 rows a day for 3 days, in row groups of 100 rows
            execute("CREATE TABLE t2 AS (SELECT x::INT x, timestamp_sequence('2024-01-01', 86_400_000L) ts FROM long_sequence(3_000)) TIMESTAMP(ts) PARTITION BY DAY");
            final String largeQuery = "SELECT x > (SELECT max(y) FROM u) b, ts FROM t2";
            assertExportMode(largeQuery, ParquetExportMode.PAGE_FRAME_BACKED);
            exportId = queueCopy("COPY (" + largeQuery + ") TO 'sub_query_row_groups' WITH FORMAT PARQUET ROW_GROUP_SIZE 100");
            execute("ALTER TABLE u ADD COLUMN z2 INT");
            Assert.assertEquals(1, runExportJobCountingCompiles());
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            final String parquetPath = parquetPath("sub_query_row_groups");
            assertParquetMatchesQuery(largeQuery, parquetPath);
            // max(y) is 2, so only x 1 and 2 fail the comparison
            assertQuery("SELECT b, count(), min(ts), max(ts) FROM read_parquet('" + parquetPath + "') ORDER BY b")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            b\tcount\tmin\tmax
                            false\t2\t2024-01-01T00:00:00.000000Z\t2024-01-01T00:01:26.400000Z
                            true\t2998\t2024-01-01T00:02:52.800000Z\t2024-01-03T23:58:33.600000Z
                            """);
            ParquetTestUtils.assertRowGroupCount(parquetPath, configuration.getFilesFacade(), 30);
            assertNoTempDirLeft();

            // c of tc turns from INT to VARCHAR right after the export compiled its select again, so the export compiles
            // it once more, and sets up a projection that computes cc as VARCHAR, with a buffer for the offsets of its
            // values, where the set-up of the stale plan computed an INT.
            execute("CREATE TABLE tc (x INT, c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tc VALUES
                    (3, 30, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-02T00:00:00.000000Z'),
                    (1, 10, '2024-01-03T00:00:00.000000Z')
                    """);
            final String retypedQuery = "SELECT x > (SELECT max(y) FROM u) b, coalesce(c, c) cc, ts FROM tc";
            assertExportMode(retypedQuery, ParquetExportMode.PAGE_FRAME_BACKED);
            exportId = queueCopy("COPY (" + retypedQuery + ") TO 'sub_query_retyped' WITH FORMAT PARQUET");
            execute("ALTER TABLE u ADD COLUMN z3 INT");
            Assert.assertEquals(2, runExportJobCountingCompiles(compileCount -> {
                if (compileCount == 1) {
                    execute("ALTER TABLE tc ALTER COLUMN c TYPE VARCHAR");
                }
            }));
            assertExportLog(exportId, STREAMING_EXPORT_LOG);
            assertParquetRows("sub_query_retyped", "ts", """
                    b\tcc\tts
                    true\t30\t2024-01-01T00:00:00.000000Z
                    false\t20\t2024-01-02T00:00:00.000000Z
                    false\t10\t2024-01-03T00:00:00.000000Z
                    """);
            ParquetTestUtils.assertColumnTypes(parquetPath("sub_query_retyped"), configuration.getFilesFacade(), ColumnType.VARCHAR, 1);
            assertParquetMatchesQuery(retypedQuery, parquetPath("sub_query_retyped"));
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportRecompilesUpToServerDefaultAttempts() throws Exception {
        // After each compile of the stale select, the table gains another column before the export opens the new plan,
        // so the open finds that plan stale again, as long as the columns keep coming. With the server default of 10
        // attempts, the export compiles its select 10 times and opens at most 11 plans: it succeeds when the plan of the
        // 10th compile stays current, and fails when that plan goes stale too.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, 10);
            for (int staleCompileCount : new int[]{9, 10}) {
                createStaleExportTable("BYPASS WAL");
                final String fileName = "stale_compiles_" + staleCompileCount;
                final String exportId = queueCopy("COPY (SELECT * FROM t) TO '" + fileName + "' WITH FORMAT PARQUET");
                execute("ALTER TABLE t ADD COLUMN extra0 INT");
                Assert.assertEquals(10, runExportJobCountingCompiles(compileCount -> {
                    if (compileCount <= staleCompileCount) {
                        execute("ALTER TABLE t ADD COLUMN extra" + compileCount + " INT");
                    }
                }));
                if (staleCompileCount == 9) {
                    assertExportLog(exportId, STREAMING_EXPORT_LOG);
                    final StringSink expectedRows = new StringSink();
                    expectedRows.put("x\ts\tts");
                    for (int i = 0; i < 10; i++) {
                        expectedRows.put("\textra").put(i);
                    }
                    final String nulls = "\tnull".repeat(10);
                    expectedRows.put('\n')
                            .put("3\ta\t2024-01-01T00:00:00.000000Z").put(nulls).put('\n')
                            .put("2\tb\t2024-01-02T00:00:00.000000Z").put(nulls).put('\n')
                            .put("1\ta\t2024-01-03T00:00:00.000000Z").put(nulls).put('\n');
                    assertParquetRows(fileName, "ts", expectedRows.toString());
                } else {
                    assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
                    // t has the metadata version 0 when created, and each of its 11 columns adds one
                    final int tableId = engine.verifyTableName("t").getTableId();
                    assertExportFailure(exportId, "failed", "cached query plan cannot be used because table schema has changed [table=t'" +
                            ", expectedTableId=" + tableId + ", actualTableId=" + tableId +
                            ", expectedMetadataVersion=10, actualMetadataVersion=11]");
                    Assert.assertFalse(exportFileExists(fileName));
                }
                assertNoTempDirLeft();
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testCopyQueuedExportStopsRecompilingAtMaxAttempts() throws Exception {
        // test_table_reference_out_of_date() reports its plan stale on every open, so the export compiles its select as
        // many times as cairo.sql.max.recompile.attempts allows and then fails with the message of the last open.
        assertMemoryLeak(() -> {
            for (int maxRecompileAttempts : new int[]{0, 2}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, maxRecompileAttempts);
                final String exportId = queueCopy("COPY (SELECT * FROM test_table_reference_out_of_date()) TO 'always_stale' WITH FORMAT PARQUET");
                Assert.assertEquals(maxRecompileAttempts, runExportJobCountingCompiles());
                assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
                assertExportFailure(exportId, "failed", "cached query plan cannot be used because table schema has changed [table=test_table_reference_out_of_date]");
                Assert.assertFalse(exportFileExists("always_stale"));
                assertNoTempDirLeft();
            }

            // A negative setting allows no compile either, even of a plan that one compile would make current.
            node1.setProperty(PropertyKey.CAIRO_SQL_MAX_RECOMPILE_ATTEMPTS, -1);
            createStaleExportTable("BYPASS WAL");
            final String exportId = queueCopy("COPY (SELECT * FROM t) TO 'negative_attempts' WITH FORMAT PARQUET");
            execute("ALTER TABLE t ADD COLUMN extra INT");
            Assert.assertEquals(0, runExportJobCountingCompiles());
            assertExportLog(exportId, FAILED_STREAMING_EXPORT_LOG);
            final int tableId = engine.verifyTableName("t").getTableId();
            assertExportFailure(exportId, "failed", "cached query plan cannot be used because table schema has changed [table=t'" +
                    ", expectedTableId=" + tableId + ", actualTableId=" + tableId +
                    ", expectedMetadataVersion=0, actualMetadataVersion=1]");
            Assert.assertFalse(exportFileExists("negative_attempts"));
            assertNoTempDirLeft();
        });
    }

    @Test
    public void testCopyQueuedExportsReuseExporterAfterRecompileAndReroute() throws Exception {
        // One export job runs an export whose projection sub-query went stale, then an export that its compile re-routes
        // to a temp table, then current exports in each streaming mode, then a DIRECT_PAGE_FRAME export whose new plan
        // has a column more than the one before, and one as wide. Each export starts from the state that the one before
        // it left in the job's exporter.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE u (y INT)");
            execute("INSERT INTO u VALUES (1), (2)");
            createStaleExportTable("BYPASS WAL");
            final String subQueryQuery = "SELECT x > (SELECT max(y) FROM u) b, ts FROM t";
            final String reroutedQuery = "SELECT * FROM t ORDER BY x";
            final String[] currentQueries = {"SELECT * FROM t", "SELECT x + 1 AS y, * FROM t", "SELECT x, s, ts FROM t ORDER BY x"};
            final ParquetExportMode[] currentModes = {ParquetExportMode.DIRECT_PAGE_FRAME, ParquetExportMode.PAGE_FRAME_BACKED, ParquetExportMode.CURSOR_BASED};
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                assertExportMode(subQueryQuery, ParquetExportMode.PAGE_FRAME_BACKED);
                queueCopy("COPY (" + subQueryQuery + ") TO 'reused_sub_query' WITH FORMAT PARQUET");
                execute("ALTER TABLE u ADD COLUMN z INT");
                Assert.assertTrue(job.run());
                assertParquetMatchesQuery(subQueryQuery, parquetPath("reused_sub_query"));

                assertExportMode(reroutedQuery, ParquetExportMode.CURSOR_BASED);
                queueCopy("COPY (" + reroutedQuery + ") TO 'reused_rerouted' WITH FORMAT PARQUET");
                execute("ALTER TABLE t ADD COLUMN b BINARY");
                Assert.assertTrue(job.run());
                assertParquetMatchesQuery(reroutedQuery, parquetPath("reused_rerouted"));

                for (int i = 0; i < currentQueries.length; i++) {
                    assertExportMode(currentQueries[i], currentModes[i]);
                    queueCopy("COPY (" + currentQueries[i] + ") TO 'reused_current_" + i + "' WITH FORMAT PARQUET");
                    Assert.assertTrue(job.run());
                    assertParquetMatchesQuery(currentQueries[i], parquetPath("reused_current_" + i));
                }

                queueCopy("COPY (SELECT * FROM t) TO 'reused_wider' WITH FORMAT PARQUET");
                execute("ALTER TABLE t ADD COLUMN extra INT");
                Assert.assertTrue(job.run());
                assertParquetMatchesQuery("SELECT * FROM t", parquetPath("reused_wider"));
                queueCopy("COPY (SELECT * FROM t) TO 'reused_as_wide' WITH FORMAT PARQUET");
                Assert.assertTrue(job.run());
                assertParquetMatchesQuery("SELECT * FROM t", parquetPath("reused_as_wide"));
            }
            assertQuery("SELECT count() FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE phase = 'success' AND status = 'finished'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n7\n");
            // No check for a temp directory left behind: an export through a temp table that writes one file keeps its
            // empty tmp_<id> directory (a known bug of SQLSerialParquetExporter.process()).
        });
    }

    @Test
    public void testCopyQueuedRetypedDescExportSurvivesConvertToNative() throws Exception {
        // The descending export of a retyped table streams its rows. The Parquet partition goes native before the export
        // runs. The scan holds no Parquet row group filter that the conversion makes stale, so the export streams the
        // queued plan, whose cursor reads the native partition through the same accessors, still descending, as the
        // temp table did.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createRetypedTables(walMode);
                final String query = "SELECT * FROM r ORDER BY ts DESC";
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                final String exportId = queueCopy("COPY (" + query + ") TO 'converted_to_native' WITH FORMAT PARQUET");
                execute("ALTER TABLE r CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
                drainWalQueue();
                Assert.assertEquals(walMode, 0, runExportJobCountingCompiles());
                assertExportLog(exportId, STREAMING_EXPORT_LOG);
                assertNoTempDirOf(exportId);
                assertParquetMatchesControl("SELECT * FROM nr ORDER BY ts DESC", "converted_to_native", null);
                dropRetypedTables();
            }
        });
    }

    @Test
    public void testCopyQueuedRetypedExportFollowsReplacedView() throws Exception {
        // The export of a view over a retyped table streams its rows. The view is replaced before the export runs, so the
        // export compiles its select again and routes the new plan by its own direction: a descending scan streams its
        // rows descending, a plan that a table scan does not order goes through a temp table, and a backward scan of
        // page frames streams its rows through the cursor exporter rather than reading its frames ascending.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            final FilesFacade ff = configuration.getFilesFacade();
            final String query = "SELECT * FROM vq";
            final String[][] replacements = {
                    // the view's new definition, its control over nr, the designated timestamp of the file
                    {"SELECT * FROM r WHERE x > 0 ORDER BY ts DESC", "SELECT * FROM nr WHERE x > 0 ORDER BY ts DESC", null},
                    {"SELECT * FROM r ORDER BY other_ts DESC", "SELECT * FROM nr ORDER BY other_ts DESC", null},
                    {"SELECT * FROM nr ORDER BY ts DESC", "SELECT * FROM nr ORDER BY ts DESC", null},
                    {"SELECT * FROM r WHERE s = 'a' LATEST ON ts PARTITION BY s", "SELECT * FROM nr WHERE s = 'a' LATEST ON ts PARTITION BY s", "ts"},
            };
            final String[] expectedLogs = {STREAMING_EXPORT_LOG, REROUTED_EXPORT_LOG, STREAMING_EXPORT_LOG, REROUTED_EXPORT_LOG};
            // the compile of the select, and for a temp table the CREATE TABLE AS SELECT
            final int[] expectedCompileCounts = {1, 2, 1, 2};
            for (int i = 0; i < replacements.length; i++) {
                execute("CREATE OR REPLACE VIEW vq AS (SELECT * FROM r)");
                drainWalAndViewQueues();
                assertExportMode(query, ParquetExportMode.CURSOR_BASED);
                final String fileName = "replaced_view_" + i;
                final String exportId = queueCopy("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET");
                execute("CREATE OR REPLACE VIEW vq AS (" + replacements[i][0] + ")");
                drainWalAndViewQueues();
                try {
                    Assert.assertEquals(expectedCompileCounts[i], runExportJobCountingCompiles());
                    assertExportLog(exportId, expectedLogs[i]);
                    assertParquetMatchesControl(replacements[i][1], fileName, replacements[i][2]);
                    Assert.assertNull(engine.getTableTokenIfExists("copy." + exportId));
                } catch (AssertionError e) {
                    throw new AssertionError(replacements[i][0], e);
                }
            }
            ParquetTestUtils.assertColumnMaxDefLevel(parquetPath("replaced_view_0"), ff, 4, 0);
            assertQuery("SELECT count() FROM read_parquet('" + parquetPath("replaced_view_1") + "') WHERE other_ts IS NULL")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testCopySampleByFillConstantOnSymbolAggregateExportsText() throws Exception {
        // The fill rows of a text constant on a SYMBOL aggregate hold keys that its symbol table does not resolve to the
        // constant, so the export writes the aggregate as text, which holds the constant, as the query returns it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE s1 (sym SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO s1 VALUES
                        ('a', 1, '2024-01-01T00:00:00.000000Z'),
                        (NULL, 2, '2024-01-01T01:00:00.000000Z'),
                        ('b', 3, '2024-01-02T00:00:00.000000Z'),
                        ('a', 4, '2024-01-02T05:00:00.000000Z')
                    """);
            for (String constant : new String[]{"1", "zz", "42"}) {
                final String query = "SELECT ts, first(sym) fs, sum(v) s FROM s1 SAMPLE BY 6h FILL('" + constant + "', 0)";
                final String fileName = "fill_" + constant;
                assertCursorExportOfSymbols(query, fileName, new int[]{}, new int[]{1});
                assertParquetMatchesQuery(query, parquetPath(fileName));
                assertQuery("SELECT * FROM read_parquet('" + parquetPath(fileName) + "')")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("""
                                ts\tfs\ts
                                2024-01-01T00:00:00.000000Z\ta\t3
                                2024-01-01T06:00:00.000000Z\t%c\t0
                                2024-01-01T12:00:00.000000Z\t%c\t0
                                2024-01-01T18:00:00.000000Z\t%c\t0
                                2024-01-02T00:00:00.000000Z\tb\t7
                                """.replace("%c", constant));
            }
        });
    }

    @Test
    public void testCopyStreamedRetypedExportKeepsDesignatedTimestamp() throws Exception {
        // The designated timestamp of a table holds no NULL, so the file of a retyped table's scan keeps it REQUIRED, at
        // the edges of its range and in both directions: designated when the rows ascend, and none when they descend.
        // A table rejects a designated timestamp before 1970, so the epoch and the step after it are the smallest ones.
        assertMemoryLeak(() -> {
            final FilesFacade ff = configuration.getFilesFacade();
            final String[][] timestampTypes = {
                    {"TIMESTAMP", "'1970-01-01T00:00:00.000001Z'", "'9999-12-31T23:59:59.999999Z'"},
                    {"TIMESTAMP_NS", "'1970-01-01T00:00:00.000000001Z'", "'2261-12-31T23:59:59.999999999Z'"},
            };
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                for (String[] timestampType : timestampTypes) {
                    for (String tableName : new String[]{"edge", "nedge"}) {
                        execute("CREATE TABLE " + tableName + " (s SYMBOL, x INT, ts " + timestampType[0] + ") TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                        execute("INSERT INTO " + tableName + " VALUES " +
                                "('a', 1, '1970-01-01T00:00:00.000000Z'), " +
                                "('b', 2, " + timestampType[1] + "), " +
                                "('a', 3, '2024-02-29T12:00:00.000000Z'), " +
                                "('b', 4, " + timestampType[2] + ")");
                    }
                    drainWalQueue();
                    execute("ALTER TABLE edge CONVERT PARTITION TO PARQUET LIST '1970-01-01', '2024-02-29'");
                    drainWalQueue();
                    execute("ALTER TABLE edge ALTER COLUMN x TYPE LONG");
                    execute("ALTER TABLE nedge ALTER COLUMN x TYPE LONG");
                    drainWalQueue();
                    bindVariableService.clear();
                    bindVariableService.setStr(0, "a");
                    // the rows after the epoch
                    bindVariableService.setTimestamp(1, 1);
                    final String[][] queries = {
                            {"SELECT * FROM %s", "ts"},
                            {"SELECT * FROM %s ORDER BY ts DESC", null},
                            {"SELECT * FROM %s WHERE s = $1", "ts"},
                            {"SELECT * FROM %s WHERE ts >= $2", "ts"},
                    };
                    for (int i = 0; i < queries.length; i++) {
                        final String query = queries[i][0].replace("%s", "edge");
                        final String controlQuery = queries[i][0].replace("%s", "nedge");
                        final String fileName = "edge_" + i;
                        try {
                            assertCopyStreamsRows(query, fileName);
                            assertParquetMatchesControl(controlQuery, fileName, queries[i][1]);
                            ParquetTestUtils.assertColumnTypes(parquetPath(fileName), ff, ColumnType.typeOf(timestampType[0]), 2);
                            ParquetTestUtils.assertColumnMaxDefLevel(parquetPath(fileName), ff, 2, 0);
                            assertQueriesPrintAlike(
                                    "SELECT ts FROM (" + controlQuery + ") ORDER BY ts",
                                    "SELECT ts FROM read_parquet('" + parquetPath(fileName) + "') ORDER BY ts"
                            );
                        } catch (AssertionError e) {
                            throw new AssertionError(walMode + ", " + timestampType[0] + ": " + query, e);
                        }
                    }
                    bindVariableService.clear();
                    execute("DROP TABLE edge");
                    execute("DROP TABLE nedge");
                    drainWalQueue();
                }
            }
        });
    }

    @Test
    public void testCopyStreamedRetypedExportWritesComputedSymbolAsString() throws Exception {
        // A streamed export of a retyped table keeps a table's SYMBOL column as SYMBOL and writes a SYMBOL that a function
        // computes as STRING, as every streamed export does. SAMPLE BY keeps the temp table, which keeps its first() a
        // SYMBOL.
        assertMemoryLeak(() -> {
            createRetypedTables("WAL");
            final FilesFacade ff = configuration.getFilesFacade();

            final String castQuery = "SELECT (s || 'x')::SYMBOL cs, x, ts FROM r";
            assertCopyStreamsRows(castQuery, "computed_symbol");
            assertParquetMatchesControl(castQuery.replace("FROM r", "FROM nr"), "computed_symbol", "ts");
            assertSymbolColumnsWrittenAsString(parquetPath("computed_symbol"), 0);

            final String groupByQuery = "SELECT s, first(s2) f FROM r";
            assertCopyStreamsRows(groupByQuery, "group_by_symbol");
            assertParquetMatchesQueryInAnyOrder(groupByQuery.replace("FROM r", "FROM nr"), parquetPath("group_by_symbol"));
            assertSymbolColumnsKept(parquetPath("group_by_symbol"), 0);
            assertSymbolColumnsWrittenAsString(parquetPath("group_by_symbol"), 1);

            final String sampleByQuery = "SELECT ts, first(s2) f FROM r SAMPLE BY 1d";
            assertCopyMaterializesRows(sampleByQuery, "sample_by_symbol");
            assertParquetMatchesControl(sampleByQuery.replace("FROM r", "FROM nr"), "sample_by_symbol", "ts");
            ParquetTestUtils.assertColumnTypes(parquetPath("sample_by_symbol"), ff, ColumnType.SYMBOL, 1);
        });
    }

    @Test
    public void testCopyStreamingExportFailureCleansUpTempDir() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            private volatile long failFd = -1;

            @Override
            public long openRW(LPSZ name, int opts) {
                long fd = super.openRW(name, opts);
                if (Utf8s.containsAscii(name, "tmp_") && Utf8s.endsWithAscii(name, "export.parquet")) {
                    failFd = fd;
                }
                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (fd == failFd && fd != -1) {
                    return -1;
                }
                return super.write(fd, address, len, offset);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE test_table (x INT, y LONG, z STRING)");
            execute("INSERT INTO test_table VALUES (1, 100, 'hello'), (2, 200, 'world')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM test_table) TO 'fail_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        status
                                        failed
                                        """);

                        // Verify no tmp_ directories remain in export root
                        File exportDir = new File(exportRoot);
                        File[] tmpDirs = exportDir.listFiles(f -> f.isDirectory() && f.getName().startsWith("tmp_"));
                        Assert.assertTrue(
                                "temp directory should have been cleaned up, but found: "
                                        + (tmpDirs != null ? tmpDirs.length : 0) + " tmp_ dir(s)",
                                tmpDirs == null || tmpDirs.length == 0
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyStreamingExportMoveFailureCleansUpTempDir() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                // Fail the final rename from the temp file to the export destination
                if (Utf8s.containsAscii(from, "tmp_") && Utf8s.endsWithAscii(to, ".parquet")) {
                    return Files.FILES_RENAME_ERR_OTHER;
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE test_table (x INT, y LONG, z STRING)");
            execute("INSERT INTO test_table VALUES (1, 100, 'hello'), (2, 200, 'world')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM test_table) TO 'move_fail_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        status
                                        failed
                                        """);

                        // Verify no tmp_ directories remain in export root
                        File exportDir = new File(exportRoot);
                        File[] tmpDirs = exportDir.listFiles(f -> f.isDirectory() && f.getName().startsWith("tmp_"));
                        Assert.assertTrue(
                                "temp directory should have been cleaned up, but found: "
                                        + (tmpDirs != null ? tmpDirs.length : 0) + " tmp_ dir(s)",
                                tmpDirs == null || tmpDirs.length == 0
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyStringInlinedTreatedAsNull() throws Exception {
        // Defensive coverage for the var-size branch in CopyExportRequestTask.writePageFrame.
        // ColumnType.isVarSize() also matches STRING, so this test ensures the var-size
        // branch does not break STRING export when the column contains a mix of NULL and
        // non-NULL values. STRING always writes a 4-byte length prefix per row to .d (even
        // for NULLs), so STRING never has an empty .d the way an all-inlined VARCHAR does;
        // this test still exercises the new branch end-to-end.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE str_inl (
                        ts TIMESTAMP,
                        id INT,
                        s STRING
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            execute("""
                    INSERT INTO str_inl VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'alpha'),
                        ('2024-01-01T01:00:00.000000Z', 2, NULL),
                        ('2024-01-01T02:00:00.000000Z', 3, 'bravo'),
                        ('2024-01-01T03:00:00.000000Z', 4, 'longer string than nine bytes')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM str_inl) TO 'str_inl_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "str_inl_out.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "str_inl_out.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\ts
                                        2024-01-01T00:00:00.000000Z\t1\talpha
                                        2024-01-01T01:00:00.000000Z\t2\t
                                        2024-01-01T02:00:00.000000Z\t3\tbravo
                                        2024-01-01T03:00:00.000000Z\t4\tlonger string than nine bytes
                                        """);
                        // Verify the NULL row is actually NULL, not an empty string.
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "str_inl_out.parquet') WHERE s IS NULL")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n1\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyTableToParquetBasicSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y long, z string)");
            execute("insert into test_table values (1, 100L, 'hello'), (2, 200L, 'world')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output1' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output1.parquet" + "\t1\tfinished\n");
                        // Verify exported data can be read back and matches original
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output1" + ".parquet') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty\tz
                                        1\t100\thello
                                        2\t200\tworld
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    // Demonstration of proper copy export test pattern
    @Test
    public void testCopyTableToParquetWithExportLog() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table test_table (x int, y long, z string)");
            execute("insert into test_table values (1, 100L, 'hello'), (2, 200L, 'world')");

            runAndFetchCopyExportID("copy test_table to 'output1' with format parquet", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> assertEventually(() -> {
            // Verify export completed successfully
            String query = "select status from \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" limit -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");

            // Verify exported data can be read back and matches original
            assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output1" + ".parquet') order by x")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            x\ty\tz
                            1\t100\thello
                            2\t200\tworld
                            """);
        });
        testCopyExport(statement, test);
    }

    @Test
    public void testCopyTableToParquetWithQuotedTableName() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"test table\" (x int, y long)");
            execute("insert into \"test table\" values (1, 100L), (2, 200L)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy 'test table' to 'output2' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output2.parquet" + "\t1\tfinished\n");
                        // Verify exported data
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output2" + ".parquet') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty
                                        1\t100
                                        2\t200
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyUseInsert() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("CREATE TABLE reject_non_select_test AS (SELECT x FROM long_sequence(2))", sqlExecutionContext);
            assertQuery("copy (INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test) to 'test_table' with format parquet;")
                    .fails(6, "table and column names that are SQL keywords have to be enclosed in double quotes, such as");
        });
    }

    @Test
    public void testCopyVarcharInlinedHybridMaterializer() throws Exception {
        // Regression: HybridColumnMaterializer.buildColumnDataFromPageFrame had the same
        // pageAddress==0 bug as CopyExportRequestTask.writePageFrame for inlined VARCHAR
        // pass-through columns. Adding a computed column to the SELECT introduces a
        // VirtualRecordCursorFactory and routes COPY through the PAGE_FRAME_BACKED hybrid
        // materializer; vc is the inlined-VARCHAR pass-through column under test.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_hyb (
                        ts TIMESTAMP,
                        id INT,
                        vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            // All values <=9 bytes -> inlined in aux entries -> .d file empty.
            execute("""
                    INSERT INTO vc_hyb VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'alpha'),
                        ('2024-01-01T01:00:00.000000Z', 2, 'bravo'),
                        ('2024-01-01T02:00:00.000000Z', 3, 'charlie')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT ts, id + 10 AS id_plus, vc FROM vc_hyb) TO 'vc_hyb_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_hyb_out.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_hyb_out.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid_plus\tvc
                                        2024-01-01T00:00:00.000000Z\t11\talpha
                                        2024-01-01T01:00:00.000000Z\t12\tbravo
                                        2024-01-01T02:00:00.000000Z\t13\tcharlie
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyVarcharInlinedMultiPartition() throws Exception {
        // Regression: each partition has its own page-frame and aux-page address.
        // Verifies that the inlined-VARCHAR colTop fix holds across multiple partitions
        // emitted as separate parquet files.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_mp (
                        ts TIMESTAMP,
                        id INT,
                        vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            // 6 rows across 3 partitions, all values <=9 bytes -> inlined.
            execute("""
                    INSERT INTO vc_mp VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'alpha'),
                        ('2024-01-01T12:00:00.000000Z', 2, 'bravo'),
                        ('2024-01-02T00:00:00.000000Z', 3, 'charlie'),
                        ('2024-01-02T12:00:00.000000Z', 4, 'delta'),
                        ('2024-01-03T00:00:00.000000Z', 5, 'echo'),
                        ('2024-01-03T12:00:00.000000Z', 6, 'foxtrot')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY vc_mp TO 'vc_mp_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_mp_out" + File.separator + "\t3\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_mp_out" + File.separator + "2024-01-01.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-01T00:00:00.000000Z\t1\talpha
                                        2024-01-01T12:00:00.000000Z\t2\tbravo
                                        """);
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_mp_out" + File.separator + "2024-01-02.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-02T00:00:00.000000Z\t3\tcharlie
                                        2024-01-02T12:00:00.000000Z\t4\tdelta
                                        """);
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_mp_out" + File.separator + "2024-01-03.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .timestamp("ts")
                                .expectSize()
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-03T00:00:00.000000Z\t5\techo
                                        2024-01-03T12:00:00.000000Z\t6\tfoxtrot
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyVarcharInlinedTreatedAsNull() throws Exception {
        // Regression: CopyExportRequestTask.writePageFrame used pageAddress==0 to detect a
        // column-top (all-null column). For VARCHAR, short strings (<=9 bytes) are stored
        // entirely inline inside aux entries, so the .d data file is empty and pageAddress
        // is legitimately 0. The streaming writer would then materialise every row as NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_inl (
                        ts TIMESTAMP,
                        id INT,
                        vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            // All values <=9 bytes -> inlined in aux entries -> .d file empty.
            execute("""
                    INSERT INTO vc_inl VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'alpha'),
                        ('2024-01-01T01:00:00.000000Z', 2, 'bravo'),
                        ('2024-01-01T02:00:00.000000Z', 3, 'charlie')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM vc_inl) TO 'vc_inl_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_inl_out.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_inl_out.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-01T00:00:00.000000Z\t1\talpha
                                        2024-01-01T01:00:00.000000Z\t2\tbravo
                                        2024-01-01T02:00:00.000000Z\t3\tcharlie
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyVarcharInlinedWithNulls() throws Exception {
        // Verifies the new aux-address colTop detector still reports actual NULLs as
        // NULL while preserving inlined non-NULL rows in the same column. The .d file
        // remains empty (every present value is <=9 bytes); only the aux entries
        // distinguish NULL from non-NULL.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_inl_nulls (
                        ts TIMESTAMP,
                        id INT,
                        vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            execute("""
                    INSERT INTO vc_inl_nulls VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'alpha'),
                        ('2024-01-01T01:00:00.000000Z', 2, NULL),
                        ('2024-01-01T02:00:00.000000Z', 3, 'bravo'),
                        ('2024-01-01T03:00:00.000000Z', 4, NULL),
                        ('2024-01-01T04:00:00.000000Z', 5, 'charlie')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM vc_inl_nulls) TO 'vc_inl_nulls_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_inl_nulls_out.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_inl_nulls_out.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-01T00:00:00.000000Z\t1\talpha
                                        2024-01-01T01:00:00.000000Z\t2\t
                                        2024-01-01T02:00:00.000000Z\t3\tbravo
                                        2024-01-01T03:00:00.000000Z\t4\t
                                        2024-01-01T04:00:00.000000Z\t5\tcharlie
                                        """);
                        // The two NULL rows must be encoded as NULL, not as empty strings.
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "vc_inl_nulls_out.parquet') WHERE vc IS NULL")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n2\n");
                        assertQuery("SELECT count(*) FROM read_parquet('" + exportRoot + File.separator + "vc_inl_nulls_out.parquet') WHERE vc IS NOT NULL")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .returns("count\n3\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyVarcharMixedInlinedAndSpilled() throws Exception {
        // Mixed VARCHAR column: some values <=9 bytes (inlined in aux) and some >9 bytes
        // (spilled to .d). Both data and aux addresses are non-zero, so the colTop heuristic
        // must not flip rows to NULL regardless of which branch is taken. Guards against any
        // future regression that swaps the var-size detector back to the data-page address.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE vc_mixed (
                        ts TIMESTAMP,
                        id INT,
                        vc VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY""");

            execute("""
                    INSERT INTO vc_mixed VALUES
                        ('2024-01-01T00:00:00.000000Z', 1, 'short'),
                        ('2024-01-01T01:00:00.000000Z', 2, 'this is definitely longer than nine bytes'),
                        ('2024-01-01T02:00:00.000000Z', 3, 'tiny'),
                        ('2024-01-01T03:00:00.000000Z', 4, 'another spilled value bigger than nine bytes')""");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT * FROM vc_mixed) TO 'vc_mixed_out' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "vc_mixed_out.parquet\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "vc_mixed_out.parquet') ORDER BY ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tid\tvc
                                        2024-01-01T00:00:00.000000Z\t1\tshort
                                        2024-01-01T01:00:00.000000Z\t2\tthis is definitely longer than nine bytes
                                        2024-01-01T02:00:00.000000Z\t3\ttiny
                                        2024-01-01T03:00:00.000000Z\t4\tanother spilled value bigger than nine bytes
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithBloomFilterColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y STRING, z DOUBLE)");
            execute("INSERT INTO test_table VALUES (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'bloom_cols' with format parquet bloom_filter_columns 'x,z'", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "bloom_cols.parquet" + "\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bloom_cols.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty\tz
                                        1\thello\t1.5
                                        2\tworld\t2.5
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithBloomFilterColumnsAndFpp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y STRING, z DOUBLE)");
            execute("INSERT INTO test_table VALUES (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'bloom_both' with format parquet bloom_filter_columns 'x,z' bloom_filter_fpp '0.05'", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "bloom_both.parquet" + "\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bloom_both.parquet') ORDER BY x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty\tz
                                        1\thello\t1.5
                                        2\tworld\t2.5
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithBloomFilterFpp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y STRING)");
            execute("INSERT INTO test_table VALUES (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'bloom_fpp' with format parquet bloom_filter_fpp '0.01'", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "bloom_fpp.parquet" + "\t1\tfinished\n");
                        assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + "bloom_fpp.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithBloomFilterNonExistentColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y STRING)");
            execute("INSERT INTO test_table VALUES (1, 'test')");
            assertQuery("copy test_table to 'bloom_bad_col' with format parquet bloom_filter_columns 'x,nonexistent'")
                    .fails(79, "bloom_filter_columns contains non-existent column: nonexistent");
        });
    }

    @Test
    public void testCopyWithCaseInsensitiveOptions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output14' with FORMAT PARQUET COMPRESSION_CODEC SNAPPY", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output14.parquet" + "\t1\tfinished\n");
                        // Verify case-insensitive options work
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output14" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithCompressionCodec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output4' with format parquet compression_codec snappy", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output4.parquet" + "\t1\tfinished\n");
                        // Verify compressed data is readable
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output4" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithCompressionLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output5' with format parquet compression_level 9", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output5.parquet" + "\t1\tfinished\n");
                        // Verify data with compression level
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output5" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithDataPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output7' with format parquet data_page_size 4096", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output7.parquet" + "\t1\tfinished\n");
                        // Verify data with custom page size
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output7" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithMultipleOptions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () -> runAndFetchCopyExportID("copy test_table to 'output13' with format parquet " +
                    "compression_codec gzip compression_level 9 " +
                    "row_group_size 5000 data_page_size 8192 " +
                    "statistics_enabled true parquet_version 2", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output13.parquet" + "\t1\tfinished\n");
                        // Verify data with multiple options
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output13.parquet" + "') order by x")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x\ty\tz
                                        1\thello\t1.5
                                        2\tworld\t2.5
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithNowFunc() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosTimestampDriver.floor("2023-01-01T10:00:01.000Z"));
            execute("create table test_table (ts timestamp, x int) timestamp(ts) partition by DAY");
            execute("insert into test_table values ('2023-01-01T10:00:00.000Z', 1), ('2023-01-02T10:00:00.000Z', 2)");
            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy (select * from test_table where ts < now()) to 'output11' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output11.parquet" + "\t1\tfinished\n");
                        // Verify partitioned data
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output11.parquet') order by ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts	x
                                        2023-01-01T10:00:00.000000Z	1
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithOutputSpecialChar() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to '💗❤️' with format parquet data_page_size 4096", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "💗❤️.parquet" + "\t1\tfinished\n");
                        // Verify data with custom page size
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "💗❤️" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                        assertQuery("select path, diskSizeHuman from export_files()  order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        💗❤️.parquet\t601.0 B
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");
            execute("create table test_table1 (x int, y string)");
            execute("insert into test_table1 values (1, 'test')");
            execute("create table test_table2 (x int, y string)");
            execute("insert into test_table2 values (1, 'test')");
            execute("create table test_table3 (x int, y string)");
            execute("insert into test_table3 values (1, 'test')");

            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy test_table to 'output8' with format parquet", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table1 to 'output9' with format parquet", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table2 to 'output10' with format parquet", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table3 to 'output11' with format parquet", sqlExecutionContext);
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" where export_path != null order by export_path, ts")
                            .noLeakCheck()
                            .returns("export_path\tnum_exported_files\tstatus\n" +
                                    exportRoot + File.separator + "output10.parquet" + "\t1\tfinished\n" +
                                    exportRoot + File.separator + "output11.parquet" + "\t1\tfinished\n" +
                                    exportRoot + File.separator + "output8.parquet" + "\t1\tfinished\n" +
                                    exportRoot + File.separator + "output9.parquet" + "\t1\tfinished\n")
                    );
            testCopyExport(stmt, test, true, 4);
        });
    }

    @Test
    public void testCopyWithParquetVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output10' with format parquet parquet_version 1", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output10.parquet" + "\t1\tfinished\n");
                        // Verify data with specific Parquet version
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output10" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithPartitionByTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts timestamp, x int) timestamp(ts) partition by DAY");
            execute("insert into test_table values ('2023-01-01T10:00:00.000Z', 1), ('2023-01-02T10:00:00.000Z', 2)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output11' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output11" + File.separator + "\t2\tfinished\n");
                        // Verify partitioned data
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output11" + File.separator + "2023-01-01.parquet') order by ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-01-01T10:00:00.000000Z\t1
                                        """);
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output11" + File.separator + "2023-01-02.parquet') order by ts")
                                .noLeakCheck()
                                .expectSize()
                                .timestamp("ts")
                                .returns("""
                                        ts\tx
                                        2023-01-02T10:00:00.000000Z\t2
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithPartitionByWithoutTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int)");
            execute("insert into test_table values (1), (2)");
            try {
                runAndFetchCopyExportID("copy test_table to 'output12' with format parquet partition_by DAY", sqlExecutionContext);
                Assert.fail("Expected failure due to missing timestamp column");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "partitioning is possible only on tables with designated timestamps");
            }

            try {
                runAndFetchCopyExportID("copy (select * from test_table) to 'output12' with format parquet partition_by DAY", sqlExecutionContext);
                Assert.fail("Expected failure due to missing timestamp column");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "partitioning is possible only on tables with designated timestamps");
            }
        });
    }

    @Test
    public void testCopyWithRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output6' with format parquet row_group_size 1000", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output6.parquet" + "\t1\tfinished\n");
                        // Verify data with custom row group size
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output6" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithSameDirs() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () -> runAndFetchCopyExportID("copy test_table to 'output13' with format parquet ", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output13.parquet" + "\t1\tfinished\n");
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        output13.parquet\t905.0 B
                                        """);
                    });

            CopyExportRunnable stmt1 = () -> runAndFetchCopyExportID("copy test_table to 'output14' with format parquet ", sqlExecutionContext);

            CopyExportRunnable test1 = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output14.parquet" + "\t1\tfinished\n");
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        output13.parquet\t905.0 B
                                        output14.parquet\t905.0 B
                                        """);
                    });

            testCopyExport(stmt, test);
            testCopyExport(stmt1, test1);

            CopyExportRunnable stmt2 = () -> runAndFetchCopyExportID("copy test_table to 'output13' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test2 = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output13.parquet" + "\t1\tfinished\n");
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("""
                                        path\tdiskSizeHuman
                                        output13.parquet\t952.0 B
                                        output14.parquet\t905.0 B
                                        """);
                    });
            execute("insert into test_table values (4, 'hello1', 3.5), (5, 'world1', 4.5)");
            testCopyExport(stmt2, test2);

            CopyExportRunnable stmt3 = () -> runAndFetchCopyExportID("copy test_table to 'output13" + File.separator + "dir1" + File.separator + "dir2' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test3 = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output13" + File.separator + "dir1" + File.separator + "dir2.parquet" + "\t1\tfinished\n");
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("path\tdiskSizeHuman\n" +
                                        "output13.parquet\t952.0 B\n" +
                                        "output13" + File.separator + "dir1" + File.separator + "dir2.parquet\t952.0 B\n" +
                                        "output14.parquet\t905.0 B\n");
                    });
            testCopyExport(stmt3, test3);

            CopyExportRunnable stmt4 = () -> runAndFetchCopyExportID("copy test_table to 'output15" + File.separator + "dir1" + File.separator + "dir2' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test4 = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output15" + File.separator + "dir1" + File.separator + "dir2.parquet" + "\t1\tfinished\n");
                        assertQuery("select path, diskSizeHuman from export_files() order by path")
                                .noLeakCheck()
                                .returns("path\tdiskSizeHuman\n" +
                                        "output13.parquet\t952.0 B\n" +
                                        "output13" + File.separator + "dir1" + File.separator + "dir2.parquet\t952.0 B\n" +
                                        "output14.parquet\t905.0 B\n" +
                                        "output15" + File.separator + "dir1" + File.separator + "dir2.parquet\t952.0 B\n");
                    });
            testCopyExport(stmt4, test4);
        });
    }

    @Test
    public void testCopyWithSameDirsParallel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");
            execute("create table test_table1 (x int, y string, z double)");
            execute("insert into test_table1 values (1, 'hello', 1.5), (2, 'world', 2.5)");
            execute("create table test_table2 (x int, y string, z double)");
            execute("insert into test_table2 values (1, 'hello', 1.5), (2, 'world', 2.5), (4, 'hello1', 3.5), (5, 'world1', 4.5)");
            execute("create table test_table3 (x int, y string, z double)");
            execute("insert into test_table3 values (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy test_table to 'output13' with format parquet ", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table1 to 'output14' with format parquet ", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table2 to 'output13" + File.separator + "dir1" + File.separator + "dir2" + "' with format parquet ", sqlExecutionContext);
                runAndFetchCopyExportID("copy test_table3 to 'output15" + File.separator + "dir1" + File.separator + "dir2' with format parquet ", sqlExecutionContext);
            };

            CopyExportRunnable test4 = () ->
                    assertEventually(() -> assertQuery("select path, diskSizeHuman from export_files() order by path")
                            .noLeakCheck()
                            .returns("path\tdiskSizeHuman\n" +
                                    "output13.parquet\t905.0 B\n" +
                                    "output13" + File.separator + "dir1" + File.separator + "dir2.parquet\t953.0 B\n" +
                                    "output14.parquet\t906.0 B\n" +
                                    "output15" + File.separator + "dir1" + File.separator + "dir2.parquet\t906.0 B\n"));
            testCopyExport(stmt, test4, true, 4);
        });
    }

    @Test
    public void testCopyWithSameOutput() throws Exception {
        AtomicBoolean pause = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (pause.get() && Utf8s.containsAscii(to, "output8")) {
                    while (pause.get()) {
                        Os.sleep(100);
                    }
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table select x, x::string as y FROM long_sequence(100_000)");
            drainWalQueue();
            pause.set(true);

            Callable<Exception> callback = () -> {
                try {
                    runAndFetchCopyExportID("copy (select y from test_table) to 'output8' with format parquet statistics_enabled true", sqlExecutionContext);
                } catch (SqlException e) {
                    CharSequence contains = "duplicate export path: output8";
                    TestUtils.assertContains(e.getMessage(), contains);
                    LOG.info().$("asserted that duplicate export failed: [message=").$(e.getFlyweightMessage()).$(", contains=").$(contains).I$();
                    return e;
                }
                return new UnsupportedOperationException();
            };

            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy (select x from test_table) to 'output8' with format parquet statistics_enabled true", sqlExecutionContext);
                try {
                    // Wait for the first export to be active before attempting the second
                    waitForActiveExport();
                } finally {
                    pause.set(false);
                }
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output8.parquet" + "\t1\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output8" + ".parquet') LIMIT 1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("""
                                        x
                                        1
                                        """);
                    });

            testCopyExport(stmt, test, callback);
        });
    }

    @Test
    public void testCopyWithSameSql() throws Exception {
        AtomicBoolean pause = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (pause.get() && Utf8s.containsAscii(to, "output8")) {
                    while (pause.get()) {
                        Os.sleep(100);
                    }
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table select x, x::string as y FROM long_sequence(100_000)");
            pause.set(true);

            Callable<Exception> callback = () -> {
                try {
                    runAndFetchCopyExportID("copy test_table to 'output9' with format parquet statistics_enabled true", sqlExecutionContext);
                } catch (SqlException e) {
                    CharSequence contains = "duplicate sql statement: test_table";
                    TestUtils.assertContains(e.getMessage(), contains);
                    LOG.info().$("asserted that duplicate export failed: [message=").$(e.getFlyweightMessage()).$(", contains=").$(contains).I$();
                    return e;
                }
                return new UnsupportedOperationException();
            };

            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy test_table to 'output8' with format parquet statistics_enabled true", sqlExecutionContext);
                try {
                    waitForActiveExport();
                } finally {
                    pause.set(false);
                }
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output8.parquet" + "\t1\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output8" + ".parquet') LIMIT 1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\t1\n");
                    });

            testCopyExport(stmt, test, callback);
        });
    }

    @Test
    public void testCopyWithSameTable() throws Exception {
        AtomicBoolean pause = new AtomicBoolean();
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int rename(LPSZ from, LPSZ to) {
                if (pause.get() && Utf8s.containsAscii(to, "output8")) {
                    while (pause.get()) {
                        Os.sleep(100);
                    }
                }
                return super.rename(from, to);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");
            pause.set(true);
            runAndFetchCopyExportID("copy test_table to 'output8' with format parquet statistics_enabled true", sqlExecutionContext);
            try {
                runAndFetchCopyExportID("copy test_table to 'output9' with format parquet statistics_enabled true", sqlExecutionContext);
                Assert.fail("Expected failure due to ongoing export to same sql statement");
            } catch (SqlException e) {
                TestUtils.assertContains(e.getMessage(), "duplicate sql statement: test_table");
            } finally {
                pause.set(false);
            }

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output8.parquet" + "\t1\tfinished\n");
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output8" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(() -> {
            }, test);
        });
    }

    @Test
    public void testCopyWithSizeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y LONG)");
            execute("INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)");

            assertQuery("COPY test_table TO 'output12' WITH FORMAT PARQUET size_limit 1000")
                    .fails(50, "size limit is not yet supported");
        });
    }

    @Test
    public void testCopyWithStatisticsDisabled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet statistics_enabled false", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "test_table.parquet" + "\t1\tfinished\n");
                        // Verify data with statistics disabled
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "test_table" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithStatisticsEnabled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output8' with format parquet statistics_enabled true", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output8.parquet" + "\t1\tfinished\n");
                        // Verify data with statistics enabled
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output8" + ".parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .returns("x\ty\n1\ttest\n");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCreateTableWithParquetPrefixDenied() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("create table \"" + configuration.getParquetExportTableNamePrefix() + "tbl\" (x int);");
                Assert.fail();
            } catch (SqlException ex) {
                TestUtils.assertContains(ex.getFlyweightMessage(), "table name cannot start with reserved prefix");
            }
        });
    }

    @Test
    public void testExportDisabled() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_SQL_COPY_EXPORT_ROOT, "");
            assertQuery("copy test_table to 'output' with format parquet")
                    .fails(28, "COPY TO is disabled ['cairo.sql.copy.export.root' is not set?]");
        });
    }

    @Test
    public void testParquetExportDoesNotCommitPerRow() throws Exception {
        // Regression test: Parquet export was committing after every row due to
        // batchSize defaulting to 0 instead of -1 in CreateTableOperationImpl constructor.
        // This caused massive performance degradation - 31 million commits for 31 million rows.
        assertMemoryLeak(() -> {
            final int rowCount = 10000;

            // Get initial commit count
            TableWriterMetrics writerMetrics = engine.getMetrics().tableWriterMetrics();
            long commitsBefore = writerMetrics.getCommitCount();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "copy (select * from generate_series('2025-01-01', '2025-01-01T02:46:39', '1s')) to 'batch_test' with format parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "batch_test.parquet" + "\t1\tfinished\n");

                        // Verify the file was created with correct row count
                        assertQuery("select count() from read_parquet('" + exportRoot + File.separator + "batch_test.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count\n" + rowCount + "\n");
                    });

            testCopyExport(stmt, test);

            // Check commits after export
            long commitsAfter = writerMetrics.getCommitCount();
            long newCommits = commitsAfter - commitsBefore;

            // The bug would cause ~10000 commits (one per row).
            // With the fix, we should have at most a handful of commits (for the temp table creation,
            // status log updates, etc.) - certainly less than 100.
            Assert.assertTrue(
                    "Expected fewer than 100 commits for " + rowCount + " rows, but got " + newCommits +
                            ". This suggests per-row commits are happening (regression of batchSize bug).",
                    newCommits < 100
            );
        });
    }

    @Test
    public void testParquetExportWithLargeSymbolTable() throws Exception {
        // Test export with 10k distinct symbols using a projection with computed columns.
        // The streaming export path handles this without creating a temp table.
        final int symbolCount = 10_000;

        assertMemoryLeak(() -> {
            execute("CREATE TABLE symbol_test (ts TIMESTAMP, sym SYMBOL, value LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL;");

            String insertQuery = "INSERT BATCH 10000 INTO symbol_test SELECT " +
                    "x::TIMESTAMP AS ts, " +
                    "'sym' || (x % " + symbolCount + ")::STRING AS sym, " +
                    "x AS value " +
                    "FROM long_sequence(" + symbolCount + ")";
            execute(insertQuery);
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (SELECT ts, sym, value + 1 AS adjusted_value FROM symbol_test ORDER BY ts) TO 'symbol_export' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "symbol_export.parquet\t1\tfinished\n");
                        // Verify distinct symbol count matches
                        assertQuery("SELECT count_distinct(sym) FROM read_parquet('" + exportRoot + File.separator + "symbol_export.parquet')")
                                .noLeakCheck()
                                .expectSize()
                                .noRandomAccess()
                                .returns("count_distinct\n" + symbolCount + "\n");
                        // Verify computed column
                        assertQuery("SELECT adjusted_value FROM read_parquet('" + exportRoot + File.separator + "symbol_export.parquet') WHERE sym = 'sym1'")
                                .noLeakCheck()
                                .returns("""
                                        adjusted_value
                                        2
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testRetypedExportKeepsTempTableForOtherPlans() throws Exception {
        // A plan whose timestamp no table scan guarantees - an aggregation, LATEST ON, a window function, a join, a
        // sort on another timestamp, a TIMESTAMP(col) claim - could hand the cursor exporter a NULL or an unordered
        // timestamp for the file's designated timestamp, so its export of a retyped column keeps the temp table.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createRetypedTables(walMode);
                execute("CREATE TABLE d (s SYMBOL, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("INSERT INTO d VALUES ('a', 100, '2024-01-01T00:30:00.000000Z'), ('b', 200, '2024-01-02T01:30:00.000000Z')");
                drainWalQueue();
                final RetypedExportShape[] shapes = {
                        new RetypedExportShape("SELECT ts, sum(x) sx FROM %s SAMPLE BY 1d", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s = 'a' LATEST ON ts PARTITION BY s", "ts"),
                        new RetypedExportShape("SELECT x, ts, row_number() OVER () rn FROM %s", "ts"),
                        // NULLs of other_ts sort last
                        new RetypedExportShape("SELECT * FROM %s ORDER BY other_ts DESC", null),
                        new RetypedExportShape("SELECT * FROM (SELECT x, other_ts, ts FROM %s WHERE other_ts IS NOT NULL ORDER BY other_ts) timestamp(other_ts)", "other_ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s IN (SELECT s FROM k) ORDER BY ts DESC", null),
                        new RetypedExportShape("SELECT a.ts, a.s, first(b.y) fy FROM %s a WINDOW JOIN d b RANGE BETWEEN 1 hour PRECEDING AND 1 hour FOLLOWING", "ts"),
                        // the multi-key ORDER BY with a negative LIMIT reads the frames backward and each frame's index
                        // forward, so the scan promises no order of its own
                        new RetypedExportShape("SELECT * FROM %s WHERE s = 'a' ORDER BY ts, x LIMIT -3", "ts"),
                        new RetypedExportShape("SELECT * FROM vrl", "SELECT * FROM nr LATEST ON ts PARTITION BY s", "ts", true),
                        // the rows of these come in no order the query fixes
                        new RetypedExportShape("SELECT ts, s, sum(x) sx FROM r SAMPLE BY 1d", "SELECT ts, s, sum(x) sx FROM nr SAMPLE BY 1d", null, false),
                        new RetypedExportShape("SELECT * FROM r LATEST ON ts PARTITION BY s", "SELECT * FROM nr LATEST ON ts PARTITION BY s", null, false),
                        new RetypedExportShape(
                                "SELECT s, x, other_ts, avg(x) OVER (PARTITION BY s) a FROM r timestamp(other_ts) WHERE s IN ('a', 'b')",
                                "SELECT s, x, other_ts, avg(x) OVER (PARTITION BY s) a FROM nr timestamp(other_ts) WHERE s IN ('a', 'b')",
                                null,
                                false
                        ),
                };
                for (int i = 0; i < shapes.length; i++) {
                    final RetypedExportShape shape = shapes[i];
                    final String fileName = "temp_table_shape_" + i;
                    try {
                        assertCopyMaterializesRows(shape.query(), fileName);
                        if (shape.isOrdered()) {
                            assertParquetMatchesControl(shape.controlQuery(), fileName, shape.timestampColumn());
                        } else {
                            assertParquetMatchesQueryInAnyOrder(shape.controlQuery(), parquetPath(fileName));
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError(walMode + ": " + shape.query(), e);
                    }
                }
                // the last three rows of s = 'a' by ts and x
                assertParquetRows("temp_table_shape_7", "ts", """
                        s\ts2\tx\tother_ts\tts
                        a\tp\t5\t2023-06-09T00:00:00.000000Z\t2024-01-02T00:00:00.000000Z
                        a\tq\t7\t2023-06-07T00:00:00.000000Z\t2024-01-02T02:00:00.000000Z
                        a\tp\t9\t2023-06-10T00:00:00.000000Z\t2024-01-03T00:00:00.000000Z
                        """);
                // the temp table keeps the NULLs of other_ts as NULLs
                for (String fileName : new String[]{"temp_table_shape_3", "temp_table_shape_11"}) {
                    assertQuery("SELECT count() FROM read_parquet('" + parquetPath(fileName) + "') WHERE other_ts IS NULL")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("count\n2\n");
                }
                // A TIMESTAMP(col) claim of another column on the scan itself: the temp table rejects its NULLs, as it
                // did before.
                assertExportMode("SELECT * FROM r timestamp(other_ts)", ParquetExportMode.TEMP_TABLE);
                execute("DROP TABLE d");
                dropRetypedTables();
            }
        });
    }

    @Test
    public void testRetypedExportStreamsTableScanShapes() throws Exception {
        // A column retyped after its partition went to Parquet is converted only in the row accessors, which the cursor
        // exporter reads every value through. An export whose plan has no timestamp, or reads the table's designated
        // timestamp through a scan, filter, projection, LIMIT or a sort on it, streams its rows. The file designates
        // the timestamp when the rows ascend and none when they descend, and holds what the same query returns over nr,
        // which turned x to LONG while every partition was native.
        assertMemoryLeak(() -> {
            for (String walMode : new String[]{"WAL", "BYPASS WAL"}) {
                createRetypedTables(walMode);
                execute("CREATE TABLE d (s SYMBOL, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
                execute("INSERT INTO d VALUES ('a', 100, '2024-01-01T00:30:00.000000Z'), ('b', 200, '2024-01-02T01:30:00.000000Z')");
                drainWalQueue();
                execute("CREATE VIEW vrlx AS (SELECT s, x FROM r LATEST ON ts PARTITION BY s)");
                execute("CREATE VIEW vrw AS (SELECT x, row_number() OVER () rn FROM r)");
                drainWalAndViewQueues();
                bindVariableService.clear();
                bindVariableService.setTimestamp(0, 1_704_153_600_000_000L); // 2024-01-02T00:00:00Z
                bindVariableService.setStr(1, "b");
                // query over r, the same query over nr, the designated timestamp of the file, and whether the query
                // fixes the order of its rows
                final RetypedExportShape[] shapes = {
                        new RetypedExportShape("SELECT * FROM %s", "ts"),
                        new RetypedExportShape("SELECT x AS a, ts AS b FROM %s", "b"),
                        new RetypedExportShape("SELECT x + 1 AS y, ts FROM %s", "ts"),
                        new RetypedExportShape("SELECT * FROM (SELECT * FROM %s WHERE x > 1)", "ts"),
                        new RetypedExportShape("WITH q AS (SELECT * FROM %s WHERE x > 1) SELECT * FROM q", "ts"),
                        new RetypedExportShape("SELECT * FROM %s ORDER BY ts DESC LIMIT 5", null),
                        new RetypedExportShape("SELECT * FROM %s LIMIT -3", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE ts >= $1", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s = $2", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s IN ('a', 'b')", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s != 'a'", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s IN (SELECT s FROM k)", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s LIKE 'a%'", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE s = 'a' AND x > 1 ORDER BY ts DESC", null),
                        new RetypedExportShape("SELECT * FROM %s WHERE s IN ('a', 'b') ORDER BY ts DESC", null),
                        new RetypedExportShape("SELECT * FROM %s ORDER BY ts, x", "ts"),
                        new RetypedExportShape("SELECT * FROM %s WHERE x > 1 ORDER BY ts DESC, x LIMIT 3", null),
                        new RetypedExportShape("SELECT s, sum(x) sx FROM %s ORDER BY s", null),
                        new RetypedExportShape("SELECT x, row_number() OVER () rn FROM %s", null),
                        new RetypedExportShape("SELECT * FROM vr", "SELECT * FROM nr", "ts", true),
                        new RetypedExportShape("SELECT * FROM vrv", "SELECT * FROM nr", "ts", true),
                        new RetypedExportShape("SELECT * FROM vrf", "SELECT * FROM nr WHERE x > 1", "ts", true),
                        new RetypedExportShape("SELECT * FROM vrw", "SELECT x, row_number() OVER () rn FROM nr", null, true),
                        // the rows of these come in no order the query fixes
                        new RetypedExportShape("SELECT DISTINCT s, x FROM r", "SELECT DISTINCT s, x FROM nr", null, false),
                        new RetypedExportShape("SELECT s, x FROM r LATEST ON ts PARTITION BY s", "SELECT s, x FROM nr LATEST ON ts PARTITION BY s", null, false),
                        new RetypedExportShape("SELECT * FROM vrlx", "SELECT s, x FROM nr LATEST ON ts PARTITION BY s", null, false),
                        new RetypedExportShape(
                                "SELECT h.offset, count() c FROM r a HORIZON JOIN d b LIST (0s, 1s) AS h",
                                "SELECT h.offset, count() c FROM nr a HORIZON JOIN d b LIST (0s, 1s) AS h",
                                null,
                                false
                        ),
                        // a set operation reports no retyped column, so it streamed before too
                        new RetypedExportShape("SELECT * FROM %s UNION ALL SELECT * FROM %s", null),
                };
                for (int i = 0; i < shapes.length; i++) {
                    final RetypedExportShape shape = shapes[i];
                    final String fileName = "streamed_shape_" + i;
                    try {
                        assertCopyStreamsRows(shape.query(), fileName);
                        if (shape.isOrdered()) {
                            assertParquetMatchesControl(shape.controlQuery(), fileName, shape.timestampColumn());
                        } else {
                            assertParquetMatchesQueryInAnyOrder(shape.controlQuery(), parquetPath(fileName));
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError(walMode + ": " + shape.query(), e);
                    }
                }
                bindVariableService.clear();
                execute("DROP VIEW vrlx");
                execute("DROP VIEW vrw");
                execute("DROP TABLE d");
                dropRetypedTables();
            }

            // A column of the view that the query passes through keeps its PARQUET(...) setting, and a computed one
            // takes its type's default: the export over streamed_v, whose table has the same settings and rows but no
            // Parquet partition, is the oracle for the encodings, and the same query over nc_v for the rows.
            createTableWithParquetSettingsAndConvertedColumn("converted", true, ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE);
            createConvertedColumnControl("nc");
            createTableWithParquetSettings("streamed");
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_153_600_000_000L); // 2024-01-02T00:00:00Z
            final String[] templates = {
                    "SELECT metric AS m, ts FROM %s",
                    "SELECT metric + 1 AS mp, ts FROM %s",
                    "SELECT * FROM (SELECT * FROM %s)",
                    "WITH w AS (SELECT * FROM %s) SELECT * FROM w",
                    "SELECT * FROM %s ORDER BY ts DESC LIMIT 5",
                    "SELECT * FROM %s LIMIT -3",
                    "SELECT * FROM %s WHERE ts >= $1",
                    "SELECT metric, count() c FROM %s ORDER BY metric",
                    "SELECT DISTINCT metric FROM %s ORDER BY metric",
                    // LATEST ON over the view keeps no designated timestamp
                    "SELECT * FROM %s LATEST ON ts PARTITION BY metric",
            };
            for (int i = 0; i < templates.length; i++) {
                final String fileName = "streamed_settings_" + i;
                try {
                    assertCopyStreamsLikeStreamedTable(templates[i], fileName);
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, templates[i].replace("%s", "nc_v"), "SELECT * FROM read_parquet('" + parquetPath(fileName) + "')", LOG, true);
                } catch (AssertionError e) {
                    throw new AssertionError(templates[i], e);
                }
            }
            bindVariableService.clear();
        });
    }

    @Test
    public void testReverseTimestampOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x TIMESTAMP);");
            execute("insert into test_table values (0), (2), (5);");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy (test_table ORDER BY x DESC) to 'output1' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        exportRoot + File.separator + "output1.parquet" + "\t1\tfinished\n");
                        // Verify exported data can be read back and matches original
                        assertQuery("select * from read_parquet('" + exportRoot + File.separator + "output1" + ".parquet')")
                                .noLeakCheck()
                                .returnsOnce("""
                                        x
                                        1970-01-01T00:00:00.000005Z
                                        1970-01-01T00:00:00.000002Z
                                        1970-01-01T00:00:00.000000Z
                                        """);
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testRowGroupColumnHasEncodingRejectsInvalidEncodingId() throws Exception {
        // The Rust JNI helper translates an integer encoding id into a parquet
        // thrift encoding byte. Anything outside the supported set must surface
        // as a CairoException whose message names the rejected id and includes
        // the row-group/column context added by the JNI wrapper.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE dict_jni_src (
                        metric INT PARQUET(RLE_DICTIONARY),
                        ts TIMESTAMP
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("""
                    INSERT INTO dict_jni_src VALUES
                        (10, '2020-01-01T00:00:00.000000Z'),
                        (20, '2020-01-01T01:00:00.000000Z')
                    """);

            final String parquetPath = exportRoot + File.separator + "dict_jni_output.parquet";
            final String query = "SELECT metric, ts FROM dict_jni_src";

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID(
                            "COPY (" + query + ") TO 'dict_jni_output' WITH FORMAT parquet",
                            sqlExecutionContext
                    );

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT export_path, num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("export_path\tnum_exported_files\tstatus\n" +
                                        parquetPath + "\t1\tfinished\n");

                        final FilesFacade ff = configuration.getFilesFacade();
                        final Log log = LogFactory.getLog(CopyExportTest.class);
                        long fd = -1;
                        long addr = 0;
                        long fileSize = 0;
                        try (Path path = new Path(); ParquetFileDecoder decoder = new ParquetFileDecoder()) {
                            path.of(parquetPath).$();
                            fd = TableUtils.openRO(ff, path.$(), log);
                            fileSize = ff.length(fd);
                            addr = TableUtils.mapRO(ff, fd, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                            decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);

                            try {
                                decoder.rowGroupColumnHasEncoding(0, 0, 99);
                                Assert.fail("expected CairoException for invalid encoding id");
                            } catch (CairoException ex) {
                                final String msg = ex.getMessage();
                                Assert.assertTrue(
                                        "error should mention 'unsupported parquet encoding id', got: " + msg,
                                        msg.contains("unsupported parquet encoding id")
                                );
                                Assert.assertTrue(
                                        "error should include the rejected id 99, got: " + msg,
                                        msg.contains("99")
                                );
                                Assert.assertTrue(
                                        "error should include the row-group/column context, got: " + msg,
                                        msg.contains("row group 0") && msg.contains("column 0")
                                );
                            }
                        } finally {
                            if (addr != 0) {
                                ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
                            }
                            if (fd != -1) {
                                ff.close(fd);
                            }
                        }
                    });

            testCopyExport(stmt, test);
        });
    }

    /**
     * A composite partition keeps dead rows between (or before) its live pieces, so its live row count is
     * no longer its file extent. {@code COPY <table> TO ... FORMAT PARQUET} used to take the
     * {@code TABLE_READER} path, which hands {@code PartitionEncoder} file rows {@code [0, liveRows)} -
     * the right NUMBER of rows read from the wrong PLACE, so the export silently held stale data while
     * its row count still matched. The export now routes a composite table through a SELECT, whose
     * page-frame cursors walk the partition piece by piece.
     */
    @Test
    public void testCopyToParquetExportsCompositePartitionCorrectly() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            execute("CREATE TABLE ct AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(2000))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2024-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO ct VALUES (999_999, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();
            // Lands only inside 2024-01-01 and makes it composite.
            execute("INSERT INTO ct SELECT x::INT + 70_000 i," +
                    " timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L) ts FROM long_sequence(40)");
            drainWalQueue();

            try (TableReader reader = engine.getReader(engine.verifyTableName("ct"))) {
                Assert.assertTrue("2024-01-01 must be composite for this test to mean anything",
                        reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("COPY ct TO 'composite_export' WITH FORMAT parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                                .noLeakCheck()
                                .expectSize()
                                .returns("status\nfinished\n");
                        assertParquetMatchesQuery(
                                "SELECT * FROM ct WHERE ts IN '2024-01-01'",
                                exportRoot + File.separator + "composite_export" + File.separator + "2024-01-01.parquet"
                        );
                        assertParquetMatchesQuery(
                                "SELECT * FROM ct WHERE ts IN '2024-01-03'",
                                exportRoot + File.separator + "composite_export" + File.separator + "2024-01-03.parquet"
                        );
                    });

            testCopyExport(stmt, test);
        });
    }

    /**
     * The enqueue-time composite check in {@code CopyExportFactory} runs on a transient reader that the
     * factory closes straight away, while the export worker opens its own, later reader. When WAL apply turns
     * a partition composite in between, that partition no longer spans file rows {@code [0, liveRows)}, so the
     * partition-by-partition encoder reads the right NUMBER of rows from the wrong PLACE: the export reports
     * {@code finished} while dropping every relocated live row. The worker re-resolves the export mode on the
     * very reader it reads from, so the transition routes the export through the materializing path.
     */
    @Test
    public void testCopyToParquetExportsPartitionTurnedCompositeAfterEnqueue() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            execute("TRUNCATE TABLE IF EXISTS \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            execute("CREATE TABLE ct AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(2000))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            // A later, plain day, so 2024-01-01 is never the active partition and the backfill is O3.
            execute("INSERT INTO ct VALUES (999_999, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();
            try (TableReader reader = engine.getReader(engine.verifyTableName("ct"))) {
                Assert.assertFalse("the table must be plain when COPY is enqueued",
                        reader.getTxFile().hasCompositePartitions());
            }
            engine.releaseAllReaders();

            // No export job runs yet, so the task waits in the queue. That orders the transition below
            // strictly after the enqueue-time mode decision and strictly before the worker's own snapshot.
            runAndFetchCopyExportID("COPY ct TO 'transition_export' WITH FORMAT parquet", sqlExecutionContext);

            // Lands only inside 2024-01-01 and makes it composite.
            execute("INSERT INTO ct SELECT x::INT + 70_000 i," +
                    " timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L) ts FROM long_sequence(40)");
            drainWalQueue();
            try (TableReader reader = engine.getReader(engine.verifyTableName("ct"))) {
                Assert.assertTrue("2024-01-01 must be composite for this test to mean anything",
                        reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                Assert.assertTrue("the queued export must be picked up", job.run());
            }

            assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");
            assertParquetMatchesQuery(
                    "SELECT * FROM ct WHERE ts IN '2024-01-01'",
                    exportRoot + File.separator + "transition_export" + File.separator + "2024-01-01.parquet"
            );
            assertParquetMatchesQuery(
                    "SELECT * FROM ct WHERE ts IN '2024-01-03'",
                    exportRoot + File.separator + "transition_export" + File.separator + "2024-01-03.parquet"
            );
        });
    }

    /**
     * A table name that needs quoting, exported through the temp table {@code CopyExportFactory} builds at
     * enqueue time. The source table name doubles as the select text, and an unquoted {@code src ct} parses as
     * table {@code src} aliased {@code ct}: with a table named {@code src} around, the export then silently
     * materializes THAT table and reports success over another table's data.
     */
    @Test
    public void testCopyToParquetExportsCompositeTableWhoseNameNeedsQuoting() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            execute("TRUNCATE TABLE IF EXISTS \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            createDecoyTableAndCompositeTableNeedingQuotes();

            runAndFetchCopyExportID("COPY \"src ct\" TO 'quoted_enqueue_export' WITH FORMAT parquet", sqlExecutionContext);
            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                Assert.assertTrue("the queued export must be picked up", job.run());
            }

            assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");
            assertParquetMatchesQuery(
                    "SELECT * FROM \"src ct\" WHERE ts IN '2024-01-01'",
                    exportRoot + File.separator + "quoted_enqueue_export" + File.separator + "2024-01-01.parquet"
            );
            assertParquetMatchesQuery(
                    "SELECT * FROM \"src ct\" WHERE ts IN '2024-01-03'",
                    exportRoot + File.separator + "quoted_enqueue_export" + File.separator + "2024-01-03.parquet"
            );
        });
    }

    /**
     * The same quoting, on the worker-side materializing fallback: the table is still plain when COPY is
     * enqueued, so the select text is built by {@code SQLSerialParquetExporter.materializingOp()} instead. Both
     * routes have to spell the source table the same way, or this one exports the decoy table.
     */
    @Test
    public void testCopyToParquetExportsTableTurnedCompositeAfterEnqueueWhoseNameNeedsQuoting() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        assertMemoryLeak(() -> {
            execute("TRUNCATE TABLE IF EXISTS \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            createDecoyTable();
            createTableNeedingQuotes();

            // Enqueued while plain, so the export is a direct table read and carries no create operation.
            runAndFetchCopyExportID("COPY \"src ct\" TO 'quoted_transition_export' WITH FORMAT parquet", sqlExecutionContext);

            makeFirstPartitionComposite();

            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                Assert.assertTrue("the queued export must be picked up", job.run());
            }

            assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("status\nfinished\n");
            assertParquetMatchesQuery(
                    "SELECT * FROM \"src ct\" WHERE ts IN '2024-01-01'",
                    exportRoot + File.separator + "quoted_transition_export" + File.separator + "2024-01-01.parquet"
            );
            assertParquetMatchesQuery(
                    "SELECT * FROM \"src ct\" WHERE ts IN '2024-01-03'",
                    exportRoot + File.separator + "quoted_transition_export" + File.separator + "2024-01-03.parquet"
            );
        });
    }

    /**
     * The worker-side materializing fallback keeps the USER's table name on the task - only the temp table name
     * the exporter hands to {@code dropTempTable()} names the temp table. A temp table creation that fails
     * before the token is resolved leaves {@code dropTempTable()} to resolve the name it was given, so a
     * signature that resolved {@code task.getTableName()} instead would resolve, and drop, the very table the
     * user asked to export.
     */
    @Test
    public void testCopyToParquetKeepsSourceTableWhenTempTableCreationFails() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");

        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public int mkdirs(Path path, int mode) {
                // The temp table's own directory, which is named after the "copy.<id>" table. The export's
                // temp FILE directory is "tmp_<id>", so only the table creation fails here.
                if (Utf8s.containsAscii(path, File.separator + "copy.")) {
                    return -1;
                }
                return super.mkdirs(path, mode);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("TRUNCATE TABLE IF EXISTS \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            execute("CREATE TABLE ct AS (SELECT x::INT i," +
                    " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(2000))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO ct VALUES (999_999, '2024-01-03T00:00:00.000000Z')");
            drainWalQueue();

            // Enqueued while plain, so the export is a direct table read: the task carries the user's own
            // table name and the worker is the one that builds the temp table.
            runAndFetchCopyExportID("COPY ct TO 'drop_guard_export' WITH FORMAT parquet", sqlExecutionContext);

            execute("INSERT INTO ct SELECT x::INT + 70_000 i," +
                    " timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L) ts FROM long_sequence(40)");
            drainWalQueue();
            try (TableReader reader = engine.getReader(engine.verifyTableName("ct"))) {
                Assert.assertTrue("2024-01-01 must be composite for this test to mean anything",
                        reader.getTxFile().isPartitionComposite(0));
            }
            engine.releaseAllReaders();

            try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
                Assert.assertTrue("the queued export must be picked up", job.run());
            }

            assertQuery("SELECT phase, status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            phase	status
                            wait_to_run	started
                            wait_to_run	finished
                            populating_data_to_temp_table	started
                            dropping_temp_table	started
                            dropping_temp_table	finished
                            populating_data_to_temp_table	failed
                            """);

            Assert.assertNotNull("the user's own table must survive a failed temp table creation",
                    engine.getTableTokenIfExists("ct"));
            assertQuery("SELECT count() FROM ct").noLeakCheck().noRandomAccess().expectSize().returns("count\n2041\n");
        });
    }

    // Runs the COPY of a factory that the test holds and asserts that it fails.
    private static void assertCopyFails(RecordCursorFactory factory, int expectedPosition, String expectedMessage) {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.fail("COPY must fail with: " + expectedMessage);
        } catch (SqlException e) {
            TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
            Assert.assertEquals(expectedPosition, e.getPosition());
        }
    }

    private static Thread createJobThread(Job job, CountDownLatch workCount, AtomicBoolean stop, int workerId) {
        return new Thread(() -> {
            try {
                while (!stop.get()) {
                    if (job.run()) {
                        break;
                    }
                    Os.sleep(10);
                }
            } finally {
                Path.clearThreadLocals();
                workCount.countDown();
            }
        });
    }

    // Helper methods for copy export operations
    // The export removes its temp directory when it fails, and when it streams its file.
    private static void assertNoTempDirLeft() {
        final File[] tmpDirs = new File(exportRoot).listFiles(f -> f.isDirectory() && f.getName().startsWith("tmp_"));
        Assert.assertTrue("temp directory must be removed", tmpDirs == null || tmpDirs.length == 0);
    }

    // The export removes the temp directory named after it when it streams its file or fails.
    private static void assertNoTempDirOf(String exportId) {
        Assert.assertFalse("temp directory of the export must be removed", new File(exportRoot, "tmp_" + exportId).exists());
    }

    private static void awaitExportsDrained() {
        while (engine.getCopyExportContext().getActiveExportId() != -1) {
            Os.pause();
        }
    }

    private static void fetchCopyExportID(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertNotNull(cursor.getRecord().getStrA(0));
        }
    }

    private static void runAndFetchCopyExportID(String copySql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (
                RecordCursorFactory factory = select(copySql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            CharSequence value = cursor.getRecord().getStrA(0);
            Assert.assertNotNull(value);
        }
    }

    // Runs the COPY on this thread and returns the id of the export it queued, which no job runs yet.
    private static String queueCopy(String copySql) throws SqlException {
        try (RecordCursorFactory factory = select(copySql)) {
            return runCopy(factory);
        }
    }

    // Runs the COPY of a factory that the test holds and returns the id of the export it queued.
    private static String runCopy(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            final String exportId = cursor.getRecord().getStrA(0).toString();
            Assert.assertFalse(cursor.hasNext());
            return exportId;
        }
    }

    // Runs the queued export on this thread to its end.
    private static void runExportJob() throws Exception {
        try (CopyExportRequestJob job = new CopyExportRequestJob(engine)) {
            Assert.assertTrue("the queued export must be picked up", job.run());
        }
    }

    private static int runExportJobCountingCompiles() throws Exception {
        return runExportJobCountingCompiles(_ -> {
        });
    }

    /**
     * Runs the queued export on this thread to its end and returns how many SQL compilers it borrowed from the engine's
     * pool: one for each compile of its select, and one for the CREATE TABLE AS SELECT of a temp table. The export
     * hands a compiler back before it opens the plan that the compiler built, and onCompile runs right then, with the
     * number of the compile, from 1. The compilers that onCompile borrows do not count.
     */
    private static int runExportJobCountingCompiles(CompileAction onCompile) throws Exception {
        final long exportThreadId = Thread.currentThread().threadId();
        final AtomicInteger compileCount = new AtomicInteger();
        final AtomicBoolean isActionRunning = new AtomicBoolean();
        final AtomicReference<Throwable> actionFailure = new AtomicReference<>();
        engine.getSqlCompilerPool().setPoolListener((_, thread, _, event, _, _) -> {
            if (event == PoolListener.EV_RETURN && thread == exportThreadId && isActionRunning.compareAndSet(false, true)) {
                try {
                    onCompile.run(compileCount.incrementAndGet());
                } catch (Throwable th) {
                    // The pool calls the listener before it frees the compiler's slot, which a throw would leak.
                    actionFailure.compareAndSet(null, th);
                } finally {
                    isActionRunning.set(false);
                }
            }
        });
        try {
            runExportJob();
        } finally {
            engine.getSqlCompilerPool().setPoolListener(null);
        }
        final Throwable failure = actionFailure.get();
        if (failure != null) {
            throw new AssertionError("the action on a compile of the export failed", failure);
        }
        return compileCount.get();
    }

    // The lines of a query's printed rows, header first and the rows sorted.
    private static String sortedLines(CharSequence printedRows) {
        final String[] lines = printedRows.toString().split("\n");
        Arrays.sort(lines, 1, lines.length);
        return String.join("\n", lines);
    }

    private synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test, boolean blocked, int waitCount) throws Exception {
        testCopyExport(statement, test, blocked, waitCount, null);
    }

    private synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test, boolean blocked, int waitCount, Callable<Exception> callback) throws Exception {
        CountDownLatch processed = new CountDownLatch(waitCount);
        execute("truncate table if exists \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
        ObjList<CopyExportRequestJob> jobs = new ObjList<>();
        ObjList<Thread> threads = new ObjList<>();
        AtomicBoolean stop = new AtomicBoolean();
        try {
            for (int i = 0; i < 4; i++) {
                CopyExportRequestJob copyRequestJob = new CopyExportRequestJob(engine, callback);
                jobs.add(copyRequestJob);
                Thread processingThread = createJobThread(copyRequestJob, processed, stop, i);
                threads.add(processingThread);
                processingThread.start();
            }
            statement.run();
            if (blocked) {
                processed.await();
            }
            drainWalQueue(engine);
            test.run();
        } finally {
            stop.set(true);
            for (int i = 0, n = threads.size(); i < n; i++) {
                threads.getQuick(i).join();
            }
            Misc.freeObjList(jobs);
        }
    }

    private synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test, Callable<Exception> callback) throws Exception {
        testCopyExport(statement, test, true, 1, callback);
    }

    private synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test) throws Exception {
        testCopyExport(statement, test, true, 1);
    }

    // bf holds 0 and 2, so its min and max cannot rule out bf = 1, and only a bloom filter lets read_parquet() skip
    // the row group.
    private void assertBloomFilterOnBf(String parquetPath, boolean hasBloomFilter) throws Exception {
        ParquetRowGroupFilter.resetRowGroupsSkipped();
        assertQuery("SELECT bf FROM read_parquet('" + parquetPath + "') WHERE bf = 1")
                .noLeakCheck()
                .returns("bf\n");
        if (hasBloomFilter) {
            Assert.assertTrue("a bloom filter on bf must skip the row group", ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
        } else {
            Assert.assertEquals("no bloom filter on bf, no row group to skip", 0, ParquetRowGroupFilter.getRowGroupsSkipped());
        }
    }

    // Runs the COPY of ct that the test holds to the end of its export, one file per day.
    private void assertCompositeCopyFactoryExports(RecordCursorFactory factory, String exportDirName, String expectedLastDayRows) throws Exception {
        final String exportId = runCopy(factory);
        runExportJob();
        assertExportSucceeded(exportId);
        final String exportDir = exportRoot + File.separator + exportDirName + File.separator;
        assertParquetMatchesQuery("SELECT * FROM ct WHERE ts IN '2024-01-01'", exportDir + "2024-01-01.parquet");
        assertQuery("SELECT * FROM read_parquet('" + exportDir + "2024-01-03.parquet')")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expectedLastDayRows);
    }

    private void assertCompositeExportKeepsParquetSetting(String exportDirName) throws Exception {
        for (String day : new String[]{"2024-01-01", "2024-01-03"}) {
            final String parquetPath = exportRoot + File.separator + exportDirName + File.separator + day + ".parquet";
            assertParquetMatchesQuery("SELECT * FROM \"src ct\" WHERE ts IN '" + day + "'", parquetPath);
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, configuration.getFilesFacade(), 0);
        }
    }

    // Runs the COPY of t that the test holds to the end of its export and returns the export id.
    private String assertCopyFactoryExports(RecordCursorFactory factory, String expectedRows) throws Exception {
        final String exportId = runCopy(factory);
        runExportJob();
        assertExportSucceeded(exportId);
        assertQuery("SELECT * FROM read_parquet('" + parquetPath("again") + "')")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expectedRows);
        return exportId;
    }

    private void assertCopyFactoryRunWhileQueuedIsDuplicate(String copySql, String copySource) throws Exception {
        try (RecordCursorFactory factory = select(copySql)) {
            final String firstId = runCopy(factory);
            try {
                // No export job runs yet, so the first export is still queued.
                assertCopyFails(factory, 0, "duplicate sql statement: " + copySource + " [id=" + firstId + ']');
            } finally {
                runExportJob();
            }
            assertExportSucceeded(firstId);
            final String nextId = assertCopyFactoryExports(factory, """
                    x\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-01T01:00:00.000000Z
                    """);
            Assert.assertNotEquals(firstId, nextId);
        }
    }

    private void assertCopyFactoryRunsAgain(String walMode, String copySql, boolean isExportedThroughSelect) throws Exception {
        createTableToCopyAgain(walMode);
        try (RecordCursorFactory factory = select(copySql)) {
            final String firstId = assertCopyFactoryExports(factory, """
                    x\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-01T01:00:00.000000Z
                    """);
            execute("INSERT INTO t VALUES (3, '2024-01-01T02:00:00.000000Z')");
            drainWalQueue();
            final String secondId = assertCopyFactoryExports(factory, """
                    x\tts
                    1\t2024-01-01T00:00:00.000000Z
                    2\t2024-01-01T01:00:00.000000Z
                    3\t2024-01-01T02:00:00.000000Z
                    """);
            Assert.assertNotEquals(firstId, secondId);
            assertExportLogTableName(secondId, isExportedThroughSelect ? "copy." + secondId : "t");
        }
        execute("DROP TABLE t");
    }

    // Runs the COPY of the query on this thread and asserts that its export materializes the rows through a temp table
    // that COPY picked when it queued the export.
    private void assertCopyMaterializesRows(String query, String fileName) throws Exception {
        assertExportMode(query, ParquetExportMode.TEMP_TABLE);
        final String exportId = queueCopy("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET");
        runExportJob();
        assertExportLog(exportId, TEMP_TABLE_EXPORT_LOG);
        Assert.assertNull("the temp table must be dropped", engine.getTableTokenIfExists("copy." + exportId));
    }

    private void assertCopyOfFusedJoinGroupByAfterBuildTableGrowth(
            String keyType,
            String keyExpr,
            String joinType,
            String fileName
    ) throws Exception {
        // f holds 10 rows and d holds 5, so the fused GROUP BY builds d and presizes its key table
        // by d's rows. COPY compiles the SELECT when it queues the export. The callback grows d to
        // 21 rows, past twice f's rows, after the export is queued and before it starts, so the
        // queued factory asks for a recompile when the export opens it, and the export compiles
        // the SELECT again, once: the new compile reads the new sizes and passes the check.
        execute("CREATE TABLE f (k " + keyType + ", v LONG)");
        execute("CREATE TABLE d (k " + keyType + ", g SYMBOL)");
        execute("INSERT INTO f SELECT " + keyExpr + ", x FROM long_sequence(10)");
        final String insertIntoD = "INSERT INTO d SELECT " + keyExpr + ", ('g' || (x % 2))::SYMBOL FROM long_sequence(";
        execute(insertIntoD + "5)");
        final String selectSql = "SELECT d.g, sum(f.v) FROM f " + joinType + "JOIN d ON f.k = d.k GROUP BY d.g";
        assertQuery(selectSql)
                .noLeakCheck()
                .assertsPlanContaining("Async Hash Join Group By", "inputSwapped: false");

        final String expected = """
                g\tsum
                g0\t106
                g1\t121
                """;
        CopyExportRunnable statement = () -> runAndFetchCopyExportID(
                "COPY (" + selectSql + ") TO '" + fileName + "' WITH FORMAT PARQUET",
                sqlExecutionContext
        );
        // The callback records the thread that runs the export after its own insert, so that the
        // listener counts only the compiles of the export.
        final AtomicLong exportThreadId = new AtomicLong(-1);
        final AtomicInteger exportCompileCount = new AtomicInteger();
        Callable<Exception> callback = () -> {
            execute(insertIntoD + "16)");
            exportThreadId.set(Thread.currentThread().threadId());
            return null;
        };
        CopyExportRunnable test = () -> assertEventually(() -> {
            assertQuery("SELECT num_exported_files, status FROM \"sys.copy_export_log\" LIMIT -1")
                    .noLeakCheck()
                    .expectSize()
                    .returns("num_exported_files\tstatus\n1\tfinished\n");
            assertQuery("SELECT * FROM read_parquet('" + exportRoot + File.separator + fileName + ".parquet') ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns(expected);
        });
        engine.getSqlCompilerPool().setPoolListener((_, thread, _, event, _, _) -> {
            if (event == PoolListener.EV_RETURN && thread == exportThreadId.get()) {
                exportCompileCount.incrementAndGet();
            }
        });
        try {
            testCopyExport(statement, test, callback);
        } finally {
            engine.getSqlCompilerPool().setPoolListener(null);
        }
        Assert.assertEquals("the export compiles the SELECT again once", 1, exportCompileCount.get());

        // A new compile of the same SELECT returns the rows that the export wrote.
        assertQuery(selectSql + " ORDER BY g")
                .noLeakCheck()
                .expectSize()
                .returns(expected);
    }

    /**
     * Exports the query with %s naming the converted_v view, which streams its rows through the cursor exporter, and
     * the query with %s naming the streamed_v view, whose table has the same settings and rows but neither a Parquet
     * partition nor a retype. Both files must hold the rows of the query and encode their columns alike.
     */
    private void assertCopyStreamsLikeStreamedTable(String queryTemplate, String fileName) throws Exception {
        final String query = queryTemplate.replace("%s", "converted_v");
        assertCopyStreamsRows(query, fileName);
        copyToParquet("COPY (" + queryTemplate.replace("%s", "streamed_v") + ") TO '" + fileName + "_streamed' WITH FORMAT PARQUET");
        assertParquetMatchesQuery(query, parquetPath(fileName));
        ParquetTestUtils.assertColumnEncodingsMatch(parquetPath(fileName + "_streamed"), parquetPath(fileName), configuration.getFilesFacade());
    }

    // Runs the COPY of the query on this thread and asserts that its export streams the rows through the cursor
    // exporter: it logs the phases of a streaming export, creates no temp table and leaves no temp directory behind.
    private void assertCopyStreamsRows(String query, String fileName) throws Exception {
        assertCopyStreamsRows(query, fileName, "");
    }

    private void assertCopyStreamsRows(String query, String fileName, String copyOptions) throws Exception {
        assertExportMode(query, ParquetExportMode.CURSOR_BASED);
        final String exportId = queueCopy("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + copyOptions);
        runExportJob();
        assertExportLog(exportId, STREAMING_EXPORT_LOG);
        Assert.assertNull("an export that streams creates no temp table", engine.getTableTokenIfExists("copy." + exportId));
        assertNoTempDirOf(exportId);
    }

    // The first column of the query passes metric of the converted_v view through, or computes from it. The export goes
    // through a temp table.
    private void assertCopyThroughTempTableEncodesFirstColumn(
            String query,
            String copyOptions,
            String fileName,
            boolean isDictionaryEncodingExpected
    ) throws Exception {
        copyThroughTempTable("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + copyOptions);
        final String parquetPath = parquetPath(fileName);
        assertParquetMatchesQuery(query, parquetPath);
        if (isDictionaryEncodingExpected) {
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, configuration.getFilesFacade(), 0);
        } else {
            ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath, configuration.getFilesFacade(), 0);
        }
    }

    /**
     * Exports the query with %s naming the converted_v view through a temp table, and the query with %s naming the
     * streamed_v view, whose table has the same settings and rows but neither a Parquet partition nor a retype, on a
     * streaming path. Both files must encode their columns alike.
     */
    private void assertCopyThroughTempTableEncodesLikeStreaming(String queryTemplate, String copyOptions, String fileName) throws Exception {
        final String query = queryTemplate.replace("%s", "converted_v");
        final String streamedQuery = queryTemplate.replace("%s", "streamed_v");
        try (RecordCursorFactory factory = select(streamedQuery)) {
            Assert.assertNotEquals(
                    streamedQuery,
                    ParquetExportMode.TEMP_TABLE,
                    ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)
            );
        }
        copyThroughTempTable("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + copyOptions);
        copyToParquet("COPY (" + streamedQuery + ") TO '" + fileName + "_streamed' WITH FORMAT PARQUET");
        assertParquetMatchesQuery(query, parquetPath(fileName));
        ParquetTestUtils.assertColumnEncodingsMatch(parquetPath(fileName + "_streamed"), parquetPath(fileName), configuration.getFilesFacade());
    }

    // Exports the query through CURSOR_BASED and asserts the rows of the file and that the listed key columns use the
    // RLE_DICTIONARY encoding of their PARQUET(...) setting.
    private void assertCursorExportEncodesKeys(String query, String fileName, int... dictionaryEncodedColumns) throws Exception {
        assertExportMode(query, ParquetExportMode.CURSOR_BASED);
        copyToParquet("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET");
        assertParquetMatchesQueryInAnyOrder(query, parquetPath(fileName));
        if (dictionaryEncodedColumns.length > 0) {
            ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath(fileName), configuration.getFilesFacade(), dictionaryEncodedColumns);
        }
    }

    private void assertCursorExportOfSymbols(String query, String fileName, int[] keptColumns, int[] stringColumns) throws Exception {
        assertCursorExportOfSymbols(query, fileName, keptColumns, stringColumns, "");
    }

    // Exports the query through CURSOR_BASED and asserts the rows of the file, in any order, and the type of each of its
    // SYMBOL columns: kept as SYMBOL, or written as STRING.
    private void assertCursorExportOfSymbols(
            String query,
            String fileName,
            int[] keptColumns,
            int[] stringColumns,
            String copyOptions
    ) throws Exception {
        try {
            assertExportMode(query, ParquetExportMode.CURSOR_BASED);
            copyToParquet("COPY (" + query + ") TO '" + fileName + "' WITH FORMAT PARQUET" + copyOptions);
            final String parquetPath = parquetPath(fileName);
            assertParquetMatchesQueryInAnyOrder(query, parquetPath);
            if (keptColumns.length > 0) {
                assertSymbolColumnsKept(parquetPath, keptColumns);
            }
            if (stringColumns.length > 0) {
                assertSymbolColumnsWrittenAsString(parquetPath, stringColumns);
            }
        } catch (AssertionError e) {
            throw new AssertionError(query, e);
        }
    }

    private void assertEventually(TestUtils.EventualCode assertion) throws Exception {
        TestUtils.assertEventually(assertion, 5, exceptionTypesToCatch);
    }

    // Asserts the message of the one row with which the export logged its failure or cancellation while it streamed
    // its file; the expected status tells the two apart.
    private void assertExportFailure(String exportId, String expectedStatus, String expectedMessage) throws Exception {
        assertQuery("SELECT phase, message FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"" +
                " WHERE id = '" + exportId + "' AND status = '" + expectedStatus + "'")
                .noLeakCheck()
                .returns("phase\tmessage\nstream_sending_data\t" + expectedMessage + '\n');
    }

    private void assertExportFinished() throws Exception {
        assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                .noLeakCheck()
                .expectSize()
                .returns("status\nfinished\n");
    }

    // Asserts the phases and statuses that the export logged, in their order.
    private void assertExportLog(String exportId, String expectedPhases) throws Exception {
        assertQuery("SELECT phase, status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "'")
                .noLeakCheck()
                .returns(expectedPhases);
    }

    // An export through a select logs the temp table named after the export, an export that reads the table logs the
    // table.
    private void assertExportLogTableName(String exportId, String expectedTableName) throws Exception {
        assertQuery("SELECT DISTINCT table_name FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "'")
                .noLeakCheck()
                .expectSize()
                .returns("table_name\n" + expectedTableName + '\n');
    }

    private void assertExportMode(String query, ParquetExportMode expectedMode) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            Assert.assertEquals(query, expectedMode, ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext));
        }
    }

    private void assertExportSucceeded(String exportId) throws Exception {
        assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "' AND phase = 'success'")
                .noLeakCheck()
                .returns("status\nfinished\n");
    }

    private void assertExportedFileCount(String exportId, int expectedFileCount) throws Exception {
        assertQuery("SELECT num_exported_files FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" WHERE id = '" + exportId + "' AND phase = 'success'")
                .noLeakCheck()
                .returns("num_exported_files\n" + expectedFileCount + '\n');
    }

    private void assertFirstPartitionOfCtComposite() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("ct"))) {
            Assert.assertTrue("2024-01-01 must be composite for this test to mean anything",
                    reader.getTxFile().isPartitionComposite(0));
        }
        engine.releaseAllReaders();
    }

    private void assertMyViewExportKeepsIdentity(String source, String fileName) throws Exception {
        CopyExportRunnable stmt = () ->
                runAndFetchCopyExportID("copy " + source + " to '" + fileName + "' with format parquet", sqlExecutionContext);

        CopyExportRunnable test = () ->
                assertEventually(() -> {
                    assertQuery("SELECT status FROM \"sys.copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("status\nfinished\n");
                    // sym belongs to the view; the decoy table has a "decoy" column instead, so exporting
                    // it fails this read outright.
                    assertQuery("select sym, v from read_parquet('" + exportRoot + File.separator + fileName + ".parquet')")
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    sym\tv
                                    BBB\t5.0
                                    """);
                });

        testCopyExport(stmt, test);
    }

    private void assertParquetExportOfConvertedColumn(String srcType, String srcValueExpr, String dstType, String expectedReadback) throws Exception {
        assertParquetExportOfConvertedColumn(srcType, srcValueExpr, dstType, expectedReadback, false, ConvertedColumnExportSource.TABLE);
    }

    private void assertParquetExportOfConvertedColumn(
            String srcType,
            String srcValueExpr,
            String dstType,
            String expectedReadback,
            ConvertedColumnExportSource exportSource
    ) throws Exception {
        assertParquetExportOfConvertedColumn(srcType, srcValueExpr, dstType, expectedReadback, false, exportSource);
    }

    private void assertParquetExportOfConvertedColumn(
            String srcType,
            String srcValueExpr,
            String dstType,
            String expectedReadback,
            boolean isMetadataCacheClearedBeforeCompile
    ) throws Exception {
        assertParquetExportOfConvertedColumn(srcType, srcValueExpr, dstType, expectedReadback, isMetadataCacheClearedBeforeCompile, ConvertedColumnExportSource.TABLE);
    }

    private void assertParquetExportOfConvertedColumn(
            String srcType,
            String srcValueExpr,
            String dstType,
            String expectedReadback,
            boolean isMetadataCacheClearedBeforeCompile,
            ConvertedColumnExportSource exportSource
    ) throws Exception {
        // Seed a WAL table, convert its single partition to parquet, then ALTER the column
        // type so the parquet partition carries a lazily-converted column. COPY (SELECT *)
        // detects the pending conversion and streams the rows that the conversion gives. Read the exported
        // file back via read_parquet (which reads the file as-is, with no lazy conversion) and
        // check that s holds the post-ALTER type and the converted values. Most values print
        // the same in either type (3 as INT or STRING), so the type check is what catches a
        // file that kept the pre-ALTER type.
        execute("CREATE TABLE pt (s " + srcType + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO pt SELECT " + srcValueExpr + " AS s, " +
                "timestamp_sequence('2024-01-01T00:00:00.000000Z', 600_000_000) AS ts FROM long_sequence(6)");
        drainWalQueue();
        if (exportSource == ConvertedColumnExportSource.VIEW_CREATED_BEFORE_TYPE_CHANGE) {
            execute("CREATE VIEW pt_v AS (SELECT * FROM pt)");
            drainWalAndViewQueues();
        }
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE pt ALTER COLUMN s TYPE " + dstType);
        drainWalAndViewQueues();
        if (exportSource == ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE) {
            execute("CREATE VIEW pt_v AS (SELECT * FROM pt)");
            drainWalAndViewQueues();
        }
        final String exportedSql = exportSource == ConvertedColumnExportSource.TABLE ? "SELECT * FROM pt" : "SELECT * FROM pt_v";

        final TableToken tableToken = engine.getTableTokenIfExists("pt");
        CopyExportRunnable stmt = () -> {
            if (isMetadataCacheClearedBeforeCompile) {
                try (MetadataCacheWriter metadataRW = engine.getMetadataCache().writeLock()) {
                    metadataRW.clearCache();
                }
                try (MetadataCacheReader metadataRO = engine.getMetadataCache().readLock()) {
                    Assert.assertNull(metadataRO.getTable(tableToken));
                }
            }
            runAndFetchCopyExportID("COPY (" + exportedSql + ") TO 'converted_col_export' WITH FORMAT parquet", sqlExecutionContext);
        };

        CopyExportRunnable test = () ->
                assertEventually(() -> {
                    assertQuery("SELECT status FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1")
                            .noLeakCheck()
                            .expectSize()
                            .returns("status\nfinished\n");
                    assertQuery("SELECT s, ts FROM read_parquet('" + exportRoot + File.separator + "converted_col_export.parquet')")
                            .noLeakCheck()
                            .expectSize()
                            .timestamp("ts")
                            .columnType(0, ColumnType.typeOf(dstType))
                            .returns(expectedReadback);
                });

        testCopyExport(stmt, test);
    }

    // Asserts that the file holds the rows of the control query in their order, reading SYMBOL, VARCHAR and STRING
    // alike, and that read_parquet() designates the given column of the file as its timestamp, or none for null.
    private void assertParquetMatchesControl(String controlQuery, String fileName, @Nullable String timestampColumn) throws Exception {
        final String parquetQuery = "SELECT * FROM read_parquet('" + parquetPath(fileName) + "')";
        try {
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, controlQuery, parquetQuery, LOG, true);
            try (RecordCursorFactory factory = select(parquetQuery)) {
                final int expectedTimestampIndex = timestampColumn == null ? -1 : factory.getMetadata().getColumnIndex(timestampColumn);
                Assert.assertEquals("designated timestamp", expectedTimestampIndex, factory.getMetadata().getTimestampIndex());
            }
        } catch (AssertionError e) {
            throw new AssertionError(fileName + " vs " + controlQuery, e);
        }
    }

    // metric, c and total are the first three columns of an export of a table that createTableWithParquetSettings*()
    // creates.
    private void assertParquetSettingsKept(String parquetPath) {
        final FilesFacade ff = configuration.getFilesFacade();
        ParquetTestUtils.assertColumnsUseDictionaryEncoding(parquetPath, ff, 0);
        ParquetTestUtils.assertColumnsUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 2);
        // c has no setting, so it keeps the LONG default, PLAIN
        ParquetTestUtils.assertColumnsDoNotUseDictionaryEncoding(parquetPath, ff, 1);
        ParquetTestUtils.assertColumnsDoNotUseEncoding(parquetPath, ff, ParquetEncoding.ENCODING_DELTA_BINARY_PACKED, 1);
    }

    /**
     * Gives pt and nt a column per conversion, in its source type, after a first row, so that the column has a column
     * top, and retypes each column to its target type: pt after its single day went to Parquet, so that a read converts
     * the values lazily, in the row accessors, and nt while its partition is native, so that the ALTER rewrites the
     * column eagerly. Asserts that the export of pt streams its rows and writes the rows of nt, each column in the type
     * that the ALTER gave nt's.
     */
    private void assertStreamedExportOfConversions(ConvertedColumnConversion[] conversions) throws Exception {
        int rowCount = 0;
        for (ConvertedColumnConversion conversion : conversions) {
            rowCount = Math.max(rowCount, conversion.values().length);
        }
        final StringSink columnNames = new StringSink();
        final StringSink rows = new StringSink();
        for (int i = 0; i < conversions.length; i++) {
            columnNames.put('c').put(i).put(", ");
        }
        columnNames.put("ts");
        for (int row = 0; row < rowCount; row++) {
            rows.put(row > 0 ? ", (" : "(");
            for (ConvertedColumnConversion conversion : conversions) {
                rows.put(row < conversion.values().length ? conversion.values()[row] : "NULL").put(", ");
            }
            rows.put('\'').put("2024-01-01T00:00:01.000000Z").put("'::TIMESTAMP + ").put(row).put(" * 1_000_000)");
        }
        for (String tableName : new String[]{"nt", "pt"}) {
            execute("CREATE TABLE " + tableName + " (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + tableName + " (ts) VALUES ('2024-01-01T00:00:00.000000Z')");
            for (int i = 0; i < conversions.length; i++) {
                execute("ALTER TABLE " + tableName + " ADD COLUMN c" + i + " " + conversions[i].sourceType());
            }
            execute("INSERT INTO " + tableName + " (" + columnNames + ") VALUES " + rows);
        }
        drainWalQueue();
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        for (String tableName : new String[]{"nt", "pt"}) {
            for (int i = 0; i < conversions.length; i++) {
                execute("ALTER TABLE " + tableName + " ALTER COLUMN c" + i + " TYPE " + conversions[i].targetType());
            }
        }
        drainWalQueue();

        assertCopyStreamsRows("SELECT * FROM pt", "conversions");
        assertParquetMatchesControl("SELECT * FROM nt", "conversions", "ts");
        try (RecordCursorFactory factory = select("SELECT * FROM nt")) {
            final RecordMetadata metadata = factory.getMetadata();
            for (int i = 0; i < conversions.length; i++) {
                final int columnIndex = metadata.getColumnIndex("c" + i);
                try {
                    ParquetTestUtils.assertColumnTypes(parquetPath("conversions"), configuration.getFilesFacade(), metadata.getColumnType(columnIndex), columnIndex);
                } catch (AssertionError e) {
                    throw new AssertionError(conversions[i].sourceType() + " to " + conversions[i].targetType(), e);
                }
            }
        }
    }

    // Asserts that the plan of the query over r names the factory, that the export of the query streams its rows, and
    // that the file holds the rows of the same query over nr, with its timestamp column REQUIRED.
    private void assertStreamedExportOfMode(String template, @Nullable String timestampColumn, String fileName, String planFragment) throws Exception {
        assertStreamedExportOfMode(template, timestampColumn, fileName, planFragment, "r", "nr");
    }

    private void assertStreamedExportOfMode(
            String template,
            @Nullable String timestampColumn,
            String fileName,
            String planFragment,
            String tableName,
            String controlTableName
    ) throws Exception {
        final String query = template.replace("%s", tableName);
        try {
            assertQuery(query).noLeakCheck().assertsPlanContaining(planFragment);
            assertCopyStreamsRows(query, fileName);
            assertParquetMatchesControl(template.replace("%s", controlTableName), fileName, timestampColumn);
            try (RecordCursorFactory factory = select("SELECT * FROM read_parquet('" + parquetPath(fileName) + "')")) {
                final int timestampIndex = factory.getMetadata().getColumnIndex("ts");
                ParquetTestUtils.assertColumnMaxDefLevel(parquetPath(fileName), configuration.getFilesFacade(), timestampIndex, 0);
            }
        } catch (AssertionError e) {
            throw new AssertionError(query, e);
        }
    }

    // Runs the COPY on this thread to its end and asserts that its export went through a temp table.
    private void copyThroughTempTable(String copySql) throws Exception {
        final String exportId = queueCopy(copySql);
        runExportJob();
        assertExportLog(exportId, TEMP_TABLE_EXPORT_LOG);
    }

    // Runs the COPY on this thread to its end.
    private void copyToParquet(String copySql) throws Exception {
        runAndFetchCopyExportID(copySql, sqlExecutionContext);
        runExportJob();
        assertExportFinished();
    }

    /**
     * Creates the table {@code tableName}, and the view {@code <tableName>_v} over it, with the columns, PARQUET(...)
     * settings and rows of {@link #createTableWithParquetSettingsAndConvertedColumn}, which retypes c from INT to LONG
     * while every partition is native, so that the table converts c eagerly: the control of an export of the converted
     * column.
     */
    private void createConvertedColumnControl(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (metric INT PARQUET(RLE_DICTIONARY), c INT, total LONG PARQUET(DELTA_BINARY_PACKED, ZSTD(3)), ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName + " VALUES " + PARQUET_SETTINGS_TABLE_ROWS);
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " ALTER COLUMN c TYPE LONG");
        drainWalQueue();
        execute("CREATE VIEW " + tableName + "_v AS (SELECT * FROM " + tableName + ")");
        drainWalAndViewQueues();
    }

    /**
     * A table {@code src}, which an unquoted {@code src ct} select text resolves to - the export that takes
     * that text reports success over THIS table's rows. Its columns differ from {@code "src ct"}'s, so the
     * parquet comparison names the confusion rather than just failing on values.
     */
    private void createDecoyTable() throws Exception {
        execute("CREATE TABLE src AS (SELECT 'decoy' s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(3))" +
                " TIMESTAMP(ts) PARTITION BY DAY");
    }

    private void createDecoyTableAndCompositeTableNeedingQuotes() throws Exception {
        createDecoyTable();
        createTableNeedingQuotes();
        makeFirstPartitionComposite();
    }

    /**
     * Creates r, whose first day goes to Parquet before x turns from INT to LONG, so that the Parquet partition holds x
     * in its old type and a read converts it lazily; nr, which holds the same rows and turns x to LONG while every
     * partition is native, so that the table converts x eagerly and reads it as any LONG column; the views vr, vrf,
     * vrv and vrl over r; and k, which holds the symbols of s.
     */
    private void createRetypedTables(String walMode) throws Exception {
        for (String tableName : new String[]{"r", "nr"}) {
            execute("CREATE TABLE " + tableName + " (s SYMBOL INDEX, s2 SYMBOL, x INT, other_ts TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
            execute("INSERT INTO " + tableName + " VALUES " + RETYPED_TABLE_ROWS);
        }
        drainWalQueue();
        execute("ALTER TABLE r CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE r ALTER COLUMN x TYPE LONG");
        execute("ALTER TABLE nr ALTER COLUMN x TYPE LONG");
        drainWalQueue();
        execute("CREATE TABLE k (s SYMBOL)");
        execute("INSERT INTO k VALUES ('a'), ('b')");
        execute("CREATE VIEW vr AS (SELECT * FROM r)");
        execute("CREATE VIEW vrf AS (SELECT * FROM r WHERE x > 1)");
        execute("CREATE VIEW vrl AS (SELECT * FROM r LATEST ON ts PARTITION BY s)");
        drainWalAndViewQueues();
        execute("CREATE VIEW vrv AS (SELECT * FROM vr)");
        drainWalAndViewQueues();
    }

    private void createTableNeedingQuotes() throws Exception {
        execute("CREATE TABLE \"src ct\" AS (SELECT x::INT i," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts FROM long_sequence(2000))" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        // A later, plain day, so 2024-01-01 is never the active partition and the backfill is O3.
        execute("INSERT INTO \"src ct\" VALUES (999_999, '2024-01-03T00:00:00.000000Z')");
        drainWalQueue();
    }

    // t holds one day of rows, so every route of an export of t writes the single file again.parquet.
    private void createTableToCopyAgain(String walMode) throws Exception {
        execute("CREATE TABLE t (x INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO t VALUES
                (1, '2024-01-01T00:00:00.000000Z'),
                (2, '2024-01-01T01:00:00.000000Z')
                """);
        drainWalQueue();
    }

    /**
     * Creates the table {@code tableName}, and the view {@code <tableName>_v} over it, with the columns, PARQUET(...)
     * settings and rows of {@link #createTableWithParquetSettingsAndConvertedColumn} but without a Parquet partition
     * or a retype, so that its exports without PARTITION_BY stream.
     */
    private void createTableWithParquetSettings(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (metric INT PARQUET(RLE_DICTIONARY), c LONG, total LONG PARQUET(DELTA_BINARY_PACKED, ZSTD(3)), ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + tableName + " VALUES " + PARQUET_SETTINGS_TABLE_ROWS);
        drainWalQueue();
        execute("CREATE VIEW " + tableName + "_v AS (SELECT * FROM " + tableName + ")");
        drainWalAndViewQueues();
    }

    /**
     * Creates the table {@code tableName} with PARQUET(...) settings on metric and total, converts its first day to
     * Parquet and retypes c from INT to LONG after that. The Parquet partition then holds a lazily converted column,
     * so that an export of the table, or of the view {@code <tableName>_v} that {@code source} asks for, reads every
     * value through the converting row accessors, never the raw page frames.
     */
    private void createTableWithParquetSettingsAndConvertedColumn(
            String tableName,
            boolean isWal,
            ConvertedColumnExportSource source
    ) throws Exception {
        execute("CREATE TABLE " + tableName + " (metric INT PARQUET(RLE_DICTIONARY), c INT, total LONG PARQUET(DELTA_BINARY_PACKED, ZSTD(3)), ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        execute("INSERT INTO " + tableName + " VALUES " + PARQUET_SETTINGS_TABLE_ROWS);
        drainWalQueue();
        if (source == ConvertedColumnExportSource.VIEW_CREATED_BEFORE_TYPE_CHANGE) {
            execute("CREATE VIEW " + tableName + "_v AS (SELECT * FROM " + tableName + ")");
            drainWalAndViewQueues();
        }
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " ALTER COLUMN c TYPE LONG");
        drainWalAndViewQueues();
        if (source == ConvertedColumnExportSource.VIEW_CREATED_AFTER_TYPE_CHANGE) {
            execute("CREATE VIEW " + tableName + "_v AS (SELECT * FROM " + tableName + ")");
            drainWalAndViewQueues();
        }
    }

    private void dropRetypedTables() throws Exception {
        execute("DROP VIEW vrv");
        execute("DROP VIEW vr");
        execute("DROP VIEW vrf");
        execute("DROP VIEW vrl");
        execute("DROP TABLE k");
        execute("DROP TABLE r");
        execute("DROP TABLE nr");
        drainWalAndViewQueues();
    }

    private void makeFirstPartitionComposite() throws Exception {
        // Lands only inside 2024-01-01 and makes it composite.
        execute("INSERT INTO \"src ct\" SELECT x::INT + 70_000 i," +
                " timestamp_sequence('2024-01-01T00:00:00.500000Z', 1_000_000L) ts FROM long_sequence(40)");
        drainWalQueue();
        try (TableReader reader = engine.getReader(engine.verifyTableName("src ct"))) {
            Assert.assertTrue("2024-01-01 must be composite for this test to mean anything",
                    reader.getTxFile().isPartitionComposite(0));
        }
        engine.releaseAllReaders();
    }

    private void assertParquetMatchesQuery(String query, String parquetPath) throws Exception {
        StringSink expectedSink = new StringSink();
        StringSink actualSink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, expectedSink);
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT * FROM read_parquet('" + parquetPath + "')", actualSink);
        TestUtils.assertEquals(expectedSink, actualSink);
    }

    // Compares the rows of the file with the rows of the query in any order, for a query whose row order no ORDER BY
    // fixes.
    private void assertParquetMatchesQueryInAnyOrder(String query, String parquetPath) throws Exception {
        final StringSink expectedSink = new StringSink();
        final StringSink actualSink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, query, expectedSink);
        TestUtils.printSql(engine, sqlExecutionContext, "SELECT * FROM read_parquet('" + parquetPath + "')", actualSink);
        Assert.assertEquals(query, sortedLines(expectedSink), sortedLines(actualSink));
    }

    // Asserts the rows of the file in the order that the export wrote them, and the designated timestamp of the file.
    private void assertParquetRows(String fileName, @Nullable String timestampColumn, String expectedRows) throws Exception {
        final QueryAssertion assertion = assertQuery("SELECT * FROM read_parquet('" + parquetPath(fileName) + "')")
                .noLeakCheck()
                .expectSize();
        if (timestampColumn != null) {
            assertion.timestamp(timestampColumn);
        }
        assertion.returns(expectedRows);
    }

    private void assertQueriesPrintAlike(String expectedQuery, String actualQuery) throws Exception {
        final StringSink expectedSink = new StringSink();
        final StringSink actualSink = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext, expectedQuery, expectedSink);
        TestUtils.printSql(engine, sqlExecutionContext, actualQuery, actualSink);
        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private void assertReadParquetColumnTypes(String parquetPath, int expectedType, int... columnIndexes) throws SqlException {
        try (RecordCursorFactory factory = select("SELECT * FROM read_parquet('" + parquetPath + "')")) {
            for (int columnIndex : columnIndexes) {
                Assert.assertEquals(
                        "read_parquet() type of column " + columnIndex,
                        ColumnType.nameOf(expectedType),
                        ColumnType.nameOf(factory.getMetadata().getColumnType(columnIndex))
                );
            }
        }
    }

    // Asserts that the file holds the listed columns as SYMBOL, which read_parquet() reads as VARCHAR, as the DIRECT
    // export of a table's SYMBOL column writes it.
    private void assertSymbolColumnsKept(String parquetPath, int... columnIndexes) throws SqlException {
        ParquetTestUtils.assertColumnTypes(parquetPath, configuration.getFilesFacade(), ColumnType.SYMBOL, columnIndexes);
        assertReadParquetColumnTypes(parquetPath, ColumnType.VARCHAR, columnIndexes);
    }

    // Asserts that the file holds the listed columns as STRING, which read_parquet() reads as STRING.
    private void assertSymbolColumnsWrittenAsString(String parquetPath, int... columnIndexes) throws SqlException {
        ParquetTestUtils.assertColumnTypes(parquetPath, configuration.getFilesFacade(), ColumnType.STRING, columnIndexes);
        assertReadParquetColumnTypes(parquetPath, ColumnType.STRING, columnIndexes);
    }

    /**
     * Creates the table fl, whose k is indexed, so that a SAMPLE BY ALIGN TO FIRST OBSERVATION of first() and last()
     * filtered on k runs the legacy SampleByFirstLast factory. Its late column is added after the first rows.
     */
    private void createFirstLastTable() throws Exception {
        execute("CREATE TABLE fl (k SYMBOL INDEX, s SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO fl VALUES
                    ('x', 'a', 1, '2024-01-01T00:00:00.000000Z'),
                    ('y', 'b', 2, '2024-01-01T00:10:00.000000Z'),
                    ('x', NULL, 3, '2024-01-01T00:20:00.000000Z'),
                    ('x', 'c', 4, '2024-01-01T01:30:00.000000Z'),
                    ('x', 'd', 5, '2024-01-02T00:00:00.000000Z'),
                    ('y', 'e', 6, '2024-01-02T00:30:00.000000Z'),
                    ('x', NULL, 7, '2024-01-02T03:00:00.000000Z')
                """);
        execute("ALTER TABLE fl ADD COLUMN late SYMBOL");
        execute("INSERT INTO fl VALUES ('x', 'f', 8, '2024-01-02T04:00:00.000000Z', 'L1'), ('x', 'g', 9, '2024-01-02T04:30:00.000000Z', NULL)");
    }

    private void createStaleExportTable(String walMode) throws Exception {
        createStaleExportTable(walMode, false);
    }

    // t holds x falling as ts rises, so that an export ordered by x writes its rows in the reverse of their storage
    // order, one per day, and s holds a symbol twice.
    private void createStaleExportTable(String walMode, boolean isSymbolIndexed) throws Exception {
        execute("CREATE TABLE t (x INT, s SYMBOL" + (isSymbolIndexed ? " INDEX" : "") + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO t VALUES
                (3, 'a', '2024-01-01T00:00:00.000000Z'),
                (2, 'b', '2024-01-02T00:00:00.000000Z'),
                (1, 'a', '2024-01-03T00:00:00.000000Z')
                """);
        drainWalQueue();
    }

    /**
     * Creates sym_src, whose sym holds NULL, an empty string, symbols of 2-, 3- and 4-byte UTF-8 characters and
     * {@link #LONG_SYMBOL}, has a PARQUET(...) setting and one out-of-order row, and sym_dim, whose symbol map holds no
     * NULL and only some of sym's symbols.
     */
    private void createSymbolExportTables(String walMode) throws Exception {
        execute("CREATE TABLE sym_src (sym SYMBOL PARQUET(RLE_DICTIONARY, ZSTD(3)), k SYMBOL, v INT PARQUET(RLE_DICTIONARY), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("INSERT INTO sym_src VALUES " + """
                ('a', 'k1', 1, '2024-01-01T00:00:00.000000Z'),
                (NULL, 'k2', 2, '2024-01-01T01:00:00.000000Z'),
                ('', 'k1', 3, '2024-01-01T02:00:00.000000Z'),
                ('é', 'k2', 4, '2024-01-02T00:00:00.000000Z'),
                ('Ж', NULL, 5, '2024-01-02T01:00:00.000000Z'),
                ('€', 'k1', 6, '2024-01-02T02:00:00.000000Z'),
                ('😀', 'k2', 7, '2024-01-03T00:00:00.000000Z'),
                """ + "('" + LONG_SYMBOL + "', 'k1', 8, '2024-01-03T01:00:00.000000Z')");
        execute("INSERT INTO sym_src VALUES ('a', 'k2', 9, '2024-01-01T00:30:00.000000Z')");
        execute("CREATE TABLE sym_dim (sym SYMBOL, w INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY " + walMode);
        execute("""
                INSERT INTO sym_dim VALUES
                    ('a', 10, '2024-01-01T00:00:00.000000Z'),
                    ('é', 40, '2024-01-02T00:00:00.000000Z'),
                    ('😀', 70, '2024-01-03T00:00:00.000000Z'),
                    ('zz', 99, '2024-01-03T01:00:00.000000Z')
                """);
        drainWalQueue();
    }

    private boolean exportFileExists(String fileName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).concat(fileName).put(".parquet").$();
            return ff.exists(path.$());
        }
    }

    // Runs the COPY on this thread, asserts that its export fails and returns the message of the failure.
    private String failedExportMessage(String copySql) throws Exception {
        runAndFetchCopyExportID(copySql, sqlExecutionContext);
        runExportJob();
        printSql("SELECT status, message FROM \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\" LIMIT -1");
        final String[] lines = sink.toString().split("\n");
        Assert.assertEquals(sink.toString(), 2, lines.length);
        Assert.assertTrue(sink.toString(), lines[1].startsWith("failed\t"));
        return lines[1].substring("failed\t".length());
    }

    private long parquetFileSize(String fileName) {
        try (Path path = new Path()) {
            path.of(parquetPath(fileName)).$();
            return configuration.getFilesFacade().length(path.$());
        }
    }

    private String parquetPath(String fileName) {
        return exportRoot + File.separator + fileName + ".parquet";
    }

    private void waitForActiveExport() throws Exception {
        // Wait for an export to be in the active state (running)
        TestUtils.assertEventually(() -> {
            long exportId = engine.getCopyExportContext().getActiveExportId();
            Assert.assertNotEquals("No active export found", -1L, exportId);
        }, 5, exceptionTypesToCatch);
    }

    // What a test does on the export's thread right after the export compiled its select again, before it opens the new
    // plan.
    @FunctionalInterface
    private interface CompileAction {
        void run(int compileCount) throws Exception;
    }

    private enum ConvertedColumnExportSource {
        TABLE,
        VIEW_CREATED_AFTER_TYPE_CHANGE,
        VIEW_CREATED_BEFORE_TYPE_CHANGE
    }

    // A column retyped over a Parquet partition, from the source type to the target type, holding the values that the
    // SQL expressions give in the source type.
    private record ConvertedColumnConversion(String sourceType, String targetType, String... values) {
    }

    // A COPY of a select, after the set-up, which COPY queues in the given mode. The change after COPY queued the export
    // makes the queued plan stale, and a fresh plan of the select after the change exports in the new mode, a recompiled
    // one in the recompiled mode. Also the copy_export_log rows of the export, the number of compiles its worker runs,
    // and the designated timestamp of its file.
    private record RecompiledExport(
            String query,
            @Nullable String setup,
            ParquetExportMode mode,
            String change,
            ParquetExportMode newMode,
            ParquetExportMode recompiledMode,
            String log,
            int compileCount,
            @Nullable String timestampColumn
    ) {
    }

    // A query over the retyped table r, the same query over nr, the designated timestamp of the file of its export, and
    // whether the query fixes the order of its rows. The template form replaces %s with r and nr.
    private record RetypedExportShape(String query, String controlQuery, @Nullable String timestampColumn,
                                      boolean isOrdered) {
        RetypedExportShape(String template, @Nullable String timestampColumn) {
            this(template.replace("%s", "r"), template.replace("%s", "nr"), timestampColumn, true);
        }
    }

    // A streaming export of the table that createStaleExportTable() creates: its query, its mode, the designated timestamp
    // of its file, the index of x in its file, which s follows, and its rows before and after the table gains the INT
    // column extra.
    private record StaleExportShape(
            String query,
            ParquetExportMode mode,
            @Nullable String timestampColumn,
            int xColumnIndex,
            String rows,
            String rowsWithExtra
    ) {
    }

    // A query that a CURSOR_BASED export runs, with the SYMBOL columns of its file that the export keeps as SYMBOL and
    // those that it writes as STRING.
    private record SymbolExportShape(String query, int[] keptColumns, int[] stringColumns) {
        SymbolExportShape(String query, int... keptColumns) {
            this(query, keptColumns, new int[0]);
        }

        static SymbolExportShape string(String query, int... stringColumns) {
            return new SymbolExportShape(query, new int[0], stringColumns);
        }
    }

    @FunctionalInterface
    public interface CopyExportRunnable {
        void run() throws Exception;
    }

    static {
        exceptionTypesToCatch.add(SqlException.class);
        exceptionTypesToCatch.add(AssertionError.class);
    }

}
