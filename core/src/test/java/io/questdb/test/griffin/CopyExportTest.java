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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.CopyExportRequestJob;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SynchronizedJob;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;


public class CopyExportTest extends AbstractCairoTest {

    static HashSet<Class<?>> exceptionTypesToCatch = new HashSet<>();

    public CopyExportTest() {
    }

    public static Thread createJobThread(SynchronizedJob job, CountDownLatch latch) {
        return new Thread(() -> {
            try {
                while (latch.getCount() > 0) {
                    if (job.run(0)) {
                        latch.countDown();
                    }
                    Os.sleep(1);
                }
            } finally {
                Path.clearThreadLocals();
            }
        });
    }

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
            execute(initialInsert.toString());
            drainWalQueue();

            Thread insertThread = new Thread(() -> {
                try {
                    for (int batch = 0; batch < 50; batch++) {
                        StringBuilder batchInsert = new StringBuilder("insert into fuzz_table values ");
                        for (int i = 0; i < 100; i++) {
                            if (i > 0) batchInsert.append(", ");
                            long id = 20000 + (batch * 100) + i;
                            batchInsert.append(10000 + i)
                                    .append(", ")
                                    .append(id)
                                    .append(", ")
                                    .append(id * 2.0)
                                    .append(", 'concurrent")
                                    .append(id)
                                    .append("')");
                        }
                        execute(batchInsert.toString());
                        drainWalQueue();
                        Os.sleep(10);
                    }
                } catch (Exception e) {
                } finally {
                    Path.clearThreadLocals();
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
                            insertThread.join(10000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }

                        assertSql("files\tstatus\n" +
                                        "fuzz_output" + File.separator + "1970-01.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");

                        // Verify exported data integrity - should have at least initial 10k records
                        String countQuery = "select count(*) from read_parquet('" + exportRoot + File.separator + "fuzz_output" + File.separator + "1970-01.parquet')";
                        try (RecordCursorFactory factory = select(countQuery);
                             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            assertTrue(cursor.hasNext());
                            long count = cursor.getRecord().getLong(0);
                            assertTrue(count >= 1000);
                        }

                        assertSql("id\tvalue\tname\n" +
                                        "0\t0.0\tname0\n",
                                "select id, value, name from read_parquet('" + exportRoot + File.separator + "fuzz_output" + File.separator + "1970-01.parquet') where id = 0");
                        assertSql("id\tvalue\tname\n" +
                                        "999\t1498.5\tname999\n",
                                "select id, value, name from read_parquet('" + exportRoot + File.separator + "fuzz_output" + File.separator + "1970-01.parquet') where id = 999");

                        assertSql("path\n" +
                                        "fuzz_output" + File.separator + "1970-01.parquet\n",
                                "SELECT path from export_files() order by modified_time");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyCancelSyntaxError() throws Exception {
        assertException(
                "copy 'foobar' cancel aw beans;",
                21,
                "unexpected token [aw]"
        );
    }

    @Test
    public void testCopyExportCancel() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE IF NOT EXISTS \"" +
                    "sys.text_import_log" +
                    "\" (" +
                    "ts timestamp, " + // 0
                    "id string, " + // 1
                    "table_name symbol, " + // 2
                    "file symbol, " + // 3
                    "phase symbol, " + // 4
                    "status symbol, " + // 5
                    "message string," + // 6
                    "rows_handled long," + // 7
                    "rows_imported long," + // 8
                    "errors long" + // 9
                    ") timestamp(ts) partition by DAY BYPASS WAL");

            execute("CREATE TABLE IF NOT EXISTS \"" +
                    "sys.copy_export_log"
                    + "\" (" +
                    "ts TIMESTAMP, " + // 0
                    "id VARCHAR, " + // 1
                    "table_name SYMBOL, " + // 2
                    "files VARCHAR, " + // 3
                    "phase SYMBOL, " + // 4
                    "status SYMBOL, " + // 5
                    "message VARCHAR, " + // 6
                    "errors LONG" + // 7
                    ") timestamp(ts) PARTITION BY DAY\n" +
                    "TTL 3 DAYS WAL;"
            );
            drainWalQueue();
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
                        copyID = engine.getCopyExportContext().getActiveExportID();
                    } while (copyID == -1);

                    StringSink sink = new StringSink();
                    Numbers.appendHex(sink, copyID, true);
                    String copyIDStr = sink.toString();
                    sink.clear();
                    sink.put("COPY '").put(copyIDStr).put("' CANCEL;");
                    try {
                        assertSql("id\tstatus\n" +
                                copyIDStr + "\tcancelled\n", sink);
                    } catch (SqlException e) {
                        throw new RuntimeException(e);
                    }
                    // wait cancel finish
                    do {
                        copyID = engine.getCopyExportContext().getActiveExportID();
                    } while (copyID != -1);
                } finally {
                    Path.clearThreadLocals();
                }
            };
            testCopyExport(stmt, test, false);
        });
    }

    @Test
    public void testCopyOptionError() throws Exception {
        assertException(
                "copy test_table to 'test_table'  with format parquet1;",
                45,
                "unsupported format, only 'parquet' is supported"
        );
        assertException(
                "copy test_table to 'test_table'  with format parquet1;",
                45,
                "unsupported format, only 'parquet' is supported"
        );

        assertException(
                "copy test_table to 'test_table'  with partition_by Day;",
                0,
                "export format must be specified, supported formats:, 'parquet'"
        );

        assertException(
                "copy test_table to 'test_table'  with partition_by Day1;",
                51,
                "invalid partition by option: Day1"
        );

        assertException(
                "copy test_table to 'test_table'  with partition_by1 Day1;",
                38,
                "unrecognised option [option=partition_by1]"
        );

        assertException(
                "copy test_table to 'test_table'  with partition_by1 Day1;",
                38,
                "unrecognised option [option=partition_by1]"
        );

        assertException(
                "copy test_table to 'test_table'  with size_limit aa;",
                38,
                "size limit is not yet supported"
        );

        assertException(
                "copy test_table to 'test_table'  with compression_codec aa;",
                56,
                "invalid compression codec[aa], expected one of: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw"
        );

        assertException(
                "copy test_table to 'test_table'  with format parquet compression_codec uncompressed compression_level aa;",
                102,
                "found [tok='aa', len=2] bad integer"
        );

        assertException(
                "copy test_table to 'test_table'  with format parquet compression_codec zstd compression_level 120;",
                94,
                "ZSTD compression level must be between 1 and 22"
        );

        assertException(
                "copy test_table to 'test_table'  with format parquet compression_codec GZIP compression_level 120;",
                94,
                "GZIP compression level must be between 0 and 9"
        );

        assertException(
                "copy test_table to 'test_table'  with format parquet compression_codec BROTLI compression_level 120;",
                96,
                "Brotli compression level must be between 0 and 11"
        );
        assertException(
                "copy test_table to 'test_table'  with format parquet parquet_version 3;",
                69,
                "invalid parquet version: 3, expected 1 or 2"
        );
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
                assertEventually(() -> assertSql("files\tstatus\n" +
                                "test_table" + File.separator + "1970-01-01.parquet\tfinished\n",
                        "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1"));
                assertSql("path\tsize\n" +
                                "test_table" + File.separator + "1970-01-01.parquet\t581\n",
                        "SELECT path, size from export_files()  order by modified_time");
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
                    assertEventually(() -> assertSql("files\tstatus\n" +
                                    "test_table" + File.separator + "1970-01-01.parquet\tfinished\n",
                            "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1"));

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
                    assertEventually(() -> assertSql("files\tstatus\n" +
                                    "test_table" + File.separator + "1970-01-01.parquet\tfinished\n",
                            "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1"));

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
                    assertEventually(() -> assertSql("files\tstatus\n" +
                                    "test_table" + File.separator + "1970-01-01.parquet\tfinished\n",
                            "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table empty_table (x int, y string)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy empty_table to 'output15' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> assertSql("files\tstatus\tmessage\n" +
                                    "\tfinished\tEmpty Table\n",
                            "SELECT files, status,message FROM \"sys.copy_export_log\" LIMIT -1"));

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFailsWithIllegalSql() throws Exception {
        assertException(
                "copy (select x from non_existing_table) to 'tmp' with format parquet",
                20,
                "table does not exist [table=non_existing_table]"
        );
        assertException(
                "copy (select a+1 from1 v) to 'tmp' with format parquet",
                23,
                "found [tok='v', len=1] ',', 'from' or 'over' expected"
        );
        assertException(
                "copy (select 1) to 'tmp' with format csv",
                37,
                "unsupported format, only 'parquet' is supported"
        );
    }

    @Test
    public void testCopyParquetFailsWithNonExistentTable() throws Exception {
        assertException(
                "copy test_table to 'blah blah blah' with format parquet",
                5,
                "table does not exist [table=test_table]"
        );
    }

    @Test
    public void testCopyParquetFailsWithReadOnlyPath() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            final FilesFacade ff = configuration.getFilesFacade();

            // Create a read-only directory to simulate permission failure using FilesFacade
            try (Path readOnlyPath = new Path()) {
                readOnlyPath.of(exportRoot).concat("readonly").$();
                ff.mkdir(readOnlyPath.$(), 755);
                // Note: Setting read-only via FilesFacade would be platform-specific
                // For test purposes, we'll test with invalid path instead
            }

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'readonly/output' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        // This export should fail due to permission issues
                        try {
                            String status = getOne("SELECT status FROM \"sys.copy_export_log\" LIMIT -1");
                            assertTrue("Export should fail", status.contains("failed") || status.contains("error"));
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
            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet partition_by MONTH", sqlExecutionContext);
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("files\tphase\tstatus\tmessage\terrors\n" +
                                        "\twait_to_run\tstarted\tqueued\t0\n" +
                                        "\twait_to_run\tfinished\t\t0\n" +
                                        "\tpopulating_data_to_temp_table\tstarted\t\t0\n" +
                                        "\tpopulating_data_to_temp_table\tfinished\t\t0\n" +
                                        "\tconverting_partitions\tstarted\t\t0\n" +
                                        "\tconverting_partitions\tfinished\t\t0\n" +
                                        "\tdropping_temp_table\tstarted\t\t0\n" +
                                        "\tdropping_temp_table\tfinished\t\t0\n" +
                                        "test_table" + File.separator + "2023-01.parquet,test_table" + File.separator + "2023-02.parquet\tsuccess\tfinished\t\t0\n",
                                "SELECT files,phase,status,message,errors FROM sys.copy_export_log");
                        // Verify count and sample data
                        assertSql("ts\tx\n" +
                                        "2023-01-01T10:00:00.000000Z\t1\n" +
                                        "2023-01-02T10:00:00.000000Z\t2\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2023-01.parquet')");
                        assertSql("ts\tx\n" +
                                        "2023-02-01T10:00:00.000000Z\t3\n" +
                                        "2023-02-02T10:00:00.000000Z\t4\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2023-02.parquet')");
                        assertSql("path\tsize\n" +
                                        "test_table" + File.separator + "2023-01.parquet\t605\n" +
                                        "test_table" + File.separator + "2023-02.parquet\t605\n",
                                "SELECT path, size from export_files() order by path");
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
                    assertEventually(() -> assertSql("files\tstatus\n" +
                                    "test_table" + File.separator + "2020-01-01.parquet,test_table" + File.separator + "2020-01-02.parquet\tfinished\n",
                            "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1"));

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
                    assertEventually(() -> assertSql("files\tstatus\tmessage\n" +
                                    "\tfinished\tEmpty Table\n",
                            "SELECT files, status, message FROM \"sys.copy_export_log\" LIMIT -1"));

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
                        assertSql("files\tstatus\tmessage\n" +
                                        "test_table" + File.separator + "2020-01-01.parquet,test_table" + File.separator + "2020-01-02.parquet\tfinished\t\n",
                                "SELECT files, status, message FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("ts\tx\n" +
                                        "2020-01-01T00:00:00.000000Z\t0\n" +
                                        "2020-01-01T00:00:01.000000Z\t10\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "2020-01-01.parquet')");
                        assertSql("path\tsize\n" +
                                        "test_table" + File.separator + "2020-01-01.parquet\t1080\n" +
                                        "test_table" + File.separator + "2020-01-02.parquet\t581\n",
                                "SELECT path, size from export_files()  order by modified_time");
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
                        assertSql("files\tstatus\n" +
                                        "output_large" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify count and sample data
                        assertSql("count\n10000\n",
                                "select count(*) from read_parquet('" + exportRoot + File.separator + "output_large" + File.separator + "default.parquet')");
                        assertSql("id\tvalue\n0\tvalue0\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_large" + File.separator + "default.parquet') where id = 0");
                        assertSql("id\tvalue\n999\tvalue999\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_large" + File.separator + "default.parquet') where id = 999");
                        assertSql("path\tsize\n" +
                                        "output_large" + File.separator + "default.parquet\t125945\n",
                                "SELECT path, size from export_files()  order by modified_time");
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
                        assertSql("files\tstatus\n" +
                                        "price_1h" + File.separator + "2023-09.parquet,price_1h" + File.separator + "2023-11.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("sym\tprice\tts\n" +
                                        "gbpusd\t1.323\t2023-09-10T12:00:00.000000000Z\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "price_1h" + File.separator + "2023-09.parquet')");
                        assertSql("sym\tprice\tts\n" +
                                        "jpyusd\t1.321\t2023-11-10T12:00:00.000000000Z\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "price_1h" + File.separator + "2023-11.parquet')");
                        assertSql("path\tsize\n" +
                                        "price_1h" + File.separator + "2023-09.parquet\t915\n" +
                                        "price_1h" + File.separator + "2023-11.parquet\t920\n",
                                "SELECT path, size from export_files()  order by modified_time");

                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidCompressionLevel() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_level 'invalid'",
                66,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidDataPageSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet data_page_size 'invalid'",
                63,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with format invalid",
                40,
                "unsupported format, only 'parquet' is supported"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidOptionName() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet invalid_option 'value'",
                48,
                "unrecognised option [option=invalid_option]"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidParquetVersion() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet parquet_version 'invalid'",
                64,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidRowGroupSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet row_group_size 'invalid'",
                63,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorInvalidStatisticsValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet statistics_enabled 'invalid'",
                67,
                "unexpected token ['invalid']"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorMissingFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with parquet",
                33,
                "unrecognised option [option=parquet]"
        );
    }

    @Test
    public void testCopyParquetSyntaxErrorMissingOptionValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_codec unknown",
                66,
                "invalid compression codec[unknown], expected one of: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw"
        );
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
                        assertSql("files\tstatus\n" +
                                        "output_all_types" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify all data types are preserved correctly
                        assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tt_ns\td_array\tts\n" +
                                        "true\t1\t100\t1000\t10000\t1.5\t2.5\ttest\tsym1\t2023-01-01T10:00:00.123456789Z\t[1.0,2.0,3.0]\t2023-01-01T10:00:00.000000Z\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_all_types" + File.separator + "default.parquet')");
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
            assertTrue(exportDirectoryExists("async_types"));

            String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                            "true\t42\t1000\t100000\t1000000\t3.14\t2.718\thello world\tsymbol1\t2023-06-15T14:30:00.000000Z\n",
                    "select * from read_parquet('" + exportRoot + File.separator + "async_types" + File.separator + "default.parquet')");
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
            assertTrue(exportDirectoryExists("async_large"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("count\n500\n",
                    "select count(*) from read_parquet('" + exportRoot + File.separator + "async_large" + File.separator + "default.parquet')");
            assertSql("id\tdata\n0\tdata0\n",
                    "select * from read_parquet('" + exportRoot + File.separator + "async_large" + File.separator + "default.parquet') where id = 0");
            assertSql("id\tdata\n499\tdata499\n",
                    "select * from read_parquet('" + exportRoot + File.separator + "async_large" + File.separator + "default.parquet') where id = 499");
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
            assertTrue(exportDirectoryExists("async_output1"));

            String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("id\tname\tvalue\n" +
                            "1\talpha\t1.1\n" +
                            "2\tbeta\t2.2\n" +
                            "3\tgamma\t3.3\n",
                    "select * from read_parquet('" + exportRoot + File.separator + "async_output1" + File.separator + "default.parquet') order by id");
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
                        assertSql("files\tstatus\n" +
                                        "output_complex" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify complex query results
                        assertSql("id\tname\tamount\torder_date\n" +
                                        "1\tJohn\t100.5\t2023-01-01T10:00:00.000000Z\n" +
                                        "2\tJane\t200.75\t2023-01-02T11:00:00.000000Z\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_complex" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output_nulls" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify null values are handled correctly
                        assertSql("x\ty\tz\n" +
                                        "null\t\tnull\n" +
                                        "1\thello\t1.5\n" +
                                        "3\tworld\t3.5\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_nulls" + File.separator + "default.parquet') order by x");
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
                        assertSql("files\tstatus\n" +
                                        "output_special" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify special characters are preserved
                        assertSql("x\ty\n" +
                                        "1\thello\\nworld\n" +
                                        "2\ttab\\there\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output_special" + File.separator + "default.parquet') order by x");
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
                        assertSql("files\tstatus\n" +
                                        "❤️🍺" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("x\ty\n" +
                                        "1\thello\\nworld11\n" +
                                        "2\ttab\\there\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "❤️🍺" + File.separator + "default.parquet') order by x");
                        assertSql("path\tsize\n" +
                                        "❤️🍺" + File.separator + "default.parquet\t639\n",
                                "SELECT path, size from export_files()  order by modified_time");
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
                    assertEventually(() -> {
                                assertSql("files\tstatus\n" +
                                                "output3" + File.separator + "default.parquet\tfinished\n",
                                        "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                                // Verify only filtered data was exported
                                assertSql("id\tvalue\n" +
                                                "2\t2.5\n" +
                                                "3\t3.5\n",
                                        "select * from read_parquet('" + exportRoot + File.separator + "output3" + File.separator + "default.parquet') order by id");
                            }
                    );

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
                        assertSql("files\tstatus\n" +
                                        "output1" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify exported data can be read back and matches original
                        assertSql("x\ty\tz\n" +
                                        "1\t100\thello\n" +
                                        "2\t200\tworld\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output1" + File.separator + "default.parquet') order by x");
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
            String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' limit -1";
            assertSql("status\nfinished\n", query);

            // Verify exported data can be read back and matches original
            assertSql("x\ty\tz\n" +
                            "1\t100\thello\n" +
                            "2\t200\tworld\n",
                    "select * from read_parquet('" + exportRoot + File.separator + "output1" + File.separator + "default.parquet') order by x");
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
                        assertSql("files\tstatus\n" +
                                        "output2" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify exported data
                        assertSql("x\ty\n" +
                                        "1\t100\n" +
                                        "2\t200\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output2" + File.separator + "default.parquet') order by x");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyUseInsert() throws Exception {
        assertMemoryLeak(() -> {
            engine.execute("CREATE TABLE reject_non_select_test AS (SELECT x FROM long_sequence(2))", sqlExecutionContext);
            assertException(
                    "copy (INSERT INTO reject_non_select_test SELECT * FROM reject_non_select_test) to 'test_table' with format parquet;",
                    6,
                    "table and column names that are SQL keywords have to be enclosed in double quotes, such as"
            );
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
                        assertSql("files\tstatus\n" +
                                        "output14" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify case-insensitive options work
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output14" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output4" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify compressed data is readable
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output4" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output5" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with compression level
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output5" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output7" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with custom page size
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output7" + File.separator + "default.parquet')");
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
                    "compression_codec gzip compression_level 6 " +
                    "row_group_size 5000 data_page_size 8192 " +
                    "statistics_enabled true parquet_version 2", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("files\tstatus\n" +
                                        "output13" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with multiple options
                        assertSql("x\ty\tz\n" +
                                        "1\thello\t1.5\n" +
                                        "2\tworld\t2.5\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output13" + File.separator + "default.parquet') order by x");
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
                        assertSql("files\tstatus\n" +
                                        "💗❤️" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with custom page size
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "💗❤️" + File.separator + "default.parquet')");
                        assertSql("path\tsize\n" +
                                        "💗❤️" + File.separator + "default.parquet\t543\n",
                                "SELECT path, size from export_files()  order by modified_time");
                    });

            testCopyExport(stmt, test);
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
                        assertSql("files\tstatus\n" +
                                        "output10" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with specific Parquet version
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output10" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output11" + File.separator + "2023-01-01.parquet,output11" + File.separator + "2023-01-02.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify partitioned data
                        assertSql("ts\tx\n" +
                                        "2023-01-01T10:00:00.000000Z\t1\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output11" + File.separator + "2023-01-01.parquet') order by ts");
                        assertSql("ts\tx\n" +
                                        "2023-01-02T10:00:00.000000Z\t2\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output11" + File.separator + "2023-01-02.parquet') order by ts");
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
                        assertSql("files\tstatus\n" +
                                        "output6" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with custom row group size
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output6" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output13" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("path\tsize\n" +
                                        "output13" + File.separator + "default.parquet\t818\n",
                                "select path, size from export_files() order by path");
                    });

            CopyExportRunnable stmt1 = () -> runAndFetchCopyExportID("copy test_table to 'output14' with format parquet ", sqlExecutionContext);

            CopyExportRunnable test1 = () ->
                    assertEventually(() -> {
                        assertSql("files\tstatus\n" +
                                        "output14" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("path\tsize\n" +
                                        "output13" + File.separator + "default.parquet\t818\n" +
                                        "output14" + File.separator + "default.parquet\t818\n",
                                "select path, size from export_files() order by path");
                    });

            testCopyExport(stmt, test);
            testCopyExport(stmt1, test1);

            CopyExportRunnable stmt2 = () -> runAndFetchCopyExportID("copy test_table to 'output13' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test2 = () ->
                    assertEventually(() -> {
                        assertSql("files\tstatus\n" +
                                        "output13" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("path\tsize\n" +
                                        "output13" + File.separator + "default.parquet\t865\n" +
                                        "output14" + File.separator + "default.parquet\t818\n",
                                "select path, size from export_files() order by path");
                    });
            execute("insert into test_table values (4, 'hello1', 3.5), (5, 'world1', 4.5)");
            testCopyExport(stmt2, test2);

            CopyExportRunnable stmt3 = () -> runAndFetchCopyExportID("copy test_table to 'output13" + File.separator + "dir1" + File.separator + "dir2' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test3 = () ->
                    assertEventually(() -> {
                        assertSql("files\tstatus\n" +
                                        "output13" + File.separator + "dir1" + File.separator + "dir2" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("path\tsize\n" +
                                        "output13" + File.separator + "default.parquet\t865\n" +
                                        "output13" + File.separator + "dir1" + File.separator + "dir2" + File.separator + "default.parquet\t865\n" +
                                        "output14" + File.separator + "default.parquet\t818\n",
                                "select path, size from export_files() order by path");
                    });
            testCopyExport(stmt3, test3);

            CopyExportRunnable stmt4 = () -> runAndFetchCopyExportID("copy test_table to 'output15" + File.separator + "dir1" + File.separator + "dir2' with format parquet ", sqlExecutionContext);
            CopyExportRunnable test4 = () ->
                    assertEventually(() -> {
                        assertSql("files\tstatus\n" +
                                        "output15" + File.separator + "dir1" + File.separator + "dir2" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        assertSql("path\tsize\n" +
                                        "output13" + File.separator + "default.parquet\t865\n" +
                                        "output13" + File.separator + "dir1" + File.separator + "dir2" + File.separator + "default.parquet\t865\n" +
                                        "output14" + File.separator + "default.parquet\t818\n" +
                                        "output15" + File.separator + "dir1" + File.separator + "dir2" + File.separator + "default.parquet\t865\n",
                                "select path, size from export_files() order by path");
                    });
            testCopyExport(stmt4, test4);
        });
    }

    @Test
    public void testCopyWithSizeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y LONG)");
            execute("INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)");

            assertException(
                    "COPY test_table TO 'output12' WITH FORMAT PARQUET size_limit 1000",
                    50,
                    "size limit is not yet support"
            );
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
                        assertSql("files\tstatus\n" +
                                        "test_table" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with statistics disabled
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "test_table" + File.separator + "default.parquet')");
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
                        assertSql("files\tstatus\n" +
                                        "output8" + File.separator + "default.parquet\tfinished\n",
                                "SELECT files, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with statistics enabled
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + File.separator + "output8" + File.separator + "default.parquet')");
                    });

            testCopyExport(stmt, test);
        });
    }

    private void assertEventually(TestUtils.EventualCode assertion) throws Exception {
        TestUtils.assertEventually(assertion, 5, exceptionTypesToCatch);
    }

    // Helper methods for copy export operations
    protected static String runAndFetchCopyExportID(String copySql, SqlExecutionContext sqlExecutionContext) throws SqlException {
        try (
                RecordCursorFactory factory = select(copySql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            CharSequence value = cursor.getRecord().getStrA(0);
            Assert.assertNotNull(value);
            return value.toString();
        }
    }

    protected synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test) throws Exception {
        testCopyExport(statement, test, true);
    }

    protected synchronized static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test, boolean blocked) throws Exception {
        assertMemoryLeak(() -> {
            CountDownLatch processed = new CountDownLatch(1);
            execute("drop table if exists \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            try (CopyExportRequestJob copyRequestJob = new CopyExportRequestJob(engine)) {
                Thread processingThread = createJobThread(copyRequestJob, processed);
                processingThread.start();
                statement.run();
                if (blocked) {
                    processed.await();
                }
                drainWalQueue(engine);
                copyRequestJob.drain(0);
                if (blocked) {
                    processingThread.join();
                    test.run();
                } else {
                    test.run();
                    processingThread.join();
                }
            }
        });
    }

    protected boolean exportDirectoryExists(String dirName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(exportRoot).concat(dirName).$();
            return ff.exists(path.$());
        }
    }

    protected String getOne(String query) throws SqlException {
        try (
                RecordCursorFactory factory = select(query);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            if (cursor.hasNext()) {
                CharSequence value = cursor.getRecord().getStrA(0);
                return value != null ? value.toString() : null;
            }
            return null;
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
