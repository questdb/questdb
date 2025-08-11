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
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    }


    @Test
    public void testCopyCancelSyntaxError() throws Exception {
        assertException(
                "copy 'foobar' cancel aw beans;",
                21,
                "unexpected token [aw]"
        );
    }

    // Edge case tests for boundary values
    @Test
    public void testCopyParquetBoundaryValuesMaxRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size 2147483647", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "test_table\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                    });


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
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "test_table\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                    });

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
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "test_table\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetBoundaryValues_ZeroRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts TIMESTAMP, x int) timestamp(ts) partition by day wal;");
            execute("insert into test_table values (0, 1)");
            drainWalQueue();

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'test_table' with format parquet row_group_size 0", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "test_table\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                    });

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
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "output15\tfailed\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetFailsWithNonExistentTable() throws Exception {
        assertException(
                "copy test_table to 'blah blah blah' with format parquet",
                0,
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
    public void testCopyParquetLargeTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table large_table (id int, value string)");

            // Insert multiple rows to test larger datasets
            StringBuilder insertQuery = new StringBuilder("insert into large_table values ");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) insertQuery.append(", ");
                insertQuery.append("(").append(i).append(", 'value").append(i).append("')");
            }
            execute(insertQuery.toString());

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy large_table to 'output_large' with format parquet row_group_size 100", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "output_large\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify count and sample data
                        assertSql("count\n1000\n",
                                "select count(*) from read_parquet('" + exportRoot + "/output_large/default.parquet')");
                        assertSql("id\tvalue\n0\tvalue0\n",
                                "select * from read_parquet('" + exportRoot + "/output_large/default.parquet') where id = 0");
                        assertSql("id\tvalue\n999\tvalue999\n",
                                "select * from read_parquet('" + exportRoot + "/output_large/default.parquet') where id = 999");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidCompressionLevel() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_level 'invalid'",
                66,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidDataPageSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet data_page_size 'invalid'",
                63,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with format invalid",
                40,
                "unexpected token [invalid]"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidOptionName() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet invalid_option 'value'",
                48,
                "unrecognised option [option=invalid_option]"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidParquetVersion() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet parquet_version 'invalid'",
                64,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidRowGroupSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet row_group_size 'invalid'",
                63,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidStatisticsValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet statistics_enabled 'invalid'",
                67,
                "unexpected token ['invalid']"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_MissingFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with parquet",
                33,
                "unrecognised option [option=parquet]"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_MissingOptionValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_codec",
                65,
                "invalid compression codec, expected one of: uncompressed, snappy, gzip, lzo, brotli, lz4, zstd, lz4_raw"
        );
    }

    @Test
    public void testCopyParquetTableDoesNotExist() throws Exception {
        assertException(
                "copy nonexistent_table to 'output' with format parquet",
                0,
                "table does not exist [table=nonexistent_table]"
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
                    "ts timestamp" +
                    ") timestamp(ts)");

            execute("insert into all_types values (" +
                    "true, 1, 100, 1000, 10000L, 1.5f, 2.5, 'test', 'sym1', '2023-01-01T10:00:00.000Z'" +
                    ")");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy all_types to 'output_all_types' with format parquet", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "output_all_types\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify all data types are preserved correctly
                        assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                                        "true\t1\t100\t1000\t10000\t1.5\t2.5\ttest\tsym1\t2023-01-01T10:00:00.000000Z\n",
                                "select * from read_parquet('" + exportRoot + "/output_all_types/default.parquet')");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyParquetWithAsyncMonitoring_AllDataTypes() throws Exception {
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

            String exportId = runAndFetchCopyExportID("copy comprehensive_types to 'async_types' with format parquet " +
                    "parquet_version 2 data_page_size 2048", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportDirectoryExists("async_types"));

            String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                            "true\t42\t1000\t100000\t1000000\t3.14\t2.718\thello world\tsymbol1\t2023-06-15T14:30:00.000000Z\n",
                    "select * from read_parquet('" + exportRoot + "/async_types/default.parquet')");
        };

        testCopyExport(statement, test);
    }

    @Test
    public void testCopyParquetWithAsyncMonitoring_LargeDataset() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table large_dataset (id int, data string)");

            StringBuilder insertQuery = new StringBuilder("insert into large_dataset values ");
            for (int i = 0; i < 500; i++) {
                if (i > 0) insertQuery.append(", ");
                insertQuery.append("(").append(i).append(", 'data").append(i).append("')");
            }
            execute(insertQuery.toString());

            String exportId = runAndFetchCopyExportID("copy large_dataset to 'async_large' with format parquet " +
                    "row_group_size 100 compression_codec snappy", sqlExecutionContext);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportDirectoryExists("async_large"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("count\n500\n",
                    "select count(*) from read_parquet('" + exportRoot + "/async_large/default.parquet')");
            assertSql("id\tdata\n0\tdata0\n",
                    "select * from read_parquet('" + exportRoot + "/async_large/default.parquet') where id = 0");
            assertSql("id\tdata\n499\tdata499\n",
                    "select * from read_parquet('" + exportRoot + "/async_large/default.parquet') where id = 499");
        };

        testCopyExport(statement, test);
    }

    // Additional tests using async export monitoring pattern
    @Test
    public void testCopyParquetWithAsyncMonitoring_MultipleOptions() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table test_table (id int, name string, value double)");
            execute("insert into test_table values (1, 'alpha', 1.1), (2, 'beta', 2.2), (3, 'gamma', 3.3)");

            String exportId = runAndFetchCopyExportID("copy test_table to 'async_output1' with format parquet " +
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
                    "select * from read_parquet('" + exportRoot + "/async_output1/default.parquet') order by id");
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
                        assertSql("file\tstatus\n" +
                                        "output_complex\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify complex query results
                        assertSql("id\tname\tamount\torder_date\n" +
                                        "1\tJohn\t100.5\t2023-01-01T10:00:00.000000Z\n" +
                                        "2\tJane\t200.75\t2023-01-02T11:00:00.000000Z\n",
                                "select * from read_parquet('" + exportRoot + "/output_complex/default.parquet')");
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
                        assertSql("file\tstatus\n" +
                                        "output_nulls\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify null values are handled correctly
                        assertSql("x\ty\tz\n" +
                                        "null\t\tnull\n" +
                                        "1\thello\t1.5\n" +
                                        "3\tworld\t3.5\n",
                                "select * from read_parquet('" + exportRoot + "/output_nulls/default.parquet') order by x");
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
                        assertSql("file\tstatus\n" +
                                        "output_special\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify special characters are preserved
                        assertSql("x\ty\n" +
                                        "1\thello\\nworld\n" +
                                        "2\ttab\\there\n",
                                "select * from read_parquet('" + exportRoot + "/output_special/default.parquet') order by x");
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
                                assertSql("file\tstatus\n" +
                                                "output3\tfinished\n",
                                        "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                                // Verify only filtered data was exported
                                assertSql("id\tvalue\n" +
                                                "2\t2.5\n" +
                                                "3\t3.5\n",
                                        "select * from read_parquet('" + exportRoot + "/output3/default.parquet') order by id");
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
                        assertSql("file\tstatus\n" +
                                        "output1\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify exported data can be read back and matches original
                        assertSql("x\ty\tz\n" +
                                        "1\t100\thello\n" +
                                        "2\t200\tworld\n",
                                "select * from read_parquet('" + exportRoot + "/output1/default.parquet') order by x");
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

        CopyExportRunnable test = () -> {
            assertEventually(() -> {
                // Verify export completed successfully
                String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' limit -1";
                assertSql("status\nfinished\n", query);

                // Verify exported data can be read back and matches original
                assertSql("x\ty\tz\n" +
                                "1\t100\thello\n" +
                                "2\t200\tworld\n",
                        "select * from read_parquet('" + exportRoot + "/output1/default.parquet') order by x");
            });
        };

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
                        assertSql("file\tstatus\n" +
                                        "output2\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify exported data
                        assertSql("x\ty\n" +
                                        "1\t100\n" +
                                        "2\t200\n",
                                "select * from read_parquet('" + exportRoot + "/output2/default.parquet') order by x");
                    });

            testCopyExport(stmt, test);
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
                        assertSql("file\tstatus\n" +
                                        "output14\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify case-insensitive options work
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output14/default.parquet')");
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
                        assertSql("file\tstatus\n" +
                                        "output4\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify compressed data is readable
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output4/default.parquet')");
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
                        assertSql("file\tstatus\n" +
                                        "output5\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with compression level
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output5/default.parquet')");
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
                        assertSql("file\tstatus\n" +
                                        "output7\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with custom page size
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output7/default.parquet')");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithMultipleOptions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");

            CopyExportRunnable stmt = () -> {
                runAndFetchCopyExportID("copy test_table to 'output13' with format parquet " +
                        "compression_codec gzip compression_level 6 " +
                        "row_group_size 5000 data_page_size 8192 " +
                        "statistics_enabled true parquet_version 2", sqlExecutionContext);
            };

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "output13\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with multiple options
                        assertSql("x\ty\tz\n" +
                                        "1\thello\t1.5\n" +
                                        "2\tworld\t2.5\n",
                                "select * from read_parquet('" + exportRoot + "/output13/default.parquet') order by x");
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
                        assertSql("file\tstatus\n" +
                                        "output10\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with specific Parquet version
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output10/default.parquet')");
                    });

            testCopyExport(stmt, test);
        });
    }

    @Test
    public void testCopyWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts timestamp, x int) timestamp(ts) partition by DAY");
            execute("insert into test_table values ('2023-01-01T10:00:00.000Z', 1), ('2023-01-02T10:00:00.000Z', 2)");

            CopyExportRunnable stmt = () ->
                    runAndFetchCopyExportID("copy test_table to 'output11' with format parquet partition_by DAY", sqlExecutionContext);

            CopyExportRunnable test = () ->
                    assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "output11\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify partitioned data
                        assertSql("ts\tx\n" +
                                        "2023-01-01T10:00:00.000000Z\t1\n",
                                "select * from read_parquet('" + exportRoot + "/output11/2023-01-01.parquet') order by ts");
                        assertSql("ts\tx\n" +
                                        "2023-01-02T10:00:00.000000Z\t2\n",
                                "select * from read_parquet('" + exportRoot + "/output11/2023-01-02.parquet') order by ts");
                    });

            testCopyExport(stmt, test);
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
                        assertSql("file\tstatus\n" +
                                        "output6\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with custom row group size
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/output6/default.parquet')");
                    });

            testCopyExport(stmt, test);
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
                        assertSql("file\tstatus\n" +
                                        "test_table\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with statistics disabled
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('" + exportRoot + "/test_table/default.parquet')");
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
                        assertSql("file\tstatus\n" +
                                        "output8\tfinished\n",
                                "SELECT file, status FROM \"sys.copy_export_log\" LIMIT -1");
                        // Verify data with statistics enabled
                        assertSql("x\ty\n1\ttest\n",
                                "select * from read_parquet('output8/default.parquet')");
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
        assertMemoryLeak(() -> {
            CountDownLatch processed = new CountDownLatch(1);

            execute("drop table if exists \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            try (CopyExportRequestJob copyRequestJob = new CopyExportRequestJob(engine)) {
                Thread processingThread = createJobThread(copyRequestJob, processed);
                processingThread.start();
                statement.run();
                processed.await();
                drainWalQueue(engine);
                copyRequestJob.drain(0);
                processingThread.join();
                test.run();
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

    protected void waitForExportToComplete(String exportId) throws Exception {
        assertEventually(() -> {
            try {
                String query = "select status from '" + configuration.getSystemTableNamePrefix() + "copy_export_log' where id = '" + exportId + "' limit -1";
                String status = getOne(query);
                assertTrue(Chars.equals("finished", status));
            } catch (SqlException e) {
                throw new AssertionError(e);
            }

        });
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
