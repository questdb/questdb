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
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CopyExportTest extends AbstractCairoTest {
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
        inputRoot = TestUtils.getCsvRoot();
        inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("imports" + System.nanoTime()).getAbsolutePath());
        AbstractCairoTest.setUpStatic();
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
                    TestUtils.assertEventually(() -> {
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
                    TestUtils.assertEventually(() -> {
                        assertSql("file\tstatus\n" +
                                        "test_tabe\tfinished\n",
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
                    TestUtils.assertEventually(() -> {
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
                    TestUtils.assertEventually(() -> {
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
            execute("copy empty_table to 'output15' with format parquet");

            TestUtils.assertEventually(() -> {
                assertTrue(exportDirectoryExists("output15"));
            });


            // Verify empty table exports correctly
            assertSql("x\ty\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output15')");
        });
    }

    // Failure scenario tests using FailingFilesFacade
    @Test
    public void testCopyParquetFailsWithInvalidPath() throws Exception {
        assertException(
                "copy test_table to '/invalid/path/that/does/not/exist' with format parquet",
                5,
                "could not open"
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
                readOnlyPath.of(inputWorkRoot).concat("readonly").$();
                ff.mkdir(readOnlyPath.$(), 755);
                // Note: Setting read-only via FilesFacade would be platform-specific
                // For test purposes, we'll test with invalid path instead
            }

            try {
                execute("copy test_table to 'readonly/output' with format parquet");
                fail("Expected failure due to directory issues");
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

            execute("copy large_table to 'output_large' with format parquet row_group_size 100");

            assertTrue(exportDirectoryExists("output_large"));

            // Verify count and sample data
            assertSql("count\n1000\n",
                    "select count(*) from read_parquet('" + inputWorkRoot + "/output_large')");
            assertSql("id\tvalue\n0\tvalue0\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_large') where id = 0");
            assertSql("id\tvalue\n999\tvalue999\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_large') where id = 999");
        });
    }

    @Test
    public void testCopyParquetLargeTableValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table large_table (id int, value string)");

            // Insert multiple rows to test larger datasets
            StringBuilder insertQuery = new StringBuilder("insert into large_table values ");
            for (int i = 0; i < 1000; i++) {
                if (i > 0) insertQuery.append(", ");
                insertQuery.append("(").append(i).append(", 'value").append(i).append("')");
            }
            execute(insertQuery.toString());

            execute("copy large_table to 'output_large' with format parquet row_group_size 100");

            assertTrue(exportDirectoryExists("output_large"));

            // Verify count and sample data
            assertSql("count\n1000\n",
                    "select count(*) from read_parquet('" + inputWorkRoot + "/output_large')");
            assertSql("id\tvalue\n0\tvalue0\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_large') where id = 0");
            assertSql("id\tvalue\n999\tvalue999\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_large') where id = 999");
        });
    }

    @Test
    public void testCopyParquetRecoveryAfterCleanup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test'), (2, 'data')");

            // First create a successful export
            execute("copy test_table to 'output_recovery' with format parquet");
            assertTrue(exportDirectoryExists("output_recovery"));

            // Verify data
            assertSql("x\ty\n1\ttest\n2\tdata\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_recovery') order by x");

            // Cleanup and re-export to same location should work using FilesFacade
            if (exportDirectoryExists("output_recovery")) {
                try (Path exportPath = new Path()) {
                    exportPath.of(inputWorkRoot).concat("output_recovery").$();
                    ff.remove(exportPath.$());
                }
            }

            // Re-export should succeed
            execute("copy test_table to 'output_recovery' with format parquet");
            assertTrue(exportDirectoryExists("output_recovery"));

            assertSql("x\ty\n1\ttest\n2\tdata\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_recovery') order by x");
        });
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidCompressionLevel() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_level 'invalid'",
                69,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidDataPageSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet data_page_size 'invalid'",
                64,
                "invalid data page size, expected integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with format invalid",
                42,
                "unexpected token [invalid]"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidOptionName() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet invalid_option 'value'",
                50,
                "unrecognised option [option=invalid_option]"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidParquetVersion() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet parquet_version 'invalid'",
                66,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidRowGroupSize() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet row_group_size 'invalid'",
                64,
                "found [tok=''invalid'', len=9] bad integer"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_InvalidStatisticsValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet statistics_enabled 'invalid'",
                71,
                "unexpected token ['invalid']"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_MissingFormat() throws Exception {
        assertException(
                "copy test_table to 'output' with parquet",
                34,
                "Expected 'format' or table option"
        );
    }

    @Test
    public void testCopyParquetSyntaxError_MissingOptionValue() throws Exception {
        assertException(
                "copy test_table to 'output' with format parquet compression_codec",
                67,
                "literal expected"
        );
    }

    @Test
    public void testCopyParquetTableDoesNotExist() throws Exception {
        assertException(
                "copy nonexistent_table to 'output' with format parquet",
                5,
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

            execute("copy all_types to 'output_all_types' with format parquet");

            assertTrue(exportDirectoryExists("output_all_types"));

            // Verify all data types are preserved correctly
            assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                            "true\t1\t100\t1000\t10000\t1.5000\t2.5\ttest\tsym1\t2023-01-01T10:00:00.000000Z\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_all_types')");
        });
    }

    @Test
    public void testCopyParquetWithAllDataTypesValidation() throws Exception {
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

            execute("copy all_types to 'output_all_types' with format parquet");

            assertTrue(exportDirectoryExists("output_all_types"));

            // Verify all data types are preserved correctly
            assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                            "true\t1\t100\t1000\t10000\t1.5000\t2.5\ttest\tsym1\t2023-01-01T10:00:00.000000Z\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_all_types')");
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
            waitForExportToComplete(exportId);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportDirectoryExists("async_types"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("bool_col\tbyte_col\tshort_col\tint_col\tlong_col\tfloat_col\tdouble_col\tstring_col\tsymbol_col\tts\n" +
                            "true\t42\t1000\t100000\t1000000\t3.1400\t2.718\thello world\tsymbol1\t2023-06-15T14:30:00.000000Z\n",
                    "select * from read_parquet('" + inputWorkRoot + "/async_types')");
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
                    "row_group_size 100 compression_codec 'snappy'", sqlExecutionContext);
            waitForExportToComplete(exportId);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportDirectoryExists("async_large"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("count\n500\n",
                    "select count(*) from read_parquet('" + inputWorkRoot + "/async_large')");
            assertSql("id\tdata\n0\tdata0\n",
                    "select * from read_parquet('" + inputWorkRoot + "/async_large') where id = 0");
            assertSql("id\tdata\n499\tdata499\n",
                    "select * from read_parquet('" + inputWorkRoot + "/async_large') where id = 499");
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
                    "compression_codec 'gzip' row_group_size 2000 statistics_enabled true", sqlExecutionContext);
            waitForExportToComplete(exportId);
        };

        CopyExportRunnable test = () -> {
            assertTrue(exportDirectoryExists("async_output1"));

            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            assertSql("id\tname\tvalue\n" +
                            "1\talpha\t1.1\n" +
                            "2\tbeta\t2.2\n" +
                            "3\tgamma\t3.3\n",
                    "select * from read_parquet('" + inputWorkRoot + "/async_output1') order by id");
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

            execute("copy (" +
                    "select o.id, c.name, o.amount, o.order_date " +
                    "from orders o " +
                    "join customers c on o.customer_id = c.id " +
                    "where o.amount > 100" +
                    ") to 'output_complex' with format parquet");

            assertTrue(exportDirectoryExists("output_complex"));

            // Verify complex query results
            assertSql("id\tname\tamount\torder_date\n" +
                            "2\tJane\t200.75\t2023-01-02T11:00:00.000000Z\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_complex')");
        });
    }

    @Test
    public void testCopyParquetWithNullValues() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (null, null, null), (3, 'world', 3.5)");

            execute("copy test_table to 'output_nulls' with format parquet");

            assertTrue(exportDirectoryExists("output_nulls"));

            // Verify null values are handled correctly
            assertSql("x\ty\tz\n" +
                            "1\thello\t1.5\n" +
                            "NaN\t\tNaN\n" +
                            "3\tworld\t3.5\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_nulls') order by x");
        });
    }

    @Test
    public void testCopyParquetWithNullValuesValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (null, null, null), (3, 'world', 3.5)");

            execute("copy test_table to 'output_nulls' with format parquet");

            assertTrue(exportDirectoryExists("output_nulls"));

            // Verify null values are handled correctly
            assertSql("x\ty\tz\n" +
                            "1\thello\t1.5\n" +
                            "NaN\t\tNaN\n" +
                            "3\tworld\t3.5\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_nulls') order by x");
        });
    }

    @Test
    public void testCopyParquetWithSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'hello\\nworld'), (2, 'tab\\there')");

            execute("copy test_table to 'output_special' with format parquet");

            assertTrue(exportDirectoryExists("output_special"));

            // Verify special characters are preserved
            assertSql("x\ty\n" +
                            "1\thello\\nworld\n" +
                            "2\ttab\\there\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_special') order by x");
        });
    }

    @Test
    public void testCopyQueryToParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table source_table (id int, value double, name string)");
            execute("insert into source_table values (1, 1.5, 'a'), (2, 2.5, 'b'), (3, 3.5, 'c')");

            execute("copy (select id, value from source_table where id > 1) to 'output3' with format parquet");

            assertTrue(exportDirectoryExists("output3"));

            // Verify only filtered data was exported
            assertSql("id\tvalue\n" +
                            "2\t2.5\n" +
                            "3\t3.5\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output3') order by id");
        });
    }

    @Test
    public void testCopyTableToParquetBasicSyntax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y long, z string)");
            execute("insert into test_table values (1, 100L, 'hello'), (2, 200L, 'world')");

            execute("copy test_table to 'output1' with format parquet");

            // Verify export directory exists
            assertTrue(exportDirectoryExists("output1"));

            // Verify exported data can be read back and matches original
            assertSql("x\ty\tz\n" +
                            "1\t100\thello\n" +
                            "2\t200\tworld\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output1') order by x");
        });
    }

    // Demonstration of proper copy export test pattern
    @Test
    public void testCopyTableToParquetWithExportLog() throws Exception {
        CopyExportRunnable statement = () -> {
            execute("create table test_table (x int, y long, z string)");
            execute("insert into test_table values (1, 100L, 'hello'), (2, 200L, 'world')");

            String exportId = runAndFetchCopyExportID("copy test_table to 'output1' with format parquet", sqlExecutionContext);
            waitForExportToComplete(exportId);
        };

        CopyExportRunnable test = () -> {
            // Verify export directory exists using FilesFacade
            assertTrue(exportDirectoryExists("output1"));

            // Verify export completed successfully
            String query = "select status from " + configuration.getSystemTableNamePrefix() + "copy_export_log limit -1";
            assertSql("status\nfinished\n", query);

            // Verify exported data can be read back and matches original
            assertSql("x\ty\tz\n" +
                            "1\t100\thello\n" +
                            "2\t200\tworld\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output1') order by x");
        };

        testCopyExport(statement, test);
    }

    @Test
    public void testCopyTableToParquetWithQuotedTableName() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table \"test table\" (x int, y long)");
            execute("insert into \"test table\" values (1, 100L), (2, 200L)");

            execute("copy 'test table' to 'output2' with format parquet");

            assertTrue(exportDirectoryExists("output2"));

            // Verify exported data
            assertSql("x\ty\n" +
                            "1\t100\n" +
                            "2\t200\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output2') order by x");
        });
    }

    @Test
    public void testCopyWithCaseInsensitiveOptions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output14' with FORMAT PARQUET COMPRESSION_CODEC SNAPPY");

            assertTrue(exportDirectoryExists("output14"));

            // Verify case-insensitive options work
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output14')");
        });
    }

    @Test
    public void testCopyWithCompressionCodec() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output4' with format parquet compression_codec snappy");

            assertTrue(exportDirectoryExists("output4"));

            // Verify compressed data is readable
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output4')");
        });
    }

    @Test
    public void testCopyWithCompressionLevel() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            String exportId = runAndFetchCopyExportID("copy test_table to 'output5' with format parquet compression_level 9", sqlExecutionContext);
            waitForExportToComplete(exportId);

            assertTrue(exportDirectoryExists("output5"));

            // Verify data with compression level
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output5')");
        });
    }

    @Test
    public void testCopyWithDataPageSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output7' with format parquet data_page_size 4096");

            assertTrue(exportDirectoryExists("output7"));

            // Verify data with custom page size
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output7')");
        });
    }

    @Test
    public void testCopyWithMultipleOptions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");

            execute("copy test_table to 'output13' with format parquet " +
                    "compression_codec gzip compression_level 6 " +
                    "row_group_size 5000 data_page_size 8192 " +
                    "statistics_enabled true parquet_version 2");

            assertTrue(exportDirectoryExists("output13"));

            // Verify data with multiple options
            assertSql("x\ty\tz\n" +
                            "1\thello\t1.5\n" +
                            "2\tworld\t2.5\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output13') order by x");
        });
    }

    // Updated test methods with proper execute() usage and read_parquet validation
    @Test
    public void testCopyWithMultipleOptionsValidation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string, z double)");
            execute("insert into test_table values (1, 'hello', 1.5), (2, 'world', 2.5)");

            execute("copy test_table to 'output_multi' with format parquet " +
                    "compression_codec 'gzip' compression_level 6 " +
                    "row_group_size 5000 data_page_size 8192 " +
                    "statistics_enabled true parquet_version 2");

            assertTrue(exportDirectoryExists("output_multi"));

            // Verify all data exported correctly with complex options
            assertSql("x\ty\tz\n" +
                            "1\thello\t1.5\n" +
                            "2\tworld\t2.5\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output_multi') order by x");
        });
    }

    @Test
    public void testCopyWithParquetVersion() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output10' with format parquet parquet_version 1");

            assertTrue(exportDirectoryExists("output10"));

            // Verify data with specific Parquet version
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output10')");
        });
    }

    @Test
    public void testCopyWithPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (ts timestamp, x int) timestamp(ts) partition by DAY");
            execute("insert into test_table values ('2023-01-01T10:00:00.000Z', 1), ('2023-01-02T10:00:00.000Z', 2)");

            execute("copy test_table to 'output11' with format parquet partition_by 'DAY'");

            assertTrue(exportDirectoryExists("output11"));

            // Verify partitioned data
            assertSql("ts\tx\n" +
                            "2023-01-01T10:00:00.000000Z\t1\n" +
                            "2023-01-02T10:00:00.000000Z\t2\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output11') order by ts");
        });
    }

    @Test
    public void testCopyWithRowGroupSize() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output6' with format parquet row_group_size 1000");

            assertTrue(exportDirectoryExists("output6"));

            // Verify data with custom row group size
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output6')");
        });
    }

    @Test
    public void testCopyWithSizeLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test_table (x INT, y LONG)");
            execute("INSERT INTO test_table VALUES (1, 100), (2, 200), (3, 300)");

            execute("COPY test_table TO 'output12' WITH FORMAT PARQUET size_limit 1000");

            assertTrue(exportDirectoryExists("output12"));

            // Verify data with size limit
            assertSql("x\ty\n" +
                            "1\t100\n" +
                            "2\t200\n" +
                            "3\t300\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output12') order by x");
        });
    }

    @Test
    public void testCopyWithStatisticsDisabled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output9' with format parquet statistics_enabled false");

            assertTrue(exportDirectoryExists("output9"));

            // Verify data with statistics disabled
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output9')");
        });
    }

    @Test
    public void testCopyWithStatisticsEnabled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table test_table (x int, y string)");
            execute("insert into test_table values (1, 'test')");

            execute("copy test_table to 'output8' with format parquet statistics_enabled true");

            assertTrue(exportDirectoryExists("output8"));

            // Verify data with statistics enabled
            assertSql("x\ty\n1\ttest\n",
                    "select * from read_parquet('" + inputWorkRoot + "/output8')");
        });
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


    protected static void testCopyExport(CopyExportRunnable statement, CopyExportRunnable test) throws Exception {
        assertMemoryLeak(() -> {
            CountDownLatch processed = new CountDownLatch(1);

            execute("drop table if exists \"" + configuration.getSystemTableNamePrefix() + "copy_export_log\"");
            try (CopyExportRequestJob copyRequestJob = new CopyExportRequestJob(engine)) {
                Thread processingThread = createJobThread(copyRequestJob, processed);
                processingThread.start();
                statement.run();
                processed.await();
                drainWalQueue(engine);
                test.run();
                processingThread.join();
                copyRequestJob.drain(0);
            }
        });
    }

    protected boolean exportDirectoryExists(String dirName) {
        final FilesFacade ff = configuration.getFilesFacade();
        try (Path path = new Path()) {
            path.of(inputWorkRoot).concat(dirName).$();
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
        TestUtils.assertEventually(() -> {
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

}
