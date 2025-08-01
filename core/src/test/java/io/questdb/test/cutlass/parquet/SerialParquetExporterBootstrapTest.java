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

package io.questdb.test.cutlass.parquet;

import io.questdb.ServerMain;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;

public class SerialParquetExporterBootstrapTest extends AbstractBootstrapTest {

    private static String inputWorkRoot;

    @Test
    public void testCopyParquetWithServerMain() throws Exception {
        assertMemoryLeak(() -> {
            inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("parquet_exports" + System.nanoTime()).getAbsolutePath());

            try (TestServerMain serverMain = startWithEnvVariables(
                    "QDB_CAIRO_SQL_COPY_ROOT", inputWorkRoot,
                    "QDB_CAIRO_SQL_COPY_WORK_ROOT", inputWorkRoot
            )) {

                try (Connection connection = getConnection("admin", "quest", serverMain.getPgWireServerPort())) {
                    try (Statement statement = connection.createStatement()) {
                        // Create test table
                        statement.execute("CREATE TABLE test_table (x INT, y LONG, z STRING)");
                        statement.execute("INSERT INTO test_table VALUES (1, 100, 'hello'), (2, 200, 'world')");

                        // Execute copy to parquet with export job processing
                        statement.execute("COPY test_table TO '" + inputWorkRoot + "/bootstrap_test' WITH FORMAT PARQUET");

                        // Wait for export to complete and verify
                        TestUtils.assertEventually(() -> {
                            assertTrue("Export directory should exist", exportDirectoryExists("bootstrap_test"));
                        });

                        // Verify exported data
                        try (ResultSet rs = statement.executeQuery(
                                "SELECT * FROM read_parquet('" + inputWorkRoot + "/bootstrap_test') ORDER BY x")) {
                            assertTrue(rs.next());
                            Assert.assertEquals(1, rs.getInt("x"));
                            Assert.assertEquals(100, rs.getLong("y"));
                            Assert.assertEquals("hello", rs.getString("z"));

                            assertTrue(rs.next());
                            Assert.assertEquals(2, rs.getInt("x"));
                            Assert.assertEquals(200, rs.getLong("y"));
                            Assert.assertEquals("world", rs.getString("z"));

                            Assert.assertFalse(rs.next());
                        }

                        // Verify export log shows success
                        TestUtils.assertEventually(() -> {
                            try (ResultSet logRs = statement.executeQuery(
                                    "SELECT status FROM sys.copy_export_log ORDER BY ts DESC LIMIT 1")) {
                                assertTrue(logRs.next());
                                Assert.assertEquals("finished", logRs.getString("status"));
                            } catch (Exception e) {
                                throw new AssertionError(e);
                            }
                        });
                    }
                }
            }
        });
    }

    @Test
    public void testCopyParquetWithMultipleOptions() throws Exception {
        assertMemoryLeak(() -> {
            inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("parquet_exports" + System.nanoTime()).getAbsolutePath());

            try (TestServerMain serverMain = startWithEnvVariables(
                    "QDB_CAIRO_SQL_COPY_ROOT", inputWorkRoot,
                    "QDB_CAIRO_SQL_COPY_WORK_ROOT", inputWorkRoot
            )) {

                try (Connection connection = getConnection("admin", "quest", serverMain.getPgWireServerPort())) {
                    try (Statement statement = connection.createStatement()) {
                        // Create test table with various data types
                        statement.execute("CREATE TABLE multi_types (" +
                                "bool_col BOOLEAN, " +
                                "int_col INT, " +
                                "long_col LONG, " +
                                "double_col DOUBLE, " +
                                "string_col STRING, " +
                                "ts TIMESTAMP" +
                                ") timestamp(ts)");

                        statement.execute("INSERT INTO multi_types VALUES (" +
                                "true, 42, 1000000, 3.14, 'test data', '2023-01-01T10:00:00.000Z'" +
                                ")");

                        // Execute copy with multiple parquet options
                        statement.execute("COPY multi_types TO '" + inputWorkRoot + "/multi_options' " +
                                "WITH FORMAT PARQUET " +
                                "compression_codec 'snappy' " +
                                "row_group_size 5000 " +
                                "statistics_enabled true");

                        // Wait for export to complete
                        TestUtils.assertEventually(() -> {
                            assertTrue("Export directory should exist", exportDirectoryExists("multi_options"));
                        });

                        // Verify exported data preserves all types
                        try (ResultSet rs = statement.executeQuery(
                                "SELECT * FROM read_parquet('" + inputWorkRoot + "/multi_options')")) {
                            assertTrue(rs.next());
                            Assert.assertEquals(true, rs.getBoolean("bool_col"));
                            Assert.assertEquals(42, rs.getInt("int_col"));
                            Assert.assertEquals(1000000, rs.getLong("long_col"));
                            Assert.assertEquals(3.14, rs.getDouble("double_col"), 0.001);
                            Assert.assertEquals("test data", rs.getString("string_col"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCopyParquetLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("parquet_exports" + System.nanoTime()).getAbsolutePath());

            try (TestServerMain serverMain = startWithEnvVariables(
                    "QDB_CAIRO_SQL_COPY_ROOT", inputWorkRoot,
                    "QDB_CAIRO_SQL_COPY_WORK_ROOT", inputWorkRoot
            )) {

                try (Connection connection = getConnection("admin", "quest", serverMain.getPgWireServerPort())) {
                    try (Statement statement = connection.createStatement()) {
                        // Create table and insert multiple rows
                        statement.execute("CREATE TABLE large_table (id INT, data STRING)");

                        // Insert data in batches to test larger datasets
                        for (int batch = 0; batch < 10; batch++) {
                            StringBuilder insertSql = new StringBuilder("INSERT INTO large_table VALUES ");
                            for (int i = 0; i < 100; i++) {
                                if (i > 0) insertSql.append(", ");
                                int id = batch * 100 + i;
                                insertSql.append("(").append(id).append(", 'data").append(id).append("')");
                            }
                            statement.execute(insertSql.toString());
                        }

                        // Export with specific row group size
                        statement.execute("COPY large_table TO '" + inputWorkRoot + "/large_dataset' " +
                                "WITH FORMAT PARQUET row_group_size 200");

                        // Wait for export to complete
                        TestUtils.assertEventually(() -> {
                            assertTrue("Export directory should exist", exportDirectoryExists("large_dataset"));
                        });

                        // Verify count and sample data
                        try (ResultSet rs = statement.executeQuery(
                                "SELECT count(*) as cnt FROM read_parquet('" + inputWorkRoot + "/large_dataset')")) {
                            assertTrue(rs.next());
                            Assert.assertEquals(1000, rs.getInt("cnt"));
                        }

                        // Verify first and last records
                        try (ResultSet rs = statement.executeQuery(
                                "SELECT * FROM read_parquet('" + inputWorkRoot + "/large_dataset') WHERE id = 0")) {
                            assertTrue(rs.next());
                            Assert.assertEquals(0, rs.getInt("id"));
                            Assert.assertEquals("data0", rs.getString("data"));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCopyParquetWithQuery() throws Exception {
        assertMemoryLeak(() -> {
            inputWorkRoot = TestUtils.unchecked(() -> temp.newFolder("parquet_exports" + System.nanoTime()).getAbsolutePath());

            try (TestServerMain serverMain = startWithEnvVariables(
                    "QDB_CAIRO_SQL_COPY_ROOT", inputWorkRoot,
                    "QDB_CAIRO_SQL_COPY_WORK_ROOT", inputWorkRoot
            )) {

                try (Connection connection = getConnection("admin", "quest", serverMain.getPgWireServerPort())) {
                    try (Statement statement = connection.createStatement()) {
                        // Create test tables
                        statement.execute("CREATE TABLE orders (id INT, customer_id INT, amount DOUBLE)");
                        statement.execute("CREATE TABLE customers (id INT, name STRING)");

                        // Insert test data
                        statement.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')");
                        statement.execute("INSERT INTO orders VALUES (1, 1, 100.50), (2, 2, 200.75), (3, 1, 50.25)");

                        // Export complex query result
                        statement.execute("COPY (" +
                                "SELECT o.id, c.name, o.amount " +
                                "FROM orders o " +
                                "JOIN customers c ON o.customer_id = c.id " +
                                "WHERE o.amount > 100" +
                                ") TO '" + inputWorkRoot + "/query_result' WITH FORMAT PARQUET");

                        // Wait for export to complete
                        TestUtils.assertEventually(() -> {
                            assertTrue("Export directory should exist", exportDirectoryExists("query_result"));
                        });

                        // Verify filtered query results
                        try (ResultSet rs = statement.executeQuery(
                                "SELECT * FROM read_parquet('" + inputWorkRoot + "/query_result') ORDER BY id")) {
                            assertTrue(rs.next());
                            Assert.assertEquals(1, rs.getInt("id"));
                            Assert.assertEquals("Alice", rs.getString("name"));
                            Assert.assertEquals(100.50, rs.getDouble("amount"), 0.01);

                            assertTrue(rs.next());
                            Assert.assertEquals(2, rs.getInt("id"));
                            Assert.assertEquals("Bob", rs.getString("name"));
                            Assert.assertEquals(200.75, rs.getDouble("amount"), 0.01);

                            Assert.assertFalse(rs.next());
                        }
                    }
                }
            }
        });
    }

    private boolean exportDirectoryExists(String dirName) {
        try (Path path = new Path()) {
            path.of(inputWorkRoot).concat(dirName).$();
            return Files.exists(path.$());
        }
    }

}