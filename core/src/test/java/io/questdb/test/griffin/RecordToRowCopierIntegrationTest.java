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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Integration tests for RecordToRowCopier implementations.
 * These tests verify that both bytecode-based and loop-based implementations
 * produce identical results across various scenarios.
 */
public class RecordToRowCopierIntegrationTest extends AbstractCairoTest {

    @Override
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_SQL_COPIER_CHUNKED, false);
    }

    @Test
    public void testCreateTableAsSelectWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 200 columns
            buildCreateTableSql(sink, "src", 200);
            execute(sink);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int col = 0; col < 200; col++) {
                sink.put(", ").put(col);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithManyColumnsAndRows() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 100 columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast(x * 1000000 as timestamp) as ts");
            for (int col = 0; col < 100; col++) {
                sink.put(", cast((x * 100 + ").put(col).put(") % 10000 as int) as col").put(col);
            }
            sink.put(" from long_sequence(100)) timestamp(ts) partition by DAY");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n100\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with many columns of various types
            sink.clear();
            sink.put("create table src (ts timestamp");
            for (int i = 0; i < 30; i++) {
                sink.put(", int_col").put(i).put(" int");
                sink.put(", long_col").put(i).put(" long");
                sink.put(", double_col").put(i).put(" double");
            }
            sink.put(") timestamp(ts) partition by DAY");
            execute(sink);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < 30; i++) {
                sink.put(", ").put(i);       // int
                sink.put(", ").put(i * 100L); // long
                sink.put(", ").put(i * 1.5);  // double
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 100 columns
            buildCreateTableSql(sink, "src", 100);
            execute(sink);

            // Insert rows with null values
            sink.clear();
            sink.put("insert into src values (0");
            for (int col = 0; col < 100; col++) {
                sink.put(", null");
            }
            sink.put(")");
            execute(sink);

            sink.clear();
            sink.put("insert into src values (1000000");
            for (int col = 0; col < 100; col++) {
                sink.put(", ").put(col);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n2\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithPartitionAndFilter() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with many columns using long_sequence
            // Space rows across different days (86400000000 microseconds = 1 day)
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 86400000000L as timestamp) as ts");
            for (int col = 0; col < 50; col++) {
                sink.put(", cast((x - 1) * 100 + ").put(col).put(" as int) as col").put(col);
            }
            sink.put(" from long_sequence(20)) timestamp(ts) partition by DAY");
            execute(sink);

            // Create table using SELECT with WHERE clause
            execute("create table dst as (select * from src where col0 >= 500)");

            // Verify - should have rows 5-19 (15 rows total)
            assertSql("count\n15\n", "select count(*) from dst");
        });
    }

    @Test
    public void testCreateTableAsSelectWithStringsAndSymbols() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with string and symbol columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000000 as timestamp) as ts");
            for (int i = 0; i < 50; i++) {
                sink.put(", concat('str', x - 1, '_', ").put(i).put(") as str_col").put(i);
                sink.put(", cast(concat('sym', (x - 1) % 5) as symbol) as sym_col").put(i);
            }
            sink.put(" from long_sequence(10)) timestamp(ts) partition by DAY");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testCreateTableAsSelectWithTypeWidening() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with narrower types
            sink.clear();
            sink.put("create table src (ts timestamp");
            for (int i = 0; i < 50; i++) {
                sink.put(", byte_col").put(i).put(" byte");
                sink.put(", short_col").put(i).put(" short");
            }
            sink.put(") timestamp(ts) partition by DAY");
            execute(sink);

            // Insert test data
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < 50; i++) {
                sink.put(", ").put(i % 127);       // byte
                sink.put(", ").put(i * 100);       // short
            }
            sink.put(")");
            execute(sink);

            // Create table with wider types using CAST
            sink.clear();
            sink.put("create table dst as (select ts");
            for (int i = 0; i < 50; i++) {
                sink.put(", cast(byte_col").put(i).put(" as long) as long_from_byte").put(i);
                sink.put(", cast(short_col").put(i).put(" as long) as long_from_short").put(i);
            }
            sink.put(" from src)");
            execute(sink);

            // Verify data was copied correctly
            assertSql("count\n1\n", "select count(*) from dst");

            // Spot check some values
            assertSql("long_from_byte0\tlong_from_byte25\tlong_from_short0\tlong_from_short25\n" +
                    "0\t25\t0\t2500\n",
                    "select long_from_byte0, long_from_byte25, long_from_short0, long_from_short25 from dst");
        });
    }

    @Test
    public void testCreateTableAsSelectVeryWideTable() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 500 columns to stress the loop-based implementation
            buildCreateTableSqlWithTypes(sink, "src", 500, "c", i -> "int");
            execute(sink);

            // Insert a row
            sink.clear();
            sink.put("insert into src values (0");
            for (int i = 0; i < 500; i++) {
                sink.put(", ").put(i);
            }
            sink.put(")");
            execute(sink);

            // Create table using SELECT from source
            execute("create table dst as (select * from src)");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst");
            assertSql("c0\tc250\tc499\n0\t250\t499\n", "select c0, c250, c499 from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testBatchInsertWithManyColumns() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 200 columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000 as timestamp) as ts");
            for (int col = 0; col < 200; col++) {
                sink.put(", cast((x - 1 + ").put(col).put(") % 1000 as int) as col").put(col);
            }
            sink.put(" from long_sequence(1000)) timestamp(ts) partition by DAY");
            execute(sink);

            buildCreateTableSql(sink, "dst", 200);
            execute(sink);

            // Batch copy
            execute("insert into dst select * from src");

            assertSql("count\n1000\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testExtremelyWideTableInsert() throws Exception {
        assertMemoryLeak(() -> {
            // Test with 100 columns to stress the loop-based implementation
            // (using fewer columns to avoid file descriptor limits on some systems)
            buildCreateTableSqlWithTypes(sink, "src_extreme", 100, "c", i -> "int");
            execute(sink);

            buildCreateTableSqlWithTypes(sink, "dst_extreme", 100, "c", i -> "int");
            execute(sink);

            // Insert a single row with all values
            sink.clear();
            sink.put("insert into src_extreme values (0");
            for (int i = 0; i < 100; i++) {
                sink.put(", ").put(i);
            }
            sink.put(")");
            execute(sink);

            // Copy it
            execute("insert into dst_extreme select * from src_extreme");

            // Verify
            assertSql("count\n1\n", "select count(*) from dst_extreme");
            assertSql("c0\tc50\tc99\n0\t50\t99\n", "select c0, c50, c99 from dst_extreme");
        });
    }

    @Test
    public void testInsertAsSelectWithComplexTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by DAY");

            execute("create table dst (" +
                    "ts timestamp, " +
                    "i int, " +
                    "l long, " +
                    "d double, " +
                    "str string, " +
                    "sym symbol, " +
                    "b boolean, " +
                    "dt date" +
                    ") timestamp(ts) partition by DAY");

            execute("insert into src values " +
                    "(0, 1, 2, 3.14, 'hello', 'world', true, '2024-01-01'), " +
                    "(1000000, 10, 20, 2.71, 'foo', 'bar', false, '2024-02-01'), " +
                    "(2000000, null, null, null, null, null, null, null)");

            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithManyRows() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 50 columns
            buildCreateTableSql(sink, "src", 50);
            execute(sink);

            buildCreateTableSql(sink, "dst", 50);
            execute(sink);

            // Insert test data with multiple rows
            buildBatchInsertValuesSql(sink, "src", 50, 10);
            execute(sink);

            // Copy data
            execute("insert into dst select * from src");

            // Verify
            assertSql("count\n10\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithPartitionedTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, val int) timestamp(ts) partition by MONTH");
            execute("create table dst (ts timestamp, val int) timestamp(ts) partition by MONTH");

            // Insert data across multiple months
            execute("insert into src values " +
                    "('2024-01-15', 1), " +
                    "('2024-02-15', 2), " +
                    "('2024-03-15', 3), " +
                    "('2024-04-15', 4)");

            execute("insert into dst select * from src");

            assertSql("count\n4\n", "select count(*) from dst");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "src", "dst", LOG);
        });
    }

    @Test
    public void testInsertAsSelectWithStringSymbolConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src_str (ts timestamp, val string) timestamp(ts) partition by DAY");
            execute("create table dst_sym (ts timestamp, val symbol) timestamp(ts) partition by DAY");

            execute("insert into src_str values (0, 'apple'), (1000000, 'banana'), (2000000, 'cherry')");
            execute("insert into dst_sym select * from src_str");

            assertSql("val\napple\nbanana\ncherry\n", "select val from dst_sym order by ts");

            // Test reverse direction
            execute("create table src_sym (ts timestamp, val symbol) timestamp(ts) partition by DAY");
            execute("create table dst_str (ts timestamp, val string) timestamp(ts) partition by DAY");

            execute("insert into src_sym values (0, 'red'), (1000000, 'green'), (2000000, 'blue')");
            execute("insert into dst_str select * from src_sym");

            assertSql("val\nred\ngreen\nblue\n", "select val from dst_str order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithTypeConversions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table src (ts timestamp, b byte, s short, i int) timestamp(ts) partition by DAY");
            execute("create table dst (ts timestamp, b long, s long, i long) timestamp(ts) partition by DAY");

            execute("insert into src values (0, 1, 2, 3), (1000000, 10, 20, 30), (2000000, 100, 200, 300)");
            execute("insert into dst select * from src");

            assertSql("count\n3\n", "select count(*) from dst");
            assertSql("b\ts\ti\n1\t2\t3\n10\t20\t30\n100\t200\t300\n", "select b, s, i from dst order by ts");
        });
    }

    @Test
    public void testInsertAsSelectWithUnionExcept() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables with many columns to trigger loop-based copier
            buildCreateTableSql(sink, "t1", 100);
            execute(sink);

            buildCreateTableSql(sink, "t2", 100);
            execute(sink);

            buildCreateTableSql(sink, "result", 100);
            execute(sink);

            // Insert data
            buildInsertValuesSql(sink, "t1", 0, 100, 0);
            execute(sink);

            buildInsertValuesSql(sink, "t2", 1000000, 100, 100);
            execute(sink);

            // Test UNION
            execute("insert into result select * from t1 union select * from t2");
            assertSql("count\n2\n", "select count(*) from result");
        });
    }

    @Test
    public void testInsertAsSelectWithWhere() throws Exception {
        assertMemoryLeak(() -> {
            // Create source table with 'id' column + 50 regular columns using long_sequence
            sink.clear();
            sink.put("create table src as (select cast((x - 1) * 1000000 as timestamp) as ts, cast(x - 1 as int) as id");
            for (int i = 0; i < 50; i++) {
                sink.put(", cast((x - 1) * 100 + ").put(i).put(" as int) as col").put(i);
            }
            sink.put(" from long_sequence(10)) timestamp(ts) partition by DAY");
            execute(sink);

            sink.clear();
            sink.put("create table dst (ts timestamp, id int");
            for (int i = 0; i < 50; i++) {
                sink.put(", col").put(i).put(" int");
            }
            sink.put(") timestamp(ts) partition by DAY");
            execute(sink);

            // Insert only rows where id > 5
            execute("insert into dst select * from src where id > 5");

            assertSql("count\n4\n", "select count(*) from dst");
            assertSql("id\n6\n7\n8\n9\n", "select id from dst order by id");
        });
    }

    /**
     * Builds an INSERT VALUES SQL statement with multiple rows.
     *
     * @param sink        the StringSink to write the SQL to
     * @param tableName   the name of the table
     * @param columnCount number of columns
     * @param rowCount    number of rows to insert
     */
    private static void buildBatchInsertValuesSql(
            StringSink sink,
            String tableName,
            int columnCount,
            int rowCount
    ) {
        sink.clear();
        sink.put("insert into ").put(tableName).put(" values ");
        for (int row = 0; row < rowCount; row++) {
            if (row > 0) {
                sink.put(", ");
            }
            sink.put('(').put(row * 1000000L);
            for (int col = 0; col < columnCount; col++) {
                sink.put(", ").put(row * 100 + col);
            }
            sink.put(')');
        }
    }

    /**
     * Builds a CREATE TABLE SQL statement with the specified columns.
     *
     * @param sink        the StringSink to write the SQL to
     * @param tableName   the name of the table
     * @param columnCount number of integer columns to create
     */
    private static void buildCreateTableSql(StringSink sink, String tableName, int columnCount) {
        sink.clear();
        sink.put("create table ").put(tableName).put(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            sink.put(", col").put(i).put(" int");
        }
        sink.put(") timestamp(ts) partition by DAY");
    }

    /**
     * Builds a CREATE TABLE SQL statement with custom column definitions.
     *
     * @param sink         the StringSink to write the SQL to
     * @param tableName    the name of the table
     * @param columnCount  number of columns to create
     * @param columnPrefix prefix for column names (e.g., "col", "c")
     * @param typeProvider function that returns the type for each column index
     */
    private static void buildCreateTableSqlWithTypes(
            StringSink sink,
            String tableName,
            int columnCount,
            String columnPrefix,
            ColumnTypeProvider typeProvider
    ) {
        sink.clear();
        sink.put("create table ").put(tableName).put(" (ts timestamp");
        for (int i = 0; i < columnCount; i++) {
            sink.put(", ").put(columnPrefix).put(i).put(" ").put(typeProvider.getType(i));
        }
        sink.put(") timestamp(ts) partition by DAY");
    }

    /**
     * Builds an INSERT VALUES SQL statement with a single row.
     *
     * @param sink        the StringSink to write the SQL to
     * @param tableName   the name of the table
     * @param timestamp   the timestamp value
     * @param columnCount number of columns
     * @param valueOffset offset to add to each column value
     */
    private static void buildInsertValuesSql(
            StringSink sink,
            String tableName,
            long timestamp,
            int columnCount,
            int valueOffset
    ) {
        sink.clear();
        sink.put("insert into ").put(tableName).put(" values (").put(timestamp);
        for (int i = 0; i < columnCount; i++) {
            sink.put(", ").put(valueOffset + i);
        }
        sink.put(")");
    }

    @FunctionalInterface
    private interface ColumnTypeProvider {
        String getType(int columnIndex);
    }
}
