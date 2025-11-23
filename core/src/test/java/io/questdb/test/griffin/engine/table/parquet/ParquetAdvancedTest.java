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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Comprehensive tests for advanced parquet features:
 * - Parquet projection (column selection)
 * - Glob reads (multiple files)
 * - calculateSize() functionality
 * - skipRows() functionality
 * - SQL variant support (UNION, JOIN, WHERE filters, GROUP BY, aggregates)
 */
@RunWith(Parameterized.class)
public class ParquetAdvancedTest extends AbstractCairoTest {
    private final boolean parallel;

    public ParquetAdvancedTest(boolean parallel) {
        this.parallel = parallel;
    }

    @Parameterized.Parameters(name = "parallel={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true},
                {false},
        });
    }

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, String.valueOf(parallel));
        super.setUp();
        inputRoot = root;
    }

    // ==================== PROJECTION TESTS ====================

    @Test
    public void testAggregateCount() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select x id from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("agg_count.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) as cnt from read_parquet('agg_count.parquet')");
                assertQueryNoLeakCheck(
                        """
                                cnt
                                100
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testAggregateMultiple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x id," +
                    " cast(x * 10 as int) as amount," +
                    " rnd_int(1, 100, 0) as value" +
                    " from long_sequence(20))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("agg_multiple.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*), sum(amount), min(amount), max(amount) " +
                        "from read_parquet('agg_multiple.parquet')");
                assertQueryNoLeakCheck(
                        """
                                count\tsum\tmin\tmax
                                20\t2100\t10\t200
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    // ==================== GLOB READS TESTS ====================

    @Test
    public void testAggregateSum() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select x id, cast(x * 10 as int) as amount from long_sequence(10))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("agg_sum.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select sum(amount) as total from read_parquet('agg_sum.parquet')");
                assertQueryNoLeakCheck(
                        """
                                total
                                550
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testCalculateSizeAccuracy() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1000;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as value" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("calc_size.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // Verify size calculation matches actual row count
                sink.clear();
                sink.put("select count(*) from read_parquet('calc_size.parquet')");
                assertQueryNoLeakCheck(
                        """
                                count
                                1000
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    // ==================== CALCULATE_SIZE TESTS ====================

    @Test
    public void testCalculateSizeMultipleFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x id from long_sequence(100))");
            execute("create table t2 as (select x id from long_sequence(50))");
            execute("create table t3 as (select x id from long_sequence(75))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader r1 = engine.getReader("t1");
                    TableReader r2 = engine.getReader("t2");
                    TableReader r3 = engine.getReader("t3")
            ) {
                path.of(root).concat("calc_1.parquet");
                PartitionEncoder.populateFromTableReader(r1, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                descriptor.clear();
                path.of(root).concat("calc_2.parquet");
                PartitionEncoder.populateFromTableReader(r2, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                descriptor.clear();
                path.of(root).concat("calc_3.parquet");
                PartitionEncoder.populateFromTableReader(r3, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                String globPath = root + "/calc_*.parquet";
                sink.clear();
                sink.put("select count(*) from read_parquet('" + globPath + "')");
                assertQueryNoLeakCheck(
                        """
                                count
                                225
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testDistinctWithParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " rnd_str(2,2,2,0) as category," +
                    " x id" +
                    " from long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("distinct.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select distinct category from read_parquet('distinct.parquet') order by category");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertTrue(count > 0);
                        }
                    }
                }
            }
        });
    }

    // ==================== SKIP_ROWS TESTS ====================

    @Test
    public void testEmptyResultSet() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select x id from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("empty_result.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select * from read_parquet('empty_result.parquet') where id > 1000");
                assertQueryNoLeakCheck(
                        "id\n",
                        sink.toString(),
                        null,
                        parallel,
                        false
                );
            }
        });
    }

    @Test
    public void testGlobEmptyPattern() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("nonexistent_*.parquet").$();

                // Attempt to read non-existent glob pattern
                try {
                    select("select * from read_parquet('" + path + "')").close();
                    Assert.fail("Should have failed for non-existent glob pattern");
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "did not return any readable parquet files");
                }
            }
        });
    }

    // ==================== UNION TESTS ====================

    @Test
    public void testGlobMultipleFilesWithProjection() throws Exception {
        assertMemoryLeak(() -> {
            // Create multiple tables
            execute("create table t1 as (select" +
                    " cast(x as int) id," +
                    " 'file1' as source," +
                    " rnd_int() as value" +
                    " from long_sequence(5))");

            execute("create table t2 as (select" +
                    " cast(x+5 as int) id," +
                    " 'file2' as source," +
                    " rnd_int() as value" +
                    " from long_sequence(5))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader1 = engine.getReader("t1");
                    TableReader reader2 = engine.getReader("t2")
            ) {
                path.of(root).concat("glob_1.parquet");
                PartitionEncoder.populateFromTableReader(reader1, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                descriptor.clear();
                path.of(root).concat("glob_2.parquet");
                PartitionEncoder.populateFromTableReader(reader2, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // Read with projection from glob pattern
                String globPath = root + "/glob_*.parquet";
                sink.clear();
                sink.put("select id, source from read_parquet('" + globPath + "') order by id");

                assertQueryNoLeakCheck(
                        """
                                id\tsource
                                1\tfile1
                                2\tfile1
                                3\tfile1
                                4\tfile1
                                5\tfile1
                                6\tfile2
                                7\tfile2
                                8\tfile2
                                9\tfile2
                                10\tfile2
                                """,
                        sink.toString(),
                        null,
                        true,
                        false
                );
            }
        });
    }

    @Test
    public void testGroupByMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " rnd_str(2,2,2,0) as cat1," +
                    " rnd_str(2,2,2,0) as cat2," +
                    " cast(x as int) as amount" +
                    " from long_sequence(50))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("group_multi.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select * from (select cat1, cat2, sum(amount) as total from read_parquet('group_multi.parquet') " +
                        "group by cat1, cat2) where total > 0");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertTrue(count > 0);
                        }
                    }
                }
            }
        });
    }

    // ==================== JOIN TESTS ====================

    @Test
    public void testGroupBySimple() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " rnd_str(2,2,2,0) as category," +
                    " x id" +
                    " from long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("group_by.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select category, count(*) as cnt from read_parquet('group_by.parquet') group by category order by category");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertTrue(count > 0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testJoinParquetAndTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table left_tbl as (select x id, 'left' as label from long_sequence(5))");
            execute("create table right_tbl as (select x id, 'right' as label from long_sequence(5))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("right_tbl")
            ) {
                path.of(root).concat("join_right.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select l.id, l.label as left_label, r.label as right_label " +
                        "from left_tbl l join read_parquet('join_right.parquet') r on l.id = r.id " +
                        "order by l.id");
                assertQueryNoLeakCheck(
                        """
                                id\tleft_label\tright_label
                                1\tleft\tright
                                2\tleft\tright
                                3\tleft\tright
                                4\tleft\tright
                                5\tleft\tright
                                """,
                        sink.toString(),
                        null,
                        true,
                        !parallel
                );
            }
        });
    }

    @Test
    public void testJoinTwoParquetFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x id, rnd_int() as value1 from long_sequence(5))");
            execute("create table t2 as (select x id, rnd_int() as value2 from long_sequence(5))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader r1 = engine.getReader("t1");
                    TableReader r2 = engine.getReader("t2")
            ) {
                path.of(root).concat("join_left.parquet");
                PartitionEncoder.populateFromTableReader(r1, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                descriptor.clear();
                path.of(root).concat("join_right.parquet");
                PartitionEncoder.populateFromTableReader(r2, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select l.id, l.value1, r.value2 " +
                        "from read_parquet('join_left.parquet') l " +
                        "join read_parquet('join_right.parquet') r on l.id = r.id");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertEquals(5, count);
                        }
                    }
                }
            }
        });
    }

    // ==================== WHERE FILTER TESTS ====================

    @Test
    public void testLargeRowCount() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10000;
            execute("create table x as (select x id, rnd_int() as value from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("large_rowcount.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('large_rowcount.parquet')");
                assertQueryNoLeakCheck(
                        """
                                count
                                10000
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testLeftJoinWithParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x id from long_sequence(5))");
            execute("create table t2 as (select x id, rnd_str(2,2,2,0) as data from long_sequence(3))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("t2")
            ) {
                path.of(root).concat("leftjoin.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select t1.id, r.data from t1 left join read_parquet('leftjoin.parquet') r on t1.id = r.id order by t1.id");
                assertQueryNoLeakCheck(
                        "id\tdata\n" +
                                "1\tJW\n" +
                                "2\tJW\n" +
                                "3\tVT\n" +
                                "4\t\n" +
                                "5\t\n",
                        sink.toString(),
                        null,
                        true,
                        false

                );
            }
        });
    }

    @Test
    public void testLimitWithParquet() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select x id from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("limit_test.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('limit_test.parquet') limit 1");
                assertQueryNoLeakCheck(
                        """
                                count
                                100
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    // ==================== AGGREGATE TESTS ====================

    @Test
    public void testOffsetWithParquet() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select x id from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("offset_test.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select id from read_parquet('offset_test.parquet') order by id limit 5, 15");
                assertQueryNoLeakCheck(
                        "id\n" +
                                "6\n" +
                                "7\n" +
                                "8\n" +
                                "9\n" +
                                "10\n" +
                                "11\n" +
                                "12\n" +
                                "13\n" +
                                "14\n" +
                                "15\n",
                        sink.toString(),
                        null,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testOrderByWithParquet() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 50;
            execute("create table x as (select x id, rnd_int() as value from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("order_by.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select id from read_parquet('order_by.parquet') order by value desc limit 5");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertEquals(5, count);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionSelectSubset() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as int_col," +
                    " rnd_long() as long_col," +
                    " rnd_double() as double_col," +
                    " rnd_str(4,4,4,2) as str_col" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("projection_test.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // Select only specific columns
                sink.clear();
                sink.put("select id, str_col from read_parquet('projection_test.parquet') order by id limit 10");
                assertQueryNoLeakCheck(
                        "id\tstr_col\n" +
                                "1\tPEHN\n" +
                                "2\t\n" +
                                "3\tVTJW\n" +
                                "4\t\n" +
                                "5\tVTJW\n" +
                                "6\tHYRX\n" +
                                "7\t\n" +
                                "8\t\n" +
                                "9\t\n" +
                                "10\t\n",
                        sink.toString(),
                        null,
                        true,
                        true
                );
            }
        });
    }

    // ==================== GROUP BY TESTS ====================

    @Test
    public void testProjectionSelectedColumnsOrder() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 50;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as a," +
                    " rnd_int() as b," +
                    " rnd_int() as c," +
                    " rnd_int() as d" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("projection_order.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // Select columns in different order
                sink.clear();
                sink.put("select d, b, id from read_parquet('projection_order.parquet') where id <= 5");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            int count = 0;
                            while (cursor.hasNext()) {
                                count++;
                            }
                            Assert.assertEquals(5, count);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSkipRowsWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as value" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("skip_rows.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                // LIMIT should trigger skipRows
                sink.clear();
                sink.put("select * from read_parquet('skip_rows.parquet') order by id limit 5, 15");
                assertQueryNoLeakCheck(
                        "id\tvalue\n" +
                                "6\t-948263339\n" +
                                "7\t1326447242\n" +
                                "8\t592859671\n" +
                                "9\t1868723706\n" +
                                "10\t-847531048\n" +
                                "11\t-1191262516\n" +
                                "12\t-2041844972\n" +
                                "13\t-1436881714\n" +
                                "14\t-1575378703\n" +
                                "15\t806715481\n",
                        sink.toString(),
                        null,
                        true,
                        true
                );
            }
        });
    }

    // ==================== EDGE CASES ====================

    @Test
    public void testSkipRowsWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 200;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as value" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("skip_where.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('skip_where.parquet') where id > 100");
                assertQueryNoLeakCheck(
                        """
                                count
                                100
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testUnionParquetAndTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x id, 'table' as source from long_sequence(5))");
            execute("create table t2 as (select x id, 'other' as source from long_sequence(5))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("t2")
            ) {
                path.of(root).concat("union_parquet.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select id, source from t1 union all select id, source from read_parquet('union_parquet.parquet') order by source, id");
                assertQueryNoLeakCheck(
                        """
                                id\tsource
                                1\tother
                                2\tother
                                3\tother
                                4\tother
                                5\tother
                                1\ttable
                                2\ttable
                                3\ttable
                                4\ttable
                                5\ttable
                                """,
                        sink.toString(),
                        null,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testUnionTwoParquetFiles() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t1 as (select x id, 'table1' as source from long_sequence(10))");
            execute("create table t2 as (select x id, 'table2' as source from long_sequence(10))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader r1 = engine.getReader("t1");
                    TableReader r2 = engine.getReader("t2")
            ) {
                path.of(root).concat("union_1.parquet");
                PartitionEncoder.populateFromTableReader(r1, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                descriptor.clear();
                path.of(root).concat("union_2.parquet");
                PartitionEncoder.populateFromTableReader(r2, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select id, source from read_parquet('union_1.parquet') " +
                        "union all select id, source from read_parquet('union_2.parquet') " +
                        "order by id");
                assertQueryNoLeakCheck(
                        """
                                id\tsource
                                1\ttable1
                                1\ttable2
                                2\ttable1
                                2\ttable2
                                3\ttable1
                                3\ttable2
                                4\ttable1
                                4\ttable2
                                5\ttable1
                                5\ttable2
                                6\ttable1
                                6\ttable2
                                7\ttable1
                                7\ttable2
                                8\ttable1
                                8\ttable2
                                9\ttable1
                                9\ttable2
                                10\ttable1
                                10\ttable2
                                """,
                        sink.toString(),
                        null,
                        true,
                        true
                );
            }
        });
    }

    @Test
    public void testWhereComplexCondition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x id," +
                    " rnd_int() as value," +
                    " rnd_str(2,2,2,0) as category" +
                    " from long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("where_complex.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('where_complex.parquet') where (id > 25 and id < 75) or category = 'XY'");
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testWhereSimpleComparison() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute("create table x as (select x id, rnd_int() as value from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("where_simple.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('where_simple.parquet') where id > 50");
                assertQueryNoLeakCheck(
                        """
                                count
                                50
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }

    @Test
    public void testWhereWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x as (select" +
                    " x id," +
                    " case when x % 2 = 0 then rnd_int() end as nullable_value" +
                    " from long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("where_nulls.parquet");
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path);

                sink.clear();
                sink.put("select count(*) from read_parquet('where_nulls.parquet') where nullable_value is not null");
                assertQueryNoLeakCheck(
                        """
                                count
                                50
                                """,
                        sink.toString(),
                        null,
                        false,
                        true
                );
            }
        });
    }
}
