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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetCompression;
import io.questdb.griffin.engine.table.parquet.ParquetVersion;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.nanotime.Nanos;
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

import static io.questdb.cairo.TableUtils.PARQUET_PARTITION_NAME;

@RunWith(Parameterized.class)
public class ReadParquetFunctionTest extends AbstractCairoTest {
    private final boolean parallel;

    public ReadParquetFunctionTest(boolean parallel) {
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

    @Test
    public void testBloomFilterPushdown() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 5000;
            execute("CREATE TABLE x AS (SELECT" +
                    " CAST(x AS INT) AS id," +
                    " CAST('val_' || x AS VARCHAR) AS name," +
                    " rnd_uuid4() AS uid," +
                    " timestamp_sequence(0, 1_000_000) AS ts" +
                    " FROM long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x");
                    DirectLongList bloomFilterColumnIndexes = new DirectLongList(3, MemoryTag.NATIVE_DEFAULT)
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                bloomFilterColumnIndexes.add(0);
                bloomFilterColumnIndexes.add(1);
                bloomFilterColumnIndexes.add(2);

                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        1000,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        bloomFilterColumnIndexes.getAddress(),
                        (int) bloomFilterColumnIndexes.size(),
                        0.01,
                        0.0,
                        -1L
                );
                Assert.assertTrue(Files.exists(path.$()));
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "id\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id = -999",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                assertQueryNoLeakCheck(
                        "id\n42\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id = 42",
                        null, parallel, false
                );
                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "name\n",
                        "SELECT name FROM read_parquet('x.parquet') WHERE name = 'no_such_value'",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                assertQueryNoLeakCheck(
                        "name\nval_100\n",
                        "SELECT name FROM read_parquet('x.parquet') WHERE name = 'val_100'",
                        null, parallel, false
                );

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "id\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id IN (-1, -2, -3)",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                bindVariableService.clear();
                bindVariableService.setInt("v", -999);
                assertQueryNoLeakCheck(
                        "id\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id = :v",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                bindVariableService.clear();
                bindVariableService.setInt("v", 42);
                assertQueryNoLeakCheck(
                        "id\n42\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id = :v",
                        null, parallel, false
                );

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                bindVariableService.clear();
                bindVariableService.setStr("v", "no_such_value");
                assertQueryNoLeakCheck(
                        "name\n",
                        "SELECT name FROM read_parquet('x.parquet') WHERE name = :v",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                bindVariableService.clear();
                bindVariableService.setInt(0, -999);
                assertQueryNoLeakCheck(
                        "id\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id = $1",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "id\n",
                        "SELECT id FROM read_parquet('x.parquet') WHERE id IS NULL",
                        null, parallel, false
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);

                assertQueryNoLeakCheck(
                        "cnt\n" + rows + "\n",
                        "SELECT COUNT(*) cnt FROM read_parquet('x.parquet') WHERE id IS NOT NULL",
                        null, false, true
                );

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "cnt\n10\n",
                        "SELECT COUNT(*) cnt FROM read_parquet('x.parquet') WHERE id BETWEEN 10 AND 1",
                        null, false, true
                );

                ParquetRowGroupFilter.resetRowGroupsSkipped();
                assertQueryNoLeakCheck(
                        "cnt\n0\n",
                        "SELECT COUNT(*) cnt FROM read_parquet('x.parquet') WHERE id BETWEEN -100 AND -50",
                        null, false, true
                );
                Assert.assertTrue(ParquetRowGroupFilter.getRowGroupsSkipped() > 0);
            }
        });
    }

    @Test
    public void testColumnMapping() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_long() end as a_long," +
                    " case when x % 2 = 0 then rnd_int() end as an_int," +
                    " rnd_timestamp('2015','2016',2) as a_ts," +
                    " rnd_timestamp_ns('2015','2016',2) as a_ns" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));
                sink.clear();
                sink.put("select a_ts, a_long from read_parquet('x.parquet')");
                // If projection pushdown is not working, a SelectedRecord operator would appear
                // above the parquet scan in the plan.
                final String expectedPlan = parallel ? """
                        parquet page frame scan
                          columns: a_ts,a_long
                        """ : """
                        parquet file sequential scan
                          columns: a_ts,a_long
                        """;
                assertPlanNoLeakCheck(
                        sink,
                        expectedPlan
                );

                assertSqlCursors0("select a_ts, a_long from x");
            }
        });
    }

    @Test
    public void testColumnProjectionDifferentOrder() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " rnd_str(4,4,4,2) as a_str," +
                    " rnd_long() as a_long," +
                    " rnd_int() as an_int" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                final String expectedPlan = parallel ? """
                        parquet page frame scan
                          columns: an_int,a_long,a_str
                        """ : """
                        parquet file sequential scan
                          columns: an_int,a_long,a_str
                        """;
                sink.clear();
                sink.put("select an_int, a_long, a_str from read_parquet('x.parquet')");
                assertPlanNoLeakCheck(
                        sink,
                        expectedPlan
                );
                assertSqlCursors0("select an_int, a_long, a_str from x");
            }
        });
    }

    @Test
    public void testColumnProjectionSingleColumn() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " rnd_str(4,4,4,2) as a_str," +
                    " rnd_long() as a_long," +
                    " rnd_int() as an_int" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                sink.clear();
                sink.put("select a_long from read_parquet('x.parquet')");
                // Select single column
                final String expectedPlan = parallel ? """
                        parquet page frame scan
                          columns: a_long
                        """ : """
                        parquet file sequential scan
                          columns: a_long
                        """;
                assertPlanNoLeakCheck(
                        sink,
                        expectedPlan
                );
                assertSqlCursors0("select a_long from x");
            }
        });
    }

    @Test
    public void testColumnProjectionWithExpression() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " rnd_str(4,4,4,2) as a_str," +
                    " rnd_long() as a_long," +
                    " rnd_int() as an_int" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                final String expectedPlan = parallel ? """
                        VirtualRecord
                          functions: [a_long+1]
                            parquet page frame scan
                              columns: a_long
                        """ : """
                        VirtualRecord
                          functions: [a_long+1]
                            parquet file sequential scan
                              columns: a_long
                        """;
                assertPlanNoLeakCheck(
                        "select a_long + 1 from read_parquet('x.parquet')",
                        expectedPlan
                );
            }
        });
    }

    @Test
    public void testCursor() throws Exception {
        testCursor(false);
    }

    @Test
    public void testCursor_rawArrayEncoding() throws Exception {
        testCursor(true);
    }

    @Test
    public void testData() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100_000;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then cast(x as int) end id," +
                    " case when x % 2 = 0 then rnd_int() end as a_long," +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                    " case when x % 2 = 0 then rnd_double_array(1) end as an_array," +
                    " case when x % 2 = 0 then rnd_boolean() end a_boolean," +
                    " case when x % 2 = 0 then rnd_short() end a_short," +
                    " case when x % 2 = 0 then rnd_byte() end a_byte," +
                    " case when x % 2 = 0 then rnd_char() end a_char," +
                    " case when x % 2 = 0 then rnd_uuid4() end a_uuid," +
                    " case when x % 2 = 0 then rnd_double() end a_double," +
                    " case when x % 2 = 0 then rnd_float() end a_float," +
                    " case when x % 2 = 0 then rnd_symbol(4,4,4,2) end as a_sym," +
                    " cast(rnd_timestamp('2015','2016',2) as date) as a_date," +
                    " rnd_long256() a_long256," +
                    " to_long128(rnd_long(), rnd_long()) a_long128," +
                    " rnd_ipv4() a_ip," +
                    " rnd_geohash(4) a_geo_byte," +
                    " rnd_geohash(8) a_geo_short," +
                    " rnd_geohash(16) a_geo_int," +
                    " rnd_geohash(32) a_geo_long," +
                    " rnd_bin(10, 20, 2) a_bin," +
                    " rnd_timestamp('2015','2016',2) as a_ts," +
                    " rnd_timestamp_ns('2015','2016',2) as a_ns," +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("select * from read_parquet('x.parquet')");
                assertSqlCursors0("x");
            }
        });
    }

    @Test
    public void testDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 100;
            execute(
                    "create table x as (select" +
                            " x id," +
                            " (x*1000000)::timestamp ts" +
                            " from long_sequence(" + rows + ")" +
                            ") timestamp(ts) partition by day"
            );

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                // No sorting is needed since we recognize the designated timestamp.
                final String query = "select * from read_parquet('x.parquet') order by ts";
                final String expectedPlan = parallel ? """
                        parquet page frame scan
                          columns: id,ts
                        """ : """
                        parquet file sequential scan
                          columns: id,ts
                        """;
                assertPlanNoLeakCheck(
                        query,
                        expectedPlan
                );
                assertQueryNoLeakCheck(
                        """
                                id\tts
                                1\t1970-01-01T00:00:01.000000Z
                                2\t1970-01-01T00:00:02.000000Z
                                3\t1970-01-01T00:00:03.000000Z
                                4\t1970-01-01T00:00:04.000000Z
                                5\t1970-01-01T00:00:05.000000Z
                                6\t1970-01-01T00:00:06.000000Z
                                7\t1970-01-01T00:00:07.000000Z
                                8\t1970-01-01T00:00:08.000000Z
                                9\t1970-01-01T00:00:09.000000Z
                                10\t1970-01-01T00:00:10.000000Z
                                """,
                        query + " limit 10",
                        "ts",
                        parallel,
                        true
                );
            }
        });
    }

    @Test
    public void testFileDeleted() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then cast(x as int) end id," +
                    " rnd_timestamp('2015','2016',2) as a_ts," +
                    " rnd_timestamp_ns('2015','2016',2) as a_ns," +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                sink.clear();
                sink.put("select * from read_parquet('x.parquet')");

                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        engine.getConfiguration().getFilesFacade().remove(path.$());
                        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "could not open, file does not exist");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFileDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                path.of(root).concat("x.parquet").$();

                // Assert 0 rows, header only
                try {
                    select("select * from read_parquet('" + path + "')  where 1 = 2").close();
                    Assert.fail();
                } catch (SqlException e) {
                    TestUtils.assertContains(e.getMessage(), "could not open, file does not exist");
                }
            }
        });
    }

    @Test
    public void testFileSchemaChanged() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " x id," +
                            " timestamp_sequence(0,10000) as ts," +
                            " timestamp_sequence(0,10000)::timestamp_ns as ns" +
                            " from long_sequence(1))"
            );
            execute(
                    "create table y as (select" +
                            " x id," +
                            " timestamp_sequence(0,10000) as ts1," +
                            " timestamp_sequence(0,10000)::timestamp_ns as ns," +
                            " 'foobar' str," +
                            " from long_sequence(1))"
            );

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader readerX = engine.getReader("x");
                    TableReader readerY = engine.getReader("y")
            ) {
                path.of(root).concat("table.parquet").$();
                PartitionEncoder.populateFromTableReader(readerX, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);

                sink.clear();
                sink.put("select * from read_parquet('table.parquet')");

                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                        }

                        // delete the file and populate from y table
                        engine.getConfiguration().getFilesFacade().remove(path.$());
                        PartitionEncoder.populateFromTableReader(readerY, partitionDescriptor, 0);
                        PartitionEncoder.encode(partitionDescriptor, path);

                        // Query the data once again - this time the Parquet schema is different.
                        try {
                            try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                                Assert.fail();
                            }
                        } catch (TableReferenceOutOfDateException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), path.asAsciiCharSequence());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLimitOffsetWithMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 5000;
            execute("create table x as (select" +
                    " x as id," +
                    " rnd_long() as a_long" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        1000,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0
                );
                Assert.assertTrue(Files.exists(path.$()));
                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit 100, 500");
                assertSqlCursors0("select * from x limit 100, 500");

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit 100, 1950");
                assertSqlCursors0("select * from x limit 100, 1950");

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit 999, 2501");
                assertSqlCursors0("select * from x limit 999, 2501");

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit 5000, 2");
                assertSqlCursors0("select * from x limit 5000, 2");

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit 5001, 2");
                assertSqlCursors0("select * from x limit 5001, 2");

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') limit -2, -1999");
                assertSqlCursors0("select * from x limit -2, -1999");
            }
        });
    }

    @Test
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1;
            execute("create table x as (select" +
                    " x id," +
                    " rnd_boolean() a_boolean," +
                    " rnd_byte() a_byte," +
                    " rnd_short() a_short," +
                    " rnd_int() an_int," +
                    " rnd_long() a_long," +
                    " rnd_float() a_float," +
                    " rnd_double() a_double," +
                    " rnd_varchar('ганьба','слава','добрий','вечір') a_varchar," +
                    " rnd_uuid4() a_uuid," +
                    " cast(timestamp_sequence(600000000000, 700) as date) a_date," +
                    " timestamp_sequence(500000000000, 600) a_ts," +
                    " timestamp_sequence_ns(500000000000000, 600000) a_ns," +
                    " timestamp_sequence(400000000000, 500) designated_ts" +
                    " from long_sequence(" + rows + ")) timestamp(designated_ts) partition by month");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                // Assert 0 rows, header only
                sink.clear();
                sink.put("select * from read_parquet('x.parquet')");

                if (parallel) {
                    assertPlanNoLeakCheck(sink, """ 
                            parquet page frame scan
                              columns: id,a_boolean,a_byte,a_short,an_int,a_long,a_float,a_double,a_varchar,a_uuid,a_date,a_ts,a_ns,designated_ts
                            """
                    );
                } else {
                    assertPlanNoLeakCheck(sink, """ 
                            parquet file sequential scan
                              columns: id,a_boolean,a_byte,a_short,an_int,a_long,a_float,a_double,a_varchar,a_uuid,a_date,a_ts,a_ns,designated_ts
                            """);
                }

                sink.put(" where 1 = 2");
                assertSqlCursors0("x where 1 = 2");
            }
        });
    }

    @Test
    public void testNativeSymbolColumnReadBack() throws Exception {
        // Verifies that read_parquet() can decode SYMBOL columns stored in QuestDB's
        // native parquet encoding (dictionary-encoded BYTE_ARRAY with LocalKeyIsGlobal format).
        // read_parquet() converts SYMBOL to VARCHAR in its metadata, so the Rust decoder
        // must resolve dictionary entries to UTF-8 strings rather than returning INT32 keys.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES ('AAA', 1, '2024-01-01T00:00:00.000000Z')");
            execute("INSERT INTO x VALUES ('BBB', 2, '2024-01-01T01:00:00.000000Z')");
            execute("INSERT INTO x VALUES ('AAA', 3, '2024-01-01T02:00:00.000000Z')");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                assertSql(
                        """
                                id\tval\tts
                                AAA\t1\t2024-01-01T00:00:00.000000Z
                                BBB\t2\t2024-01-01T01:00:00.000000Z
                                AAA\t3\t2024-01-01T02:00:00.000000Z
                                """,
                        "SELECT * FROM read_parquet('x.parquet')"
                );
            }
        });
    }

    @Test
    public void testOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " rnd_varchar('foo1', 'foo2', 'foo3') as a_varchar1," +
                    " rnd_varchar('bar1', 'bar2', 'bar3') as a_varchar2" +
                    " from long_sequence(" + rows + "))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("select * from read_parquet('x.parquet') order by a_varchar1, a_varchar2");
                assertSqlCursors0("select * from x order by a_varchar1, a_varchar2");
            }
        });
    }

    @Test
    public void testParquetVarcharAllNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " NULL::VARCHAR AS v" +
                    " FROM long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('hello', 'world', 'foo', 'bar') AS v" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharConcat() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('hello', 'world') AS v" +
                    " FROM long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT id, concat(v, '!') AS v2 FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT id, concat(v, '!') AS v2 FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharEmptyStrings() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 2 = 0 THEN '' ELSE rnd_varchar('hello', 'world') END AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('a', 'b', 'c', 'd') AS v" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT v, count() FROM read_parquet('x.parquet') GROUP BY v ORDER BY v");
                assertSqlCursors0("SELECT v, count() FROM x GROUP BY v ORDER BY v");
            }
        });
    }

    @Test
    public void testParquetVarcharLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(1, 50, 0) AS v" +
                    " FROM long_sequence(100_000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(1, 40, 0) AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT id, length(v) AS len FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT id, length(v) AS len FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharLongStrings() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(20, 100, 0) AS v" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_int() AS an_int," +
                    " rnd_long() AS a_long," +
                    " rnd_double() AS a_double," +
                    " rnd_varchar('foo', 'bar', 'baz') AS a_varchar," +
                    " rnd_boolean() AS a_bool" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('a1', 'a2', 'a3') AS v1," +
                    " rnd_varchar('b1', 'b2', 'b3') AS v2," +
                    " rnd_varchar('c1', 'c2', 'c3') AS v3" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharNonAsciiRoundtrip() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE" +
                    "   WHEN x % 8 = 0 THEN NULL" +
                    "   WHEN x % 8 = 1 THEN CAST('héllo wörld' AS VARCHAR)" +
                    "   WHEN x % 8 = 2 THEN CAST('こんにちは世界' AS VARCHAR)" +
                    "   WHEN x % 8 = 3 THEN CAST('Привет мир' AS VARCHAR)" +
                    "   WHEN x % 8 = 4 THEN CAST('🎉🎊🎈🎁' AS VARCHAR)" +
                    "   WHEN x % 8 = 5 THEN CAST('مرحبا بالعالم' AS VARCHAR)" +
                    "   WHEN x % 8 = 6 THEN CAST('café résumé naïve' AS VARCHAR)" +
                    "   ELSE CAST('混合ABC日本語123' AS VARCHAR)" +
                    " END AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharNullCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 2 = 0 THEN rnd_varchar('yes', 'no') ELSE NULL END AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') WHERE v IS NOT NULL ORDER BY id");
                assertSqlCursors0("SELECT * FROM x WHERE v IS NOT NULL ORDER BY id");

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') WHERE v IS NULL ORDER BY id");
                assertSqlCursors0("SELECT * FROM x WHERE v IS NULL ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('zzz', 'aaa', 'mmm', 'bbb') AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY v, id");
                assertSqlCursors0("SELECT * FROM x ORDER BY v, id");
            }
        });
    }

    @Test
    public void testParquetVarcharUnicode() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('\u0433\u0430\u043d\u044c\u0431\u0430','\u0441\u043b\u0430\u0432\u0430','\u0434\u043e\u0431\u0440\u0438\u0439','\u0432\u0435\u0447\u0456\u0440') AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharWhereFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 4 = 0 THEN NULL ELSE rnd_varchar('alpha', 'beta', 'gamma', 'delta') END AS v" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') WHERE v IS NOT NULL ORDER BY id");
                assertSqlCursors0("SELECT * FROM x WHERE v IS NOT NULL ORDER BY id");
            }
        });
    }

    @Test
    public void testParquetVarcharWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 3 = 0 THEN NULL ELSE rnd_varchar('abc', 'def', 'ghi') END AS v" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceCastToSymbol() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT id, v::SYMBOL AS v FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT id, v::SYMBOL AS v FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceCompoundGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('a', 'b', 'c') AS v," +
                    " (x % 5)::INT AS grp" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT v, grp, count() AS cnt FROM read_parquet('x.parquet') GROUP BY v, grp ORDER BY v, grp");
                assertSqlCursors0("SELECT v, grp, count() AS cnt FROM x GROUP BY v, grp ORDER BY v, grp");
            }
        });
    }

    @Test
    public void testVarcharSliceCopyToLarge() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(5, 30, 1) AS v" +
                    " FROM long_sequence(10_000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                execute("CREATE TABLE y (id LONG, v VARCHAR)");
                execute("INSERT INTO y SELECT * FROM read_parquet('x.parquet')");

                sink.clear();
                sink.put("SELECT * FROM y ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceDistinct() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT DISTINCT v FROM read_parquet('x.parquet') ORDER BY v");
                assertSqlCursors0("SELECT DISTINCT v FROM x ORDER BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceEqualityFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') WHERE v = 'alpha' ORDER BY id");
                assertSqlCursors0("SELECT * FROM x WHERE v = 'alpha' ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceGroupByWithLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('abc', 'de', 'f', 'ghij', NULL) AS v" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT v, length(v) AS len, count() AS cnt FROM read_parquet('x.parquet') GROUP BY v, length(v) ORDER BY v");
                assertSqlCursors0("SELECT v, length(v) AS len, count() AS cnt FROM x GROUP BY v, length(v) ORDER BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceHighCardinalityGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(10, 30, 0) AS v" +
                    " FROM long_sequence(10_000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT v, count() AS cnt FROM read_parquet('x.parquet') GROUP BY v ORDER BY v");
                assertSqlCursors0("SELECT v, count() AS cnt FROM x GROUP BY v ORDER BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceInsertAsSelect() throws Exception {
        // INSERT INTO ... SELECT ... FROM read_parquet() must correctly copy
        // VARCHAR_SLICE columns into a native VARCHAR column.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('hello', 'world', NULL, '') AS v" +
                    " FROM long_sequence(100))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                execute("CREATE TABLE y (id LONG, v VARCHAR)");
                execute("INSERT INTO y SELECT * FROM read_parquet('x.parquet')");

                sink.clear();
                sink.put("SELECT * FROM y ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceIsNullFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 3 = 0 THEN NULL ELSE rnd_varchar('a', 'b', 'c') END AS v" +
                    " FROM long_sequence(300))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') WHERE v IS NULL ORDER BY id");
                assertSqlCursors0("SELECT * FROM x WHERE v IS NULL ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceJoin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma') AS v" +
                    " FROM long_sequence(100))");
            execute("CREATE TABLE b AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'delta') AS v," +
                    " rnd_long() AS val" +
                    " FROM long_sequence(100))");

            try (
                    Path pathA = new Path();
                    Path pathB = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader readerA = engine.getReader("a");
                    TableReader readerB = engine.getReader("b")
            ) {
                pathA.of(root).concat("a.parquet");
                PartitionEncoder.populateFromTableReader(readerA, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, pathA);
                Assert.assertTrue(Files.exists(pathA.$()));

                pathB.of(root).concat("b.parquet");
                PartitionEncoder.populateFromTableReader(readerB, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, pathB);
                Assert.assertTrue(Files.exists(pathB.$()));

                sink.clear();
                sink.put("SELECT a.id, a.v, b.val FROM read_parquet('a.parquet') a JOIN read_parquet('b.parquet') b ON a.v = b.v ORDER BY a.id, b.id");
                assertSqlCursors0("SELECT a.id, a.v, b.val FROM a JOIN b ON a.v = b.v ORDER BY a.id, b.id");
            }
        });
    }

    @Test
    public void testVarcharSliceLatestOn() throws Exception {
        // LATEST ON with a VARCHAR_SLICE column from read_parquet() must not
        // throw "invalid type" error.
        // Parallel parquet scan does not support LATEST ON, so skip in parallel mode.
        if (parallel) {
            return;
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_varchar('a', 'b', 'c') AS v," +
                    " x AS val," +
                    " timestamp_sequence('2024-01-01', 1000000) AS ts" +
                    " FROM long_sequence(20)) TIMESTAMP(ts)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') LATEST ON ts PARTITION BY v");
                assertSqlCursors0("SELECT * FROM x LATEST ON ts PARTITION BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceLimitOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(500))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id LIMIT 50, 60");
                assertSqlCursors0("SELECT * FROM x ORDER BY id LIMIT 50, 60");
            }
        });
    }

    @Test
    public void testVarcharSliceMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(5000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        1000L,
                        0L,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0
                );
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT * FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceOrderByLimitMultipleRowGroups() throws Exception {
        // Regression test: the Async Top K path stores a comparator reference
        // pointing to decoded Parquet row group data. Between frames,
        // releaseParquetBuffers() frees that data, leaving a dangling pointer.
        // The comparator then reads freed memory, producing wrong sort results.
        // We use many small row groups and ORDER BY varchar LIMIT to trigger
        // the LimitedSizeLongTreeChain cross-frame comparison path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar(5, 20, 0) AS v" +
                    " FROM long_sequence(50_000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                // Use a very small row group size (10) to create many row groups
                // (5,000), ensuring the Async Top K workers process many frames
                // and release buffers between them.
                PartitionEncoder.encodeWithOptions(
                        partitionDescriptor,
                        path,
                        ParquetCompression.COMPRESSION_UNCOMPRESSED,
                        true,
                        false,
                        10,
                        0,
                        ParquetVersion.PARQUET_VERSION_V1,
                        0.0
                );
                Assert.assertTrue(Files.exists(path.$()));

                // ORDER BY varchar LIMIT triggers the Async Top K execution plan
                // in the parallel path. Run multiple times to catch non-determinism.
                // Use ORDER BY v, id to break ties deterministically.
                for (int i = 0; i < 5; i++) {
                    sink.clear();
                    sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY v, id LIMIT 10");
                    assertSqlCursors0("SELECT * FROM x ORDER BY v, id LIMIT 10");
                }
            }
        });
    }

    @Test
    public void testVarcharSliceOrderByWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " CASE WHEN x % 4 = 0 THEN NULL ELSE rnd_varchar('alpha', 'beta', 'gamma') END AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') ORDER BY v");
                assertSqlCursors0("SELECT * FROM x ORDER BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceSampleBy() throws Exception {
        // SAMPLE BY is not supported in parallel parquet scan mode.
        if (parallel) {
            return;
        }
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " rnd_varchar('a', 'b', 'c') AS v," +
                    " rnd_long() AS val," +
                    " timestamp_sequence('2024-01-01', 1_000_000) AS ts" +
                    " FROM long_sequence(100)) TIMESTAMP(ts)");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT ts, v, count() FROM read_parquet('x.parquet') SAMPLE BY 1s");
                assertSqlCursors0("SELECT ts, v, count() FROM x SAMPLE BY 1s");
            }
        });
    }

    @Test
    public void testVarcharSliceStringFunctions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(200))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT id, substring(v, 1, 3) AS sub, left(v, 2) AS l, right(v, 2) AS r, upper(v) AS u, lower(v) AS lo FROM read_parquet('x.parquet') ORDER BY id");
                assertSqlCursors0("SELECT id, substring(v, 1, 3) AS sub, left(v, 2) AS l, right(v, 2) AS r, upper(v) AS u, lower(v) AS lo FROM x ORDER BY id");
            }
        });
    }

    @Test
    public void testVarcharSliceSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('alpha', 'beta', 'gamma', 'delta') AS v" +
                    " FROM long_sequence(1000))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM (SELECT v, count() AS cnt FROM read_parquet('x.parquet') GROUP BY v) WHERE cnt > 1 ORDER BY v");
                assertSqlCursors0("SELECT * FROM (SELECT v, count() AS cnt FROM x GROUP BY v) WHERE cnt > 1 ORDER BY v");
            }
        });
    }

    @Test
    public void testVarcharSliceUnionAllWithString() throws Exception {
        // VARCHAR_SLICE (from read_parquet) UNION ALL with STRING column. The
        // matrix resolves this to STRING, requiring a VARCHAR_SLICE→STRING cast.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE vc AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('hello', 'world', NULL) AS v" +
                    " FROM long_sequence(10))");
            execute("CREATE TABLE str AS (SELECT" +
                    " x AS id," +
                    " rnd_str(3, 6, 0) AS v" +
                    " FROM long_sequence(10))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("vc")
            ) {
                path.of(root).concat("vc.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                sink.clear();
                sink.put("SELECT * FROM read_parquet('vc.parquet') UNION ALL SELECT * FROM str");
                assertSqlCursors0("SELECT * FROM vc UNION ALL SELECT * FROM str");
            }
        });
    }

    @Test
    public void testVarcharSliceUnionAllWithVarchar() throws Exception {
        // VARCHAR_SLICE (from read_parquet) UNION ALL with native VARCHAR must not
        // trigger an assertion in generateCastFunctions().
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (SELECT" +
                    " x AS id," +
                    " rnd_varchar('hello', 'world', NULL) AS v" +
                    " FROM long_sequence(10))");

            try (
                    Path path = new Path();
                    PartitionDescriptor partitionDescriptor = new PartitionDescriptor();
                    TableReader reader = engine.getReader("x")
            ) {
                path.of(root).concat("x.parquet");
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path.$()));

                // VARCHAR_SLICE (read_parquet) UNION ALL VARCHAR (native table)
                sink.clear();
                sink.put("SELECT * FROM read_parquet('x.parquet') UNION ALL SELECT * FROM x");
                assertSqlCursors0("SELECT * FROM x UNION ALL SELECT * FROM x");

                // VARCHAR (native table) UNION ALL VARCHAR_SLICE (read_parquet)
                sink.clear();
                sink.put("SELECT * FROM x UNION ALL SELECT * FROM read_parquet('x.parquet')");
                assertSqlCursors0("SELECT * FROM x UNION ALL SELECT * FROM x");
            }
        });
    }

    private static void assertSqlCursors0(CharSequence expectedSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    sink,
                    LOG,
                    true
            );
        }
    }

    private void testCursor(boolean rawArrayEncoding) throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, String.valueOf(rawArrayEncoding));
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute(
                    "create table x as (select" +
                            " case when x % 2 = 0 then cast(x as int) end id," +
                            " case when x % 2 = 0 then rnd_int() end as a_long," +
                            " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                            " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
                            " case when x % 2 = 0 then rnd_double_array(1) end as an_array," +
                            " case when x % 2 = 0 then rnd_boolean() end a_boolean," +
                            " case when x % 2 = 0 then rnd_short() end a_short," +
                            " case when x % 2 = 0 then rnd_byte() end a_byte," +
                            " case when x % 2 = 0 then rnd_char() end a_char," +
                            " case when x % 2 = 0 then rnd_uuid4() end a_uuid," +
                            " case when x % 2 = 0 then rnd_double() end a_double," +
                            " case when x % 2 = 0 then rnd_float() end a_float," +
                            " case when x % 2 = 0 then rnd_symbol(4,4,4,2) end as a_sym," +
                            " cast(rnd_timestamp('2015','2016',2) as date) as a_date," +
                            " rnd_long256() a_long256," +
                            " rnd_ipv4() a_ip," +
                            " rnd_geohash(4) a_geo_byte," +
                            " rnd_geohash(8) a_geo_short," +
                            " rnd_geohash(16) a_geo_int," +
                            " rnd_geohash(32) a_geo_long," +
                            " rnd_bin(10, 20, 2) a_bin," +
                            " timestamp_sequence('2015', " + Micros.DAY_MICROS + ") as a_ts," +
                            " timestamp_sequence_ns('2015', " + Nanos.DAY_NANOS + ") as a_ns," +
                            " from long_sequence(" + rows + ")) timestamp (a_ts) partition by YEAR"
            );
            // create a newer partition, so that 2015 partition is no longer the active one
            execute("insert into x (a_ts) values ('2016-01-01T00:00:00.000Z')");
            drainWalQueue();

            execute("alter table x convert partition to parquet where a_ts > 0");

            engine.releaseInactive();
            try (Path path = new Path()) {
                path.concat(engine.verifyTableName("x")).concat("2015.2").concat(PARQUET_PARTITION_NAME);

                sink.clear();
                sink.put("select * from read_parquet('").put(path).put("')");
                assertQueryNoLeakCheck(
                        """
                                id\ta_long\ta_str\ta_varchar\tan_array\ta_boolean\ta_short\ta_byte\ta_char\ta_uuid\ta_double\ta_float\ta_sym\ta_date\ta_long256\ta_ip\ta_geo_byte\ta_geo_short\ta_geo_int\ta_geo_long\ta_bin\ta_ts\ta_ns
                                null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-24T20:19:13.843Z\t0x2705e02c613acfc405374f5fbcef4819523eb59d99c647af9840ad8800156d26\t138.69.22.149\t0000\t11001010\t0000101000111011\t10100111010101011100000010101100\t\t2015-01-01T00:00:00.000000Z\t2015-01-01T00:00:00.000000000Z
                                2\t-461611463\tHYRX\t0L#YS\\%~\\2o#/ZUAI6Q,]K+BuHiX\t[0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514,0.456344569609078,0.8664158914718532,0.40455469747939254,0.4149517697653501,0.5659429139861241,0.05384400312338511]\ttrue\t10633\t99\tU\t516e1efd-8bbc-4cf6-b7b4-f6e41fbfd55f\t0.9566236549439661\t0.11585981\tGPGW\t2015-08-13T19:00:41.832Z\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t160.13.39.44\t1011\t01111111\t1001110001101111\t01100001100100000101101000010010\t\t2015-01-02T00:00:00.000000Z\t2015-01-02T00:00:00.000000000Z
                                null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-03-18T03:51:48.548Z\t0x8151081b8acafaddb0c1415d6f1c1b8d4b0a72b3339b8c7c1872e79ea1032246\t10.242.4.147\t0101\t11011101\t0100111110110111\t11111011010111010100001100001100\t\t2015-01-03T00:00:00.000000Z\t2015-01-03T00:00:00.000000000Z
                                4\t462277692\tCPSW\tc⾩ᷚM䘣⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01W씌䒙\uD8F2\uDE8E\t[0.5780746276543334,0.40791879008699594,0.12663676991275652,0.21485589614090927]\ttrue\t20602\t34\tR\t75d5b0b0-cb85-4935-bdb4-b866b1f58ae4\t0.09303344348778264\t0.7586254\t\t2015-02-18T07:26:10.141Z\t0x4a27205d291d7f124c83d07de0778e771bd70aaefd486767588c16c272288827\t10.195.39.84\t0010\t00011111\t0000101111100010\t10100011101111000010110110010001\t00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12\t2015-01-04T00:00:00.000000Z\t2015-01-04T00:00:00.000000000Z
                                null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-10-28T22:55:20.040Z\t0x60add80ba563dff19c7a8bc4087de26d94ccc98dfff078f4402416cee4460c10\t141.95.223.227\t0100\t10110011\t1111000100111001\t11000011101010100011100100001101\t00000000 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a
                                00000010 ef 88 cb\t2015-01-05T00:00:00.000000Z\t2015-01-05T00:00:00.000000000Z
                                6\t-1912522421\tCPSW\t\t[0.8108032283138068,0.5090837921075583,0.07828020681514525,0.31221963822035725,0.49153268154777974,0.9067923725015784,0.8379891991223047,0.29168465906260244,0.7165847318191405]\tfalse\t-1263\t94\tG\t5b915a23-c7cd-4bdd-a265-e3472b31b408\t0.7653255982993546\t0.1511578\tIBBT\t2015-10-08T17:04:47.213Z\t0xb920fef32b29e2e175c293f14debaf6235974aa062e344d4428960f4997837e7\t144.121.84.78\t1100\t10010111\t1101000000111011\t01101011001011110110101010101100\t00000000 11 e2 a3 24 4e 44 a8 0d fe 27 ec\t2015-01-06T00:00:00.000000Z\t2015-01-06T00:00:00.000000000Z
                                null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-02T02:46:25.381Z\t0xb52d3beafd7e60e76c2bf46366ae0e15674cfe523d1cfeb2ce57f611173ce55d\t108.81.46.184\t1111\t11111100\t0001011011110000\t11010011001111011010010100110010\t00000000 bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85 20
                                00000010 53 3b 51\t2015-01-07T00:00:00.000000Z\t2015-01-07T00:00:00.000000000Z
                                8\t346891421\tVTJW\t1naz>A0'wQ@dx+UJo`-\t[0.5169565007469263,0.17094358360735395,0.15369837085455984,0.8461211697505234,0.5449970817079417,0.537020248377422,0.8405815493567417,0.005327467706811806,0.44638626240707313,0.31852531484741486,0.6479617440673516,0.605050319285447,0.7566252942139543,0.12217702189166091]\ttrue\t-21636\t33\tH\t9e0dcffb-7520-4bca-848a-d6b8f6962219\t0.3153349572730255\t0.0032519698\tSXUX\t2015-10-17T02:03:18.991Z\t0x41b1a691af3ce51f91a63337ac2e96836e87bac2e97465292db016df5ec48315\t126.254.162.195\t1110\t11001011\t1001101111110101\t11101000001101101011011100100011\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d
                                00000010 ad 11 bc\t2015-01-08T00:00:00.000000Z\t2015-01-08T00:00:00.000000000Z
                                null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t\t0x25ab6a3b3808d94d30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b9\t213.3.185.243\t0010\t01011011\t0100011111011011\t11110011001101101110011001111101\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1
                                00000010 00\t2015-01-09T00:00:00.000000Z\t2015-01-09T00:00:00.000000000Z
                                10\t2013697528\t\t\t[0.15274858078119136,0.7887510806568455,0.7468602267994937,0.23567419576658333,0.9976896430755934]\ttrue\t12861\t120\tI\ta0cd12e6-d39f-469a-9f88-06288f4b53ad\t0.9316283568969537\t0.8791439\tGPGW\t2015-03-04T04:42:20.407Z\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\t248.76.18.163\t0011\t11001010\t1101000010010101\t10101011110101111111110000001011\t00000000 87 28 92 a3 9b e3 cb c2 64 8a b0 35\t2015-01-10T00:00:00.000000Z\t2015-01-10T00:00:00.000000000Z
                                """,
                        sink,
                        null,
                        "a_ts",
                        parallel,
                        true
                );
            }
        });
    }
}
