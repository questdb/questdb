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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.Timestamps;
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
        super.setUp();
        inputRoot = root;
        node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, parallel);
    }

    @Test
    public void testColumnMapping() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_long() end as a_long," +
                    " case when x % 2 = 0 then rnd_int() end as an_int," +
                    " rnd_timestamp('2015','2016',2) as a_ts" +
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
                assertSqlCursors("select a_ts, a_long from x", sink);
            }
        });
    }

    @Test
    public void testCursor() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then cast(x as int) end id," +
                    " case when x % 2 = 0 then rnd_int() end as a_long," +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
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
                    " timestamp_sequence('2015', " + (Timestamps.AVG_YEAR_MICROS / 4) + ") as a_ts," +
                    " from long_sequence(" + rows + ")) timestamp (a_ts) partition by YEAR");

            execute("alter table x convert partition to parquet where a_ts > 0");

            engine.releaseInactive();
            try (Path path = new Path()) {
                path.concat(engine.verifyTableName("x")).concat("2015.2").concat(PARQUET_PARTITION_NAME);

                sink.clear();
                sink.put("select * from read_parquet('").put(path).put("')");
                assertQueryNoLeakCheck(
                        "id\ta_long\ta_str\ta_varchar\ta_boolean\ta_short\ta_byte\ta_char\ta_uuid\ta_double\ta_float\ta_sym\ta_date\ta_long256\ta_ip\ta_geo_byte\ta_geo_short\ta_geo_int\ta_geo_long\ta_bin\ta_ts\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-24T20:19:13.843Z\t0x2705e02c613acfc405374f5fbcef4819523eb59d99c647af9840ad8800156d26\t138.69.22.149\t0000\t11001010\t0000101000111011\t10100111010101011100000010101100\t\t2015-01-01T00:00:00.000000Z\n" +
                                "2\t-461611463\tHYRX\t0L#YS\\%~\\2o#/ZUAI6Q,]K+BuHiX\tfalse\t3428\t25\tO\t71660a9b-0890-42f0-aa0a-ccd425e948d4\t0.38642336707855873\t0.9205\tGPGW\t2015-02-04T13:09:51.166Z\t0x51686790e59377ca68653a6cd896f81ed4e0ddcd6eb2cff1c736a8b67656c4f1\t250.26.136.156\t1001\t10001000\t1110111101101001\t01010111101100101101110010010001\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\t2015-04-02T07:27:18.000000Z\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-08-16T07:46:57.313Z\t0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885\t107.3.2.123\t1110\t00110010\t0010010010010011\t01110010110101010111111110111011\t00000000 64 d2 ad 49 1c f2 3c ed 39 ac a8 3b a6\t2015-07-02T14:54:36.000000Z\n" +
                                "4\t-283321892\tCPSW\t\tfalse\t-22894\t70\tZ\tdb217d41-156b-4ee1-a90c-04663c808638\t0.3679848625908545\t0.8231\tGPGW\t2015-05-24T01:10:00.026Z\t0x0ec6c3651b1c029f825c96def9f2fcc2b942438168662cb7aa21f9d816335363\t241.72.62.41\t1110\t00100111\t1111001111111000\t11100111010111100111111001011011\t00000000 43 1d 57 34 04 23 8d d8 57 91 88 28 a5 18 93 bd\n" +
                                "00000010 0b 61 f5\t2015-10-01T22:21:54.000000Z\n",
                        sink,
                        null,
                        null,
                        parallel,
                        true
                );
            }
        });
    }

    @Test
    public void testData() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1000_000;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then cast(x as int) end id," +
                    " case when x % 2 = 0 then rnd_int() end as a_long," +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_varchar(1, 40, 1) end as a_varchar," +
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
                assertSqlCursors("x", sink);
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
                    select("select * from read_parquet('" + path + "')  where 1 = 2");
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
            execute("create table x as (select" +
                    " x id," +
                    " timestamp_sequence(0,10000) as ts" +
                    " from long_sequence(1))");
            execute("create table y as (select" +
                    " x id," +
                    " 'foobar' str," +
                    " timestamp_sequence(0,10000) as ts" +
                    " from long_sequence(1))");

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
                    assertPlanNoLeakCheck(sink, "parquet page frame scan\n");
                } else {
                    assertPlanNoLeakCheck(sink, "parquet file sequential scan\n");
                }

                sink.put(" where 1 = 2");
                assertSqlCursors("x where 1 = 2", sink);
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
                assertSqlCursors("select * from x order by a_varchar1, a_varchar2", sink);
            }
        });
    }

    protected static void assertSqlCursors(CharSequence expectedSql, CharSequence actualSql) throws SqlException {
        try (SqlCompiler sqlCompiler = engine.getSqlCompiler()) {
            TestUtils.assertSqlCursors(
                    sqlCompiler,
                    sqlExecutionContext,
                    expectedSql,
                    actualSql,
                    LOG,
                    true
            );
        }
    }
}
