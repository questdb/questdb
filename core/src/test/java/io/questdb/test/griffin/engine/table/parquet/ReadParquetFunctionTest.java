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
    public void testColumnMapping() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then rnd_str(4,4,4,2) end as a_str," +
                    " case when x % 2 = 0 then rnd_long() end as a_long," +
                    " case when x % 2 = 0 then rnd_int() end as an_int," +
                    " rnd_timestamp('2015','2016',2) as a_ts," +
                    " rnd_timestamp('2015'::timestamp_ns,'2016',2) as a_ns" +
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
                assertSqlCursors0("select a_ts, a_long from x");
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
                    " rnd_timestamp('2015'::timestamp_ns,'2016',2) as a_ns," +
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
    public void testFileDeleted() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            execute("create table x as (select" +
                    " case when x % 2 = 0 then cast(x as int) end id," +
                    " rnd_timestamp('2015','2016',2) as a_ts," +
                    " rnd_timestamp('2015'::timestamp_ns,'2016',2) as a_ns," +
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
                            " 'foobar' str," +
                            " timestamp_sequence(0,10000) as ts," +
                            " timestamp_sequence(0,10000)::timestamp_ns as ns" +
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
                    " timestamp_sequence(500000000000000::timestamp_ns, 600000) a_ns," +
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
                assertSqlCursors0("x where 1 = 2");
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
                            " timestamp_sequence('2015'::timestamp_ns, " + Nanos.DAY_NANOS + ") as a_ns," +
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
                        "id\ta_long\ta_str\ta_varchar\tan_array\ta_boolean\ta_short\ta_byte\ta_char\ta_uuid\ta_double\ta_float\ta_sym\ta_date\ta_long256\ta_ip\ta_geo_byte\ta_geo_short\ta_geo_int\ta_geo_long\ta_bin\ta_ts\ta_ns\n" +
                                "null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-24T20:19:13.843Z\t0x2705e02c613acfc405374f5fbcef4819523eb59d99c647af9840ad8800156d26\t138.69.22.149\t0000\t11001010\t0000101000111011\t10100111010101011100000010101100\t\t2015-01-01T00:00:00.000000Z\t2015-01-01T00:00:00.000000000Z\n" +
                                "2\t-461611463\tHYRX\t0L#YS\\%~\\2o#/ZUAI6Q,]K+BuHiX\t[0.8001121139739173,0.38642336707855873,0.92050039469858,0.16381374773748514,0.456344569609078,0.8664158914718532,0.40455469747939254,0.4149517697653501,0.5659429139861241,0.05384400312338511]\ttrue\t10633\t99\tU\t516e1efd-8bbc-4cf6-b7b4-f6e41fbfd55f\t0.9566236549439661\t0.11585981\tGPGW\t2015-08-13T19:00:41.832Z\t0x772c8b7f9505620ebbdfe8ff0cd60c64712fde5706d6ea2f545ded49c47eea61\t160.13.39.44\t1011\t01111111\t1001110001101111\t01100001100100000101101000010010\t\t2015-01-02T00:00:00.000000Z\t2015-01-02T00:00:00.000000000Z\n" +
                                "null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-03-18T03:51:48.548Z\t0x8151081b8acafaddb0c1415d6f1c1b8d4b0a72b3339b8c7c1872e79ea1032246\t10.242.4.147\t0101\t11011101\t0100111110110111\t11111011010111010100001100001100\t\t2015-01-03T00:00:00.000000Z\t2015-01-03T00:00:00.000000000Z\n" +
                                "4\t462277692\tCPSW\tc⾩ᷚM䘣⟩Mqk㉳+\u0093ً\uDAF5\uDE17qRӽ-\uDBED\uDC98\uDA30\uDEE01W씌䒙\uD8F2\uDE8E\t[0.5780746276543334,0.40791879008699594,0.12663676991275652,0.21485589614090927]\ttrue\t20602\t34\tR\t75d5b0b0-cb85-4935-bdb4-b866b1f58ae4\t0.09303344348778264\t0.7586254\t\t2015-02-18T07:26:10.141Z\t0x4a27205d291d7f124c83d07de0778e771bd70aaefd486767588c16c272288827\t10.195.39.84\t0010\t00011111\t0000101111100010\t10100011101111000010110110010001\t00000000 ff 70 3a c7 8a b3 14 cd 47 0b 0c 39 12\t2015-01-04T00:00:00.000000Z\t2015-01-04T00:00:00.000000000Z\n" +
                                "null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-10-28T22:55:20.040Z\t0x60add80ba563dff19c7a8bc4087de26d94ccc98dfff078f4402416cee4460c10\t141.95.223.227\t0100\t10110011\t1111000100111001\t11000011101010100011100100001101\t00000000 2e 42 fa f5 6e 8f 80 e3 54 b8 07 b1 32 57 ff 9a\n" +
                                "00000010 ef 88 cb\t2015-01-05T00:00:00.000000Z\t2015-01-05T00:00:00.000000000Z\n" +
                                "6\t-1912522421\tCPSW\t\t[0.8108032283138068,0.5090837921075583,0.07828020681514525,0.31221963822035725,0.49153268154777974,0.9067923725015784,0.8379891991223047,0.29168465906260244,0.7165847318191405]\tfalse\t-1263\t94\tG\t5b915a23-c7cd-4bdd-a265-e3472b31b408\t0.7653255982993546\t0.1511578\tIBBT\t2015-10-08T17:04:47.213Z\t0xb920fef32b29e2e175c293f14debaf6235974aa062e344d4428960f4997837e7\t144.121.84.78\t1100\t10010111\t1101000000111011\t01101011001011110110101010101100\t00000000 11 e2 a3 24 4e 44 a8 0d fe 27 ec\t2015-01-06T00:00:00.000000Z\t2015-01-06T00:00:00.000000000Z\n" +
                                "null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-02T02:46:25.381Z\t0xb52d3beafd7e60e76c2bf46366ae0e15674cfe523d1cfeb2ce57f611173ce55d\t108.81.46.184\t1111\t11111100\t0001011011110000\t11010011001111011010010100110010\t00000000 bb 2a 4b af 8f 89 df 35 8f da fe 33 98 80 85 20\n" +
                                "00000010 53 3b 51\t2015-01-07T00:00:00.000000Z\t2015-01-07T00:00:00.000000000Z\n" +
                                "8\t346891421\tVTJW\t1naz>A0'wQ@dx+UJo`-\t[0.5169565007469263,0.17094358360735395,0.15369837085455984,0.8461211697505234,0.5449970817079417,0.537020248377422,0.8405815493567417,0.005327467706811806,0.44638626240707313,0.31852531484741486,0.6479617440673516,0.605050319285447,0.7566252942139543,0.12217702189166091]\ttrue\t-21636\t33\tH\t9e0dcffb-7520-4bca-848a-d6b8f6962219\t0.3153349572730255\t0.0032519698\tSXUX\t2015-10-17T02:03:18.991Z\t0x41b1a691af3ce51f91a63337ac2e96836e87bac2e97465292db016df5ec48315\t126.254.162.195\t1110\t11001011\t1001101111110101\t11101000001101101011011100100011\t00000000 16 f3 89 a3 83 64 de d6 fd c4 5b c4 e9 19 47 8d\n" +
                                "00000010 ad 11 bc\t2015-01-08T00:00:00.000000Z\t2015-01-08T00:00:00.000000000Z\n" +
                                "null\tnull\t\t\tnull\tfalse\t0\t0\t\t\tnull\tnull\t\t\t0x25ab6a3b3808d94d30ec2498d018efdd67bf677cfe82f2528ebaf26ca19a89b9\t213.3.185.243\t0010\t01011011\t0100011111011011\t11110011001101101110011001111101\t00000000 c5 60 b7 d1 5a 0c e9 db 51 13 4d 59 20 c9 37 a1\n" +
                                "00000010 00\t2015-01-09T00:00:00.000000Z\t2015-01-09T00:00:00.000000000Z\n" +
                                "10\t2013697528\t\t\t[0.15274858078119136,0.7887510806568455,0.7468602267994937,0.23567419576658333,0.9976896430755934]\ttrue\t12861\t120\tI\ta0cd12e6-d39f-469a-9f88-06288f4b53ad\t0.9316283568969537\t0.8791439\tGPGW\t2015-03-04T04:42:20.407Z\t0x8ca81bb363d7ac4585afb517c8aee023d107b1affff76a2c79189b578191a8f6\t248.76.18.163\t0011\t11001010\t1101000010010101\t10101011110101111111110000001011\t00000000 87 28 92 a3 9b e3 cb c2 64 8a b0 35\t2015-01-10T00:00:00.000000Z\t2015-01-10T00:00:00.000000000Z\n",
                        sink,
                        null,
                        null,
                        parallel,
                        true
                );
            }
        });
    }
}
