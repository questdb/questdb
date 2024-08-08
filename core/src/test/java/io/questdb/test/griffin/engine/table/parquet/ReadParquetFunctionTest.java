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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReadParquetFunctionTest extends AbstractCairoTest {
    @Before
    public void setUp() {
        super.setUp();
        inputRoot = root;
    }

    @Test
    public void testCursor() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 10;
            ddl("create table x as (select" +
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
                    " timestamp_sequence('2015',2) as a_ts," +
                    " from long_sequence(" + rows + ")) timestamp (a_ts) partition by YEAR");

            ddl("alter table x convert partition to parquet where a_ts > 0");

            engine.releaseInactive();
            try (Path path = new Path()) {
                path.concat(engine.verifyTableName("x")).concat("2015.parquet");

                sink.clear();
                sink.put("select * from read_parquet('").put(path).put("')");
                assertQuery("id\ta_long\ta_str\ta_varchar\ta_boolean\ta_short\ta_byte\ta_char\ta_uuid\ta_double\ta_float\ta_sym\ta_date\ta_long256\ta_ip\ta_geo_byte\ta_geo_short\ta_geo_int\ta_geo_long\ta_bin\ta_ts\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-11-24T20:19:13.843Z\t0x2705e02c613acfc405374f5fbcef4819523eb59d99c647af9840ad8800156d26\t138.69.22.149\t0000\t11001010\t0000101000111011\t10100111010101011100000010101100\t\t2015-01-01T00:00:00.000000Z\n" +
                                "2\t-461611463\tHYRX\t0L#YS\\%~\\2o#/ZUAI6Q,]K+BuHiX\tfalse\t3428\t25\tO\t71660a9b-0890-42f0-aa0a-ccd425e948d4\t0.38642336707855873\t0.9205\tGPGW\t2015-02-04T13:09:51.166Z\t0x51686790e59377ca68653a6cd896f81ed4e0ddcd6eb2cff1c736a8b67656c4f1\t250.26.136.156\t1001\t10001000\t1110111101101001\t01010111101100101101110010010001\t00000000 e5 61 2f 64 0e 2c 7f d7 6f b8 c9 ae 28 c7 84 47\t2015-01-01T00:00:00.000002Z\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-08-16T07:46:57.313Z\t0xc6dfacdd3f3c52b88b4e4831499fc2a526567f4430b46b7f78c594c496995885\t107.3.2.123\t1110\t00110010\t0010010010010011\t01110010110101010111111110111011\t00000000 64 d2 ad 49 1c f2 3c ed 39 ac a8 3b a6\t2015-01-01T00:00:00.000004Z\n" +
                                "4\t-283321892\tCPSW\t\tfalse\t-22894\t70\tZ\tdb217d41-156b-4ee1-a90c-04663c808638\t0.3679848625908545\t0.8231\tGPGW\t2015-05-24T01:10:00.026Z\t0x0ec6c3651b1c029f825c96def9f2fcc2b942438168662cb7aa21f9d816335363\t241.72.62.41\t1110\t00100111\t1111001111111000\t11100111010111100111111001011011\t00000000 43 1d 57 34 04 23 8d d8 57 91 88 28 a5 18 93 bd\n" +
                                "00000010 0b 61 f5\t2015-01-01T00:00:00.000006Z\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-04-15T00:02:37.661Z\t0x70061ac6a4115ca72121bcf90e43824476ffd1a81bf39767b92d0771d78263eb\t142.167.96.106\t0100\t11111111\t1000111111011111\t00001000010100101111101101110000\t\t2015-01-01T00:00:00.000008Z\n" +
                                "6\t-1726426588\t\t\uDB42\uDC86W씌䒙\uD8F2\uDE8E>\uDAE6\uDEE3gX夺\uDA02\uDE66\uDA29\uDE0E⋜\uD9DC\uDEB3\uD90B\uDDC5cᣮաf@ץ;윦\u0382宏\ttrue\t29923\t36\tH\t89f3ac11-65aa-40b1-a61a-2e7ca9350807\t0.95820305972778\t0.7707\t\t\t0x7ab72c8ee7c4dea1c54dc9aa8e01394b65464a26629d85cb508ce19ec75e8088\t245.42.76.207\t1001\t10000010\t1111010101000000\t10101101010100011001100001111001\t00000000 2b b3 71 a7 d5 af 11 96 37 08\t2015-01-01T00:00:00.000010Z\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-03-25T09:21:52.776Z\t0x6a44a113d18bd82a6784e4a783247d88546c26b247358a5497a77df30ee95def\t202.243.69.162\t0111\t11010111\t0110101110000011\t11100001001100101011101100010111\t\t2015-01-01T00:00:00.000012Z\n" +
                                "8\t-158323100\tVTJW\tU,MF|yrXB,=B)iRv59Q,?/qbOku|U#EHD\tfalse\t7583\t105\tS\tc04ba3c4-3305-482a-8174-454ba2921cbb\t0.798471808479839\t0.2942\t\t2015-10-06T15:07:04.728Z\t0xb23ff8774a5db433b19ddb7ff5abcafec82c35a389f834dababcd0482f05618f\t53.137.68.152\t1101\t00100011\t0001111000101110\t11010100111101010000100010001011\t\t2015-01-01T00:00:00.000014Z\n" +
                                "null\tnull\t\t\tfalse\t0\t0\t\t\tnull\tnull\t\t2015-08-19T07:45:23.196Z\t0x0ceb6d48cda39eb24f38804270a4a64349b5760a687d8cf838cbb9ae96e9ecdc\t10.101.5.227\t0011\t01001000\t1111111101011000\t11001010001000000100111000111110\t00000000 c4 4a c9 cf fb 9d 63 ca 94 00 6b dd 18 fe 71 76\n" +
                                "00000010 bc\t2015-01-01T00:00:00.000016Z\n" +
                                "10\t835713605\t\tt0&{b4BKKUL\\'k1X^{DaU%\ttrue\t19244\t66\tC\t4cedbc17-2f67-4fb6-a4d3-28f097940cde\t0.16698121316984016\t0.3383\tGPGW\t2015-03-18T21:31:57.871Z\t0xdb679af7e4282aa3d7a37bf63a1a9a89b88b0261a4770cf3bd27bad88085f716\t154.162.204.131\t0001\t11010000\t0001101100100110\t01010100100010110010010100101000\t\t2015-01-01T00:00:00.000018Z\n",
                        sink, null, null, false, true);
            }
        });
    }

    @Test
    public void testData() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1000_000;
            ddl("create table x as (select" +
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
            ddl("create table x as (select" +
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
                    try (RecordCursorFactory factory2 = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        engine.getConfiguration().getFilesFacade().remove(path.$());
                        try (RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)) {
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
    public void testMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final long rows = 1;
            ddl("create table x as (select" +
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

                assertPlanNoLeakCheck(sink, "parquet file sequential scan\n");

                sink.put(" where 1 = 2");
                assertSqlCursors("x where 1 = 2", sink);
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
