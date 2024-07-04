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
import org.junit.Test;

public class ParquetFileReaderFunctionTest extends AbstractCairoTest {

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
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path));

                sink.clear();
                sink.put("select " +
                        "id," +
                        "a_long," +
                        "a_str," +
                        "a_varchar," +
                        "a_boolean," +
                        "a_short," +
                        "a_byte," +
                        "a_char," +
                        "a_uuid," +
                        "a_double," +
                        "a_float," +
                        "a_sym," +
                        "a_date," +
                        "a_long256," +
                        "cast(a_ip as IPV4) as a_ip," +
                        "cast(a_geo_byte as geohash(4b)) as a_geo_byte," +
                        "cast(a_geo_short as geohash(8b)) as a_geo_short," +
                        "cast(a_geo_int as geohash(16b)) as a_geo_int," +
                        "cast(a_geo_long as geohash(32b)) as a_geo_long," +
                        "a_bin," +
                        "a_ts," +
                        " from read_parquet('").put(path).put("')");
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
                sink.put("select * from read_parquet('").put(path).put("')");

                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    try (RecordCursorFactory factory2 = compiler.compile(sink, sqlExecutionContext).getRecordCursorFactory()) {
                        engine.getConfiguration().getFilesFacade().remove(path.$());
                        try (RecordCursor cursor2 = factory2.getCursor(sqlExecutionContext)) {
                            Assert.fail();
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getMessage(), "could not open read-only");
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
                    TestUtils.assertContains(e.getMessage(), "could not open read-only");
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
                path.of(root).concat("x.parquet").$();
                PartitionEncoder.populateFromTableReader(reader, partitionDescriptor, 0);
                PartitionEncoder.encode(partitionDescriptor, path);
                Assert.assertTrue(Files.exists(path));
                // Assert 0 rows, header only
                sink.clear();
                sink.put("select * from read_parquet('").put(path).put("')");

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
