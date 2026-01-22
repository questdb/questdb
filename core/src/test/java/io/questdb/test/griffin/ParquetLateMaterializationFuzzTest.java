/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class ParquetLateMaterializationFuzzTest extends AbstractCairoTest {
    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final StringSink sql = new StringSink();

    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1 + rnd.nextInt(100));
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 1 + rnd.nextInt(8));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10 + rnd.nextInt(100));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, 10 + rnd.nextInt(100));
        super.setUp();
    }

    @Test
    public void testAggregatesNotKeyed() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_long), avg(an_int), count(a_date) from x where id%12=3");
    }

    @Test
    public void testAggregatesWithGroupByBySymbol() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_long), avg(an_int), count(a_date),a_symbol from x where id%13=5 order by a_symbol");
    }

    @Test
    public void testCountBooleanAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select last(a_boolean) from x where id%13=2");
    }

    @Test
    public void testCountDistinctSymbolAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select count_distinct(a_symbol) from x where id%12=9");
    }

    @Test
    public void testCountOnly() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select count() from x where id%11=5");
    }

    @Test
    public void testFirstCharAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_char) from x where id in (199, 199, 201, 202)");
    }

    @Test
    public void testFirstDoubleAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_double) from x where id%23=7");
    }

    @Test
    public void testFirstFloatAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_float) from x where id%19=4");
    }

    @Test
    public void testFirstGeoHashByteAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_geo_byte) from x where id%19 < 2");
    }

    @Test
    public void testFirstGeoHashIntAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_geo_int) from x where id%37=9");
    }

    @Test
    public void testFirstIpv4Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_ip) from x where id%29=11");
    }

    @Test
    public void testFirstStringAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_string) from x where id%12=3");
    }

    @Test
    public void testFirstUuidAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_uuid) from x where id%15=0");
    }

    @Test
    public void testLastVarcharAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select last(a_varchar) from x where id%31 between 10 and 12");
    }

    @Test
    public void testLateMaterializationBinaryColumn() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_bin(8, 16, 0) a_bin,
                                        rnd_int() an_int,
                                        rnd_str('a','b') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    CharSequence query = generateRndInListSql("filter_col");
                    final StringSink expected = new StringSink();
                    CharSequence query1 = "select first(base64(a_bin, 100)), last(base64(a_bin, 100)) from x where filter_col%13=7 group by a_string order by a_string";
                    final StringSink expected1 = new StringSink();
                    engine.print(query, expected, sqlExecutionContext);
                    engine.print(query1, expected1, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                    TestUtils.assertSql(engine, sqlExecutionContext, query1, sink, expected1);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationBooleanFilter() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        x % 10 = 0 filter_col,
                                        rnd_int() an_int,
                                        rnd_long() a_long,
                                        rnd_double() a_double,
                                        rnd_str('a','b','c') a_string,
                                        rnd_varchar('x','y','z') a_varchar,
                                        rnd_symbol('s1','s2','s3') a_symbol,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    engine.print("x where filter_col = true", expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, "x where filter_col = true", sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationColTops() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute("create table x (id long, filter_col int, ts timestamp) timestamp(ts) partition by day;", sqlExecutionContext);
                    engine.execute(
                            """
                                    insert into x
                                    select x, cast(x as int), timestamp_sequence(0, 60000000)
                                    from long_sequence(2000);""",
                            sqlExecutionContext
                    );

                    engine.execute("alter table x add column new_int int;", sqlExecutionContext);
                    engine.execute("alter table x add column new_string string;", sqlExecutionContext);
                    engine.execute("alter table x add column new_symbol symbol;", sqlExecutionContext);
                    engine.execute("alter table x add column new_double double;", sqlExecutionContext);

                    engine.execute(
                            """
                                    insert into x
                                    select
                                      500 + x,
                                      cast(500 + x as int),
                                      timestamp_sequence(50000000, 1000000),
                                      rnd_int(),
                                      rnd_str('a','b','c'),
                                      rnd_symbol('s1','s2','s3'),
                                      rnd_double()
                                    from long_sequence(1000);""",
                            sqlExecutionContext
                    );

                    final StringSink expected1 = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected1, sqlExecutionContext);

                    final StringSink expected2 = new StringSink();
                    final CharSequence query2 = "select sum(new_int), count(new_symbol) from x where filter_col % 12 = 3";
                    engine.print(query2, expected2, sqlExecutionContext);

                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected1);
                    TestUtils.assertSql(engine, sqlExecutionContext, query2, sink, expected2);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationDateColumn() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        cast(timestamp_sequence(0, 86400000000) as date) a_date,
                                        rnd_int() an_int,
                                        rnd_double() a_double,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    CharSequence query = "select max(a_date), min(a_date) from x where filter_col%11 = 9";
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationFuzzMixedPartitions() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_int() an_int,
                                        rnd_double() a_double,
                                        rnd_str('a','b','c') a_string,
                                        rnd_symbol('s1','s2') a_symbol,
                                        timestamp_sequence(0, 60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by hour;""",
                            sqlExecutionContext
                    );

                    final StringSink expected1 = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected1, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts < '1970-01-01T12:00:00Z'", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected1);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationGeoHashColumns() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_geohash(4) geo_byte,
                                        rnd_geohash(8) geo_short,
                                        rnd_geohash(16) geo_int,
                                        rnd_geohash(32) geo_long,
                                        rnd_int() an_int,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationIpv4Column() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_ipv4() an_ip,
                                        rnd_int() an_int,
                                        rnd_str('a','b') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationLong256Column() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_long256() a_long256,
                                        rnd_int() an_int,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationMultiplePartitions() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_int() an_int,
                                        rnd_double() a_double,
                                        rnd_str('a','b','c') a_string,
                                        rnd_symbol('s1','s2','s3') a_symbol,
                                        timestamp_sequence(0, 60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by hour;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationSelectSpecificColumns() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_int() an_int,
                                        rnd_long() a_long,
                                        rnd_double() a_double,
                                        rnd_str('a','b','c') a_string,
                                        rnd_symbol('s1','s2') a_symbol,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    engine.print("select id, a_double, a_symbol from x where filter_col%15 = 13", expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, "select id, a_double, a_symbol from x where filter_col%15 = 13", sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationUuidColumn() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_uuid4() a_uuid,
                                        rnd_int() an_int,
                                        rnd_str('a','b') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationVarcharFilter() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        rnd_varchar('target','other1','other2','other3','other4') filter_col,
                                        rnd_int() an_int,
                                        rnd_double() a_double,
                                        rnd_str('s1','s2','s3') a_string,
                                        rnd_bin(1,8,0) a_bin,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    engine.print("x where filter_col = 'target'", expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, "x where filter_col = 'target'", sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationWithArrayColumn() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        array[x * 1.0, x * 2.0, x * 3.0] an_array,
                                        rnd_int() an_int,
                                        rnd_str('a','b') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    final StringSink expected1 = new StringSink();
                    final CharSequence query1 = ("select first(an_array), last(an_array) from x where filter_col%13=7 group by a_string order by a_string");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.print(query1, expected1, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                    TestUtils.assertSql(engine, sqlExecutionContext, query1, sink, expected1);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationWithLimit() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x % 5 as int) filter_col,
                                        rnd_int() an_int,
                                        rnd_double() a_double,
                                        rnd_str('a','b','c') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final StringSink expected2 = new StringSink();
                    final StringSink query = generateRndInListSql("filter_col");
                    final StringSink query2 = new StringSink();
                    query2.put(query);
                    query.put(" limit 7");
                    query2.put(" limit -7");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.print(query2, expected2, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                    TestUtils.assertSql(engine, sqlExecutionContext, query2, sink, expected2);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testLateMaterializationWithNulls() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        rnd_int(0, 100, 2) nullable_int,
                                        rnd_long(0, 100, 2) nullable_long,
                                        rnd_double(2) nullable_double,
                                        rnd_str('a','b','c', null) nullable_string,
                                        rnd_symbol('s1','s2', null) nullable_symbol,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    final StringSink query = generateRndInListSql("filter_col");
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }

    @Test
    public void testMaxTimestampAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select max(ts) from x where id%41=17");
    }

    @Test
    public void testMinDateAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select min(a_date) from x where id % 21 in(3, 17)");
    }

    @Test
    public void testRandomInListFilterOnId() throws Exception {
        testLateMaterializationAllTypesLowSelectivity(generateRndInListSql("id"));
    }

    @Test
    public void testSumByteAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_byte) from x where id%9=1");
    }

    @Test
    public void testSumLong256Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_long256) from x where id%31=15");
    }

    @Test
    public void testSumShortAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_short) from x where id%17=8");
    }

    @Test
    public void testSymbolEqualityFilter() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("x where a_symbol = 'a'");
    }

    private StringSink generateRndInListSql(CharSequence column) {
        sql.clear();
        final int inListCount = 1 + rnd.nextInt(80);
        sql.put("x where ").put(column).put(" in (");
        for (int i = 0; i < inListCount; i++) {
            if (i > 0) {
                sql.put(", ");
            }
            sql.put(1 + rnd.nextInt(1000));
        }
        sql.put(")");
        return sql;
    }

    private void testLateMaterializationAllTypesLowSelectivity(CharSequence query) throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        rnd_boolean() a_boolean,
                                        rnd_byte() a_byte,
                                        rnd_short() a_short,
                                        rnd_char() a_char,
                                        rnd_int(-10, 10, 0) an_int,
                                        rnd_long(-10, 10, 0) a_long,
                                        rnd_float() a_float,
                                        rnd_double() a_double,
                                        rnd_symbol('a','b','c','d', 'e','f','g','h','i','j','k') a_symbol,
                                        rnd_str('hello', 'world', '!') a_string,
                                        rnd_varchar('💗❤️','❤️😊','🙏') a_varchar,
                                        rnd_bin(1, 8, 0) a_bin,
                                        rnd_ipv4() a_ip,
                                        rnd_uuid4() a_uuid,
                                        rnd_long256() a_long256,
                                        rnd_geohash(4) a_geo_byte,
                                        rnd_geohash(16) a_geo_int,
                                        cast(timestamp_sequence(0,1000000) as date) a_date,
                                        timestamp_sequence(0, 60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    engine.print(query, expected, sqlExecutionContext);
                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);
                },
                configuration,
                LOG
        );
    }
}
