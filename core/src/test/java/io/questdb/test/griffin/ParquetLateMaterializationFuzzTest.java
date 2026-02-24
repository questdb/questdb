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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED;

public class ParquetLateMaterializationFuzzTest extends AbstractCairoTest {
    private final Rnd rnd = TestUtils.generateRandom(LOG);
    private final StringSink sql = new StringSink();
    private boolean enableJitCompiler;

    @Before
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1 + rnd.nextInt(100));
        node1.setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1 + rnd.nextInt(4));
        node1.setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 1 + rnd.nextInt(8));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 10 + rnd.nextInt(100));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, 10 + rnd.nextInt(100));
        enableJitCompiler = rnd.nextBoolean();
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
    public void testFirstGeoHashLongAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_geo_long) from x where id%37=9");
    }

    @Test
    public void testFirstGeoHashShortAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_geo_short) from x where id%37=9");
    }

    @Test
    public void testFirstIpv4Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_ip) from x where id%29=11");
    }

    @Test
    public void testFirstStringAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_string) from x where id%12=3", true, false);
    }

    @Test
    public void testFirstUuidAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select first(a_uuid) from x where id%15=0");
    }

    @Test
    public void testLastVarcharAggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select last(a_varchar) from x where id%31 between 10 and 12", false, true);
    }

    @Test
    public void testLateMaterializationBinaryColumn() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                                      timestamp_sequence(130000000000, 60000000),
                                      rnd_int(),
                                      rnd_str('a','b','c'),
                                      rnd_symbol('s1','s2','s3'),
                                      rnd_double()
                                    from long_sequence(2000);""",
                            sqlExecutionContext
                    );

                    final StringSink expected1 = new StringSink();
                    final CharSequence query = generateRndInListSql("filter_col");
                    engine.print(query, expected1, sqlExecutionContext);

                    final StringSink expected2 = new StringSink();
                    final CharSequence query2 = "select sum(new_int), count(new_symbol), last(new_string), first(new_double) from x where filter_col % 12 = 3";
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
        testLateMaterializationWithArrayColumn(false);
    }

    @Test
    public void testLateMaterializationWithArrayColumn_rawEncoding() throws Exception {
        testLateMaterializationWithArrayColumn(true);
    }

    @Test
    public void testLateMaterializationWithLimit() throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
    public void testSumDecimal128Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_decimal128) from x where id%23=11");
    }

    @Test
    public void testSumDecimal16Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_decimal16) from x where id%11=4");
    }

    @Test
    public void testSumDecimal256Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select last(a_decimal256) from x where id%29=13");
    }

    @Test
    public void testSumDecimal32Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_decimal32) from x where id%13=6");
    }

    @Test
    public void testSumDecimal64Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_decimal64) from x where id%19=7");
    }

    @Test
    public void testSumDecimal8Aggregate() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("select sum(a_decimal8) from x where id%7=3");
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

    @Test
    public void testTopK() throws Exception {
        testLateMaterializationAllTypesLowSelectivity("SELECT * FROM x where id%499 = 0 ORDER BY id, ts DESC LIMIT 3;");
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
        testLateMaterializationAllTypesLowSelectivity(query, false, false);
    }

    private void testLateMaterializationAllTypesLowSelectivity(CharSequence query, boolean checkStrLen, boolean checkVarcharLen) throws Exception {
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
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
                                        rnd_geohash(8) a_geo_short,
                                        rnd_geohash(16) a_geo_int,
                                        rnd_geohash(32) a_geo_long,
                                        rnd_decimal(2, 1, 0) a_decimal8,
                                        rnd_decimal(4, 2, 0) a_decimal16,
                                        rnd_decimal(9, 2, 0) a_decimal32,
                                        rnd_decimal(18, 3, 0) a_decimal64,
                                        rnd_decimal(38, 7, 0) a_decimal128,
                                        rnd_decimal(76, 10, 0) a_decimal256,
                                        cast(timestamp_sequence(0,1000000) as date) a_date,
                                        timestamp_sequence(0, 60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    final StringSink expected = new StringSink();
                    engine.print(query, expected, sqlExecutionContext);
                    long expectedTotalStrLen = 0;
                    if (checkStrLen) {
                        try (RecordCursorFactory factory = engine.select("select first(a_string) from x where id%12=4 group by a_symbol", sqlExecutionContext)) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                while (cursor.hasNext()) {
                                    Record record = cursor.getRecord();
                                    expectedTotalStrLen += record.getStrLen(0);
                                }
                            }
                        }
                    }

                    if (checkVarcharLen) {
                        try (RecordCursorFactory factory = engine.select("select first(a_varchar) from x where id%12=5 group by a_symbol", sqlExecutionContext)) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                while (cursor.hasNext()) {
                                    Record record = cursor.getRecord();
                                    expectedTotalStrLen += record.getVarcharSize(0);
                                }
                            }
                        }
                    }

                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);
                    TestUtils.assertSql(engine, sqlExecutionContext, query, sink, expected);

                    if (checkStrLen) {
                        try (RecordCursorFactory factory = engine.select("select first(a_string) from x where id%12=4 group by a_symbol", sqlExecutionContext)) {
                            long totalStrLen = 0;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                while (cursor.hasNext()) {
                                    Record record = cursor.getRecord();
                                    totalStrLen += record.getStrLen(0);
                                }
                                Assert.assertEquals(expectedTotalStrLen, totalStrLen);
                            }
                        }
                    }

                    if (checkVarcharLen) {
                        try (RecordCursorFactory factory = engine.select("select first(a_varchar) from x where id%12=5 group by a_symbol", sqlExecutionContext)) {
                            long totalStrLen = 0;
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                while (cursor.hasNext()) {
                                    Record record = cursor.getRecord();
                                    totalStrLen += record.getVarcharSize(0);
                                }
                                Assert.assertEquals(expectedTotalStrLen, totalStrLen);
                            }
                        }
                    }
                },
                configuration,
                LOG
        );
    }

    private void testLateMaterializationWithArrayColumn(boolean rawArrayEncoding) throws Exception {
        node1.setProperty(CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, rawArrayEncoding);
        WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    sqlExecutionContext.setJitMode(enableJitCompiler ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
                    engine.execute(
                            """
                                    create table x as (
                                      select
                                        x id,
                                        cast(x as int) filter_col,
                                        -- 1D with nulls and varying shapes
                                        case
                                          when x % 7 = 0 then null::double[]
                                          when x % 7 = 1 then array[x * 1.0]
                                          when x % 7 = 2 then array[x * 1.0, x * 2.0]
                                          when x % 7 = 3 then array[x * 1.0, x * 2.0, x * 3.0]
                                          when x % 7 = 4 then array[x * 1.0, x * 2.0, x * 3.0, x * 4.0]
                                          when x % 7 = 5 then array[x * 1.0, x * 2.0, x * 3.0, x * 4.0, x * 5.0]
                                          else array[x * 1.0, x * 2.0]
                                        end arr_1d_nulls,
                                        -- 1D without nulls and varying shapes
                                        case
                                          when x % 4 = 0 then array[x * 1.0]
                                          when x % 4 = 1 then array[x * 1.0, x * 2.0]
                                          when x % 4 = 2 then array[x * 1.0, x * 2.0, x * 3.0]
                                          else array[x * 1.0, x * 2.0, x * 3.0, x * 4.0]
                                        end arr_1d,
                                        -- 1D with NaN elements, nulls, and varying shapes
                                        case
                                          when x % 7 = 0 then null::double[]
                                          when x % 7 = 1 then array[NaN]
                                          when x % 7 = 2 then array[x * 1.0, NaN]
                                          when x % 7 = 3 then array[NaN, x * 2.0, x * 3.0]
                                          when x % 7 = 4 then array[x * 1.0, NaN, x * 3.0, NaN]
                                          when x % 7 = 5 then array[x * 1.0, x * 2.0, NaN, x * 4.0, x * 5.0]
                                          else array[NaN, NaN]
                                        end arr_1d_nan,
                                        -- 2D with nulls and varying shapes
                                        case
                                          when x % 5 = 0 then null::double[][]
                                          when x % 5 = 1 then array[[x * 1.0, x * 2.0]]
                                          when x % 5 = 2 then array[[x * 1.0, x * 2.0], [x * 3.0, x * 4.0]]
                                          when x % 5 = 3 then array[[x * 1.0], [x * 2.0], [x * 3.0]]
                                          else array[[x * 1.0, x * 2.0, x * 3.0]]
                                        end arr_2d_nulls,
                                        -- 2D without nulls and varying shapes
                                        case
                                          when x % 3 = 0 then array[[x * 1.0, x * 2.0]]
                                          when x % 3 = 1 then array[[x * 1.0, x * 2.0], [x * 3.0, x * 4.0]]
                                          else array[[x * 1.0], [x * 2.0], [x * 3.0]]
                                        end arr_2d,
                                        -- 2D with NaN elements, nulls, and varying shapes
                                        case
                                          when x % 5 = 0 then null::double[][]
                                          when x % 5 = 1 then array[[NaN, x * 2.0]]
                                          when x % 5 = 2 then array[[x * 1.0, NaN], [NaN, x * 4.0]]
                                          when x % 5 = 3 then array[[NaN], [x * 2.0], [NaN]]
                                          else array[[x * 1.0, NaN, x * 3.0]]
                                        end arr_2d_nan,
                                        -- 3D with nulls and varying shapes
                                        case
                                          when x % 4 = 0 then null::double[][][]
                                          when x % 4 = 1 then array[[[x * 1.0, x * 2.0]]]
                                          when x % 4 = 2 then array[[[x * 1.0], [x * 2.0]], [[x * 3.0], [x * 4.0]]]
                                          else array[[[x * 1.0, x * 2.0], [x * 3.0, x * 4.0]]]
                                        end arr_3d_nulls,
                                        -- 3D without nulls
                                        case
                                          when x % 3 = 0 then array[[[x * 1.0, x * 2.0]]]
                                          when x % 3 = 1 then array[[[x * 1.0], [x * 2.0]], [[x * 3.0], [x * 4.0]]]
                                          else array[[[x * 1.0, x * 2.0], [x * 3.0, x * 4.0]]]
                                        end arr_3d,
                                        -- 3D with NaN elements, nulls, and varying shapes
                                        case
                                          when x % 4 = 0 then null::double[][][]
                                          when x % 4 = 1 then array[[[NaN, x * 2.0]]]
                                          when x % 4 = 2 then array[[[x * 1.0], [NaN]], [[NaN], [x * 4.0]]]
                                          else array[[[NaN, x * 2.0], [x * 3.0, NaN]]]
                                        end arr_3d_nan,
                                        rnd_int() an_int,
                                        rnd_str('a','b') a_string,
                                        timestamp_sequence(0,60000000) as ts
                                      from long_sequence(2000)
                                    ) timestamp(ts) partition by day;""",
                            sqlExecutionContext
                    );

                    // No filter: aggregates on all array columns
                    final StringSink expected0 = new StringSink();
                    final CharSequence query0 = "select first(arr_1d_nulls), last(arr_1d_nulls), first(arr_1d), last(arr_1d),"
                            + " first(arr_1d_nan), last(arr_1d_nan),"
                            + " first(arr_2d_nulls), last(arr_2d_nulls), first(arr_2d), last(arr_2d),"
                            + " first(arr_2d_nan), last(arr_2d_nan),"
                            + " first(arr_3d_nulls), last(arr_3d_nulls), first(arr_3d), last(arr_3d),"
                            + " first(arr_3d_nan), last(arr_3d_nan) from x";
                    engine.print(query0, expected0, sqlExecutionContext);

                    // Random IN-list filter (mixed null/non-null rows)
                    final StringSink expected1 = new StringSink();
                    final CharSequence query1 = generateRndInListSql("filter_col");
                    engine.print(query1, expected1, sqlExecutionContext);

                    // Aggregate with filter and group by on nullable/NaN arrays
                    final StringSink expected2 = new StringSink();
                    final CharSequence query2 = "select first(arr_1d_nulls), last(arr_1d_nulls),"
                            + " first(arr_1d_nan), last(arr_1d_nan),"
                            + " first(arr_2d_nulls), last(arr_2d_nulls),"
                            + " first(arr_2d_nan), last(arr_2d_nan),"
                            + " first(arr_3d_nulls), last(arr_3d_nulls),"
                            + " first(arr_3d_nan), last(arr_3d_nan)"
                            + " from x where filter_col%13=7 group by a_string order by a_string";
                    engine.print(query2, expected2, sqlExecutionContext);

                    // Aggregate with filter and group by on non-nullable arrays
                    final StringSink expected3 = new StringSink();
                    final CharSequence query3 = "select first(arr_1d), last(arr_1d),"
                            + " first(arr_2d), last(arr_2d),"
                            + " first(arr_3d), last(arr_3d)"
                            + " from x where filter_col%11=3 group by a_string order by a_string";
                    engine.print(query3, expected3, sqlExecutionContext);

                    // Select arrays with modulo filter (tests late materialization of array columns)
                    final StringSink expected4 = new StringSink();
                    final CharSequence query4 = "select arr_1d_nulls, arr_1d, arr_1d_nan,"
                            + " arr_2d_nulls, arr_2d, arr_2d_nan,"
                            + " arr_3d_nulls, arr_3d, arr_3d_nan"
                            + " from x where filter_col % 17 = 5";
                    engine.print(query4, expected4, sqlExecutionContext);

                    // No filter: select all array columns directly
                    final StringSink expected5 = new StringSink();
                    final CharSequence query5 = "select arr_1d_nulls, arr_1d, arr_1d_nan,"
                            + " arr_2d_nulls, arr_2d, arr_2d_nan,"
                            + " arr_3d_nulls, arr_3d, arr_3d_nan from x";
                    engine.print(query5, expected5, sqlExecutionContext);

                    engine.execute("alter table x convert partition to parquet where ts >= 0", sqlExecutionContext);

                    TestUtils.assertSql(engine, sqlExecutionContext, query0, sink, expected0);
                    TestUtils.assertSql(engine, sqlExecutionContext, query1, sink, expected1);
                    TestUtils.assertSql(engine, sqlExecutionContext, query2, sink, expected2);
                    TestUtils.assertSql(engine, sqlExecutionContext, query3, sink, expected3);
                    TestUtils.assertSql(engine, sqlExecutionContext, query4, sink, expected4);
                    TestUtils.assertSql(engine, sqlExecutionContext, query5, sink, expected5);
                },
                configuration,
                LOG
        );
    }
}
