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
package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.StreamGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncStreamGroupByRecordCursorFactory;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class StreamGroupByTest extends AbstractCairoTest {
    private final int frameRows;
    private final boolean parallel;
    private final int workers;

    public StreamGroupByTest(boolean parallel, int workers, int frameRows) {
        this.parallel = parallel;
        this.workers = workers;
        this.frameRows = frameRows;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, frameRows={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 1, 17},
                {true, 0, 17},
                {true, 1, 1},
                {true, 4, 17},
                {true, 4, 256}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(parallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, frameRows);
        super.setUp();
    }

    @Test
    public void testAggregatesAcrossFrames() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v), sum(d), avg(v), avg(d), "
                            + "min(v), max(v), min(d), max(d), first(v), last(v), first_not_null(v), last_not_null(v), "
                            + "first(d), last(d), first_not_null(d), last_not_null(d) "
                            + "from $table order by bucket");
        });
    }

    @Test
    public void testAllNullGroups() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1000000L) ts, "
                    + "case when x > 60 and x <= 120 then x end v "
                    + "from long_sequence(181)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(v), sum(v), avg(v), min(v), max(v), "
                            + "first(v), last(v), first_not_null(v), last_not_null(v) from $table order by bucket");
        });
    }

    @Test
    public void testFilteredInput() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String sql = "select timestamp_floor('1m', ts) bucket, count(*), sum(v), first(v), last(v) "
                    + "from $table where v % 7 = 0 order by bucket";
            try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                assertStream(actual, !parallel);
                Assert.assertEquals(read(expected, context), read(actual, context));
            }
        });
    }

    @Test
    public void testRepeatedGroupBySubquery() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String sql = "with g as (select timestamp_floor('1m', ts) bucket, sum(v) s from $table) "
                    + "select a.bucket, a.s, b.s from g a join g b on a.bucket = b.bucket order by a.bucket";
            try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
                final String expectedText = read(expected, context);
                for (int iteration = 0; iteration < 3; iteration++) {
                    Assert.assertEquals(expectedText, read(actual, context));
                }
                final String plan = TestUtils.printSqlToString(engine, context,
                        "explain " + sql.replace("$table", "trades"), new StringSink());
                Assert.assertTrue(plan, plan.contains("Stream Group By"));
            }
        });
    }

    @Test
    public void testCalendarBuckets() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String key : new String[]{"date_trunc('month', ts)", "timestamp_floor('1y', ts)",
                    "timestamp_floor('15m', ts)", "timestamp_floor_utc('1d', ts, null, '00:00', null)",
                    "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)", "timestamp_floor('3w', ts)",
                    "timestamp_ceil('h', ts)", "year(ts)", "ts::long"}) {
                assertMatchesHash(compiler, context, "select " + key + " bucket, count(*), sum(v), avg(d) from $table order by bucket");
            }
        });
    }

    @Test
    public void testConstantTimestampShifts() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String key : new String[]{
                    "dateadd('h', 1, ts)",
                    "dateadd('H', 1, ts)",
                    "dateadd('u', 1, ts)",
                    "dateadd('M', 0, ts)",
                    "dateadd('w', -1, ts)",
                    "dateadd('h', 1, ts, '+05:30')",
                    "dateadd('u', 1, ts, '+05:30')",
                    "dateadd('y', 0, ts, '+05:30')",
                    "ts + 1000L",
                    "ts - 1000L",
                    "to_timezone(ts, '+05:30')",
                    "to_utc(ts, '+05:30')",
                    "timestamp_floor_utc('1h', ts, null, '00:00', '+05:30')",
                    "timestamp_floor_utc('1d', ts, null, '00:00', '-04:00')",
                    "timestamp_floor('1h', ts, null, '00:00', '+05:30')"
            }) {
                assertMatchesHash(compiler, context,
                        "select " + key + " bucket, count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
        });
    }

    @Test
    public void testDstSubDayBuckets() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, v long) timestamp(ts) partition by day", context);
            for (String start : new String[]{"2024-03-10T05:00:00", "2024-11-03T04:00:00"}) {
                engine.execute("insert into trades select timestamp_sequence('" + start + "'::timestamp, 60000000L), x from long_sequence(600)", context);
            }
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            for (String bucket : new String[]{"17m", "1h", "3h"}) {
                assertMatchesHash(compiler, context,
                        "select timestamp_floor_utc('" + bucket + "', ts, null, '00:00', 'America/New_York') bucket, "
                                + "count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
        });
    }

    @Test
    public void testTimestampDomainBounds() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades (ts timestamp, v long) TIMESTAMP(ts) PARTITION BY YEAR", context);
            engine.execute("INSERT INTO trades VALUES (0::timestamp, 1), (1::timestamp, 2), "
                    + "('1970-01-01T23:59:59.999999', 3), ('1999-12-31T23:59:59.999999', 4), "
                    + "('2000-02-29T23:59:59.999999', 5), ('9999-12-31T23:59:59.999999', 6)", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, v long)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            for (String key : new String[]{"year(ts)", "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)",
                    "timestamp_floor('2147483647M', ts)", "timestamp_floor('3w', ts)", "timestamp_ceil('y', ts)",
                    "date_trunc('millennium', ts)", "dateadd('h', 24, ts)", "ts - 1000L",
                    "timestamp_floor_utc('3M', ts, null, '00:00', '+05:30')"}) {
                assertMatchesHash(compiler, context, "SELECT " + key + " bucket, count(*), sum(v), first(v), last(v) "
                        + "FROM $table ORDER BY bucket");
            }
        });
    }

    @Test
    public void testTimestampOverrideUsesHash() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, other timestamp, v long) timestamp(ts) partition by day", context);
            engine.execute("insert into trades values "
                    + "('2024-01-01', '2024-01-02', 1), "
                    + "('2024-01-02', null, 2), "
                    + "('2024-01-03', '2024-01-01', 3), "
                    + "('2024-01-04', '2024-01-02', 4), "
                    + "('2024-01-05', null, 5)", context);
            for (String source : new String[]{"trades", "(select v, other, ts from trades)"}) {
                try (RecordCursorFactory factory = compiler.compile(
                        "select other, count(*), sum(v) from " + source + " timestamp(other) sample by 1d", context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                    Assert.assertEquals("other\tcount\tsum\n"
                            + "\t2\t7\n"
                            + "2024-01-01T00:00:00.000000Z\t1\t3\n"
                            + "2024-01-02T00:00:00.000000Z\t2\t5\n", read(factory, context));
                }
            }
        });
    }

    @Test
    public void testColumnTopsAndExpressions() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            engine.execute("alter table trades add column late double", context);
            engine.execute("insert into trades values ('2024-01-02T00:00:00', 5, 4.0, 'A', 9.0)", context);
            engine.execute("drop table hash_input", context);
            engine.execute("create table hash_input (ts timestamp, v long, d double, sym symbol, late double)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1d', ts) bucket, sum(late), sum(v + 1), avg(d * 2), first(v), last(v) from $table order by bucket");
        });
    }

    @Test
    public void testEmptyAndSingleRow() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades (ts timestamp, v long) timestamp(ts)", context);
            engine.execute("create table hash_input (ts timestamp, v long)", context);
            final String sql = "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket";
            assertMatchesHash(compiler, context, sql);
            engine.execute("insert into trades values ('2024-01-01', null)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context, sql);
        });
    }

    @Test
    public void testEveryRowIsAGroup() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1U', ts) bucket, count(*), sum(v), avg(d) from $table order by bucket");
        });
    }

    @Test
    public void testLimitAndAggregateFilter() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket limit 3");
            assertMatchesHash(compiler, context,
                    "select * from (select timestamp_floor('1m', ts) bucket, count(*) c, sum(v) from $table) where c > 10 order by bucket limit 2, 5");
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v) from $table order by bucket desc limit 3");
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (RecordCursorFactory expected = compiler.compile(
                    "select timestamp_floor('1m', ts) ts, sum(v), avg(d) from hash_input order by ts", context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(
                         "select ts, sum(v), avg(d) from trades sample by 1m align to calendar", context).getRecordCursorFactory()) {
                assertStream(actual, true);
                Assert.assertEquals(read(expected, context), read(actual, context));
                Assert.assertEquals(0, actual.getMetadata().getTimestampIndex());
            }
        });
    }

    @Test
    public void testUnsafeKeysAndAggregatesUseHash() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String sql : new String[]{
                    "select timestamp_floor('1m', ts), sym, sum(v) from trades",
                    "select timestamp_floor('1m', ts), count_distinct(v) from trades",
                    "select timestamp_floor_utc('1d', ts, null, '01:30', 'America/New_York'), sum(v) from trades",
                    "select timestamp_floor('1m', ts), sum(rnd_long()) from trades",
                    "select timestamp_floor('2147483647d', ts), sum(v) from trades",
                    "select timestamp_floor('3n', ts), sum(v) from trades",
                    "select dateadd('M', 1, ts), sum(v) from trades",
                    "select timestamp_floor('1h', ts, null, '00:00', 'America/New_York'), sum(v) from trades",
                    "select ts, sum(v) from trades sample by 1m fill(prev) align to calendar"
            }) {
                try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                    read(factory, context);
                }
            }
        });
    }

    @Test
    public void testExplain() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final String plan = TestUtils.printSqlToString(engine, context,
                    "explain select ts, sum(v) from trades sample by 1m align to calendar", new StringSink());
            Assert.assertEquals("QUERY PLAN\n"
                    + (parallel ? "Async Stream Group By workers: " + Math.max(1, workers) : "Stream Group By") + "\n"
                    + "  keys: [ts]\n"
                    + "  keyFunctions: [timestamp_floor_utc('1m',ts)]\n"
                    + "  values: [sum(v)]\n"
                    + "    PageFrame\n"
                    + "        Row forward scan\n"
                    + "        Frame forward scan on: trades\n", plan);
        });
    }

    @Test
    public void testNanosecondTimestamps() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence_ns('2024-01-01'::timestamp_ns, 123456789L) ts, x v "
                    + "from long_sequence(501)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp_ns, v long)", context);
            engine.execute("insert into hash_input select * from trades", context);
            for (String key : new String[]{"timestamp_floor('7s', ts)", "timestamp_floor('1n', ts)",
                    "timestamp_floor('3M', ts)", "timestamp_floor('5y', ts)", "date_trunc('month', ts)",
                    "dateadd('h', -1, ts, '+05:30')", "timestamp_ceil('n', ts)", "year(ts)"}) {
                assertMatchesHash(compiler, context, "select " + key + " bucket, count(*), sum(v), first(v), last(v) from $table order by bucket");
            }
            try (RecordCursorFactory factory = compiler.compile(
                    "select date_trunc('millennium', ts), sum(v) from trades", context).getRecordCursorFactory()) {
                assertStream(factory, false);
            }
            assertMatchesHash(compiler, context,
                    "select dateadd('h', -1, ts) bucket, count(*), sum(v) from $table order by bucket");
            assertMatchesHash(compiler, context,
                    "select ts::timestamp bucket, count(*), sum(v) from $table order by bucket");
            for (String key : new String[]{"dateadd('h', 1, ts)", "timestamp_floor_utc('1h', ts, null, '00:00', '+01:00')",
                    "timestamp_floor_utc('1w', ts, null, '00:00', null)", "timestamp_floor('1w', ts)",
                    "dateadd('h', 1, ts, '+05:30')", "timestamp_ceil('h', ts)"}) {
                try (RecordCursorFactory factory = compiler.compile(
                        "select " + key + ", sum(v) from trades", context).getRecordCursorFactory()) {
                    assertStream(factory, false);
                }
            }
        });
    }

    @Test
    public void testParquet() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            engine.execute("alter table trades convert partition to parquet where ts >= 0", context);
            assertMatchesHash(compiler, context,
                    "select timestamp_floor('1m', ts) bucket, count(*), sum(v), avg(d), first(v), last(v) from $table order by bucket");
        });
    }

    @Test
    public void testFixedStateTypes() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1000000L) ts, "
                    + "case when x % 5 = 0 then null else (x % 37)::int end i, "
                    + "case when x % 7 = 0 then null else (x % 29)::float end f, "
                    + "(x % 11)::byte b, (x % 17)::short s, (65 + x % 5)::char ch, "
                    + "x % 2 = 0 flag, ('2024-01-01'::timestamp + x * 1000)::date dt, "
                    + "case when x % 3 = 0 then null else 'str' end str, "
                    + "case when x % 3 = 0 then null else 'str'::varchar end vc "
                    + "from long_sequence(701)) timestamp(ts) partition by day", context);
            engine.execute("create table hash_input (ts timestamp, i int, f float, b byte, s short, ch char, flag boolean, dt date, str string, vc varchar)", context);
            engine.execute("insert into hash_input select * from trades", context);
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "sum(i), avg(i), min(i), max(i), count(i), first(i), last(i), first_not_null(i), last_not_null(i), "
                    + "sum(f), avg(f), min(f), max(f), count(f), first(f), last(f), first_not_null(f), last_not_null(f), "
                    + "sum(b), min(b), max(b), count(b), first(b), last(b), "
                    + "sum(s), min(s), max(s), count(s), first(s), last(s), "
                    + "min(ch), max(ch), count(ch), first(ch), last(ch), first(flag), last(flag), "
                    + "min(dt), max(dt), count(dt), first(dt), last(dt), "
                    + "min(ts), max(ts), count(ts), first(ts), last(ts), count(str), count(vc) "
                    + "from $table order by bucket");
        });
    }

    @Test
    public void testStatisticalAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final StringBuilder functions = new StringBuilder("count(*)");
            for (String function : new String[]{"ksum", "nsum", "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
                    "skewness", "skewness_samp", "skewness_pop", "kurtosis", "kurtosis_samp", "kurtosis_pop"}) {
                functions.append(", round(").append(function).append("(d), 8)");
            }
            functions.append(", round(geomean(d + 1), 8)");
            for (String function : new String[]{"covar_samp", "covar_pop", "corr", "regr_slope", "regr_r2", "regr_intercept",
                    "weighted_stddev_rel", "weighted_stddev_freq", "weighted_avg", "vwap"}) {
                functions.append(", round(").append(function).append("(d, v), 8)");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
        });
    }

    @Test
    public void testDecimalAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String type : new String[]{"decimal(2,0)", "decimal(4,1)", "decimal(9,2)", "decimal(18,2)", "decimal(38,2)", "decimal(60,2)"}) {
                final String arg = "d::" + type;
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, count(" + arg + "), "
                        + "sum(" + arg + "), avg(" + arg + "), avg(" + arg + ", 3), min(" + arg + "), max(" + arg + "), "
                        + "first(" + arg + "), last(" + arg + "), first_not_null(" + arg + "), last_not_null(" + arg + ") "
                        + "from $table order by bucket");
            }
        });
    }

    @Test
    public void testBitwiseAndBooleanAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            final StringBuilder functions = new StringBuilder("avg(v::short), bool_and(v > 10), bool_or(v > 10), mode(v > 10)");
            for (String type : new String[]{"byte", "short", "int", "long"}) {
                for (String function : new String[]{"bit_and", "bit_or", "bit_xor"}) {
                    functions.append(", ").append(function).append("(v::").append(type).append(')');
                }
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
        });
    }

    @Test
    public void testOtherFixedWidthTypes() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String arg : new String[]{"v::int::ipv4", "to_uuid(v, v)", "sym", "v::geohash(5b)", "v::geohash(10b)",
                    "v::geohash(20b)", "v::geohash(40b)"}) {
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, count(" + arg + "), "
                        + "first(" + arg + "), last(" + arg + "), first_not_null(" + arg + "), last_not_null(" + arg + ") "
                        + "from $table order by bucket");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "sum(v::long256), min(v::int::ipv4), max(v::int::ipv4) from $table order by bucket");
        });
    }

    @Test
    public void testArgMinMax() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String value : new String[]{"'x'::char", "d", "v", "ts"}) {
                final StringBuilder functions = new StringBuilder("count(*)");
                for (String key : new String[]{"v::double", "v", "ts"}) {
                    functions.append(", arg_min(").append(value).append(',').append(key).append(')');
                    functions.append(", arg_max(").append(value).append(',').append(key).append(')');
                }
                assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, " + functions + " from $table order by bucket");
            }
            assertMatchesHash(compiler, context, "select timestamp_floor('1m', ts) bucket, "
                    + "arg_min(to_uuid(v, v), ts), arg_max(to_uuid(v, v), ts), "
                    + "arg_min(ts, to_uuid(v, v)), arg_max(ts, to_uuid(v, v)) from $table order by bucket");
        });
    }

    @Test
    public void testBoundedDispatch() throws Exception {
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            for (String bucket : new String[]{"1d", "7s", "1U"}) {
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('" + bucket + "', ts) bucket, "
                        + "count(*), sum(v), first(v), last(v) FROM $table ORDER BY bucket");
            }
        });
    }

    @Test
    public void testBindVariableReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            context.getBindVariableService().setLong(0, 1);
            final String sql = "SELECT timestamp_floor('1m', ts) bucket, sum(v + $1), $1 tag FROM ";
            try (RecordCursorFactory expected = compiler.compile(sql + "hash_input ORDER BY bucket", context).getRecordCursorFactory();
                 RecordCursorFactory actual = compiler.compile(sql + "trades ORDER BY bucket", context).getRecordCursorFactory()) {
                assertStream(actual, true);
                for (int i = 0; i < 3; i++) {
                    context.getBindVariableService().setLong(0, i * 17L);
                    Assert.assertEquals(read(expected, context), read(actual, context));
                }
            }
        });
    }

    @Test
    public void testLongSequence() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE hash_input AS (SELECT x FROM long_sequence(101))", context);
            for (String source : new String[]{"long_sequence(101)", "long_sequence(101, 1, 2)"}) {
                final String sql = "SELECT x, count(*), sum(x), first(x), last(x) FROM ";
                try (RecordCursorFactory expected = compiler.compile(sql + "hash_input ORDER BY x", context).getRecordCursorFactory();
                     RecordCursorFactory actual = compiler.compile(sql + source + " ORDER BY x", context).getRecordCursorFactory()) {
                    assertStream(expected, false);
                    assertStream(actual, true, false);
                    final String expectedText = read(expected, context);
                    for (int i = 0; i < 3; i++) {
                        try (RecordCursor cursor = actual.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            cursor.toTop();
                            final StringSink sink = new StringSink();
                            CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                            Assert.assertEquals(expectedText, sink.toString());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSerialOnlyAggregates() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + "isOrdered(v), isOrdered(v::int::ipv4), haversine_dist_deg(d, d, ts), "
                    + "first(v::symbol), last(v::symbol) FROM $table ORDER BY bucket", false);
        });
    }

    @Test
    public void testStatisticalNullGroups() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE trades AS (SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1000000L) ts, "
                    + "CASE WHEN x = 61 OR x > 120 THEN (x % 19)::double END d "
                    + "FROM long_sequence(181)) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("CREATE TABLE hash_input (ts timestamp, d double)", context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + "count(d), round(stddev_pop(d), 8), round(stddev_samp(d), 8), "
                    + "round(skewness_pop(d), 8), round(kurtosis_pop(d), 8), "
                    + "round(corr(d, d), 8), round(covar_pop(d, d), 8), round(regr_slope(d, d), 8) "
                    + "FROM $table ORDER BY bucket");
        });
    }

    @Test
    public void testStoredFixedWidthTypes() throws Exception {
        run((engine, compiler, context) -> {
            final String columns = "(ts timestamp, ip ipv4, id uuid, sym symbol, dec decimal(18,2), "
                    + "g1 geohash(5b), g2 geohash(10b), g4 geohash(20b), g8 geohash(40b))";
            engine.execute("CREATE TABLE trades " + columns + " TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO trades SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1000000L) ts, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE x::int::ipv4 END ip, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE to_uuid(x, x) END id, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE (x % 19)::symbol END sym, "
                    + "CASE WHEN x <= 120 OR x % 7 = 0 THEN null ELSE (x % 31)::decimal(18,2) END dec, "
                    + "CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(5b)) END AS g1, CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(10b)) END AS g2, "
                    + "CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(20b)) END AS g4, CASE WHEN x <= 120 THEN null ELSE CAST(x AS geohash(40b)) END AS g8 "
                    + "FROM long_sequence(701)", context);
            engine.execute("CREATE TABLE hash_input " + columns, context);
            engine.execute("INSERT INTO hash_input SELECT * FROM trades", context);
            final StringBuilder functions = new StringBuilder("count(*)");
            for (String column : new String[]{"ip", "id", "sym", "dec", "g1", "g2", "g4", "g8"}) {
                final int start = functions.length();
                for (String function : new String[]{"count", "first", "last", "first_not_null", "last_not_null"}) {
                    functions.append(',').append(function).append('(').append(column).append(')');
                }
                assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket"
                        + functions.substring(start) + " FROM $table ORDER BY bucket");
            }
            functions.append(", sum(dec), avg(dec), min(dec), max(dec), min(ip), max(ip)");
            assertMatchesHash(compiler, context, "SELECT timestamp_floor('1m', ts) bucket, "
                    + functions + " FROM $table ORDER BY bucket");
        });
    }

    @Test
    public void testMemoryIsReleasedOnClose() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (RecordCursorFactory factory = compiler.compile(
                    "select timestamp_floor('1U', ts) bucket, count(*), sum(v) from trades", context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                assertStream(factory, true);
                context.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 5; iteration++) {
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(tracker.getUsed() > 0);
                            cursor.toTop();
                            Assert.assertTrue(cursor.hasNext());
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                    }
                } finally {
                    context.setMemoryTracker(null);
                }
            }
        });
    }

    @Test
    public void testMemoryLimitAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (RecordCursorFactory factory = compiler.compile(
                    "select timestamp_floor('1U', ts) bucket, count(*), sum(v), avg(d) from trades", context).getRecordCursorFactory()) {
                assertStream(factory, true);
                for (int iteration = 0; iteration < 3; iteration++) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, parallel && frameRows > 1 ? 128 : 1);
                    try {
                        read(factory, context);
                        Assert.fail("expected memory limit breach");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                    } finally {
                        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    }
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        long count = 0;
                        while (cursor.hasNext()) {
                            count++;
                        }
                        Assert.assertEquals(1001, count);
                    }
                }
            }
        });
    }

    @Test
    public void testCancellationAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createTable(compiler, context);
            try (NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration() {
                        @Override
                        public int getCircuitBreakerThrottle() {
                            return 0;
                        }
                    });
                 SqlExecutionContextImpl cancellable = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                         .with(context.getSecurityContext(), null, null, -1, breaker);
                 RecordCursorFactory factory = compiler.compile(
                         "select timestamp_floor('1U', ts) bucket, count(*), sum(v) from trades", cancellable).getRecordCursorFactory()) {
                assertStream(factory, true);
                for (int iteration = 0; iteration < 3; iteration++) {
                    try (RecordCursor cursor = factory.getCursor(cancellable)) {
                        Assert.assertTrue(cursor.hasNext());
                        breaker.cancel();
                        try {
                            while (cursor.hasNext()) {
                            }
                            Assert.fail("expected cancellation");
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getFlyweightMessage().toString(), e.isInterruption());
                        }
                    } finally {
                        breaker.clear();
                    }
                    Assert.assertFalse(read(factory, cancellable).isEmpty());
                }
            }
        });
    }

    @Test
    public void testDstRepeatedBucketUsesHash() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-11-03T04:50:00'::timestamp, 1200000000L) ts, x v "
                    + "from long_sequence(7)) timestamp(ts) partition by day", context);
            try (RecordCursorFactory factory = compiler.compile(
                    "select timestamp_floor_utc('1d', ts, null, '01:30', 'America/New_York') bucket, count(*), sum(v) "
                            + "from trades order by bucket", context).getRecordCursorFactory()) {
                assertStream(factory, false);
                Assert.assertEquals("bucket\tcount\tsum\n"
                        + "2024-11-02T05:30:00.000000Z\t3\t8\n"
                        + "2024-11-03T05:30:00.000000Z\t2\t7\n"
                        + "2024-11-03T06:30:00.000000Z\t2\t13\n", read(factory, context));
            }
        });
    }

    private void assertMatchesHash(SqlCompiler compiler, SqlExecutionContext context, String sql) throws Exception {
        assertMatchesHash(compiler, context, sql, parallel);
    }

    private void assertMatchesHash(SqlCompiler compiler, SqlExecutionContext context, String sql, boolean expectedParallel) throws Exception {
        try (RecordCursorFactory expected = compiler.compile(sql.replace("$table", "hash_input"), context).getRecordCursorFactory();
             RecordCursorFactory actual = compiler.compile(sql.replace("$table", "trades"), context).getRecordCursorFactory()) {
            assertStream(expected, false);
            try {
                assertStream(actual, true, expectedParallel);
            } catch (AssertionError error) {
                throw new AssertionError(sql + '\n' + TestUtils.printSqlToString(context.getCairoEngine(), context,
                        "explain " + sql.replace("$table", "trades"), new StringSink()), error);
            }
            final String expectedText = read(expected, context);
            for (int iteration = 0; iteration < 3; iteration++) {
                try (RecordCursor cursor = actual.getCursor(context)) {
                    cursor.hasNext();
                    cursor.toTop();
                    final StringSink sink = new StringSink();
                    CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                    Assert.assertEquals(sql, expectedText, sink.toString());
                    cursor.toTop();
                    sink.clear();
                    CursorPrinter.println(cursor, actual.getMetadata(), sink, true, false);
                    Assert.assertEquals(sql, expectedText, sink.toString());
                }
                try (RecordCursor cursor = actual.getCursor(context)) {
                    cursor.hasNext();
                }
                for (RecordCursorFactory f = actual; f != null; f = f.getBaseFactory()) {
                    if (f instanceof AsyncStreamGroupByRecordCursorFactory stream) {
                        Assert.assertEquals(0, stream.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                }
            }
        }
    }

    private void assertStream(RecordCursorFactory factory, boolean expected) {
        assertStream(factory, expected, parallel);
    }

    private void assertStream(RecordCursorFactory factory, boolean expected, boolean expectedParallel) {
        boolean found = false;
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof AsyncStreamGroupByRecordCursorFactory) {
                Assert.assertTrue(expectedParallel);
                found = true;
            } else if (f instanceof StreamGroupByRecordCursorFactory) {
                Assert.assertFalse(expectedParallel);
                found = true;
            }
        }
        Assert.assertEquals(expected, found);
    }

    private static void createTable(SqlCompiler compiler, SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("create table trades as (select "
                + "timestamp_sequence('2024-01-01T00:00:00'::timestamp, 1000000L) ts, "
                + "case when x % 11 = 0 then null else x end v, "
                + "case when x % 13 = 0 then null else (x % 29)::double end d, "
                + "case when x % 2 = 0 then 'A' else 'B' end::symbol sym "
                + "from long_sequence(1001)) timestamp(ts) partition by day", context);
        context.getCairoEngine().execute("create table hash_input (ts timestamp, v long, d double, sym symbol)", context);
        context.getCairoEngine().execute("insert into hash_input select * from trades", context);
    }

    private static String read(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(context)) {
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        }
        return sink.toString();
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workers == 0 ? null : new TestWorkerPool(workers), runnable, configuration, LOG));
    }
}
