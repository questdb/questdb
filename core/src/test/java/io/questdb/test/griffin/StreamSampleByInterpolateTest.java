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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.MemoryTrackerWorkload;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
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
public class StreamSampleByInterpolateTest extends AbstractCairoTest {
    private final boolean parallel;
    private final String timestampType;
    private final int workers;

    public StreamSampleByInterpolateTest(boolean parallel, int workers, String timestampType) {
        this.parallel = parallel;
        this.workers = workers;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "parallel={0}, workers={1}, type={2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {false, 0, "timestamp"},
                {false, 0, "timestamp_ns"},
                {true, 0, "timestamp"},
                {true, 4, "timestamp"},
                {true, 4, "timestamp_ns"}
        });
    }

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(parallel));
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 2);
        super.setUp();
    }

    @Test
    public void testAggregateStatesAndNulls() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            query(context, "SELECT ts, avg(price) mean, sum(price) total, count() n, count(price) nn "
                    + "FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR")
                    .returns(precision("""
                            ts\tmean\ttotal\tn\tnn
                            2024-01-01T00:00:00.000000Z\t2.0\t4.0\t2\t2
                            2024-01-01T01:00:00.000000Z\t5.0\t10.0\t2\t2
                            2024-01-01T02:00:00.000000Z\t8.0\t16.0\t2\t2
                            2024-01-01T03:00:00.000000Z\tnull\tnull\t1\t1
                            2024-01-01T04:00:00.000000Z\tnull\tnull\t1\t0
                            """));
        });
    }

    @Test
    public void testDescendingTimestampsFail() throws Exception {
        run((engine, compiler, context) -> {
            final String sql = "SELECT ts, k, sum(v) s FROM (SELECT (7_200_000_000 - x * 3_600_000_000)::" + timestampType
                    + " ts, 'k' || (x % 2) k, x v FROM long_sequence(3)) TIMESTAMP(ts) SAMPLE BY 1h FILL(LINEAR)";
            for (String align : new String[]{" ALIGN TO CALENDAR", " ALIGN TO FIRST OBSERVATION"}) {
                try (RecordCursorFactory factory = compiler.compile(sql + align, context).getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(context)) {
                    cursor.hasNext();
                    Assert.fail("expected descending timestamps to fail");
                } catch (CairoException e) {
                    TestUtils.assertEquals("SAMPLE BY FILL(LINEAR) requires ascending designated timestamp order", e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testEmptyAndSingleBucket() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            final String sql = "SELECT ts, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR";
            query(context, sql).returns("ts\tmean\n");
            engine.execute("INSERT INTO prices VALUES ('2024-01-01T00:10', 'a', 2), ('2024-01-01T00:20', 'a', 4)", context);
            query(context, sql).returns(precision("ts\tmean\n2024-01-01T00:00:00.000000Z\t3.0\n"));
        });
    }

    @Test
    public void testFilteredInput() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            query(context, "SELECT ts, avg(price) mean, count() n FROM prices WHERE price > 1 "
                    + "SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR")
                    .returns(precision("""
                            ts\tmean\tn
                            2024-01-01T00:00:00.000000Z\t3.0\t1
                            2024-01-01T01:00:00.000000Z\t5.5\t1
                            2024-01-01T02:00:00.000000Z\t8.0\t2
                            """));
        });
    }

    @Test
    public void testFirstObservationKeepsLegacy() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01T00:10', 'a', 1), ('2024-01-01T02:10', 'a', 3)", context);
            assertQuery("SELECT ts, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO FIRST OBSERVATION")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").expectSize()
                    .withPlanNotContaining("Stream Group By")
                    .returns(precision("""
                            ts\tmean
                            2024-01-01T00:10:00.000000Z\t1.0
                            2024-01-01T01:10:00.000000Z\t2.0
                            2024-01-01T02:10:00.000000Z\t3.0
                            """));
        });
    }

    @Test
    public void testKeyedBoundaryExtrapolation() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('1970-01-01T00:00', 'a', 1), ('1970-01-01T01:10', 'b', 7), "
                    + "('1970-01-01T02:10', 'a', 3), ('1970-01-01T03:10', 'b', 11)", context);
            query(context, "SELECT ts, sym, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR) "
                    + "ALIGN TO CALENDAR ORDER BY ts, sym")
                    .returns(precision("""
                            ts\tsym\tmean
                            1970-01-01T00:00:00.000000Z\ta\t1.0
                            1970-01-01T00:00:00.000000Z\tb\t5.0
                            1970-01-01T01:00:00.000000Z\ta\t2.0
                            1970-01-01T01:00:00.000000Z\tb\t7.0
                            1970-01-01T02:00:00.000000Z\ta\t3.0
                            1970-01-01T02:00:00.000000Z\tb\t9.0
                            1970-01-01T03:00:00.000000Z\ta\t4.0
                            1970-01-01T03:00:00.000000Z\tb\t11.0
                            """));
            assertMatchesLegacy(context, "SELECT ts, sym, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR)",
                    " ORDER BY ts, sym");
        });
    }

    @Test
    public void testKeyedIgnoresSortLimit() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 8 * 1024);
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 120_000_000L)::" + timestampType
                    + ", 'A', x::double FROM long_sequence(2049)", context);
            assertMatchesLegacy(context, "SELECT ts, sym, avg(price) mean FROM prices SAMPLE BY 1m FILL(LINEAR)", " ORDER BY ts, sym");
        });
    }

    @Test
    public void testKeyedLateKeysKeepTimestampOrder() throws Exception {
        run((engine, compiler, context) -> {
            for (String type : new String[]{"SYMBOL", "VARCHAR"}) {
                final String table = "late_" + type;
                engine.execute("CREATE TABLE " + table + " (ts " + timestampType + ", sym " + type + ", price DOUBLE) "
                        + "TIMESTAMP(ts) PARTITION BY DAY", context);
                engine.execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00', 'a', 1), ('2024-01-01T01:00', 'a', 3), "
                        + "('2024-01-01T02:00', 'b', 10), ('2024-01-01T03:00', 'b', 14), ('2024-01-01T04:00', 'a', 9)", context);
                final String sql = "SELECT ts, sym, avg(price) mean FROM " + table + " SAMPLE BY 1h FILL(LINEAR)";
                query(context, sql + " ALIGN TO CALENDAR")
                        .returns(precision("""
                                ts\tsym\tmean
                                2024-01-01T00:00:00.000000Z\ta\t1.0
                                2024-01-01T00:00:00.000000Z\tb\t2.0
                                2024-01-01T01:00:00.000000Z\ta\t3.0
                                2024-01-01T01:00:00.000000Z\tb\t6.0
                                2024-01-01T02:00:00.000000Z\ta\t5.0
                                2024-01-01T02:00:00.000000Z\tb\t10.0
                                2024-01-01T03:00:00.000000Z\ta\t7.0
                                2024-01-01T03:00:00.000000Z\tb\t14.0
                                2024-01-01T04:00:00.000000Z\ta\t9.0
                                2024-01-01T04:00:00.000000Z\tb\t18.0
                                """));
                assertMatchesLegacy(context, sql, " ORDER BY ts, sym");
            }
        });
    }

    @Test
    public void testKeyedMapLimitAndReuse() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 1024);
        setProperty(PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, 0);
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices SELECT "
                    + "timestamp_sequence('2024-01-01'::timestamp, 120_000_000L)::" + timestampType
                    + ", 'k' || x, x::double FROM long_sequence(2049)", context);
            context.getBindVariableService().setDouble("max_price", 2049);
            final String sql = "SELECT ts, sym, avg(price) mean FROM prices WHERE price <= :max_price "
                    + "SAMPLE BY 1m FILL(LINEAR) ALIGN TO CALENDAR";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Stream Group By");
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory();
                 MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                context.setMemoryTracker(tracker);
                try {
                    for (int iteration = 0; iteration < 3; iteration++) {
                        context.getBindVariableService().setDouble("max_price", 2049);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            cursor.hasNext();
                            Assert.fail("expected map resize limit breach");
                        } catch (CairoException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "limit of 0 resizes exceeded in FastMap");
                        }
                        Assert.assertEquals(0, tracker.getUsed());
                        context.getBindVariableService().setDouble("max_price", 3);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            for (int pass = 0; pass < 2; pass++) {
                                int count = 0;
                                while (cursor.hasNext()) {
                                    final int bucket = count / 3;
                                    final int key = count % 3;
                                    TestUtils.assertEquals("k" + (key + 1), cursor.getRecord().getSymA(1));
                                    Assert.assertEquals(bucket == 2 * key ? key + 1 : Double.NaN, cursor.getRecord().getDouble(2), 0.0);
                                    count++;
                                }
                                Assert.assertEquals(15, count);
                                cursor.toTop();
                            }
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
    public void testKeyedMultipleKeysAndSingleObservation() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE prices (ts " + timestampType + ", sym SYMBOL, venue INT, price DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'a', 1, 1), ('2024-01-01T01:00', null, 1, 5), "
                    + "('2024-01-01T03:00', 'a', 2, 7)", context);
            assertMatchesLegacy(context, "SELECT ts, sym, venue, avg(price) mean FROM prices "
                    + "SAMPLE BY 1h FILL(LINEAR)", " ORDER BY ts, sym, venue");
        });
    }

    @Test
    public void testKeyedMatchesModel() throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final int keyCount = 1 + rnd.nextInt(40);
        final int bucketLimit = 1 + rnd.nextInt(60);
        final long hour = timestampType.equals("timestamp_ns") ? 3_600_000_000_000L : 3_600_000_000L;
        final long start = 1_704_067_200L * (hour / 3_600L);
        final boolean[][] hasData = new boolean[bucketLimit][keyCount];
        final long[][] sums = new long[bucketLimit][keyCount];
        final double[][] means = new double[bucketLimit][keyCount];
        final int[][] mins = new int[bucketLimit][keyCount];
        final float[][] maxes = new float[bucketLimit][keyCount];
        final StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
        int bucketCount = 1;
        int rowCount = 0;
        for (int key = 0; key < keyCount; key++) {
            final int firstBucket = key == 0 ? 0 : rnd.nextInt(bucketLimit);
            final int density = rnd.nextInt(4);
            for (int bucket = firstBucket; bucket < bucketLimit; bucket++) {
                if (bucket != firstBucket && (density == 0 || rnd.nextInt(3) >= density)) {
                    continue;
                }
                long count = 0;
                double sum = 0;
                for (int i = 0, n = 1 + rnd.nextInt(3); i < n; i++) {
                    final long minute = key == 0 && bucket == 0 && i == 0 ? 0 : rnd.nextInt(60);
                    final long v = rnd.nextInt(101) - 50;
                    final double d = rnd.nextInt(21);
                    final int value = rnd.nextInt(2_001) - 1_000;
                    final float f = rnd.nextInt(201) - 100;
                    insert.append(rowCount++ == 0 ? "(" : ",(").append(start + bucket * hour + minute * (hour / 60)).append("::").append(timestampType)
                            .append(", 'k").append(key).append("', ").append(v).append(", ").append(d)
                            .append(", ").append(value).append(", ").append(f).append(')');
                    sums[bucket][key] = count == 0 ? v : sums[bucket][key] + v;
                    mins[bucket][key] = count == 0 ? value : Math.min(mins[bucket][key], value);
                    maxes[bucket][key] = count == 0 ? f : Math.max(maxes[bucket][key], f);
                    sum += d;
                    count++;
                }
                means[bucket][key] = sum / count;
                hasData[bucket][key] = true;
                bucketCount = Math.max(bucketCount, bucket + 1);
            }
        }
        for (int key = 0; key < keyCount; key++) {
            final IntList buckets = new IntList();
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                if (hasData[bucket][key]) {
                    buckets.add(bucket);
                }
            }
            if (buckets.size() == 1) {
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    if (bucket != buckets.getQuick(0)) {
                        sums[bucket][key] = Numbers.LONG_NULL;
                        means[bucket][key] = Double.NaN;
                        mins[bucket][key] = Numbers.INT_NULL;
                        maxes[bucket][key] = Float.NaN;
                    }
                }
                continue;
            }
            for (int i = 1, n = buckets.size(); i < n; i++) {
                for (int bucket = buckets.getQuick(i - 1) + 1; bucket < buckets.getQuick(i); bucket++) {
                    interpolate(sums, means, mins, maxes, key, bucket, buckets.getQuick(i - 1), buckets.getQuick(i), start, hour);
                }
            }
            for (int bucket = 0; bucket < buckets.getQuick(0); bucket++) {
                interpolate(sums, means, mins, maxes, key, bucket, buckets.getQuick(0), buckets.getQuick(1), start, hour);
            }
            final int last = buckets.getLast();
            for (int bucket = last + 1; bucket < bucketCount; bucket++) {
                interpolate(sums, means, mins, maxes, key, bucket, last - 1, last, start, hour);
            }
        }

        final int rows = bucketCount;
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE t (ts " + timestampType + ", k SYMBOL, v LONG, d DOUBLE, i INT, f FLOAT) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute(insert.toString(), context);
            final String sql = "SELECT ts, k, sum(v) s, avg(d) a, min(i) mi, max(f) mf FROM t SAMPLE BY 1h FILL(LINEAR)";
            for (String align : new String[]{" ALIGN TO CALENDAR", " ALIGN TO FIRST OBSERVATION"}) {
                try (RecordCursorFactory factory = compiler.compile(sql + align, context).getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(context)) {
                    final Record record = cursor.getRecord();
                    final int[] keyOrder = new int[keyCount];
                    final boolean[] isSeen = new boolean[keyCount];
                    int row = 0;
                    while (cursor.hasNext()) {
                        final int bucket = row / keyCount;
                        final int key = Numbers.parseInt(record.getSymA(1), 1, record.getSymA(1).length());
                        if (bucket == 0) {
                            Assert.assertFalse(isSeen[key]);
                            isSeen[key] = true;
                            keyOrder[row] = key;
                        } else {
                            Assert.assertEquals(keyOrder[row % keyCount], key);
                        }
                        final String at = align + ", bucket " + bucket + ", key " + key;
                        Assert.assertEquals(at, start + bucket * hour, record.getTimestamp(0));
                        Assert.assertEquals(at, sums[bucket][key], record.getLong(2));
                        Assert.assertEquals(at, means[bucket][key], record.getDouble(3), 0.0);
                        Assert.assertEquals(at, mins[bucket][key], record.getInt(4));
                        Assert.assertEquals(at, maxes[bucket][key], record.getFloat(5), 0.0f);
                        row++;
                    }
                    Assert.assertEquals(rows * keyCount, row);
                }
            }
        });
    }

    @Test
    public void testKeyedMemoryLimitAndReuse() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            final String sql = "SELECT ts, sym, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR";
            assertQuery(sql).withEngine(engine).withContext(context).noLeakCheck().assertsPlanContaining("Stream Group By");
            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 1);
                try (RecordCursor cursor = factory.getCursor(context)) {
                    cursor.hasNext();
                    Assert.fail("expected memory limit breach");
                } catch (CairoException e) {
                    Assert.assertTrue(e.getFlyweightMessage().toString(), e.isOutOfMemory());
                } finally {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                }
                try (MemoryTracker tracker = engine.getMemoryTrackerProvider().acquire(
                        context.getSecurityContext(), 42, MemoryTrackerWorkload.MAT_VIEW_REFRESH)) {
                    context.setMemoryTracker(tracker);
                    try {
                        for (int iteration = 0; iteration < 3; iteration++) {
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                if (iteration > 0) {
                                    Assert.assertTrue(cursor.hasNext());
                                    Assert.assertTrue(tracker.getUsed() > 0);
                                    cursor.toTop();
                                    int count = 0;
                                    while (cursor.hasNext()) {
                                        count++;
                                    }
                                    Assert.assertEquals(10, count);
                                }
                            }
                            Assert.assertEquals(0, tracker.getUsed());
                        }
                    } finally {
                        context.setMemoryTracker(null);
                    }
                }
            }
        });
    }

    @Test
    public void testMixedFillKeepsLegacy() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'a', 1), ('2024-01-01T02:00', 'a', 3)", context);
            assertQuery("SELECT ts, avg(price) mean, count() n FROM prices SAMPLE BY 1h FILL(LINEAR, 0) ALIGN TO CALENDAR")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").noRandomAccess()
                    .withPlanNotContaining("Stream Group By")
                    .returns(precision("""
                            ts\tmean\tn
                            2024-01-01T00:00:00.000000Z\t1.0\t1
                            2024-01-01T01:00:00.000000Z\t2.0\t0
                            2024-01-01T02:00:00.000000Z\t3.0\t1
                            """));
        });
    }

    @Test
    public void testParquetColumnTop() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE prices (ts " + timestampType + ", sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'a'), ('2024-01-01T00:10', 'a')", context);
            engine.execute("ALTER TABLE prices ADD COLUMN price DOUBLE", context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01T23:00', 'a', 2), ('2024-01-02T00:10', 'a', 4)", context);
            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET LIST '2024-01-01'", context);
            Assert.assertEquals("isParquet\ntrue\n", TestUtils.printSqlToString(engine, context,
                    "SELECT isParquet FROM table_partitions('prices') WHERE name = '2024-01-01'", new StringSink()).toString());
            assertMatchesLegacy(context, "SELECT ts, avg(price), sum(price), count(price) FROM prices SAMPLE BY 1h FILL(LINEAR)", "");
        });
    }

    @Test
    public void testScalarFunctionsMatchLegacy() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            assertMatchesLegacy(context, "SELECT avg(price), sum(price), min(price), "
                    + "max(price), first(price), last(price), count(), ts FROM prices SAMPLE BY 1h FILL(LINEAR)", "");
        });
    }

    @Test
    public void testScalarSubQueryArgument() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            assertMatchesLegacy(context, "SELECT ts, sum(CASE WHEN price::string::double > "
                    + "(SELECT min(price) FROM prices) THEN 1 ELSE 0 END) chosen, avg(price) mean "
                    + "FROM prices SAMPLE BY 1h FILL(LINEAR)", "");
        });
    }

    @Test
    public void testTemporalFallbacks() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01T00:10', 'a', 1), ('2024-01-01T02:10', 'a', 3)", context);
            final String prefix = "SELECT ts, avg(price) mean FROM prices SAMPLE BY ";
            assertLegacy(context, prefix + "1h FILL(LINEAR) ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'", """
                    ts\tmean
                    2024-01-01T00:00:00.000000Z\t1.0
                    2024-01-01T01:00:00.000000Z\t2.0
                    2024-01-01T02:00:00.000000Z\t3.0
                    """);
            assertLegacy(context, prefix + "1h FILL(LINEAR) ALIGN TO CALENDAR WITH OFFSET '00:15'", """
                    ts\tmean
                    2023-12-31T23:15:00.000000Z\t1.0
                    2024-01-01T00:15:00.000000Z\t2.0
                    2024-01-01T01:15:00.000000Z\t3.0
                    """);
            assertLegacy(context, prefix + "1h FROM '2024-01-01T00:30' TO '2024-01-01T03:30' FILL(LINEAR)", """
                    ts\tmean
                    2024-01-01T01:30:00.000000Z\t3.0
                    """);
            assertLegacy(context, prefix + "1M FILL(LINEAR) ALIGN TO CALENDAR", """
                    ts\tmean
                    2024-01-01T00:00:00.000000Z\t2.0
                    """);
        });
    }

    @Test
    public void testTimestampProjection() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            insertPrices(context);
            assertMatchesLegacy(context, "SELECT ts, avg(price) mean, ts time_again FROM prices SAMPLE BY 1h FILL(LINEAR)", "");
            final String sql = "SELECT avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR)";
            final String expected = TestUtils.printSqlToString(engine, context,
                    sql + " ALIGN TO FIRST OBSERVATION", new StringSink()).toString();
            assertQuery(sql + " ALIGN TO CALENDAR").withEngine(engine).withContext(context).noLeakCheck().expectSize()
                    .withPlanContaining("Stream Group By").returns(expected);
        });
    }

    @Test
    public void testTwoPointFunctionKeepsLegacy() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', 'a', 0), ('2024-01-01T02:00', 'a', 0)", context);
            assertLegacy(context, "SELECT ts, haversine_dist_deg(price, price, ts) distance "
                    + "FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR", """
                    ts\tdistance
                    2024-01-01T00:00:00.000000Z\t0.0
                    2024-01-01T01:00:00.000000Z\t0.0
                    2024-01-01T02:00:00.000000Z\t0.0
                    """);
        });
    }

    @Test
    public void testUnsupportedLinearFunctions() throws Exception {
        run((engine, compiler, context) -> {
            createPrices(context);
            for (String function : new String[]{"ksum", "nsum"}) {
                assertQuery("SELECT ts, " + function + "(price) FROM prices SAMPLE BY 1h FILL(LINEAR) ALIGN TO CALENDAR")
                        .withEngine(engine).withContext(context).noLeakCheck()
                        .failsWith("support for LINEAR fill is not yet implemented");
            }
        });
    }

    @Test
    public void testVarcharAndDerivedKeys() throws Exception {
        run((engine, compiler, context) -> {
            engine.execute("CREATE TABLE prices (ts " + timestampType + ", market VARCHAR, price DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY", context);
            engine.execute("INSERT INTO prices VALUES ('2024-01-01', '東京', 1), ('2024-01-01T01:00', null, 5), "
                    + "('2024-01-01T03:00', '東京', 7)", context);
            assertMatchesLegacy(context, "SELECT ts, market, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR)",
                    " ORDER BY ts, market");
            final String sql = "SELECT ts, upper(market) market, avg(price) mean FROM prices SAMPLE BY 1h FILL(LINEAR)";
            final String expected = TestUtils.printSqlToString(engine, context,
                    sql + " ALIGN TO FIRST OBSERVATION ORDER BY ts, market", new StringSink()).toString();
            assertQuery(sql + " ALIGN TO CALENDAR ORDER BY ts, market")
                    .withEngine(engine).withContext(context).noLeakCheck().timestamp("ts").expectSize()
                    .withPlanContaining("Sample By").withPlanNotContaining("Stream Group By").returns(expected);
        });
    }

    private static void interpolate(long[][] sums, double[][] means, int[][] mins, float[][] maxes, int key, int bucket, int x1, int x2, long start, long hour) {
        final long x = start + bucket * hour;
        final long x1Timestamp = start + x1 * hour;
        final long x2Timestamp = start + x2 * hour;
        sums[bucket][key] = (long) line(x, x1Timestamp, sums[x1][key], x2Timestamp, sums[x2][key]);
        means[bucket][key] = line(x, x1Timestamp, means[x1][key], x2Timestamp, means[x2][key]);
        mins[bucket][key] = (int) line(x, x1Timestamp, mins[x1][key], x2Timestamp, mins[x2][key]);
        maxes[bucket][key] = (float) line(x, x1Timestamp, maxes[x1][key], x2Timestamp, maxes[x2][key]);
    }

    private static double line(long x, long x1, double y1, long x2, double y2) {
        return (y1 * (x2 - x) + y2 * (x - x1)) / (x2 - x1);
    }

    private void assertLegacy(SqlExecutionContext context, String sql, String expected) throws Exception {
        assertQuery(sql).withEngine(context.getCairoEngine()).withContext(context).noLeakCheck().timestamp("ts").expectSize()
                .withPlanNotContaining("Stream Group By").returns(precision(expected));
    }

    private void assertMatchesLegacy(SqlExecutionContext context, String sql, String suffix) throws Exception {
        final String legacy = sql + " ALIGN TO FIRST OBSERVATION" + suffix;
        final String legacyPlan = TestUtils.printSqlToString(context.getCairoEngine(), context, "EXPLAIN " + legacy, new StringSink()).toString();
        Assert.assertFalse(legacyPlan, legacyPlan.contains("Stream Group By"));
        final String expected = TestUtils.printSqlToString(context.getCairoEngine(), context, legacy, new StringSink()).toString();
        query(context, sql + " ALIGN TO CALENDAR" + suffix).returns(expected);
    }

    private void createPrices(SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("CREATE TABLE prices (ts " + timestampType
                + ", sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
    }

    private static void insertPrices(SqlExecutionContext context) throws Exception {
        context.getCairoEngine().execute("INSERT INTO prices VALUES ('2024-01-01', 'a', 1), "
                + "('2024-01-01T00:10', 'b', 3), ('2024-01-01T02:00', 'a', 7), "
                + "('2024-01-01T02:10', 'b', 9), ('2024-01-01T04:00', 'a', null)", context);
    }

    private String precision(String expected) {
        return timestampType.equals("timestamp_ns") ? expected.replace(".000000Z", ".000000000Z") : expected;
    }

    private QueryAssertion query(SqlExecutionContext context, String sql) throws Exception {
        final String plan = TestUtils.printSqlToString(context.getCairoEngine(), context, "EXPLAIN " + sql, new StringSink()).toString();
        Assert.assertEquals(plan, parallel, plan.contains("Async Stream Group By") || plan.contains("Async JIT Stream Group By"));
        return assertQuery(sql).withEngine(context.getCairoEngine()).withContext(context).noLeakCheck()
                .timestamp("ts").expectSize().withPlanContaining("Sample By", "fill: linear", "Stream Group By");
    }

    private void run(CustomisableRunnable runnable) throws Exception {
        assertMemoryLeak(() -> TestUtils.execute(workers == 0 ? null : new TestWorkerPool(workers), runnable, configuration, LOG));
    }
}
