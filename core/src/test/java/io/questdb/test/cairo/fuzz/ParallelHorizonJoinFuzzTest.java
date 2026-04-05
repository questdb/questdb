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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelHorizonJoinFuzzTest extends AbstractCairoTest {
    private static final int MIN_PAGE_FRAME_MAX_ROWS = 100;
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * MIN_PAGE_FRAME_MAX_ROWS;
    private final boolean convertToParquet;
    private final boolean enableParallelHorizonJoin;
    private final Rnd rnd;

    public ParallelHorizonJoinFuzzTest() {
        this.rnd = TestUtils.generateRandom(LOG);
        this.enableParallelHorizonJoin = rnd.nextBoolean();
        this.convertToParquet = rnd.nextBoolean();
    }

    @Override
    @Before
    public void setUp() {
        final int pageFrameMaxRows = MIN_PAGE_FRAME_MAX_ROWS + rnd.nextInt(10);
        // Async horizon join uses small page frames.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, pageFrameMaxRows);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, pageFrameMaxRows);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 1 + rnd.nextInt(4));
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1 + rnd.nextInt(PAGE_FRAME_COUNT));
        // Randomize the sharding threshold so that both sharded and non-sharded paths are exercised.
        // Keyed tests produce ~500 group keys (100 symbols × 5 offsets), so a range of 1-1000
        // gives roughly equal chances for both paths.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, 1 + rnd.nextInt(1000));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WORK_STEALING_THRESHOLD, 1 + rnd.nextInt(16));
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(enableParallelHorizonJoin));
        // Randomize adaptive backward-to-forward scan switch thresholds.
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_ABSOLUTE_THRESHOLD, 1 + rnd.nextLong(262_144));
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_MIN_GAP, 1 + rnd.nextLong(2_048));
        setProperty(PropertyKey.CAIRO_SQL_HORIZON_JOIN_BWD_SCAN_SWITCH_FACTOR, 1 + rnd.nextLong(16));
        super.setUp();
    }

    @Test
    public void testParallelHorizonJoinAdaptiveScan() throws Exception {
        testParallelHorizonJoinAdaptiveScan(
                "LIST (-1s, 0s) AS h",
                new long[]{-1_000_000, 0}
        );
    }

    @Test
    public void testParallelHorizonJoinAdaptiveScan2() throws Exception {
        testParallelHorizonJoinAdaptiveScan(
                "RANGE FROM 0 TO 1s STEP 1s AS h",
                new long[]{0, 1_000_000}
        );
    }

    @Test
    public void testParallelHorizonJoinFiltered() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelHorizonJoinFilteredThreadUnsafe() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "concat(t.side, '_00') = 'sell_00'"
        );
    }

    @Test
    public void testParallelHorizonJoinFilteredWithBindVariables() throws Exception {
        testParallelHorizonJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = :side"
        );
    }

    @Test
    public void testParallelHorizonJoinLateMaterialization() throws Exception {
        // price ranges from 10 to 30; price > 28 passes ~10% of rows,
        // well below the 20% selectivity threshold for late materialization.
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.price > 28.0"
        );
    }

    @Test
    public void testParallelHorizonJoinLateMaterializationNotKeyed() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                "t.price > 28.0"
        );
    }

    @Test
    public void testParallelHorizonJoinList() throws Exception {
        testParallelHorizonJoin(
                "LIST (-2s, 0s, 2s) AS h",
                new long[]{-2_000_000, 0, 2_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinListManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "LIST (-10s, -7s, -5s, -3s, -1s, 0s, 1s, 3s, 5s, 7s, 10s) AS h",
                new long[]{-10_000_000, -7_000_000, -5_000_000, -3_000_000, -1_000_000, 0, 1_000_000, 3_000_000, 5_000_000, 7_000_000, 10_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinNotKeyed() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinNotKeyedManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -5s TO 5s STEP 1s AS h",
                rangeOffsets(-5, 5),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinRange() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinRangeManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -10s TO 10s STEP 1s AS h",
                rangeOffsets(-10, 10),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinWithoutOnClause() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -1s TO 1s STEP 1s AS h",
                rangeOffsets(-1, 1),
                false,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFiltered() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFilteredThreadUnsafe() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "concat(t.side, '_00') = 'sell_00'"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinFilteredWithBindVariables() throws Exception {
        testParallelMultiHorizonJoin(
                (sqlExecutionContext) -> {
                    BindVariableService bindVariableService = sqlExecutionContext.getBindVariableService();
                    bindVariableService.clear();
                    bindVariableService.setStr("side", "sell");
                },
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                "t.side = :side"
        );
    }

    @Test
    public void testParallelMultiHorizonJoinKeyed() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                true,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinManyOffsets() throws Exception {
        testParallelMultiHorizonJoin(
                "LIST (-10s, -5s, -1s, 0s, 1s, 5s, 10s) AS h",
                new long[]{-10_000_000, -5_000_000, -1_000_000, 0, 1_000_000, 5_000_000, 10_000_000},
                true,
                null
        );
    }

    @Test
    public void testParallelMultiHorizonJoinNotKeyed() throws Exception {
        testParallelMultiHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2),
                false,
                null
        );
    }

    // Verifies that per-worker x per-slave flat list indexing is correct
    // in BaseAsyncMultiHorizonJoinAtom. Uses workerCount=3 and slaveCount=2
    // with mixed keyed/non-keyed slaves so that the bug (slave-major construction
    // vs worker-major access formula) produces null/non-null map mismatches.
    @Test
    public void testParallelMultiHorizonJoinWorkerSlaveIndexing() throws Exception {
        final long[] offsetsMicros = {-1_000_000, 0, 1_000_000};

        // Slave 0 is keyed (ON clause), slave 1 is non-keyed (no ON clause).
        // With wrong indexing, some workers get null ASOF maps for the keyed slave
        // and non-null maps for the non-keyed slave, producing wrong results.
        String horizonQuery = """
                SELECT h.offset AS h_offset, t.sym,
                       count(p0.bid) AS cnt_bid0, max(p0.bid) AS max_bid0,
                       count(p1.ask) AS cnt_ask1, max(p1.ask) AS max_ask1
                FROM trades t
                HORIZON JOIN prices0 AS p0 ON (t.sym = p0.sym)
                HORIZON JOIN prices1 AS p1
                    LIST (-1s, 0s, 1s) AS h
                ORDER BY h_offset, t.sym
                """;

        // Reference query: per-offset ASOF JOINs.
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset, sym, count(bid0) AS cnt_bid0, max(bid0) AS max_bid0,");
        ref.append(" count(ask1) AS cnt_ask1, max(ask1) AS max_ask1 FROM (");
        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset, t.sym");
            ref.append(", p0.bid AS bid0, p1.ask AS ask1");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ").append(offsetsMicros[i]);
            ref.append(", ts) AS ts, sym FROM trades) TIMESTAMP(ts)) t");
            ref.append(" ASOF JOIN prices0 p0 ON (t.sym = p0.sym)");
            ref.append(" ASOF JOIN prices1 p1");
        }
        ref.append(") GROUP BY h_offset, sym ORDER BY h_offset, sym");
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            // 3 workers vs 2 slaves to trigger the indexing mismatch.
            final WorkerPool pool = new WorkerPool(() -> 3);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // Force parallel execution regardless of randomized setUp() value.
                        sqlExecutionContext.setParallelHorizonJoinEnabled(true);

                        engine.execute(
                                """
                                        CREATE TABLE trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT '2020-01-01T00:05'::timestamp + (3_600_000 * x) AS ts,"
                                        + "      rnd_symbol('A', 'B', 'C', 'D', 'E') AS sym"
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        // Slave 0: keyed (ON clause), bid prices.
                        engine.execute(
                                """
                                        CREATE TABLE prices0 (
                                            ts TIMESTAMP,
                                            sym SYMBOL CAPACITY 128,
                                            bid DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices0"
                                        + "  SELECT '2020-01-01'::timestamp + (360_000 * x) AS ts,"
                                        + "      rnd_symbol('A', 'B', 'C', 'D', 'E') AS sym,"
                                        + "      rnd_double() * 50.0 AS bid"
                                        + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        // Slave 1: non-keyed (no ON clause), ask prices.
                        engine.execute(
                                """
                                        CREATE TABLE prices1 (
                                            ts TIMESTAMP,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices1"
                                        + "  SELECT '2020-01-01'::timestamp + (720_000 * x) AS ts,"
                                        + "      rnd_double() * 200.0 + 100.0 AS ask"
                                        + "  FROM long_sequence(" + 5 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("Multi HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static long[] rangeOffsets(int fromSec, int toSec) {
        int count = (toSec - fromSec) + 1;
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = (fromSec + (long) i) * 1_000_000L;
        }
        return offsets;
    }

    private void testParallelHorizonJoin(
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        testParallelHorizonJoin(null, horizonClause, offsetsMicros, keyed, filter);
    }

    private void testParallelHorizonJoin(
            BindVariablesInitializer initializer,
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        // Build HORIZON JOIN query.
        String horizonQuery = "SELECT h.offset AS h_offset"
                + (keyed ? ", t.sym" : "")
                + ", count(p.bid) AS cnt_bid, max(p.ask) AS max_ask"
                + " FROM trades t"
                + " HORIZON JOIN prices p"
                + (keyed ? " ON (t.sym = p.sym)" : "")
                + " " + horizonClause
                + (filter != null ? " WHERE " + filter : "")
                + " ORDER BY h_offset"
                + (keyed ? ", t.sym" : "");

        // Build reference query: UNION ALL of ASOF JOINs per offset, then GROUP BY.
        // HORIZON JOIN at offset O is equivalent to ASOF JOIN where master timestamps
        // are shifted forward by O microseconds.
        String innerFilter = filter != null ? filter.replace("t.", "") : null;
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(", count(bid) AS cnt_bid, max(ask) AS max_ask FROM (");

        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            if (keyed) {
                ref.append(", t.sym");
            }
            ref.append(", p.bid, p.ask");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym, side, price, amount FROM trades");
            if (innerFilter != null) {
                ref.append(" WHERE ").append(innerFilter);
            }
            ref.append(") TIMESTAMP(ts)) t ASOF JOIN prices p");
            if (keyed) {
                ref.append(" ON (t.sym = p.sym)");
            }
        }

        ref.append(") GROUP BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(" ORDER BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                """
                                        CREATE TABLE IF NOT EXISTS trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL,
                                                side SYMBOL,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        // 3_600_000 us = 3.6s per row; with ROW_COUNT rows this spans
                        // multiple hourly partitions, allowing Parquet conversion
                        // (the most recent partition stays native).
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT "
                                        + "      '2020-01-01T00:05'::timestamp + (3600000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) AS sym, "
                                        + "      rnd_symbol('buy', 'sell') as side, "
                                        + "      rnd_double() * 20 + 10 AS price, "
                                        + "      rnd_double() * 20 + 10 AS amount "
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        CREATE TABLE prices (
                                            ts TIMESTAMP,
                                            sym SYMBOL CAPACITY 1024,
                                            bid DOUBLE,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        // Price rows at 360_000 us = 360ms spacing, 10x denser than trades.
                        engine.execute(
                                "INSERT INTO prices "
                                        + "  SELECT "
                                        + "      '2020-01-01'::timestamp + (360000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) as sym, "
                                        + "      rnd_double() * 10.0 + 5.0 as bid, "
                                        + "      rnd_double() * 10.0 + 5.0 as ask "
                                        + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    // Tests keyed HORIZON JOIN with data designed to trigger the adaptive
    // backward-to-forward scan switch. Prices have a rare symbol only at the
    // beginning, causing deep backward scans that trigger the switch.
    private void testParallelHorizonJoinAdaptiveScan(
            String horizonClause,
            long[] offsetsMicros
    ) throws Exception {
        // Build HORIZON JOIN query (keyed, no filter).
        String horizonQuery = "SELECT h.offset AS h_offset, t.sym"
                + ", count(p.bid) AS cnt_bid, max(p.ask) AS max_ask"
                + " FROM trades t"
                + " HORIZON JOIN prices p ON (t.sym = p.sym)"
                + " " + horizonClause
                + " ORDER BY h_offset, t.sym";

        // Build reference query: UNION ALL of ASOF JOINs per offset.
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset, sym, count(bid) AS cnt_bid, max(ask) AS max_ask FROM (");
        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            ref.append(", t.sym, p.bid, p.ask");
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym FROM trades) TIMESTAMP(ts)) t ASOF JOIN prices p ON (t.sym = p.sym)");
        }
        ref.append(") GROUP BY h_offset, sym ORDER BY h_offset, sym");
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        // 50,000 prices at 1us spacing (all within one partition).
                        // RARE at row 0, COMMON everywhere else.
                        // The rare key causes deep backward scans that trigger the adaptive switch.
                        engine.execute(
                                """
                                        CREATE TABLE prices (
                                            ts TIMESTAMP,
                                            sym SYMBOL,
                                            bid DOUBLE,
                                            ask DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        INSERT INTO prices
                                        SELECT dateadd('u', x::int, '2020-01-01T00:00:00.000000Z'),
                                               CASE WHEN x = 1 THEN 'RARE' ELSE 'COMMON' END,
                                               x * 0.01 + 5.0,
                                               x * 0.01 + 5.5
                                        FROM long_sequence(50_000)
                                        """,
                                sqlExecutionContext
                        );

                        // 20 trades at 2,000us spacing, alternating COMMON/RARE.
                        // All in one partition so ASOF position gaps are exact row counts (~2,000).
                        engine.execute(
                                """
                                        CREATE TABLE trades (
                                            ts TIMESTAMP,
                                            sym SYMBOL
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                """
                                        INSERT INTO trades
                                        SELECT dateadd('u', (2000 * x)::int, '2020-01-01T00:00:00.000000Z'),
                                               CASE WHEN x % 2 = 0 THEN 'RARE' ELSE 'COMMON' END
                                        FROM long_sequence(20)
                                        """,
                                sqlExecutionContext
                        );

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            engine.execute("ALTER TABLE prices CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private void testParallelMultiHorizonJoin(
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        testParallelMultiHorizonJoin(null, horizonClause, offsetsMicros, keyed, filter);
    }

    private void testParallelMultiHorizonJoin(
            BindVariablesInitializer initializer,
            String horizonClause,
            long[] offsetsMicros,
            boolean keyed,
            String filter
    ) throws Exception {
        // Randomize 2-4 slave tables
        int slaveCount = 2 + rnd.nextInt(3);

        // Build multi-slave HORIZON JOIN query
        StringBuilder hq = new StringBuilder();
        hq.append("SELECT h.offset AS h_offset");
        if (keyed) {
            hq.append(", t.sym");
        }
        for (int s = 0; s < slaveCount; s++) {
            hq.append(", count(p").append(s).append(".bid) AS cnt_bid").append(s);
            hq.append(", max(p").append(s).append(".ask) AS max_ask").append(s);
        }
        hq.append(" FROM trades t");
        for (int s = 0; s < slaveCount; s++) {
            hq.append(" HORIZON JOIN prices").append(s).append(" AS p").append(s);
            if (keyed) {
                hq.append(" ON (t.sym = p").append(s).append(".sym)");
            }
        }
        hq.append(' ').append(horizonClause);
        if (filter != null) {
            hq.append(" WHERE ").append(filter);
        }
        hq.append(" ORDER BY h_offset");
        if (keyed) {
            hq.append(", t.sym");
        }
        String horizonQuery = hq.toString();

        // Build reference query: per offset, chain ASOF JOINs for all slaves
        String innerFilter = filter != null ? filter.replace("t.", "") : null;
        StringBuilder ref = new StringBuilder();
        ref.append("SELECT h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        for (int s = 0; s < slaveCount; s++) {
            ref.append(", count(bid").append(s).append(") AS cnt_bid").append(s);
            ref.append(", max(ask").append(s).append(") AS max_ask").append(s);
        }
        ref.append(" FROM (");

        for (int i = 0; i < offsetsMicros.length; i++) {
            if (i > 0) {
                ref.append(" UNION ALL ");
            }
            ref.append("SELECT CAST(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
            if (keyed) {
                ref.append(", t.sym");
            }
            for (int s = 0; s < slaveCount; s++) {
                ref.append(", p").append(s).append(".bid AS bid").append(s);
                ref.append(", p").append(s).append(".ask AS ask").append(s);
            }
            ref.append(" FROM (SELECT * FROM (SELECT dateadd('u', ")
                    .append(offsetsMicros[i])
                    .append(", ts) AS ts, sym, side, price, amount FROM trades");
            if (innerFilter != null) {
                ref.append(" WHERE ").append(innerFilter);
            }
            ref.append(") TIMESTAMP(ts)) t");
            for (int s = 0; s < slaveCount; s++) {
                ref.append(" ASOF JOIN prices").append(s).append(" p").append(s);
                if (keyed) {
                    ref.append(" ON (t.sym = p").append(s).append(".sym)");
                }
            }
        }

        ref.append(") GROUP BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        ref.append(" ORDER BY h_offset");
        if (keyed) {
            ref.append(", sym");
        }
        String referenceQuery = ref.toString();

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        if (initializer != null) {
                            initializer.init(sqlExecutionContext);
                        }

                        engine.execute(
                                """
                                        CREATE TABLE IF NOT EXISTS trades (
                                                ts TIMESTAMP,
                                                sym SYMBOL,
                                                side SYMBOL,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY HOUR;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT "
                                        + "      '2020-01-01T00:05'::timestamp + (3600000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) AS sym, "
                                        + "      rnd_symbol('buy', 'sell') as side, "
                                        + "      rnd_double() * 20 + 10 AS price, "
                                        + "      rnd_double() * 20 + 10 AS amount "
                                        + "  FROM long_sequence(" + ROW_COUNT + ");",
                                sqlExecutionContext
                        );

                        for (int s = 0; s < slaveCount; s++) {
                            engine.execute(
                                    "CREATE TABLE prices" + s + " ("
                                            + "    ts TIMESTAMP,"
                                            + "    sym SYMBOL CAPACITY 1024,"
                                            + "    bid DOUBLE,"
                                            + "    ask DOUBLE"
                                            + ") TIMESTAMP(ts) PARTITION BY HOUR;",
                                    sqlExecutionContext
                            );
                            // Each slave gets independently generated data
                            engine.execute(
                                    "INSERT INTO prices" + s
                                            + "  SELECT "
                                            + "      '2020-01-01'::timestamp + (360000*x) + rnd_long(-200, 200, 0) as ts, "
                                            + "      rnd_symbol_zipf(100, 2.0) as sym, "
                                            + "      rnd_double() * 10.0 + 5.0 as bid, "
                                            + "      rnd_double() * 10.0 + 5.0 as ask "
                                            + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                    sqlExecutionContext
                            );
                        }

                        if (convertToParquet) {
                            engine.execute("ALTER TABLE trades CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            for (int s = 0; s < slaveCount; s++) {
                                engine.execute("ALTER TABLE prices" + s + " CONVERT PARTITION TO PARQUET WHERE ts >= 0", sqlExecutionContext);
                            }
                        }

                        final StringSink horizonSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, horizonQuery, horizonSink);

                        final StringSink referenceSink = new StringSink();
                        TestUtils.printSql(engine, sqlExecutionContext, referenceQuery, referenceSink);

                        try {
                            TestUtils.assertEquals(referenceSink, horizonSink);
                        } catch (AssertionError e) {
                            LOG.error().$("Multi HORIZON JOIN query: ").$(horizonQuery).$();
                            LOG.error().$("Reference query: ").$(referenceQuery).$();
                            throw e;
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
