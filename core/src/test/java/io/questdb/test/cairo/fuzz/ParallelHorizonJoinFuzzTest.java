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

package io.questdb.test.cairo.fuzz;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

// This is not a fuzz test in traditional sense, but it's multithreaded, and we want to run it
// in CI frequently along with other fuzz tests.
public class ParallelHorizonJoinFuzzTest extends AbstractCairoTest {
    private static final int PAGE_FRAME_COUNT = 4; // also used to set queue size, so must be a power of 2
    private static final int PAGE_FRAME_MAX_ROWS = 100;
    private static final int ROW_COUNT = 10 * PAGE_FRAME_COUNT * PAGE_FRAME_MAX_ROWS;
    private final boolean enableParallelHorizonJoin;

    public ParallelHorizonJoinFuzzTest() {
        this.enableParallelHorizonJoin = TestUtils.generateRandom(LOG).nextBoolean();
        LOG.info().$("parallel horizon join enabled: ").$(enableParallelHorizonJoin).$();
    }

    @Override
    @Before
    public void setUp() {
        // Async horizon join uses small page frames.
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, PAGE_FRAME_MAX_ROWS);
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, PAGE_FRAME_MAX_ROWS);
        // We intentionally use small values for shard count and reduce
        // queue capacity to exhibit various edge cases.
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_SHARD_COUNT, 2);
        setProperty(PropertyKey.CAIRO_PAGE_FRAME_REDUCE_QUEUE_CAPACITY, PAGE_FRAME_COUNT);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, String.valueOf(enableParallelHorizonJoin));
        super.setUp();
    }

    @Test
    public void testParallelHorizonJoinFiltered() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2, 1),
                true,
                "t.side = 'sell'"
        );
    }

    @Test
    public void testParallelHorizonJoinFilteredThreadUnsafe() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2, 1),
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
                rangeOffsets(-2, 2, 1),
                true,
                "t.side = :side"
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
                rangeOffsets(-2, 2, 1),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinNotKeyedManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -5s TO 5s STEP 1s AS h",
                rangeOffsets(-5, 5, 1),
                false,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinRange() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -2s TO 2s STEP 1s AS h",
                rangeOffsets(-2, 2, 1),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinRangeManyOffsets() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -10s TO 10s STEP 1s AS h",
                rangeOffsets(-10, 10, 1),
                true,
                null
        );
    }

    @Test
    public void testParallelHorizonJoinWithoutOnClause() throws Exception {
        testParallelHorizonJoin(
                "RANGE FROM -1s TO 1s STEP 1s AS h",
                rangeOffsets(-1, 1, 1),
                false,
                null
        );
    }

    private static long[] rangeOffsets(int fromSec, int toSec, int stepSec) {
        int count = (toSec - fromSec) / stepSec + 1;
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = (fromSec + (long) stepSec * i) * 1_000_000L;
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
            ref.append("SELECT cast(").append(offsetsMicros[i]).append(" AS long) AS h_offset");
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
                                                sym SYMBOL CAPACITY 2048,
                                                side SYMBOL CAPACITY 4,
                                                price DOUBLE,
                                                amount DOUBLE
                                        ) TIMESTAMP(ts) PARTITION BY DAY;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO trades"
                                        + "  SELECT "
                                        + "      '2020-01-01T00:05'::timestamp + (10000*x) + rnd_long(-200, 200, 0) as ts, "
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
                                        ) TIMESTAMP(ts) PARTITION BY DAY;
                                        """,
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO prices "
                                        + "  SELECT "
                                        + "      '2020-01-01'::timestamp + (60000*x) + rnd_long(-200, 200, 0) as ts, "
                                        + "      rnd_symbol_zipf(100, 2.0) as sym, "
                                        + "      rnd_double() * 10.0 + 5.0 as bid, "
                                        + "      rnd_double() * 10.0 + 5.0 as ask "
                                        + "  FROM long_sequence(" + 10 * ROW_COUNT + ");",
                                sqlExecutionContext
                        );

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

    private interface BindVariablesInitializer {
        void init(SqlExecutionContext sqlExecutionContext) throws SqlException;
    }
}
