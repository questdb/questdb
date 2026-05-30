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

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TwapGroupByFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testTwapAcceptsDesignatedTimestampCast() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // ts::timestamp is an identity cast on the designated timestamp.
            // The cast factory returns the column unwrapped, so twap() still
            // recognizes it as the designated timestamp and the query is
            // accepted, matching plain twap(price, ts) in testTwapBasic.
            assertQueryNoLeakCheck(
                    "twap\n16.666666666666668\n",
                    "SELECT twap(price, ts::timestamp) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapAllNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (null, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T00:00:10.000000Z'),
                    (null, '2024-01-01T00:00:20.000000Z')
                    """);
            assertQueryNoLeakCheck(
                    "twap\nnull\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapAllSameTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:00.000000Z'),
                    (30.0, '2024-01-01T00:00:00.000000Z')
                    """);
            // all same timestamp => simple average: (10 + 20 + 30) / 3 = 20.0
            assertQueryNoLeakCheck(
                    "twap\n20.0\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapBasic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // weighted_sum = 10 * 10_000_000 + 20 * 20_000_000 = 500_000_000
            // total_duration = 30_000_000
            // twap = 500_000_000 / 30_000_000 = 16.666666666666668
            assertQueryNoLeakCheck(
                    "twap\n16.666666666666668\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertQueryNoLeakCheck(
                    "twap\nnull\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapParallelAllNullPrices() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            null::DOUBLE price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(100_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    sym\ttwap
                                    A\tnull
                                    B\tnull
                                    C\tnull
                                    D\tnull
                                    E\tnull
                                    """,
                            sql,
                            null,
                            sqlExecutionContext,
                            true,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelChunky() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(2_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelConstantPrice() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C') sym,
                            42.0 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(500_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    // constant price => twap = 42.0 for all groups
                    assertQueryNoLeakCheck(
                            compiler,
                            """
                                    sym\ttwap
                                    A\t42.0
                                    B\t42.0
                                    C\t42.0
                                    """,
                            sql,
                            null,
                            sqlExecutionContext,
                            true,
                            true
                    );
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelMergeNullDestValidSrc() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // First half has null prices, second half has valid prices.
            // Some chunks will be empty (null dest) that merge with non-empty src.
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x <= 1_000_000 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(2_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelMergeValidDestNullSrc() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // First half has valid prices, second half has null prices.
            // Some chunks will be non-empty (valid dest) merging with empty src.
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x > 500_000 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelNoGroupBy() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT twap(price, ts) FROM tbl";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSingleGroup() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // All rows belong to the same symbol group — exercises merge across chunks for a single key
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            'X' sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSmallDataset() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Very few rows — some parallel chunks may be completely empty
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B') sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1_000_000) ts
                        FROM long_sequence(10)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelSparseNulls() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Alternating null/non-null prices — exercises null skipping at chunk boundaries
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol('A','B','C','D','E') sym,
                            CASE WHEN x % 2 = 0 THEN null ELSE rnd_double() * 100 END price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapParallelWithManyGroups() throws Exception {
        assertMemoryLeak((TestUtils.LeakProneCode) () -> {
            // Many distinct keys — stresses the merge map
            execute("""
                    CREATE TABLE tbl AS (
                        SELECT
                            rnd_symbol_zipf(200, 1.5) sym,
                            rnd_double() * 100 price,
                            timestamp_sequence(0, 1000) ts
                        FROM long_sequence(1_000_000)
                    ) TIMESTAMP(ts)
                    """);
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
                    String sql = "SELECT sym, twap(price, ts) FROM tbl GROUP BY sym ORDER BY sym";
                    TestUtils.assertSqlCursors(engine, sqlExecutionContext, sql, sql, LOG);
                }, configuration, LOG);
            }
        });
    }

    @Test
    public void testTwapRejectedInHorizonJoin() throws Exception {
        // A HORIZON JOIN groups its output by horizon offset (and join key), so
        // the group-by aggregator does not receive rows in ascending
        // designated-timestamp order. The join callsite therefore reports the
        // base as non-ascending, and twap() - which relies on each page frame's
        // rows already being sorted by the timestamp argument - is rejected at
        // compile time. The timestamp argument here is the master's designated
        // timestamp, so the rejection is on scan direction, not on the
        // timestamp argument itself.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, twap(p.price, t.ts) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                    14,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectedInMultiHorizonJoin() throws Exception {
        // Same reasoning as the single-slave HORIZON JOIN, but routed through
        // the multi-slave code path (RANGE on the last HORIZON JOIN only).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, twap(p.price, t.ts) FROM trades t HORIZON JOIN prices p ON (t.sym = p.sym) HORIZON JOIN prices p2 ON (t.sym = p2.sym) RANGE FROM 0s TO 2s STEP 1s AS h",
                    14,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectedInWindowJoin() throws Exception {
        // A WINDOW JOIN aggregates slave rows within a time window around each
        // master row, so the aggregator does not see rows in ascending
        // designated-timestamp order. The join callsite reports the base as
        // non-ascending and twap() is rejected at compile time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE trades (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE prices (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            assertExceptionNoLeakCheck(
                    "SELECT t.sym, twap(p.price, t.ts) FROM trades t WINDOW JOIN prices p ON (t.sym = p.sym) RANGE BETWEEN 1 MINUTE PRECEDING AND 1 MINUTE FOLLOWING",
                    14,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsConvertingTimestampCast() throws Exception {
        // ts is a microsecond timestamp, so ts::timestamp_ns is a *converting*
        // cast: CastTimestampToTimestampFunctionFactory wraps the column in a
        // Func that rescales micros to nanos. Unlike the identity ts::timestamp
        // cast (see testTwapAcceptsDesignatedTimestampCast), ColumnFunction.unwrap
        // cannot see the designated-timestamp column through that wrapper, so
        // twap() rejects the argument as not being the designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts::timestamp_ns) FROM tbl",
                    7,
                    "twap() requires the table's designated timestamp as the second argument",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsDescendingScan() throws Exception {
        // The aggregate appends rows in scan order using the timestamp as the
        // sort key and treats each per-frame batch as already key-sorted. A
        // backward scan delivers rows in reverse order within a page frame,
        // breaking that invariant and producing wrong output silently. This is
        // a distinct rejection path from the metadata-mismatch reorder tests:
        // here the base reports SCAN_DIRECTION_BACKWARD while still carrying the
        // designated timestamp, so the timestamp-argument check passes and only
        // the scan-direction check fires.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (price DOUBLE, grp SYMBOL, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO t VALUES
                    (1.0, 'a', '2024-01-01T00:00:00.000000Z'),
                    (2.0, 'a', '2024-01-01T01:00:00.000000Z')
                    """);
            // ORDER BY ts DESC inside the inner SELECT compiles to a backward
            // page-frame scan when paired with LIMIT - the inner SELECT
            // without LIMIT is dropped by the optimiser.
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts) FROM (SELECT * FROM t ORDER BY ts DESC LIMIT 10)",
                    7,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
            assertExceptionNoLeakCheck(
                    "SELECT grp, twap(price, ts) FROM (SELECT * FROM t ORDER BY ts DESC LIMIT 10) GROUP BY grp",
                    12,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsNonDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // 'ts' is the designated timestamp; 'ts2' is an ordinary timestamp column.
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts)");
            // A non-designated timestamp column is rejected.
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts2) FROM tbl",
                    7,
                    "twap() requires the table's designated timestamp as the second argument",
                    false
            );
            // A table with no designated timestamp at all is rejected too.
            execute("CREATE TABLE no_ts (price DOUBLE, ts TIMESTAMP)");
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts) FROM no_ts",
                    7,
                    "twap() requires the table's designated timestamp as the second argument",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsOrderByNonTimestampSubquery() throws Exception {
        // A sort by a non-timestamp column reports SCAN_DIRECTION_FORWARD but
        // delivers rows out of designated-timestamp order. The sort drops the
        // designated timestamp from its metadata; a `timestamp(ts)` clause on
        // the outer subquery re-attaches the column by name. The compiler
        // must still reject the query because the rows are not actually in
        // ascending timestamp order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP, key SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (5.0,  '2024-01-01T00:00:00.000000Z', 'A'),
                    (50.0, '2024-01-01T00:00:30.000000Z', 'A'),
                    (10.0, '2024-01-01T00:01:00.000000Z', 'A')
                    """);
            assertExceptionNoLeakCheck(
                    "SELECT key, twap(price, ts) FROM (SELECT * FROM tbl ORDER BY price ASC) timestamp(ts) GROUP BY key",
                    12,
                    "twap() requires the base query to provide ascending designated timestamp order",
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsSampleByFillOverNonTimestampOrderedBase() throws Exception {
        // A SAMPLE BY ... FILL(...) query takes the serial SAMPLE BY path, where TWAP was
        // previously validated against a hardcoded "ascending timestamp" assumption. A base
        // ordered by a non-timestamp column reports SCAN_DIRECTION_FORWARD but delivers rows
        // out of designated-timestamp order: the sort drops the designated timestamp from its
        // metadata and an outer timestamp(ts) clause re-attaches it by name. Feeding such rows
        // to TWAP silently produced the plain average instead of the step-function value, so
        // the compiler must reject the query on every fill mode.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (30.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (10.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // Oracle: over the real ts-ordered base the step-function TWAP for the single
            // bucket is (30*10 + 20*20) / 30 = 23.333..., not the plain average (30+20+10)/3
            // = 20.0 that the reordered base silently produced before the fix.
            assertQueryNoLeakCheck(
                    "ts\ttwap\n2024-01-01T00:00:00.000000Z\t23.333333333333332\n",
                    "SELECT ts, twap(price, ts) FROM tbl SAMPLE BY 1m",
                    "ts",
                    true,
                    true
            );
            // FILL(linear) reaches the interpolated branch; FILL(null)/FILL(prev) reach the
            // other serial branch. Every fill mode must reject the price-ordered base.
            final String reorderedBase = "FROM (SELECT price, ts FROM tbl ORDER BY price ASC LIMIT 10) timestamp(ts) SAMPLE BY 1m ";
            for (String fill : new String[]{"FILL(null)", "FILL(prev)", "FILL(linear)"}) {
                assertExceptionNoLeakCheck(
                        "SELECT twap(price, ts) " + reorderedBase + fill,
                        7,
                        "twap() requires the base query to provide ascending designated timestamp order",
                        false
                );
            }
            // The same query shape with a genuinely ts-ordered base (timestamp re-attached by
            // name) is still accepted and returns the correct value -- the fix does not
            // over-reject legitimate SAMPLE BY FILL queries.
            assertQueryNoLeakCheck(
                    "ts\ttwap\n2024-01-01T00:00:00.000000Z\t23.333333333333332\n",
                    "SELECT ts, twap(price, ts) FROM (SELECT price, ts FROM tbl ORDER BY ts ASC) timestamp(ts) SAMPLE BY 1m FILL(null)",
                    "ts",
                    false,
                    false
            );
        });
    }

    @Test
    public void testTwapRejectsSampleByOverLatestByLightSubQuery() throws Exception {
        // A LATEST ON ... over a derived sub-query compiles to LatestByLightRecordCursorFactory,
        // which emits one row per partition key in map order -- NOT in designated-timestamp order.
        // It therefore advertises no designated timestamp at all, so SAMPLE BY/twap must reject it
        // instead of silently averaging out-of-order rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (i INT, sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                    (1, 'A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    (1, 'B', 20.0, '2024-01-01T00:00:10.000000Z'),
                    (1, 'B', 40.0, '2024-01-01T00:00:50.000000Z'),
                    (1, 'A', 30.0, '2024-01-01T00:01:40.000000Z')
                    """);
            final String latestByLight = "FROM (SELECT ts, sym, price, i AS i1 FROM a) WHERE i1 > 0 LATEST ON ts PARTITION BY sym ";
            // No-FILL rewrites to a keyed group-by; twap validation rejects it because its second
            // argument is no longer a designated timestamp (the light sub-query advertises none).
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts) " + latestByLight + "SAMPLE BY 1d",
                    7,
                    "twap() requires the table's designated timestamp as the second argument",
                    false
            );
            // FILL stays on the serial SAMPLE BY path; it rejects because the light sub-query
            // provides no designated timestamp to sample by.
            assertExceptionNoLeakCheck(
                    "SELECT twap(price, ts) " + latestByLight + "SAMPLE BY 1d FILL(null)",
                    0,
                    "base query does not provide designated TIMESTAMP column",
                    false
            );
        });
    }

    @Test
    public void testTwapSingleRow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("INSERT INTO tbl VALUES (42.5, '2024-01-01T00:00:00.000000Z')");
            // single row => total_duration = 0, fallback to price_sum / count = 42.5
            assertQueryNoLeakCheck(
                    "twap\n42.5\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapSomeNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (null, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:20.000000Z')
                    """);
            // null price at 10s is skipped
            // weighted_sum = 10.0 * 20_000_000 = 200_000_000
            // total_duration = 20_000_000
            // twap = 200_000_000 / 20_000_000 = 10.0
            assertQueryNoLeakCheck(
                    "twap\n10.0\n",
                    "SELECT twap(price, ts) FROM tbl",
                    null,
                    false,
                    true
            );
        });
    }

    @Test
    public void testTwapWithGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO tbl VALUES
                    ('A', 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 100.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 20.0, '2024-01-01T00:00:10.000000Z'),
                    ('B', 200.0, '2024-01-01T00:00:20.000000Z'),
                    ('A', 30.0, '2024-01-01T00:00:30.000000Z')
                    """);
            // Group A: weighted_sum = 10*10_000_000 + 20*20_000_000 = 500_000_000, dur = 30_000_000, twap = 16.666666666666668
            // Group B: weighted_sum = 100*20_000_000 = 2_000_000_000, dur = 20_000_000, twap = 100.0
            assertQueryNoLeakCheck(
                    """
                            sym\ttwap
                            A\t16.666666666666668
                            B\t100.0
                            """,
                    "SELECT sym, twap(price, ts) FROM tbl ORDER BY sym",
                    null,
                    true,
                    true
            );
        });
    }

    @Test
    public void testTwapWithSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z'),
                    (100.0, '2024-01-01T00:01:00.000000Z'),
                    (200.0, '2024-01-01T00:01:20.000000Z')
                    """);
            // Bucket 1 (00:00:00 - 00:01:00): same as testTwapBasic = 16.666666666666668
            // Bucket 2 (00:01:00 - 00:02:00): weighted_sum = 100*20_000_000 = 2_000_000_000, dur = 20_000_000, twap = 100.0
            assertQueryNoLeakCheck(
                    """
                            ts\ttwap
                            2024-01-01T00:00:00.000000Z\t16.666666666666668
                            2024-01-01T00:01:00.000000Z\t100.0
                            """,
                    "SELECT ts, twap(price, ts) FROM tbl SAMPLE BY 1m",
                    "ts",
                    true,
                    true
            );
        });
    }

    @Test
    public void testTwapWithSampleByFillPrev() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl (price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO tbl VALUES
                    (10.0, '2024-01-01T00:00:00.000000Z'),
                    (20.0, '2024-01-01T00:00:10.000000Z'),
                    (30.0, '2024-01-01T00:00:30.000000Z'),
                    (100.0, '2024-01-01T00:02:00.000000Z'),
                    (200.0, '2024-01-01T00:02:20.000000Z')
                    """);
            // Bucket 00:00 has data: twap = 16.666666666666668
            // Bucket 00:01 is empty: FILL(prev) => 16.666666666666668
            // Bucket 00:02 has data: twap = 100.0
            assertQueryNoLeakCheck(
                    """
                            ts\ttwap
                            2024-01-01T00:00:00.000000Z\t16.666666666666668
                            2024-01-01T00:01:00.000000Z\t16.666666666666668
                            2024-01-01T00:02:00.000000Z\t100.0
                            """,
                    "SELECT ts, twap(price, ts) FROM tbl SAMPLE BY 1m FILL(prev)",
                    "ts",
                    false,
                    false
            );
        });
    }
}
