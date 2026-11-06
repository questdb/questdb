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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * ASOF and LT JOIN when a group of slave rows that share one timestamp straddles a slave
 * time frame boundary (a page frame inside a partition, or a Parquet row group). ASOF
 * returns the last row at or before the master timestamp in storage order, so the answer
 * is the last row of the tie group, which sits in the next frame. The serial time frame
 * algorithms used to stop at the end of the first frame when an earlier master row had
 * already positioned the slave cursor there.
 * <p>
 * The quotes table has one quote per second, except rows x = 900..tieHi, which all sit at
 * 900 s. The frames are 1000 rows, so the tie group crosses the first frame boundary.
 * Symbol K is quoted at x = 500 and at x = tieHi, the last row of the tie group, so a
 * cursor that misses the next frame returns x = 500 (keyed) or x = 1000 (non-keyed).
 * The first trade (A at 10 s) positions the slave cursor in the first frame, which is what
 * the defect needs. HORIZON JOIN (an ASOF lookup per offset) and WINDOW JOIN with the
 * prevailing row find their rows another way; they are checked on the same data.
 */
public class AsOfJoinTieFrameStraddleTest extends AbstractCairoTest {
    private static final String COLUMNS = "t.ts, t.sym, q.ts qts, q.sym qsym, q.bid";
    private final ArrayList<String> failures = new ArrayList<>();

    @Test
    public void testTieCoversWholeFrame() throws Exception {
        // the tie group x = 900..2100 covers the whole second frame (x = 1001..2000)
        assertTieStraddle(2100, false);
    }

    @Test
    public void testTieStraddlesDefaultPageFrames() throws Exception {
        // no frame size override: 3M rows in one partition, the tie group x = 900k..1.3M
        // crosses the default page frame boundaries
        assertMemoryLeak(() -> {
            execute("CREATE TABLE quotes AS (SELECT " +
                    "'2024-01-01'::timestamp + (CASE WHEN x BETWEEN 900000 AND 1300000 THEN 900000 ELSE x END) * 10_000L ts, " +
                    "(CASE WHEN x IN (500, 1300000) THEN 'K' WHEN x % 2 = 0 THEN 'A' ELSE 'B' END)::symbol sym, " +
                    "x::double bid FROM long_sequence(3000000)) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades VALUES ('2024-01-01T00:00:00.100000Z', 'A', 1), ('2024-01-01T02:30:00.000000Z', 'K', 2)");
            final String expected = """
                    ts\tsym\tqts\tqsym\tbid
                    2024-01-01T00:00:00.100000Z\tA\t2024-01-01T00:00:00.100000Z\tA\t10.0
                    2024-01-01T02:30:00.000000Z\tK\t2024-01-01T02:30:00.000000Z\tK\t1300000.0
                    """;
            assertJoin("", "trades t ASOF JOIN quotes q", "AsOf Join Fast", false, expected);
            assertJoin("asof_fast(t q)", "trades t ASOF JOIN quotes q ON (sym)", "AsOf Join Fast", true, expected);
            assertJoin("", "trades t ASOF JOIN quotes q ON (sym)", "AsOf Join Memoized Scan", true, expected);
            assertJoin("asof_dense(t q)", "trades t ASOF JOIN quotes q ON (sym)", "AsOf Join Dense Single Symbol", true, expected);
            assertJoin("asof_parallel(t q)", "trades t ASOF JOIN quotes q ON (sym)", "Async AsOf Join", true, expected);
            assertJoin("asof_memoized(t q)", "trades t ASOF JOIN quotes q ON (sym)", "AsOf Join Memoized Scan", true, expected);
            assertJoin("asof_linear(t q)", "trades t ASOF JOIN quotes q ON (sym)", "AsOf Join Light", true, expected);
            assertNoFailures();
        });
    }

    @Test
    public void testTieStraddlesFrameBoundary() throws Exception {
        assertTieStraddle(1100, false);
    }

    @Test
    public void testTieStraddlesParquetRowGroups() throws Exception {
        // the partition is Parquet with 1000-row row groups: the tie group crosses a row group boundary
        assertTieStraddle(1100, true);
    }

    private void assertAggregate(String sql, String expected) throws Exception {
        try {
            assertQuery(sql).noLeakCheck().sizeMayVary().inferRandomAccess().inferTimestamp().returns(expected);
        } catch (AssertionError e) {
            failures.add(sql + "\n" + e.getMessage());
        }
    }

    private void assertJoin(String hint, String from, String algo, boolean keyed, String expected) throws Exception {
        final String sql = "SELECT " + (hint.isEmpty() ? "" : "/*+ " + hint + " */ ") + COLUMNS + " FROM " + from;
        final QueryAssertion assertion = assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .inferTimestamp()
                .sizeMayVary()
                .withPlanContaining(algo);
        if (!keyed) {
            // the keyed and the non-keyed factories share their plan names
            assertion.withPlanNotContaining("condition:", "symbolKeyJoin");
        }
        try {
            assertion.returns(expected);
        } catch (AssertionError e) {
            // collect, so that one run names every algorithm that fails
            failures.add(sql + "\n" + e.getMessage());
        }
    }

    private void assertNoFailures() {
        if (!failures.isEmpty()) {
            Assert.fail(failures.size() + " joins failed:\n\n" + String.join("\n\n", failures));
        }
    }

    private void assertTieStraddle(int tieHi, boolean parquet) throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1000);
        assertMemoryLeak(() -> {
            // 1000-row page frames, so 1000-row time frames
            sqlExecutionContext.changePageFrameSizes(1000, 1000);
            final String quotesSql = "SELECT " +
                    "'2024-01-01'::timestamp + (CASE WHEN x BETWEEN 900 AND " + tieHi + " THEN 900 ELSE x END) * 1_000_000L ts, " +
                    "(CASE WHEN x IN (500, " + tieHi + ") THEN 'K' WHEN x % 2 = 0 THEN 'A' ELSE 'B' END)::symbol sym, " +
                    "(CASE WHEN x IN (500, " + tieHi + ") THEN 'K' WHEN x % 2 = 0 THEN 'A' ELSE 'B' END)::varchar s, " +
                    "x::double bid FROM long_sequence(3000)";
            execute("CREATE TABLE quotes AS (" + quotesSql + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE quotes_ix AS (" + quotesSql + "), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // a later partition, so that the first one can be converted to Parquet
            execute("INSERT INTO quotes VALUES ('2024-01-02', 'Z', 'Z', -1)");
            execute("INSERT INTO quotes_ix VALUES ('2024-01-02', 'Z', 'Z', -1)");
            if (parquet) {
                execute("ALTER TABLE quotes CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
                execute("ALTER TABLE quotes_ix CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            }
            execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, s VARCHAR, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades VALUES ('2024-01-01T00:00:10.000000Z', 'A', 'A', 1), ('2024-01-01T00:15:00.000000Z', 'K', 'K', 2)");
            execute("CREATE TABLE trades_lt (ts TIMESTAMP, sym SYMBOL, s VARCHAR, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO trades_lt VALUES ('2024-01-01T00:00:10.000000Z', 'A', 'A', 1), ('2024-01-01T00:15:00.000001Z', 'K', 'K', 2)");

            // ASOF: K at 900 s must get the last row of the tie group, x = tieHi (a K quote);
            // the non-keyed join gets the same row, as it is the last row at 900 s
            final String expected = "ts\tsym\tqts\tqsym\tbid\n" +
                    "2024-01-01T00:00:10.000000Z\tA\t2024-01-01T00:00:10.000000Z\tA\t10.0\n" +
                    "2024-01-01T00:15:00.000000Z\tK\t2024-01-01T00:15:00.000000Z\tK\t" + tieHi + ".0\n";

            // keyed on a symbol, every algorithm
            final String keyed = "trades t ASOF JOIN quotes q ON (sym)";
            assertJoin("asof_fast(t q)", keyed, "AsOf Join Fast", true, expected);
            // no hint: the small-master choice
            assertJoin("", keyed, "AsOf Join Memoized Scan", true, expected);
            assertJoin("asof_dense(t q)", keyed, "AsOf Join Dense Single Symbol", true, expected);
            assertJoin("asof_parallel(t q)", keyed, "Async AsOf Join", true, expected);
            assertJoin("asof_memoized(t q)", keyed, "AsOf Join Memoized Scan", true, expected);
            assertJoin("asof_memoized_driveby(t q)", keyed, "driveByCache: true", true, expected);
            assertJoin("asof_index(t q)", "trades t ASOF JOIN quotes_ix q ON (sym)", "AsOf Join Indexed Scan", true, expected);
            assertJoin("asof_linear(t q)", keyed, "AsOf Join Light", true, expected);
            // keyed on a VARCHAR: the generic key sink and the multi-key Dense
            assertJoin("asof_fast(t q)", "trades t ASOF JOIN quotes q ON (s)", "AsOf Join Fast", true, expected);
            assertJoin("", "trades t ASOF JOIN quotes q ON (s)", "AsOf Join Dense", true, expected);
            // keyed, filtered slave: the stolen filter, with and without a projection
            assertJoin("", "trades t ASOF JOIN (SELECT * FROM quotes WHERE sym IN ('A', 'B', 'K')) q ON (sym)", "Filtered AsOf Join Fast", true, expected);
            assertJoin("", "trades t ASOF JOIN (SELECT ts, sym, bid FROM quotes WHERE bid > 0) q ON (sym)", "Filtered AsOf Join Fast", true, expected);
            assertJoin("asof_parallel(t q)", "trades t ASOF JOIN (SELECT * FROM quotes WHERE sym IN ('A', 'B', 'K')) q ON (sym)", "Async AsOf Join", true, expected);

            // non-keyed
            assertJoin("", "trades t ASOF JOIN quotes q", "AsOf Join Fast", false, expected);
            assertJoin("", "trades t ASOF JOIN (SELECT * FROM quotes WHERE bid > 0) q", "Filtered AsOf Join Fast", false, expected);
            assertJoin("", "trades t ASOF JOIN (SELECT ts, sym, bid FROM quotes WHERE bid > 0) q", "Filtered AsOf Join Fast", false, expected);
            assertJoin("asof_linear(t q)", "trades t ASOF JOIN quotes q", "AsOf Join", false, expected);

            // TOLERANCE: the row the defect returned (x = 500 at 500 s) is outside it, the right one inside
            final String keyedTol = "trades t ASOF JOIN quotes q ON (sym) TOLERANCE 1m";
            assertJoin("asof_fast(t q)", keyedTol, "AsOf Join Fast", true, expected);
            assertJoin("asof_parallel(t q)", keyedTol, "Async AsOf Join", true, expected);
            assertJoin("asof_dense(t q)", keyedTol, "AsOf Join Dense Single Symbol", true, expected);
            assertJoin("asof_memoized(t q)", keyedTol, "AsOf Join Memoized Scan", true, expected);
            assertJoin("asof_index(t q)", "trades t ASOF JOIN quotes_ix q ON (sym) TOLERANCE 1m", "AsOf Join Indexed Scan", true, expected);
            assertJoin("asof_linear(t q)", keyedTol, "AsOf Join Light", true, expected);
            assertJoin("", "trades t ASOF JOIN (SELECT * FROM quotes WHERE sym IN ('A', 'B', 'K')) q ON (sym) TOLERANCE 1m", "Filtered AsOf Join Fast", true, expected);
            assertJoin("", "trades t ASOF JOIN quotes q TOLERANCE 1m", "AsOf Join Fast", false, expected);
            assertJoin("", "trades t ASOF JOIN (SELECT * FROM quotes WHERE bid > 0) q TOLERANCE 1m", "Filtered AsOf Join Fast", false, expected);

            // LT: K at 900.000001 s must get x = tieHi; A at 10 s gets the row before 10 s
            final String expectedLtKeyed = "ts\tsym\tqts\tqsym\tbid\n" +
                    "2024-01-01T00:00:10.000000Z\tA\t2024-01-01T00:00:08.000000Z\tA\t8.0\n" +
                    "2024-01-01T00:15:00.000001Z\tK\t2024-01-01T00:15:00.000000Z\tK\t" + tieHi + ".0\n";
            final String expectedLtNoKey = "ts\tsym\tqts\tqsym\tbid\n" +
                    "2024-01-01T00:00:10.000000Z\tA\t2024-01-01T00:00:09.000000Z\tB\t9.0\n" +
                    "2024-01-01T00:15:00.000001Z\tK\t2024-01-01T00:15:00.000000Z\tK\t" + tieHi + ".0\n";
            assertJoin("", "trades_lt t LT JOIN quotes q ON (sym)", "Lt Join Light", true, expectedLtKeyed);
            assertJoin("", "trades_lt t LT JOIN quotes q", "Lt Join Fast", false, expectedLtNoKey);
            assertJoin("asof_linear(t q)", "trades_lt t LT JOIN quotes q", "Lt Join", false, expectedLtNoKey);
            assertJoin("", "trades_lt t LT JOIN quotes q TOLERANCE 1m", "Lt Join Fast", false, expectedLtNoKey);
            assertJoin("", "trades_lt t LT JOIN quotes q ON (sym) TOLERANCE 1m", "Lt Join Light", true, expectedLtKeyed);

            // HORIZON JOIN at offset 0 is an ASOF lookup per master row; WINDOW JOIN with a zero-width
            // window and the prevailing row sees the whole tie group, its last row last
            final String expectedBids = "ts\tsym\tbid\n" +
                    "2024-01-01T00:00:10.000000Z\tA\t10.0\n" +
                    "2024-01-01T00:15:00.000000Z\tK\t" + tieHi + ".0\n";
            assertAggregate("SELECT t.ts, t.sym, sum(q.bid) bid FROM trades t HORIZON JOIN quotes q ON (sym) LIST (0) AS h ORDER BY t.ts", expectedBids);
            assertAggregate("SELECT t.ts, t.sym, sum(q.bid) bid FROM trades t HORIZON JOIN quotes q LIST (0) AS h ORDER BY t.ts", expectedBids);
            assertAggregate("SELECT t.ts, t.sym, last(q.bid) bid FROM trades t WINDOW JOIN quotes q ON (t.sym = q.sym) " +
                    "RANGE BETWEEN 0 seconds PRECEDING AND 0 seconds PRECEDING INCLUDE PREVAILING ORDER BY t.ts", expectedBids);
            assertAggregate("SELECT t.ts, t.sym, last(q.bid) bid FROM trades t WINDOW JOIN quotes q " +
                    "RANGE BETWEEN 0 seconds PRECEDING AND 0 seconds PRECEDING INCLUDE PREVAILING ORDER BY t.ts", expectedBids);
            assertNoFailures();
        });
    }
}
