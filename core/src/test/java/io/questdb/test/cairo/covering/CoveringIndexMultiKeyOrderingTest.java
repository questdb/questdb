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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ordering correctness for queries served by the covering (posting) index.
 * <p>
 * The covering index is meant to be a transparent storage optimization: a query
 * over it must return exactly what the same query over the plain index path
 * returns. These tests pin that contract by running every query twice on the
 * same table -- once normally (covering) and once with the {@code no_covering}
 * hint (plain FilterOnValues path, the oracle) -- and asserting the two agree.
 * A handful of explicit-value tests pin the canonical multi-key shapes so the
 * intended output is documented in-line.
 * <p>
 * This suite guards two ordering defects that were fixed:
 * <ul>
 * <li><b>Multi-key key grouping.</b> The multi-key covering cursor used to emit
 * rows grouped by (partition, key) -- draining one key's posting list before the
 * next -- rather than merging the keys by row id / timestamp. For keys whose
 * timestamps interleave, that disagreed with the oracle on every shape that
 * expects timestamp order (bare scan, LIMIT, negative LIMIT, ORDER BY ts). Both
 * the record and page-frame cursors now k-way merge the keys by row id.</li>
 * <li><b>ORDER BY ts DESC + LIMIT.</b> Even for a single key or disjoint ranges
 * (no interleaving), {@code ORDER BY ts DESC LIMIT N} used to limit the ascending
 * scan and then reverse, returning the wrong end -- because codegen pushed the
 * limit into the parallel scan ahead of the sort. The limit is now left to the
 * outer LimitRecordCursorFactory whenever a sort reorders the scan's output. See
 * {@link #testSingleKeyOrderByTsDescLimitExplicit}.</li>
 * </ul>
 * {@link #testReferenceOracleIsTimestampOrdered} pins that the oracle itself is
 * sound, and {@link #testOrderBySymStaysConsistent} that key grouping is still
 * used when the query actually asks for key order.
 */
public class CoveringIndexMultiKeyOrderingTest extends AbstractCairoTest {

    // Shapes that expect global timestamp order; the covering path must match the
    // plain path on all of them.
    private static final String[] TS_ORDER_TAILS = {
            "",
            "LIMIT 3",
            "LIMIT 7",
            "LIMIT 100",
            "LIMIT -3",
            "LIMIT -7",
            "LIMIT -100",
            "LIMIT 2,5",
            "LIMIT -5,-2",
            "ORDER BY ts",
            "ORDER BY ts LIMIT 3",
            "ORDER BY ts LIMIT -3",
            "ORDER BY ts DESC",
            "ORDER BY ts DESC LIMIT 3",
            "ORDER BY ts DESC LIMIT -3",
    };

    // Shapes with an explicit non-timestamp order; both paths route through the
    // same sort / key-grouping, so they must agree regardless of the bug.
    private static final String[] EXPLICIT_ORDER_TAILS = {
            "ORDER BY sym",
            "ORDER BY sym, ts",
            "ORDER BY price",
            "ORDER BY price DESC",
    };

    @Test
    public void testBindVariableLimitCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_bv");
            // Unknown-sign bind-variable limit: both signs must match the oracle.
            bindVariableService.clear();
            bindVariableService.setLong("lim", -3);
            assertOneShapeMatchesOracle("t_bv", "price", "sym IN ('A','B') AND price > 0", "LIMIT :lim");
            bindVariableService.setLong("lim", 3);
            assertOneShapeMatchesOracle("t_bv", "price", "sym IN ('A','B') AND price > 0", "LIMIT :lim");
            bindVariableService.setLong("lim", -100);
            assertOneShapeMatchesOracle("t_bv", "price", "sym IN ('A','B') AND price > 0", "LIMIT :lim");
        });
    }

    @Test
    public void testDeferredSymbolInListCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_def");
            // 'Z' does not exist in the symbol table -> deferred symbol resolution.
            assertCoveringMatchesOracle("t_def", "price", "sym IN ('A','B','Z') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testDisjointRangesStayConsistent() throws Exception {
        // With disjoint key ranges, key-grouped order already equals timestamp
        // order, so the multi-key grouping defect could not show here even before
        // the fix -- this documents why a disjoint-range test cannot catch that
        // bug. It still exercises the (independent) ORDER BY ts DESC + LIMIT path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_disj (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_disj " +
                    "SELECT dateadd('d', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                    "CASE WHEN x <= 5 THEN 'A' ELSE 'B' END, x::DOUBLE FROM long_sequence(10)");
            execute("ALTER TABLE t_disj ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            assertCoveringMatchesOracle("t_disj", "price", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testFilterDropsRowsCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_drop");
            // Residual filter eliminates the low half; the tail must still be correct.
            assertCoveringMatchesOracle("t_drop", "price", "sym IN ('A','B') AND price > 4", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testMultiColumnProjectionCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_proj");
            assertCoveringMatchesOracle("t_proj", "ts, sym, price", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testMultiPartitionInterleavedCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            // 6h spacing over 20 rows -> 5 DAY partitions, 4 interleaved rows each.
            execute("CREATE TABLE t_mp (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_mp " +
                    "SELECT dateadd('h', (x * 6)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                    "CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, x::DOUBLE FROM long_sequence(20)");
            execute("ALTER TABLE t_mp ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            assertCoveringMatchesOracle("t_mp", "price", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testNoResidualFilterCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_nofilter");
            // No residual predicate: covering is used without the filter wrapper.
            assertCoveringMatchesOracle("t_nofilter", "price", "sym IN ('A','B')", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testOrderBySymStaysConsistent() throws Exception {
        // GREEN today: when the query explicitly asks for key order (or a non-ts
        // order that forces a sort), covering and oracle must agree. Confirms the
        // bug is specific to timestamp-order expectations and that key-grouping is
        // legitimate when actually requested.
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_sym");
            assertCoveringMatchesOracle("t_sym", "ts, sym, price", "sym IN ('A','B') AND price > 0", EXPLICIT_ORDER_TAILS);
        });
    }

    @Test
    public void testReferenceOracleIsTimestampOrdered() throws Exception {
        // Validates the oracle itself: the no_covering (plain index) path returns
        // strict timestamp order for interleaved keys. If this ever goes red the
        // oracle is unsound and the other tests' conclusions do not hold.
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_oracle");
            assertResult("price\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n",
                    "SELECT /*+ no_covering */ price FROM t_oracle WHERE sym IN ('A','B') AND price > 0");
            assertResult("price\n8.0\n9.0\n10.0\n",
                    "SELECT /*+ no_covering */ price FROM t_oracle WHERE sym IN ('A','B') AND price > 0 LIMIT -3");
            assertResult("price\n1.0\n2.0\n3.0\n",
                    "SELECT /*+ no_covering */ price FROM t_oracle WHERE sym IN ('A','B') AND price > 0 LIMIT 3");
        });
    }

    @Test
    public void testSingleKeyFilterDropTopSubFrameNegativeLimit() throws Exception {
        // Single-key backward path with a 2-row frame cap: the residual filter
        // (price > 5) drops rows inside the surviving top descending sub-frame,
        // exactly where a descending-accumulation off-by-one under a negative
        // limit would hide. Cross-checked against the oracle across the full tail
        // matrix (incl. LIMIT -3/-7/-100).
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE t_fdsf (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
                execute("INSERT INTO t_fdsf " +
                        "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(10)");
                execute("ALTER TABLE t_fdsf ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
                assertCoveringMatchesOracle("t_fdsf", "price", "sym = 'A' AND price > 5", TS_ORDER_TAILS);
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testSingleKeyNegativeLimitLargePartitionRealloc() throws Exception {
        // 10k rows in a single partition with the default (large) frame cap: the
        // single descending sub-frame starts at INITIAL_CAPACITY and reallocs
        // mid-fill via growFrameBuffers as it accumulates rows -- a relocation the
        // <=10-row negative-limit tests never reach. LIMIT -5000 returns the last
        // 5000 rows by timestamp; cross-checked against the oracle.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_big (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_big " +
                    "SELECT dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(10_000)");
            execute("ALTER TABLE t_big ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            assertOneShapeMatchesOracle("t_big", "price", "sym = 'A' AND price > 0", "LIMIT -5000");
        });
    }

    @Test
    public void testSingleKeyNegativeLimitRegression() throws Exception {
        // Single-key covering is globally timestamp-ordered. Cross-check every
        // shape against the oracle: plain and ORDER BY ts limits (both signs) and
        // the ORDER BY ts DESC + LIMIT shapes must all agree. See
        // testSingleKeyOrderByTsDescLimitExplicit for the DESC cases in detail.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_sk (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_sk " +
                    "SELECT dateadd('h', (x * 6)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(20)");
            execute("ALTER TABLE t_sk ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            assertCoveringMatchesOracle("t_sk", "price", "sym = 'A' AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testSingleKeyOrderByTsDescLimitExplicit() throws Exception {
        // One key, hours 1..10, price == hour. ORDER BY ts DESC is newest-first:
        // 10,9,...,1. Both shapes below were wrong on the covering path before the
        // fix even though there is no interleaving (a pushed-down limit truncated
        // the ascending scan ahead of the sort).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_skd (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO t_skd " +
                    "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE FROM long_sequence(10)");
            execute("ALTER TABLE t_skd ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            final String pred = " WHERE sym = 'A' AND price > 0";
            // Sanity: the oracle path is correct for both shapes.
            assertResult("price\n10.0\n9.0\n8.0\n", "SELECT /*+ no_covering */ price FROM t_skd" + pred + " ORDER BY ts DESC LIMIT 3");
            assertResult("price\n3.0\n2.0\n1.0\n", "SELECT /*+ no_covering */ price FROM t_skd" + pred + " ORDER BY ts DESC LIMIT -3");
            // Positive limit must take the first 3 of DESC order (the three
            // newest), not the three oldest reversed.
            assertResult("price\n10.0\n9.0\n8.0\n", "SELECT price FROM t_skd" + pred + " ORDER BY ts DESC LIMIT 3");
            // Negative limit must take the last 3 of DESC order (the three oldest).
            assertResult("price\n3.0\n2.0\n1.0\n", "SELECT price FROM t_skd" + pred + " ORDER BY ts DESC LIMIT -3");
        });
    }

    @Test
    public void testSingleKeyVarWidthNegativeLimitCrossCheck() throws Exception {
        // Single-key backward (descending) scan projecting var-width covered
        // columns -- a VARCHAR with a NULL and a DOUBLE[] -- under the full tail
        // matrix incl. LIMIT -3/-7/-100. Exercises var-data offset and sentinel
        // handling on the backward path, both as one frame (default cap) and
        // split into 2-row descending sub-frames. Every existing negative-limit
        // test covers fixed-width DOUBLE only.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_skvw (ts TIMESTAMP, sym SYMBOL, price DOUBLE, vc VARCHAR, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_skvw " +
                    "SELECT dateadd('h', (x * 6)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A', x::DOUBLE, " +
                    "CASE WHEN x = 4 THEN NULL::VARCHAR ELSE ('row_' || x)::VARCHAR END, " +
                    "ARRAY[x::DOUBLE, (x + 1)::DOUBLE] FROM long_sequence(20)");
            execute("ALTER TABLE t_skvw ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, vc, arr)");
            assertCoveringMatchesOracle("t_skvw", "ts, price, vc, arr", "sym = 'A' AND price > 0", TS_ORDER_TAILS);
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
            try {
                assertCoveringMatchesOracle("t_skvw", "ts, price, vc, arr", "sym = 'A' AND price > 0", TS_ORDER_TAILS);
            } finally {
                CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
            }
        });
    }

    @Test
    public void testSinglePartitionInterleavedCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_sp");
            assertCoveringMatchesOracle("t_sp", "price", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testSinglePartitionInterleavedExplicit() throws Exception {
        // Explicit expected values for the canonical interleaved shape:
        // A on odd hours (1,3,5,7,9), B on even hours (2,4,6,8,10), price == hour.
        assertMemoryLeak(() -> {
            createInterleavedSinglePartition("t_exp");
            final String pred = " WHERE sym IN ('A','B') AND price > 0";
            // Bare scan must be timestamp-ordered, like the plain index.
            assertResult("price\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n",
                    "SELECT price FROM t_exp" + pred);
            // Positive LIMIT: first N by timestamp.
            assertResult("price\n1.0\n2.0\n3.0\n", "SELECT price FROM t_exp" + pred + " LIMIT 3");
            // Negative LIMIT: last N by timestamp.
            assertResult("price\n8.0\n9.0\n10.0\n", "SELECT price FROM t_exp" + pred + " LIMIT -3");
            // Explicit ORDER BY ts (no limit) must not be silently violated.
            assertResult("price\n1.0\n2.0\n3.0\n4.0\n5.0\n6.0\n7.0\n8.0\n9.0\n10.0\n",
                    "SELECT price FROM t_exp" + pred + " ORDER BY ts");
            assertResult("price\n8.0\n9.0\n10.0\n", "SELECT price FROM t_exp" + pred + " ORDER BY ts LIMIT -3");
            assertResult("price\n10.0\n9.0\n8.0\n7.0\n6.0\n5.0\n4.0\n3.0\n2.0\n1.0\n",
                    "SELECT price FROM t_exp" + pred + " ORDER BY ts DESC");
            assertResult("price\n10.0\n9.0\n8.0\n", "SELECT price FROM t_exp" + pred + " ORDER BY ts DESC LIMIT 3");
        });
    }

    @Test
    public void testSubFrameSplitInterleavedCrossCheck() throws Exception {
        // Force several sub-frames per partition (2-row cap) so the bug is probed
        // with chunked frame emission as well.
        CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(2);
        try {
            assertMemoryLeak(() -> {
                createInterleavedSinglePartition("t_sf");
                assertCoveringMatchesOracle("t_sf", "price", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
            });
        } finally {
            CoveringIndexRecordCursorFactory.setMaxRowsPerFrameForTesting(-1);
        }
    }

    @Test
    public void testThreeKeysInterleavedCrossCheck() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_3k (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t_3k " +
                    "SELECT dateadd('h', (x * 6)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                    "CASE WHEN x % 3 = 1 THEN 'A' WHEN x % 3 = 2 THEN 'B' ELSE 'C' END, x::DOUBLE FROM long_sequence(21)");
            execute("ALTER TABLE t_3k ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            assertCoveringMatchesOracle("t_3k", "price", "sym IN ('A','B','C') AND price > 0", TS_ORDER_TAILS);
        });
    }

    @Test
    public void testVarWidthCoveredColumnsNegativeLimitCrossCheck() throws Exception {
        // Interleaved multi-key scan projecting var-width covered columns -- a
        // VARCHAR with a NULL and a DOUBLE[]. Positive/bare limits exercise the
        // forward k-way merge var-data reads; negative limits route through the
        // serial fallback. Full tail matrix incl. LIMIT -3/-7/-100, cross-checked
        // against the oracle.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_mkvw (ts TIMESTAMP, sym SYMBOL, price DOUBLE, vc VARCHAR, arr DOUBLE[]) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
            execute("INSERT INTO t_mkvw " +
                    "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                    "CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, x::DOUBLE, " +
                    "CASE WHEN x = 3 THEN NULL::VARCHAR ELSE ('row_' || x)::VARCHAR END, " +
                    "ARRAY[x::DOUBLE, (x + 1)::DOUBLE] FROM long_sequence(10)");
            execute("ALTER TABLE t_mkvw ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, vc, arr)");
            assertCoveringMatchesOracle("t_mkvw", "ts, sym, price, vc, arr", "sym IN ('A','B') AND price > 0", TS_ORDER_TAILS);
        });
    }

    /**
     * Runs each shape twice on {@code table} -- once covering, once with the
     * {@code no_covering} oracle hint -- and asserts the results match. Confirms
     * the covering plan really is a covering plan so a pass cannot be vacuous.
     * Collects all mismatches so a single run reports the full red/green map.
     */

    private void assertCoveringMatchesOracle(String table, String projection, String predicate, String[] tails) throws Exception {
        StringBuilder fails = new StringBuilder();
        for (String tail : tails) {
            String suffix = " WHERE " + predicate + (tail.isEmpty() ? "" : " " + tail);
            String coveringQ = "SELECT " + projection + " FROM " + table + suffix;
            String referenceQ = "SELECT /*+ no_covering */ " + projection + " FROM " + table + suffix;

            String covPlan = getPlan(coveringQ);
            if (!covPlan.contains("CoveringIndex")) {
                fails.append("\n  [").append(label(tail)).append("] not a covering plan, comparison would be vacuous:\n    ")
                        .append(covPlan.replace("\n", "\n    ")).append('\n');
                continue;
            }
            String ref = runToString(referenceQ);
            String cov = runToString(coveringQ);
            if (!ref.equals(cov)) {
                fails.append("\n  [").append(label(tail)).append("]\n")
                        .append("      oracle  : ").append(oneLine(ref)).append('\n')
                        .append("      covering: ").append(oneLine(cov)).append('\n');
            }
        }
        if (fails.length() > 0) {
            Assert.fail("covering disagrees with no_covering oracle on " + table + ":\n" + fails);
        }
    }

    private void assertOneShapeMatchesOracle(String table, String projection, String predicate, String tail) throws Exception {
        assertCoveringMatchesOracle(table, projection, predicate, new String[]{tail});
    }

    private void assertResult(String expected, String query) throws SqlException {
        printSql(query);
        Assert.assertEquals(query, expected, sink.toString());
    }

    private void createInterleavedSinglePartition(String table) throws SqlException {
        // Single partition (PARTITION BY YEAR), keys interleaved in time:
        // A on odd hours, B on even hours, price == hour, ts strictly increasing.
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL");
        execute("INSERT INTO " + table + " " +
                "SELECT dateadd('h', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), " +
                "CASE WHEN x % 2 = 1 THEN 'A' ELSE 'B' END, x::DOUBLE FROM long_sequence(10)");
        execute("ALTER TABLE " + table + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
    }

    private String getPlan(String query) throws SqlException {
        try (RecordCursorFactory factory = select(query)) {
            planSink.clear();
            factory.toPlan(planSink);
            return planSink.getSink().toString();
        }
    }

    private String label(String tail) {
        return tail.isEmpty() ? "(no tail)" : tail;
    }

    private String oneLine(String s) {
        return s.replace("\n", " ").trim();
    }

    private String runToString(String query) {
        try {
            printSql(query);
            return sink.toString();
        } catch (Throwable t) {
            return "ERROR " + t.getClass().getSimpleName() + ": " + t.getMessage();
        }
    }
}
