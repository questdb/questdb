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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Row-level coverage for the paths where
 * {@link io.questdb.griffin.WhereClauseParser#analyzeMonotonicTimestamp} compiles a scalar
 * sub-query bound and then DECLINES to prune. The speculative compile consumes the sub-query's
 * own model (its WHERE clause is extracted into intrinsics and cleared), so unless the model is
 * restored, the residual filter re-generates the sub-query from a consumed model and silently
 * loses the sub-query's WHERE - the outer query then filters against the wrong bound and drops
 * qualifying rows.
 *
 * <p>Every test pairs the designated-timestamp query (which takes the monotonic-timestamp path)
 * with the same predicate over a NON-designated timestamp column (which does not), so the two
 * must return the same rows.
 */
public class ScalarSubqueryDeclinedPruningTest extends AbstractCairoTest {
    private static final String EXPECTED_TWO_ROWS = "i\tts\n" +
            "3\t2020-06-03T00:00:00.000000Z\n" +
            "4\t2020-06-04T00:00:00.000000Z\n";
    private static final Log LOG = LogFactory.getLog(ScalarSubqueryDeclinedPruningTest.class);
    private static final String STABILITY_GUARD_QUERY = "SELECT i, ts FROM t WHERE dateadd('h', 1, ts) >= " +
            "(SELECT lo FROM bi WHERE sym = 'X' ORDER BY lo DESC LIMIT 1)";

    // A bare designated-timestamp column BETWEEN a sub-query and a non-constant function never
    // reaches analyzeMonotonicTimestamp: analyzeBetween0() translates the lo bound through
    // translateBetweenToTimestampModel(), which compiles the sub-query, and only then does the hi
    // bound (a column expression, neither constant nor runtime-constant) fail the translation. The
    // BETWEEN node survives into the residual, which must still see the sub-query's own WHERE.
    @Test
    public void testDeclinedBareColumnBetweenKeepsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createOrBetweenBoundTable();
            // non-designated ts2 oracle: the BETWEEN translation is not attempted
            assertQuery("SELECT i, ts FROM t WHERE ts2 BETWEEN " +
                    "(SELECT lo FROM bo WHERE sym = 'X' ORDER BY lo DESC LIMIT 1) AND dateadd('h', 1, ts2)")
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
            assertQuery("SELECT i, ts FROM t WHERE ts BETWEEN " +
                    "(SELECT lo FROM bo WHERE sym = 'X' ORDER BY lo DESC LIMIT 1) AND dateadd('h', 1, ts2)")
                    .timestamp("ts")
                    .withPlanContaining("DeferredSingleSymbolFilterPageFrame", "Index forward scan on: sym")
                    .returns(EXPECTED_TWO_ROWS);
        });
    }

    // Both bounds of a BETWEEN are sub-queries: the first one is compiled, and only the second
    // one trips the stability guard. The first bound's model must be restored too.
    @Test
    public void testDeclinedBetweenSecondBoundKeepsBothSubQueryFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // non-designated ts2 oracle: the monotonic-timestamp pruning path is not taken
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts2) BETWEEN " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1) AND " +
                    "(SELECT lo FROM bi WHERE sym = 'Y' ORDER BY lo DESC LIMIT 1)")
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts) BETWEEN " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1) AND " +
                    "(SELECT lo FROM bi WHERE sym = 'Y' ORDER BY lo DESC LIMIT 1)")
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
        });
    }

    // resolveScalarBound() compiles the sub-query before it discovers the cursor does not return a
    // single TIMESTAMP column (BOUND_FAIL). That decline never reaches a result set: the residual
    // filter cannot compare a TIMESTAMP expression with a non-timestamp scalar sub-query, so the
    // query fails to compile whether or not it took the monotonic-timestamp path.
    @Test
    public void testDeclinedBoundFailFailsToCompile() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts2) >= " +
                    "(SELECT d FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1)")
                    .fails(51, "cannot compare TIMESTAMP and DATE");
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts) >= " +
                    "(SELECT d FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1)")
                    .fails(50, "cannot compare TIMESTAMP and DATE");
        });
    }

    // timestamp_ceil('M', ...) has no arithmetic inverse (month buckets are not epoch-aligned), so
    // foldInvertProbe() grades the chain NONE and pruning is declined AFTER the bound was compiled.
    @Test
    public void testDeclinedInvertProbeKeepsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = "i\tts\n" +
                    "1\t2020-06-01T00:00:00.000000Z\n" +
                    "2\t2020-06-02T00:00:00.000000Z\n" +
                    "3\t2020-06-03T00:00:00.000000Z\n" +
                    "4\t2020-06-04T00:00:00.000000Z\n";
            // non-designated ts2 oracle: the monotonic-timestamp pruning path is not taken
            assertQuery("SELECT i, ts FROM t WHERE timestamp_ceil('M', ts2) >= " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1)")
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("SELECT i, ts FROM t WHERE timestamp_ceil('M', ts) >= " +
                    "(SELECT lo FROM bi WHERE sym = 'X' AND k > 0 LIMIT 1)")
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    // The bound sub-query carries its own nested scalar sub-query in its WHERE clause: the nested
    // model is consumed by the speculative compile too, so it must be restored as well.
    @Test
    public void testDeclinedNestedSubQueryKeepsInnerFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String query = "SELECT i, ts FROM t WHERE dateadd('h', 1, %s) >= " +
                    "(SELECT lo FROM bi WHERE lo <= (SELECT max(lo) FROM bi WHERE sym = 'X') ORDER BY lo DESC LIMIT 1)";
            // non-designated ts2 oracle: the monotonic-timestamp pruning path is not taken
            assertQuery(String.format(query, "ts2"))
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
            assertQuery(String.format(query, "ts"))
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
        });
    }

    // The OR-union path: extractOrTimestampIntervals() walks the disjuncts left to right, compiles
    // the sub-query bound of the first one, and then bails on the second one (dateadd() over a
    // column is neither constant nor runtime-constant). The whole OR node stays in the residual,
    // which must still see the sub-query's own WHERE.
    @Test
    public void testDeclinedOrUnionKeepsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createOrBetweenBoundTable();
            final String expected = "i\tts\n" +
                    "3\t2020-06-03T00:00:00.000000Z\n";
            // non-designated ts2 oracle: the OR interval extraction is not attempted
            assertQuery("SELECT i, ts FROM t WHERE ts2 = " +
                    "(SELECT lo FROM bo WHERE sym = 'X' ORDER BY lo DESC LIMIT 1) OR ts2 = dateadd('h', 1, ts2)")
                    .timestamp("ts")
                    .returns(expected);
            assertQuery("SELECT i, ts FROM t WHERE ts = " +
                    "(SELECT lo FROM bo WHERE sym = 'X' ORDER BY lo DESC LIMIT 1) OR ts = dateadd('h', 1, ts2)")
                    .timestamp("ts")
                    .withPlanContaining("DeferredSingleSymbolFilterPageFrame", "Index forward scan on: sym")
                    .returns(expected);
        });
    }

    // The stability guard: ORDER BY ... LIMIT 1 over an indexed symbol lookup is not provably stable
    // within one execution, so pruning is declined and the predicate stays a residual filter. The
    // residual must still see the sub-query's own WHERE (sym = 'X').
    @Test
    public void testDeclinedStabilityGuardKeepsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // non-designated ts2 oracle: the monotonic-timestamp pruning path is not taken
            assertQuery("SELECT i, ts FROM t WHERE dateadd('h', 1, ts2) >= " +
                    "(SELECT lo FROM bi WHERE sym = 'X' ORDER BY lo DESC LIMIT 1)")
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
            assertQuery(STABILITY_GUARD_QUERY)
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
        });
    }

    // Parallel execution mode: a real worker pool runs the residual through the async filter, which
    // re-compiles the sub-query node per worker when the filter is not thread-safe.
    @Test
    public void testDeclinedStabilityGuardParallelFilter() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 4)) {
                TestUtils.execute(pool, (engine, _, executionContext) -> {
                    engine.execute("CREATE TABLE t (i INT, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", executionContext);
                    engine.execute("INSERT INTO t VALUES " +
                            "(1, '2020-06-01T00:00:00.000000Z', '2020-06-01T00:00:00.000000Z'), " +
                            "(2, '2020-06-02T00:00:00.000000Z', '2020-06-02T00:00:00.000000Z'), " +
                            "(3, '2020-06-03T00:00:00.000000Z', '2020-06-03T00:00:00.000000Z'), " +
                            "(4, '2020-06-04T00:00:00.000000Z', '2020-06-04T00:00:00.000000Z')", executionContext);
                    engine.execute("CREATE TABLE bi (sym SYMBOL INDEX, lo TIMESTAMP, d DATE, k LONG)", executionContext);
                    engine.execute("INSERT INTO bi VALUES " +
                            "('Y', '2021-01-01T00:00:00.000000Z', '2021-01-01T00:00:00.000Z'::date, 1_609_459_200_000_000), " +
                            "('X', '2020-06-02T12:00:00.000000Z', '2020-06-02T12:00:00.000Z'::date, 1_591_099_200_000_000), " +
                            "('X', '2020-06-01T00:00:00.000000Z', '2020-06-01T00:00:00.000Z'::date, 1_590_969_600_000_000)", executionContext);
                    assertQuery(STABILITY_GUARD_QUERY)
                            .withEngine(engine)
                            .withContext(executionContext)
                            .noLeakCheck()
                            .timestamp("ts")
                            .returns(EXPECTED_TWO_ROWS);
                }, configuration, LOG);
            }
        });
    }

    // The residual sub-query must still resolve its symbol key through the index rather than
    // degrading to a full scan of the bound table.
    @Test
    public void testDeclinedStabilityGuardResidualPlanKeepsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(STABILITY_GUARD_QUERY)
                    .assertsPlanContaining("DeferredSingleSymbolFilterPageFrame", "Index forward scan on: sym");
        });
    }

    // WAL storage mode for the outer table: the compile path is identical, so the declined
    // predicate must return the same rows as over a non-WAL table.
    @Test
    public void testDeclinedStabilityGuardWalTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables(" WAL");
            drainWalQueue();
            assertQuery(STABILITY_GUARD_QUERY)
                    .timestamp("ts")
                    .returns(EXPECTED_TWO_ROWS);
        });
    }

    // A bound table whose filtered maximum coincides with a row of t, so an equality/BETWEEN lower
    // bound matches. The 'Y' row comes first for the same reason as in createTables().
    private void createOrBetweenBoundTable() throws Exception {
        execute("CREATE TABLE bo (sym SYMBOL INDEX, lo TIMESTAMP)");
        execute("INSERT INTO bo VALUES " +
                "('Y', '2021-01-01T00:00:00.000000Z'), " +
                "('X', '2020-06-03T00:00:00.000000Z'), " +
                "('X', '2020-06-01T00:00:00.000000Z')");
    }

    private void createTables() throws Exception {
        createTables("");
    }

    private void createTables(String tableOptions) throws Exception {
        execute("CREATE TABLE t (i INT, ts TIMESTAMP, ts2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY" + tableOptions);
        execute("INSERT INTO t VALUES " +
                "(1, '2020-06-01T00:00:00.000000Z', '2020-06-01T00:00:00.000000Z'), " +
                "(2, '2020-06-02T00:00:00.000000Z', '2020-06-02T00:00:00.000000Z'), " +
                "(3, '2020-06-03T00:00:00.000000Z', '2020-06-03T00:00:00.000000Z'), " +
                "(4, '2020-06-04T00:00:00.000000Z', '2020-06-04T00:00:00.000000Z')");
        // The 'Y' row is inserted first on purpose: dropping the sub-query's own WHERE makes the
        // unfiltered scan pick it up, which moves the bound far into the future and empties the
        // outer result.
        execute("CREATE TABLE bi (sym SYMBOL INDEX, lo TIMESTAMP, d DATE, k LONG)");
        execute("INSERT INTO bi VALUES " +
                "('Y', '2021-01-01T00:00:00.000000Z', '2021-01-01T00:00:00.000Z'::date, 1_609_459_200_000_000), " +
                "('X', '2020-06-02T12:00:00.000000Z', '2020-06-02T12:00:00.000Z'::date, 1_591_099_200_000_000), " +
                "('X', '2020-06-01T00:00:00.000000Z', '2020-06-01T00:00:00.000Z'::date, 1_590_969_600_000_000)");
    }
}
