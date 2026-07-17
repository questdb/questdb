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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Regression tests for the {@link io.questdb.griffin.WhereClauseParser} reentrancy bug where
 * subquery-valued designated-timestamp bounds (e.g. {@code ts >= (select ... )}) clobbered the
 * outer query's parser state. Depending on predicate order this either skipped the covering
 * index (async filter fallback) or produced an AssertionError / empty-key scan.
 *
 * <p>The table mirrors the shape that triggered the report: a nanosecond designated timestamp,
 * an indexed symbol carrying a covering (INCLUDE) sidecar, and Parquet column encodings.
 */
public class CoveringSubqueryBoundReproTest extends AbstractCairoTest {

    private final String constantBounds =
            "select ts, value from sensor where series = 's1' " +
                    "and ts >= '2021-11-23T12:51:23.700716000Z' " +
                    "and ts <= '2021-11-23T17:59:17.060338000Z'";
    private final String subqueryBounds =
            "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100)";
    private final String subqueryBoundsSymLast =
            "select ts, value from sensor where " +
                    "ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100) " +
                    "and series = 's1'";

    private void createSchema() throws Exception {
        execute("create table sensor (" +
                "    ts timestamp_ns parquet(delta_binary_packed, lz4_raw)," +
                "    series symbol index type posting include (value, ts) parquet(rle_dictionary, lz4_raw, bloom_filter)," +
                "    value float parquet(plain, lz4_raw)" +
                ") timestamp(ts) partition by hour");
        execute("create table bounds (lo timestamp_ns, hi timestamp_ns, sel int)");
        execute("insert into bounds values (" +
                "'2021-11-23T12:51:23.700716000Z'::timestamp_ns, " +
                "'2021-11-23T17:59:17.060338000Z'::timestamp_ns, 100)");
    }

    // ~3000 rows across ~8 hourly partitions on 2021-11-23, several symbols incl. 's1',
    // plus boundary rows exactly at lo and hi to exercise inclusive bounds.
    private void seedData() throws Exception {
        execute("insert into sensor " +
                "select ('2021-11-23T12:00:00.000000000Z'::timestamp_ns + (x * 7_000_000_000L))::timestamp_ns, " +
                "       rnd_symbol('s1','s2','s3','s4','s1'), " +
                "       rnd_float() " +
                "from long_sequence(3000)");
        // exact boundary rows for 's1'
        execute("insert into sensor values " +
                "('2021-11-23T12:51:23.700716000Z','s1', 42.0), " +   // == lo (inclusive)
                "('2021-11-23T17:59:17.060338000Z','s1', 43.0), " +   // == hi (inclusive)
                "('2021-11-23T12:51:23.700715999Z','s1', 44.0), " +   // just below lo (excluded)
                "('2021-11-23T17:59:17.060338001Z','s1', 45.0)");     // just above hi (excluded)
    }

    private void assertPlanContains(String sql, String needle) throws Exception {
        assertQuery(sql).assertsPlanContaining(needle);
    }

    private void assertUsesCoveringIndex(String sql) throws Exception {
        assertQuery(sql).assertsPlanContaining("CoveringIndex on: series");
        assertQuery(sql).assertsPlanNotContaining("Async Filter", "Async JIT Filter");
    }

    @Test
    public void testExactQueryUsesCoveringIndexNativePartitions() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // the constant-bounds oracle must itself use the covering index, so the
            // assertSqlCursors row-order comparison below is apples-to-apples
            assertUsesCoveringIndex(constantBounds);
            assertUsesCoveringIndex(subqueryBounds);
            assertUsesCoveringIndex(subqueryBoundsSymLast);
            // rows must equal the constant-bounds query over a real multi-partition dataset
            assertSqlCursors(constantBounds, subqueryBounds);
            assertSqlCursors(constantBounds, subqueryBoundsSymLast);
        });
    }

    @Test
    public void testExactQueryUsesCoveringIndexParquetPartitions() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // Convert partitions 12..16 to PARQUET, leaving partition 17 (which holds the
            // == hi and just-above-hi boundary rows) native, so the scan straddles both
            // storage formats.
            execute("alter table sensor convert partition to parquet where ts < '2021-11-23T17:00:00.000000000Z'");
            assertUsesCoveringIndex(subqueryBounds);
            assertUsesCoveringIndex(subqueryBoundsSymLast);
            assertSqlCursors(constantBounds, subqueryBounds);
            assertSqlCursors(constantBounds, subqueryBoundsSymLast);
        });
    }

    @Test
    public void testScalarSubqueryTimestampBoundsRejectMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, value INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO events VALUES ('2024-01-02', 1)");
            execute("CREATE TABLE multiple_bounds (ts TIMESTAMP)");
            execute("INSERT INTO multiple_bounds VALUES ('2024-01-01'), ('2024-01-02')");

            final String direct = "SELECT * FROM events WHERE ts >= (SELECT ts FROM multiple_bounds)";
            assertExceptionNoLeakCheck(
                    direct,
                    direct.indexOf("(SELECT") + 1,
                    "scalar sub-query returned more than one row"
            );

            final String equality = "SELECT * FROM events WHERE ts = (SELECT ts FROM multiple_bounds)";
            assertExceptionNoLeakCheck(
                    equality,
                    equality.indexOf("(SELECT") + 1,
                    "scalar sub-query returned more than one row"
            );

            final String upper = "SELECT * FROM events WHERE ts <= (SELECT ts FROM multiple_bounds)";
            assertExceptionNoLeakCheck(
                    upper,
                    upper.indexOf("(SELECT") + 1,
                    "scalar sub-query returned more than one row"
            );

            final String between = "SELECT * FROM events WHERE ts BETWEEN (SELECT ts FROM multiple_bounds) AND '2024-01-03'";
            assertExceptionNoLeakCheck(
                    between,
                    between.indexOf("(SELECT") + 1,
                    "scalar sub-query returned more than one row"
            );

            final String monotonic = "SELECT * FROM events WHERE dateadd('d', 1, ts) >= (SELECT ts FROM multiple_bounds)";
            assertExceptionNoLeakCheck(
                    monotonic,
                    monotonic.indexOf("(SELECT") + 1,
                    "scalar sub-query returned more than one row"
            );
        });
    }

    @Test
    public void testSingleBoundAndInList() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String singleBoundSub = "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds where sel = 100)";
            String singleBoundConst = "select ts, value from sensor where series = 's1' " +
                    "and ts >= '2021-11-23T12:51:23.700716000Z'";
            assertUsesCoveringIndex(singleBoundSub);
            assertSqlCursors(singleBoundConst, singleBoundSub);

            String inListSub = "select ts, value from sensor where series in ('s1','s3') " +
                    "and ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100)";
            String inListConst = "select ts, value from sensor where series in ('s1','s3') " +
                    "and ts >= '2021-11-23T12:51:23.700716000Z' " +
                    "and ts <= '2021-11-23T17:59:17.060338000Z'";
            assertUsesCoveringIndex(inListSub);
            assertSqlCursors(inListConst, inListSub);
        });
    }

    // Excluded-key (negation) + OR key paths: these accumulate into keyExclNodes /
    // tempKeyExcludedValues / orIntrinsicNodes, which the fix also saves/restores.
    // Exercise both predicate orderings so the outer key state is live when the
    // nested bound subquery is compiled.
    @Test
    public void testNegatedAndOrKeyWithSubqueryBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // negated key, sym-last so the excluded-key state is live at nested compile
            String negSub = "select ts, value from sensor where " +
                    "ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100) " +
                    "and series != 's2'";
            String negConst = "select ts, value from sensor where " +
                    "ts >= '2021-11-23T12:51:23.700716000Z' " +
                    "and ts <= '2021-11-23T17:59:17.060338000Z' " +
                    "and series != 's2'";
            assertSqlCursors(negConst, negSub);

            // OR of two symbol equalities, sym-last. This is not an indexed-key path
            // (it stays a JIT filter), but the subquery ts bounds must still be pruned
            // into an interval scan and rows must match the constant-bounds oracle.
            String orSub = "select ts, value from sensor where " +
                    "ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100) " +
                    "and (series = 's1' or series = 's3')";
            String orConst = "select ts, value from sensor where " +
                    "ts >= '2021-11-23T12:51:23.700716000Z' " +
                    "and ts <= '2021-11-23T17:59:17.060338000Z' " +
                    "and (series = 's1' or series = 's3')";
            assertPlanContains(orSub, "Interval forward scan on: sensor");
            assertSqlCursors(orConst, orSub);
        });
    }

    // Depth-2 reentrancy: the bound subqueries themselves carry a timestamp-vs-subquery
    // predicate, so compiling them re-enters extract() a second level deep.
    @Test
    public void testNestedDepth2SubqueryBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String nestedSub = "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds where sel = 100 and lo >= (select min(lo) from bounds)) " +
                    "and ts <= (select hi from bounds where sel = 100 and hi <= (select max(hi) from bounds))";
            assertUsesCoveringIndex(nestedSub);
            assertSqlCursors(constantBounds, nestedSub);
        });
    }

    // The bound subquery has its OWN indexed symbol key. Compiling it re-enters
    // extract(); if the nested call is not started clean it reverts the outer
    // query's key-node intrinsic marks (revertNodes on the shared ExpressionNode).
    @Test
    public void testNestedSubqueryWithIndexedKey() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            execute("create table bounds_idx (lo timestamp_ns, hi timestamp_ns, tag symbol index)");
            execute("insert into bounds_idx values (" +
                    "cast('2021-11-23T12:51:23.700716000Z' as timestamp_ns), " +
                    "cast('2021-11-23T17:59:17.060338000Z' as timestamp_ns), 'g1')");
            // sym-first: series processed after the ts bounds
            String subSymFirst = "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds_idx where tag = 'g1') " +
                    "and ts <= (select hi from bounds_idx where tag = 'g1')";
            // sym-last: series processed BEFORE the ts bounds, so it is in keyNodes when the
            // nested (indexed) subquery runs clearAllKeys()/revertNodes()
            String subSymLast = "select ts, value from sensor where " +
                    "ts >= (select lo from bounds_idx where tag = 'g1') " +
                    "and ts <= (select hi from bounds_idx where tag = 'g1') " +
                    "and series = 's1'";
            assertUsesCoveringIndex(subSymFirst);
            assertSqlCursors(constantBounds, subSymFirst);
            assertUsesCoveringIndex(subSymLast);
            assertSqlCursors(constantBounds, subSymLast);
        });
    }

    @Test
    public void testNestedLatestBySubqueryPreservesResidualFilter() throws Exception {
        configOverrideUseWithinLatestByOptimisation();

        assertMemoryLeak(() -> {
            execute("CREATE TABLE events (ts TIMESTAMP, sym SYMBOL INDEX, value INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO events VALUES
                        ('2024-01-01', 'a', 1),
                        ('2024-01-02', 'a', 2),
                        ('2024-01-03', 'a', 3),
                        ('2024-01-02', 'b', 2)
                    """);
            execute("CREATE TABLE latest_bounds (lo TIMESTAMP, selector SYMBOL) TIMESTAMP(lo) PARTITION BY DAY");
            execute("INSERT INTO latest_bounds VALUES ('2024-01-02', 'x')");

            assertQuery("""
                    SELECT ts, sym, value
                    FROM events
                    WHERE ts >= (
                          SELECT lo
                          FROM latest_bounds
                          WHERE selector = 'x'
                          LATEST ON lo PARTITION BY selector
                      )
                      AND sym = 'a'
                      AND value = 2
                    ORDER BY ts
                    """)
                    .withPlanContaining("Index forward scan on: sym")
                    .withPlanNotContaining("Async Filter", "Async JIT Filter")
                    .timestamp("ts")
                    .returns("""
                            ts\tsym\tvalue
                            2024-01-02T00:00:00.000000Z\ta\t2
                            """);
        });
    }

    // Empty subquery result -> null bound. Must behave identically to a constant NULL bound
    // (both return no rows) and must not crash or corrupt the key scan.
    @Test
    public void testEmptySubqueryResult() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String emptySub = "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds where sel = 999) " +
                    "and ts <= (select hi from bounds where sel = 999)";
            assertSqlCursors("select ts, value from sensor where 1 = 2", emptySub);
        });
    }

    // BETWEEN with scalar-subquery bounds (ts BETWEEN (select ...) AND (select ...)) must parse and
    // extract a runtime interval, identically to constant/bind-variable BETWEEN bounds. Before the
    // fix the parser rejected the subquery ("constant expected") and, even past parsing, the
    // boundary translator ignored the QUERY node.
    @Test
    public void testBetweenSubqueryBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String betweenSub = "select ts, value from sensor where series = 's1' " +
                    "and ts between (select lo from bounds where sel = 100) and (select hi from bounds where sel = 100)";
            String betweenConst = "select ts, value from sensor where series = 's1' " +
                    "and ts between '2021-11-23T12:51:23.700716000Z' and '2021-11-23T17:59:17.060338000Z'";
            // BETWEEN is exactly [lo, hi], so the interval fully replaces the predicate: covering
            // index with no residual async filter, same as the constant-bounds oracle.
            assertUsesCoveringIndex(betweenConst);
            assertUsesCoveringIndex(betweenSub);
            assertSqlCursors(betweenConst, betweenSub);

            // Without the key predicate the interval surfaces directly in the plan.
            String betweenNoKey = "select ts, value from sensor where " +
                    "ts between (select lo from bounds where sel = 100) and (select hi from bounds where sel = 100)";
            assertPlanContains(betweenNoKey, "Interval forward scan on: sensor");
        });
    }

    @Test
    public void testSubqueryBoundsReleasedWhenIntervalAlreadyEmpty() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();

            // The parser visits the rightmost AND terms first, so the contradictory constants
            // empty the interval model before it compiles the runtime scalar bound.
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts >= (SELECT lo FROM bounds WHERE sel = 100)
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts <= (SELECT hi FROM bounds WHERE sel = 100)
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts = (SELECT lo FROM bounds WHERE sel = 100)
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");

            // Exercise each BETWEEN boundary combination after the model is already empty.
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts BETWEEN (SELECT lo FROM bounds WHERE sel = 100)
                                     AND (SELECT hi FROM bounds WHERE sel = 100)
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts BETWEEN (SELECT lo FROM bounds WHERE sel = 100)
                                     AND '2021-11-23T17:59:17.060338000Z'
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");
            assertQuery("""
                    SELECT ts FROM sensor
                    WHERE ts BETWEEN '2021-11-23T12:51:23.700716000Z'
                                     AND (SELECT hi FROM bounds WHERE sel = 100)
                      AND ts > '2022-01-01'
                      AND ts < '2021-01-01'
                    """).timestamp("ts").returns("ts\n");

            // Reuse the parser after every empty-model ownership path.
            assertSqlCursors(constantBounds, subqueryBounds);
        });
    }

    @Test
    public void testBetweenSubqueryBoundsReleasedWithNullBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String empty = "SELECT ts FROM sensor WHERE 1 = 2";
            assertSqlCursors(
                    empty,
                    "SELECT ts FROM sensor WHERE ts BETWEEN " +
                            "(SELECT lo FROM bounds WHERE sel = 100) AND NULL"
            );
            assertSqlCursors(
                    empty,
                    "SELECT ts FROM sensor WHERE ts BETWEEN NULL AND " +
                            "(SELECT hi FROM bounds WHERE sel = 100)"
            );
            // NOT BETWEEN with NULL intentionally imposes no interval restriction.
            assertSqlCursors(
                    "SELECT ts FROM sensor",
                    "SELECT ts FROM sensor WHERE ts NOT BETWEEN " +
                            "(SELECT lo FROM bounds WHERE sel = 100) AND NULL"
            );
            assertSqlCursors(
                    "SELECT ts FROM sensor",
                    "SELECT ts FROM sensor WHERE ts NOT BETWEEN NULL AND " +
                            "(SELECT hi FROM bounds WHERE sel = 100)"
            );
        });
    }

    // A BETWEEN whose FIRST bound is a qualifying single-timestamp subquery but whose SECOND bound
    // is a non-qualifying subquery (wrong column count / non-timestamp) must not leak the first
    // bound's compiled cursor factory. The parser adopts the first (lo) boundary function, then the
    // second (hi) boundary fails to qualify and the BETWEEN falls back to the between(NCC) factory,
    // which rejects the non-timestamp / multi-column hi cursor. The error position points at the
    // SECOND subquery, proving the failure happens AFTER the first endpoint was adopted (not an
    // early parse failure before it), and assertMemoryLeak proves the retained first boundary
    // function is freed on the rollback path with no leak.
    @Test
    public void testBetweenSubqueryFirstBoundRetainedThenSecondBoundFails() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();

            // Control: the SAME qualifying first (lo) bound with a qualifying second (hi) bound
            // compiles into a covering-index interval scan. This proves the first endpoint is
            // genuinely adopted, so the failures below exercise the rollback-after-first-endpoint
            // path rather than an early parse failure that never reaches the second endpoint.
            assertUsesCoveringIndex("select ts, value from sensor where series = 's1' " +
                    "and ts between (select lo from bounds where sel = 100) and (select hi from bounds where sel = 100)");

            // second bound subquery returns an INT column, not a timestamp -> does not qualify
            String badType = "select ts, value from sensor where " +
                    "ts between (select lo from bounds where sel = 100) and (select sel from bounds where sel = 100)";
            assertExceptionNoLeakCheck(
                    badType,
                    badType.lastIndexOf("(select") + 1,
                    "cannot compare TIMESTAMP and INT"
            );

            // second bound subquery returns TWO columns -> does not qualify
            String badArity = "select ts, value from sensor where " +
                    "ts between (select lo from bounds where sel = 100) and (select lo, hi from bounds where sel = 100)";
            assertExceptionNoLeakCheck(
                    badArity,
                    badArity.lastIndexOf("(select") + 1,
                    "select must provide exactly one column"
            );
        });
    }

    // The AND inside a subquery bound's own WHERE must not be miscounted as the BETWEEN separator
    // (it sits at a deeper scope than betweenStartScopeDepth), and NOT BETWEEN must work too.
    @Test
    public void testBetweenSubqueryBoundsWithNestedAndAndNegation() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // each bound subquery carries an AND in its WHERE
            String betweenSub = "select ts, value from sensor where series = 's1' " +
                    "and ts between (select lo from bounds where sel = 100 and lo is not null) " +
                    "and (select hi from bounds where sel = 100 and hi is not null)";
            String betweenConst = "select ts, value from sensor where series = 's1' " +
                    "and ts between '2021-11-23T12:51:23.700716000Z' and '2021-11-23T17:59:17.060338000Z'";
            assertUsesCoveringIndex(betweenSub);
            assertSqlCursors(betweenConst, betweenSub);

            String notBetweenSub = "select ts, value from sensor where series = 's1' " +
                    "and ts not between (select lo from bounds where sel = 100) and (select hi from bounds where sel = 100)";
            String notBetweenConst = "select ts, value from sensor where series = 's1' " +
                    "and ts not between '2021-11-23T12:51:23.700716000Z' and '2021-11-23T17:59:17.060338000Z'";
            assertSqlCursors(notBetweenConst, notBetweenSub);
        });
    }

    // A monotonic transform of the timestamp compared to a subquery bound
    // (dateadd('h',1,ts) >= (select ...)) must prune to a runtime interval, inverting the
    // transform the same way as a constant/bind-variable bound. Before the fix,
    // resolveScalarBound rejected the QUERY node (isFunc excludes it), so no interval was
    // extracted and the predicate stayed a full-scan residual filter.
    @Test
    public void testMonotonicTransformSubqueryBound() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // Without a key predicate the runtime interval surfaces directly in the plan.
            // dateadd('h',1,ts) >= lo  inverts to  ts >= lo - 1h, i.e. 11:51:23.700716.
            String monoNoKey = "select ts, value from sensor where " +
                    "dateadd('h', 1, ts) >= (select lo from bounds where sel = 100)";
            assertPlanContains(monoNoKey, "Interval forward scan on: sensor");
            assertPlanContains(monoNoKey, "2021-11-23T11:51:23.700716000Z");
            String monoNoKeyConst = "select ts, value from sensor where " +
                    "dateadd('h', 1, ts) >= '2021-11-23T12:51:23.700716000Z'";
            assertSqlCursors(monoNoKeyConst, monoNoKey);

            // With the covering-index key predicate the runtime interval is applied inside the
            // covering-index cursor (not shown as a separate scan line), so assert it still uses
            // the covering index and returns the same rows as the constant-bound oracle.
            String monoSub = "select ts, value from sensor where series = 's1' " +
                    "and dateadd('h', 1, ts) >= (select lo from bounds where sel = 100)";
            String monoConst = "select ts, value from sensor where series = 's1' " +
                    "and dateadd('h', 1, ts) >= '2021-11-23T12:51:23.700716000Z'";
            assertPlanContains(monoSub, "CoveringIndex on: series");
            assertSqlCursors(monoConst, monoSub);
        });
    }

    // A strict '>' monotonic bound at the timestamp domain ceiling (dateadd(...) > MAX) is
    // unsatisfiable: no value can exceed Long.MAX_VALUE. Before the guard the inverter turned the
    // strict '>' into a closed '>= bound + 1'; at Long.MAX_VALUE that +1 wrapped to Numbers.LONG_NULL
    // (the open-lower sentinel) and opened the runtime interval to the whole storable domain
    // [MIN, MAX], scanning every row before the residual rejected them all (correct result, O(N)
    // scan). The row result is 0 either way, so the runtime-evaluated interval in the plan is what
    // proves the pruning: pre-fix intervals: [("MIN","MAX")], post-fix intervals: []. Covers both
    // TIMESTAMP (micros) and TIMESTAMP_NS (nanos) precision.
    @Test
    public void testStrictGreaterOverflowBoundPrunesToEmptyInterval() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
            execute("CREATE TABLE bmax (ts TIMESTAMP)");
            execute("INSERT INTO bmax VALUES (9223372036854775807L::timestamp)"); // Long.MAX_VALUE
            final String q = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) > (SELECT ts FROM bmax)";
            assertQuery(q)
                    .timestamp("ts")
                    .noCircuitBreakerCheck()
                    .withPlanContaining("Interval forward scan on: t", "intervals: []")
                    .withPlanNotContaining("(\"MIN\"")
                    .returns("ts\tv\n");

            execute("CREATE TABLE tn (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tn VALUES ('2024-01-01T00:00:00.000000000Z', 1), ('2024-01-02T00:00:00.000000000Z', 2)");
            execute("CREATE TABLE bmaxn (ts TIMESTAMP_NS)");
            execute("INSERT INTO bmaxn VALUES (9223372036854775807L::timestamp_ns)"); // Long.MAX_VALUE
            final String qn = "SELECT ts, v FROM tn WHERE dateadd('h', 1, ts) > (SELECT ts FROM bmaxn)";
            assertQuery(qn)
                    .timestamp("ts")
                    .noCircuitBreakerCheck()
                    .withPlanContaining("Interval forward scan on: tn", "intervals: []")
                    .withPlanNotContaining("(\"MIN\"")
                    .returns("ts\tv\n");
        });
    }

    // The symmetric strict '<' monotonic bound at the domain floor (dateadd(...) < MIN+1) must NOT
    // be pruned to an empty interval, unlike the '>' ceiling case above. A forward shift that
    // overflows the long boundary (reachable for uncapped nanos designated timestamps) can push
    // dateadd(ts) below the bound, so a real row could satisfy '< MIN+1'; the chain inversion
    // correctly declines (NONE) and leaves the predicate to the residual filter (a full-domain scan,
    // intervals: [("MIN","MAX")]). This pins that we did not add an unsafe symmetric guard that would
    // silently drop such rows. On a normal table with no wrapping rows the result is 0 rows.
    @Test
    public void testStrictLessUnderflowBoundStaysResidual() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2)");
            execute("CREATE TABLE bmin (ts TIMESTAMP)");
            execute("INSERT INTO bmin VALUES ((-9223372036854775807L)::timestamp)"); // Long.MIN_VALUE + 1
            final String q = "SELECT ts, v FROM t WHERE dateadd('h', 1, ts) < (SELECT ts FROM bmin)";
            assertQuery(q)
                    .timestamp("ts")
                    .noCircuitBreakerCheck()
                    .withPlanContaining("Interval forward scan on: t")
                    .withPlanNotContaining("intervals: []")
                    .returns("ts\tv\n");

            execute("CREATE TABLE tn (ts TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO tn VALUES ('2024-01-01T00:00:00.000000000Z', 1), ('2024-01-02T00:00:00.000000000Z', 2)");
            execute("CREATE TABLE bminn (ts TIMESTAMP_NS)");
            execute("INSERT INTO bminn VALUES ((-9223372036854775807L)::timestamp_ns)"); // Long.MIN_VALUE + 1
            final String qn = "SELECT ts, v FROM tn WHERE dateadd('h', 1, ts) < (SELECT ts FROM bminn)";
            assertQuery(qn)
                    .timestamp("ts")
                    .noCircuitBreakerCheck()
                    .withPlanContaining("Interval forward scan on: tn")
                    .withPlanNotContaining("intervals: []")
                    .returns("ts\tv\n");
        });
    }

    // Two monotonic subquery-bound predicates with different transforms must each invert with
    // their OWN chain. The runtime inverter retains the monotonic chain, so it must be a private
    // copy (as for bind-variable bounds) rather than the shared tempMonotonicChain, otherwise the
    // second predicate's compileMonotonicChain clears/overwrites the first inverter's chain and
    // both prune with the same (wrong) transform, widening the interval.
    @Test
    public void testTwoDistinctMonotonicSubqueryBounds() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // dateadd('h',5,ts) >= lo -> ts >= lo-5h (07:51:23.700716)
            // dateadd('h',1,ts) >= lo -> ts >= lo-1h (11:51:23.700716)
            // intersection lower bound is the tighter one, lo-1h.
            String q = "select ts, value from sensor where " +
                    "dateadd('h', 5, ts) >= (select lo from bounds where sel = 100) " +
                    "and dateadd('h', 1, ts) >= (select lo from bounds where sel = 100)";
            assertPlanContains(q, "2021-11-23T11:51:23.700716000Z");
            String qConst = "select ts, value from sensor where " +
                    "dateadd('h', 5, ts) >= '2021-11-23T12:51:23.700716000Z' " +
                    "and dateadd('h', 1, ts) >= '2021-11-23T12:51:23.700716000Z'";
            assertSqlCursors(qConst, q);
        });
    }

    // OR of timestamp-equality subquery bounds (ts = (sub) OR ts = (sub)) is interval-optimized as
    // a runtime UNION: each disjunct unions its point into the model, and a NULL/empty subquery
    // bound is the empty-set identity under UNION (it contributes nothing rather than collapsing
    // the whole set). This test is the correctness safety net: it pins the SAME rows in every
    // ordering - both non-empty, first empty, second empty, and both empty - so the optimization
    // returns exactly the residual-scan rows. The companion plan test
    // (testOrOfTimestampEqualsSubqueryBoundsUsesIntervalUnion) is the red proof of the plan change.
    @Test
    public void testOrOfTimestampEqualsSubqueryBoundsStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String orSub = "select ts, value from sensor where " +
                    "ts = (select lo from bounds where sel = 100) or ts = (select hi from bounds where sel = 100)";
            String orConst = "select ts, value from sensor where " +
                    "ts = '2021-11-23T12:51:23.700716000Z' or ts = '2021-11-23T17:59:17.060338000Z'";
            // both non-empty: rows must match the constant-bounds oracle
            assertSqlCursors(orConst, orSub);

            // reversed subquery order must produce the same rows (union is commutative)
            String orSubReversed = "select ts, value from sensor where " +
                    "ts = (select hi from bounds where sel = 100) or ts = (select lo from bounds where sel = 100)";
            assertSqlCursors(orConst, orSubReversed);

            // first disjunct empty (NULL bound) must NOT drop the second disjunct's row
            String orFirstEmpty = "select ts, value from sensor where " +
                    "ts = (select lo from bounds where sel = 999) or ts = (select hi from bounds where sel = 100)";
            String orFirstEmptyOracle = "select ts, value from sensor where ts = '2021-11-23T17:59:17.060338000Z'";
            assertSqlCursors(orFirstEmptyOracle, orFirstEmpty);

            // second disjunct empty must be symmetric
            String orSecondEmpty = "select ts, value from sensor where " +
                    "ts = (select lo from bounds where sel = 100) or ts = (select hi from bounds where sel = 999)";
            String orSecondEmptyOracle = "select ts, value from sensor where ts = '2021-11-23T12:51:23.700716000Z'";
            assertSqlCursors(orSecondEmptyOracle, orSecondEmpty);

            // both disjuncts empty (both bounds NULL): the union is the empty set, no rows
            String orBothEmpty = "select ts, value from sensor where " +
                    "ts = (select lo from bounds where sel = 999) or ts = (select hi from bounds where sel = 999)";
            String orBothEmptyOracle = "select ts, value from sensor limit 0";
            assertSqlCursors(orBothEmptyOracle, orBothEmpty);
        });
    }

    // Plan red proof: `ts = (sub) OR ts = (sub)` now prunes via a runtime interval UNION instead of
    // a residual full-partition scan. Pre-fix this was a Frame forward scan + Filter (O(N)); post-
    // fix it is an Interval forward scan (O(H)). The correctness safety net above proves the rows
    // are unchanged across every NULL/empty/non-empty ordering.
    @Test
    public void testOrOfTimestampEqualsSubqueryBoundsUsesIntervalUnion() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            String orSub = "select ts, value from sensor where " +
                    "ts = (select lo from bounds where sel = 100) or ts = (select hi from bounds where sel = 100)";
            assertPlanContains(orSub, "Interval forward scan on: sensor");
        });
    }

    // The two AND-orderings must both compile (no AssertionError) and produce identical rows.
    @Test
    public void testOrderingsAgree() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            assertSqlCursors(subqueryBounds, subqueryBoundsSymLast);
        });
    }
}
