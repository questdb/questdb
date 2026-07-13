package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
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

    private void createSchema() throws Exception {
        execute("create table sensor (" +
                "    ts timestamp_ns parquet(delta_binary_packed, lz4_raw)," +
                "    series symbol index type posting include (value, ts) parquet(rle_dictionary, lz4_raw, bloom_filter)," +
                "    value float parquet(plain, lz4_raw)" +
                ") timestamp(ts) partition by hour");
        execute("create table bounds (lo timestamp_ns, hi timestamp_ns, sel int)");
        execute("insert into bounds values (" +
                "cast('2021-11-23T12:51:23.700716000Z' as timestamp_ns), " +
                "cast('2021-11-23T17:59:17.060338000Z' as timestamp_ns), 100)");
    }

    // ~3000 rows across ~8 hourly partitions on 2021-11-23, several symbols incl. 's1',
    // plus boundary rows exactly at lo and hi to exercise inclusive bounds.
    private void seedData() throws Exception {
        execute("insert into sensor " +
                "select (cast('2021-11-23T12:00:00.000000000Z' as timestamp_ns) + (x * 7000000000L))::timestamp_ns, " +
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

    private final String subqueryBounds =
            "select ts, value from sensor where series = 's1' " +
                    "and ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100)";

    private final String subqueryBoundsSymLast =
            "select ts, value from sensor where " +
                    "ts >= (select lo from bounds where sel = 100) " +
                    "and ts <= (select hi from bounds where sel = 100) " +
                    "and series = 's1'";

    private final String constantBounds =
            "select ts, value from sensor where series = 's1' " +
                    "and ts >= '2021-11-23T12:51:23.700716000Z' " +
                    "and ts <= '2021-11-23T17:59:17.060338000Z'";

    private void assertUsesCoveringIndex(String sql) throws Exception {
        sink.clear();
        printSql("explain " + sql);
        String plan = sink.toString();
        Assert.assertTrue("expected covering index in plan but got:\n" + plan,
                plan.contains("CoveringIndex on: series"));
        Assert.assertFalse("unexpected async-filter fallback in plan:\n" + plan,
                plan.contains("Async Filter") || plan.contains("Async JIT Filter"));
    }

    private void assertPlanContains(String sql, String needle) throws Exception {
        sink.clear();
        printSql("explain " + sql);
        String plan = sink.toString();
        Assert.assertTrue("expected plan to contain '" + needle + "' but got:\n" + plan, plan.contains(needle));
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
    public void testBetweenSubqueryBoundsReleasedWhenIntervalAlreadyEmpty() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();

            // The parser visits the rightmost AND terms first, so the contradictory constants
            // empty the interval model before it compiles each BETWEEN boundary combination.
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
    // bound's compiled cursor factory. The query errors (type mismatch), but the retained first
    // boundary function must still be freed on the rollback path. Runs under assertMemoryLeak.
    @Test
    public void testBetweenSubqueryFirstBoundRetainedThenSecondBoundFails() throws Exception {
        assertMemoryLeak(() -> {
            createSchema();
            seedData();
            // second bound subquery returns an INT column, not a timestamp -> does not qualify
            String bad = "select ts, value from sensor where " +
                    "ts between (select lo from bounds where sel = 100) and (select sel from bounds where sel = 100)";
            try {
                execute(bad);
                org.junit.Assert.fail("expected the malformed BETWEEN to error");
            } catch (Exception expected) {
                // expected: the BETWEEN cannot be satisfied with a non-timestamp bound
            }
            // second bound subquery returns TWO columns -> does not qualify
            String bad2 = "select ts, value from sensor where " +
                    "ts between (select lo from bounds where sel = 100) and (select lo, hi from bounds where sel = 100)";
            try {
                execute(bad2);
                org.junit.Assert.fail("expected the malformed BETWEEN to error");
            } catch (Exception expected) {
                // expected
            }
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

    // OR of timestamp-equality subquery bounds (ts = (sub) OR ts = (sub)) is deliberately NOT
    // interval-optimized: the OR interval model anchors the first disjunct with an INTERSECT that
    // collapses the whole set to empty when that bound resolves to NULL (an empty subquery is a
    // natural NULL source), which would silently drop the other disjunct's rows. So it must stay a
    // correct residual full scan and, crucially, return the right rows in every ordering —
    // including when the FIRST disjunct's subquery is empty.
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
