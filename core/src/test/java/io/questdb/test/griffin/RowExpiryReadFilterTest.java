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

import io.questdb.cairo.MetadataCacheWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Verifies the read-time row-expiry filter (approach A: subquery rewrite). When a table carries an
 * EXPIRE ROWS policy, the parser transparently rewrites every reference to it into a nested
 * {@code SELECT * FROM t WHERE NOT(<predicate>)} so expired rows are invisible to ALL reads:
 * plain SELECT, JOIN, sub-query, CTE, and aggregation.
 * <p>
 * {@code now()} is pinned via {@link #setCurrentMicros(long)} so the time-based predicates are
 * deterministic. 2024-01-10T00:00:00Z = 1704844800000000 micros; now()-1d = 2024-01-09T00:00:00Z.
 */
public class RowExpiryReadFilterTest extends AbstractCairoTest {

    // 2024-01-10T00:00:00.000000Z
    private static final long NOW_MICROS = 1704844800000000L;

    @Test
    public void testAggregationOverPolicied() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            // AAA: one expired (2024-01-05) + one live (2024-01-09T12) -> count 1
            // BBB: two live -> count 2
            // CCC: only expired -> absent from grouped result
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('AAA', 2.0, '2024-01-09T12:00:00.000000Z')");
            execute("insert into t values ('BBB', 3.0, '2024-01-09T06:00:00.000000Z')");
            execute("insert into t values ('BBB', 4.0, '2024-01-09T18:00:00.000000Z')");
            execute("insert into t values ('CCC', 5.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();

            assertSql(
                    "sym\tc\n" +
                            "AAA\t1\n" +
                            "BBB\t2\n",
                    "select sym, count() c from t order by sym"
            );
        });
    }

    @Test
    public void testExplicitAliasResolvesAgainstFilteredTable() throws Exception {
        // An explicit alias on a policied table must still resolve (alias.col) and the rows must be
        // filtered. Guards the interaction between expandExpiringTable's alias and setModelAlias*.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            assertSql(
                    "sym\tv\n" +
                            "BBB\t2.0\n",
                    "select a.sym, a.v from t a order by a.sym"
            );
        });
    }

    @Test
    public void testJoin() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            // normal dimension table, no policy
            execute("create table dim (sym symbol, name string)");
            execute("insert into dim values ('AAA', 'alpha')");
            execute("insert into dim values ('BBB', 'bravo')");
            drainWalQueue();

            // Only the live row (BBB) should survive the join; the expired AAA row is invisible.
            assertSql(
                    "sym\tv\tname\n" +
                            "BBB\t2.0\tbravo\n",
                    "select t.sym, t.v, dim.name from t join dim on t.sym = dim.sym order by t.sym"
            );
        });
    }

    @Test
    public void testSelfJoinBothSidesFiltered() throws Exception {
        // Two references to the same policied table in one query. The recursion guard is add/removed
        // around each expansion, so both the FROM side and the JOIN side must be filtered.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            // If either side leaked the expired AAA row, the join would produce extra/incorrect rows.
            assertSql(
                    "sym\tsym1\n" +
                            "BBB\tBBB\n",
                    "select a.sym, b.sym from t a join t b on a.sym = b.sym order by a.sym"
            );
        });
    }

    @Test
    public void testNonTimePredicate() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // v<2 -> expired
            execute("insert into t values ('BBB', 2.0, '2024-01-06T00:00:00.000000Z')"); // v>=2 -> live
            execute("insert into t values ('CCC', 9.0, '2024-01-07T00:00:00.000000Z')"); // v>=2 -> live
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "BBB\t2.0\t2024-01-06T00:00:00.000000Z\n" +
                            "CCC\t9.0\t2024-01-07T00:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n2\n", "select count() from t");
        });
    }

    @Test
    public void testNoPolicyUnaffected() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            // No EXPIRE ROWS clause: every row must be visible (regression guard for the hot path).
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')");
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "AAA\t1.0\t2024-01-05T00:00:00.000000Z\n" +
                            "BBB\t2.0\t2024-01-09T12:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n2\n", "select count() from t");
        });
    }

    @Test
    public void testSubqueryAndCte() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            final String expected = "sym\tv\tts\n" +
                    "BBB\t2.0\t2024-01-09T12:00:00.000000Z\n";

            // inline sub-query
            assertSql(expected, "select * from (select * from t) order by ts");
            // CTE
            assertSql(expected, "with x as (select * from t) select * from x order by ts");
            // nested deeper to exercise recursion through parseSelectFrom repeatedly
            assertSql(expected, "select * from (select * from (select * from t)) order by ts");
        });
    }

    @Test
    public void testBareNowPredicateUsesOptimizedFilter() throws Exception {
        // Exercises SqlParser's optimized NOT(col < now()) -> col >= now() flip (so the planner prunes).
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS); // cutoff is exactly now() = 2024-01-10T00:00:00Z
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-09T23:59:59.999999Z')"); // < now() -> expired
            execute("insert into t values ('BBB', 2.0, '2024-01-10T00:00:00.000000Z')"); // == now() -> live (>=)
            execute("insert into t values ('CCC', 3.0, '2024-01-11T00:00:00.000000Z')"); // > now() -> live
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "BBB\t2.0\t2024-01-10T00:00:00.000000Z\n" +
                            "CCC\t3.0\t2024-01-11T00:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n2\n", "select count() from t");
        });
    }

    @Test
    public void testTimePredicateHidesExpiredRows() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            // expired: strictly older than now()-1d (2024-01-09T00:00:00Z)
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into t values ('BBB', 2.0, '2024-01-08T23:59:59.999999Z')");
            // live: >= now()-1d
            execute("insert into t values ('CCC', 3.0, '2024-01-09T00:00:00.000000Z')");
            execute("insert into t values ('DDD', 4.0, '2024-01-09T12:00:00.000000Z')");
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "CCC\t3.0\t2024-01-09T00:00:00.000000Z\n" +
                            "DDD\t4.0\t2024-01-09T12:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n2\n", "select count() from t");
        });
    }

    @Test
    public void testCompoundPredicateEndingInNow() throws Exception {
        // Regression: a compound predicate that happens to END in "< now()" must NOT be mis-parsed as a
        // bare "<col> < now()". NOT(v < 2.0 AND ts < now()) keeps any row that is not BOTH cheap AND old.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS); // now() = 2024-01-10T00:00:00Z
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0 AND ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // cheap + old    -> expired
            execute("insert into t values ('BBB', 1.0, '2024-01-11T00:00:00.000000Z')"); // cheap + future -> live
            execute("insert into t values ('CCC', 5.0, '2024-01-05T00:00:00.000000Z')"); // dear + old     -> live
            execute("insert into t values ('DDD', 5.0, '2024-01-11T00:00:00.000000Z')"); // dear + future  -> live
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "CCC\t5.0\t2024-01-05T00:00:00.000000Z\n" +
                            "BBB\t1.0\t2024-01-11T00:00:00.000000Z\n" +
                            "DDD\t5.0\t2024-01-11T00:00:00.000000Z\n",
                    "select * from t order by ts, sym"
            );
            assertSql("count\n3\n", "select count() from t");
        });
    }

    @Test
    public void testNullPredicateColumnRows() throws Exception {
        // A row whose predicate column is NULL has NOT expired: a row expires only when the predicate is
        // TRUE, and "v < 2.0" is UNKNOWN (not TRUE) for NULL v, so the row MUST stay visible. The keep
        // filter is CASE WHEN (v < 2.0) THEN false ELSE true END (keeps FALSE and NULL); a plain
        // "WHERE NOT(v < 2.0)" would (wrongly) drop the NULL row — QuestDB filtering is three-valued.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");  // v<2 -> expired
            execute("insert into t values ('BBB', 5.0, '2024-01-06T00:00:00.000000Z')");  // v>=2 -> live
            execute("insert into t values ('CCC', null, '2024-01-07T00:00:00.000000Z')"); // v NULL -> live
            drainWalQueue();

            assertSql("sym\tv\n" + "BBB\t5.0\n" + "CCC\tnull\n", "select sym, v from t order by sym");
        });
    }

    @Test
    public void testInsertAsSelectFromPoliciedReadsLiveOnly() throws Exception {
        // Reading a policied table as the SOURCE of INSERT...SELECT copies only live rows (the read
        // filter applies to the SELECT source); the unrelated destination table is unaffected.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table src (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into src values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into src values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            execute("create table dst (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalQueue();

            execute("insert into dst select * from src");
            drainWalQueue();

            assertSql("sym\tv\n" + "BBB\t2.0\n", "select sym, v from dst order by sym");
        });
    }

    @Test
    public void testCtasWithExpire() throws Exception {
        // CREATE ... AS SELECT captures the EXPIRE predicate and validates it against the SELECT's output
        // columns BEFORE the table (and its copied data) is created; the read filter then hides expired
        // CTAS rows. src carries no policy, so the CTAS copies both rows and only t2's policy filters.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table src (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into src values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired in t2
            execute("insert into src values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live in t2
            drainWalQueue();

            execute("create table t2 as (select * from src) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            drainWalQueue();

            assertSql("sym\tv\n" + "BBB\t2.0\n", "select sym, v from t2 order by sym");
        });
    }

    @Test
    public void testInPredicateAtCreate() throws Exception {
        // Regression: 'col IN (...)' must work as an EXPIRE predicate at CREATE — the bare IN must not be
        // mistaken for the IN VOLUME clause. NOT(sym IN (...)) keeps rows whose sym is not in the list.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN sym IN ('AAA', 'CCC')");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // in list -> expired
            execute("insert into t values ('BBB', 2.0, '2024-01-06T00:00:00.000000Z')"); // not in   -> live
            execute("insert into t values ('CCC', 3.0, '2024-01-07T00:00:00.000000Z')"); // in list -> expired
            drainWalQueue();

            assertSql("sym\tv\n" + "BBB\t2.0\n", "select sym, v from t order by sym");
        });
    }

    @Test
    public void testPerTablePolicyGateAfterFullHydration() throws Exception {
        // Per-table policy gate (perf): after startup hydration completes (fullyHydrated=true), the read
        // filter only takes the cache lock + looks up the predicate for tables in the policied-id set, not
        // for every table once any policy exists. This verifies the production path (tests normally run
        // with fullyHydrated=false, taking the catch-all branch): a table CREATEd or ALTERed to carry a
        // policy AFTER hydration must still be filtered (its id is added before it becomes queryable), and
        // a non-policied table must be unaffected.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            // Force fullyHydrated=true so mayTableHaveExpiryPolicy() consults the id set, not !fullyHydrated.
            engine.getMetadataCache().onStartupAsyncHydrator();

            // (a) Policy at CREATE, after hydration.
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 5.0, '2024-01-06T00:00:00.000000Z')"); // live
            drainWalQueue();
            assertSql("sym\tv\n" + "BBB\t5.0\n", "select sym, v from t order by sym");

            // (b) A non-policied table created after hydration is unaffected (no filter).
            execute("create table plain (v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into plain values (1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into plain values (5.0, '2024-01-06T00:00:00.000000Z')");
            drainWalQueue();
            assertSql("v\n" + "1.0\n" + "5.0\n", "select v from plain order by v");

            // (c) Policy added via ALTER after hydration (markExpiryPolicyPossible adds the id).
            execute("alter table plain set expire rows when v < 2.0");
            drainWalQueue();
            assertSql("v\n" + "5.0\n", "select v from plain order by v");
        });
    }

    @Test
    public void testReadFilterFallsBackToMetadataOnCacheMiss() throws Exception {
        // Cache-miss fallback regression: when the metadata cache has no entry for a (resolvable) policied
        // table — e.g. during the brief async metadata hydration at startup — the read filter must still
        // apply, by falling back to the authoritative table metadata. Simulated by clearing the cache (the
        // monotonic anyExpiryPolicySeen gate stays open, so the lookup still runs and hits the empty cache);
        // without the fallback the filter is silently skipped and expired rows leak.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            // Evict cached metadata so the next lookup misses, mimicking the pre-hydration window.
            try (MetadataCacheWriter w = engine.getMetadataCache().writeLock()) {
                w.clearCache();
            }

            assertSql(
                    "sym\tv\tts\n" +
                            "BBB\t2.0\t2024-01-09T12:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n1\n", "select count() from t");
        });
    }

    @Test
    public void testUpdateDoesNotTouchExpiredRows() throws Exception {
        // The read filter must also apply to UPDATE: a logically-expired row must not be updated. Verified
        // by dropping the policy afterwards (revealing all rows) and checking the expired row is unchanged.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            drainWalQueue();

            execute("update t set v = 9.0");
            drainWalQueue();

            assertSql("sym\tv\n" + "BBB\t9.0\n", "select sym, v from t order by sym");

            // Reveal all rows: the expired AAA row must be UNCHANGED (the UPDATE skipped it).
            execute("alter table t drop expire");
            drainWalQueue();
            assertSql(
                    "sym\tv\n" +
                            "AAA\t1.0\n" +
                            "BBB\t9.0\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testUpdateWithWhereOnPolicied() throws Exception {
        // The user WHERE is ANDed with the keep-filter: only live rows matching the WHERE are updated.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN ts < dateadd('d', -1, now())");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // expired
            execute("insert into t values ('BBB', 2.0, '2024-01-09T12:00:00.000000Z')"); // live
            execute("insert into t values ('CCC', 3.0, '2024-01-09T18:00:00.000000Z')"); // live
            drainWalQueue();

            execute("update t set v = 9.0 where sym = 'BBB'");
            drainWalQueue();
            assertSql(
                    "sym\tv\n" +
                            "BBB\t9.0\n" +
                            "CCC\t3.0\n",
                    "select sym, v from t order by sym"
            );
        });
    }

    @Test
    public void testCompoundOrPredicate() throws Exception {
        // NOT(v < 2.0 OR ts < now()) == v >= 2.0 AND ts >= now(): only the dear AND future row survives.
        assertMemoryLeak(() -> {
            setCurrentMicros(NOW_MICROS);
            execute("create table t (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                    "EXPIRE ROWS WHEN v < 2.0 OR ts < now()");
            execute("insert into t values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // cheap & old   -> expired
            execute("insert into t values ('BBB', 1.0, '2024-01-11T00:00:00.000000Z')"); // cheap         -> expired
            execute("insert into t values ('CCC', 5.0, '2024-01-05T00:00:00.000000Z')"); // old           -> expired
            execute("insert into t values ('DDD', 5.0, '2024-01-11T00:00:00.000000Z')"); // dear & future -> live
            drainWalQueue();

            assertSql(
                    "sym\tv\tts\n" +
                            "DDD\t5.0\t2024-01-11T00:00:00.000000Z\n",
                    "select * from t order by ts"
            );
            assertSql("count\n1\n", "select count() from t");
        });
    }
}
