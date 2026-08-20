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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the window-based EXPIRE ROWS retention modes on PASSTHROUGH materialized views:
 * <ul>
 *     <li>{@code KEEP HIGHEST|LOWEST <col> [PARTITION BY <cols>]} — keep the group max/min (all ties);</li>
 *     <li>{@code KEEP <N> HIGHEST|LOWEST <col> [PARTITION BY <cols>]} — keep the top-N by a column;</li>
 *     <li>{@code WHEN <window predicate>} — an arbitrary window-function predicate (the escape hatch).</li>
 * </ul>
 * All desugar to / are a window predicate behind the projection-CASE read filter. Physical cleanup is
 * verified separately. Shared data (passthrough view over base):
 * <pre>
 *   A: 1.0@d1, 3.0@d2, 2.0@d3   (max=3.0, min=1.0)
 *   B: 5.0@d1, 5.0@d2, 4.0@d3   (max=5.0 TIE, min=4.0)
 *   C: null@d1, null@d2         (all-NULL group)
 *   D: 7.0@d1                   (single row; global max)
 * </pre>
 */
public class RowExpiryWindowTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testExpiryWindowCompositeKeyRequiresEveryKey() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS KEEP HIGHEST v PARTITION BY k, region");

            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A' AND region = 'X'").noLeakCheck().returns("""
                    k\tregion\tv
                    A\tX\t9.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A' AND region = 'X'")
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: k");

            // A predicate on only one component leaves the composite partition identity open. The barrier
            // keeps the whole-view window plan rather than applying the partial key constraint below it.
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowPartitionKeyAliasAndBindPushDown() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS KEEP HIGHEST v PARTITION BY k");
            sqlExecutionContext.getBindVariableService().clear();
            sqlExecutionContext.getBindVariableService().setStr(0, "A");

            assertQuery("SELECT x.k, x.region, x.v FROM mv x WHERE x.k = $1 ORDER BY region").noLeakCheck().returns("""
                    k\tregion\tv
                    A\tX\t9.0
                    """);
            assertQuery("SELECT x.k, x.region, x.v FROM mv x WHERE x.k = $1")
                    .noLeakCheck()
                    .withContext(sqlExecutionContext)
                    .assertsPlanContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowPartitionKeyPushDownPreservesResults() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS KEEP HIGHEST v PARTITION BY k");

            final String expected = "k\tregion\tv\n" +
                    "A\tX\t9.0\n";
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A' ORDER BY region").noLeakCheck().returns(expected);
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A'")
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: k");

            // A non-key predicate must remain outside the window. Pushing v > 5 below KEEP HIGHEST would
            // change ranks for other policies and is never part of the cloned partition constraint.
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A' AND v > 5 ORDER BY region").noLeakCheck().returns(expected);
        });
    }

    @Test
    public void testExpiryWindowRawPolicyUsesOnlyCommonSemanticKeys() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k)");
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A' ORDER BY region").noLeakCheck().returns("""
                    k\tregion\tv
                    A\tX\t9.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A'")
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: k");

            execute("DROP MATERIALIZED VIEW mv");
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base), INDEX (k)
                    EXPIRE ROWS WHEN v < max(v) OVER (PARTITION BY k)
                    OR v < max(v) OVER (PARTITION BY region)""");
            drainWalAndMatViewQueues();
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowNonKeyEqualityShapesStayOutsideWindow() throws Exception {
        // Only an equality between a semantic partition key and a constant or bind variable is cloned below
        // the window. Every other shape has to stay outside it, and an OR is the one that shows why: the
        // window would see a subset that no longer holds key A's real maximum, and the query would report
        // A's second-highest row as a survivor.
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS KEEP HIGHEST v PARTITION BY k");
            // Kept rows: (A, X, 9.0) and (B, X, 7.0).
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'B' OR v < 5 ORDER BY k").noLeakCheck().returns("""
                    k	region	v
                    B	X	7.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k = 'B' OR v < 5")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");

            // An IN list is not an '=' node, so it stays outside even though it names the key.
            assertQuery("SELECT k, region, v FROM mv WHERE k IN ('A')").noLeakCheck().returns("""
                    k	region	v
                    A	X	9.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k IN ('A')")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");

            // A range leaves the partition identity open.
            assertQuery("SELECT k, region, v FROM mv WHERE k > 'A'").noLeakCheck().returns("""
                    k	region	v
                    B	X	7.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k > 'A'")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowColumnToColumnPredicateStaysOutsideWindow() throws Exception {
        // A column-to-column equality names the key but constrains it to another row value, not to a fixed
        // partition. Pushed below the window it would hand key A only its k = region row, whose v is not
        // A's maximum, and that row would surface as a survivor.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base (k SYMBOL INDEX, region SYMBOL, v DOUBLE, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 'A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'X', 9.0, '2024-01-02T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base), INDEX (k)
                    EXPIRE ROWS KEEP HIGHEST v PARTITION BY k""");
            drainWalAndMatViewQueues();

            // A's kept row is (A, X, 9.0), so nothing satisfies k = region.
            assertQuery("SELECT k, region, v FROM mv").noLeakCheck().returns("""
                    k	region	v
                    A	X	9.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k = region").noLeakCheck().returns("k\tregion\tv\n");
            assertQuery("SELECT k, region, v FROM mv WHERE k = region")
                    .noLeakCheck()
                    .assertsPlanNotContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowNullKeyEqualityPushesDown() throws Exception {
        // "k = null" is an equality to a constant, and the null-key rows are a partition of their own, so
        // cloning it below the window cannot change any other key's winner. It pushes down like any other
        // key equality, and the kept row stays the null key's maximum.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base (k SYMBOL INDEX, region SYMBOL, v DOUBLE, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO base VALUES
                    (null, 'X', 5.0, '2024-01-01T00:00:00.000000Z'),
                    (null, 'X', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('A', 'X', 9.0, '2024-01-01T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base), INDEX (k)
                    EXPIRE ROWS KEEP HIGHEST v PARTITION BY k""");
            drainWalAndMatViewQueues();

            assertQuery("SELECT k, region, v FROM mv ORDER BY v").noLeakCheck().returns("""
                    k	region	v
                    	X	5.0
                    A	X	9.0
                    """);
            assertQuery("SELECT k, region, v FROM mv WHERE k = null").noLeakCheck().returns("""
                    k	region	v
                    	X	5.0
                    """);
            // The clone lands below CachedWindowLight, where it becomes an index seek on the null key.
            assertQuery("SELECT k, region, v FROM mv WHERE k = null")
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: k");
        });
    }

    @Test
    public void testExpiryWindowRepeatedReferencesPushDownIndependently() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedView("EXPIRE ROWS KEEP HIGHEST v PARTITION BY k");

            assertQuery("SELECT k FROM mv WHERE k = 'A' UNION ALL SELECT k FROM mv WHERE k = 'B'").noRandomAccess().noLeakCheck().returns("""
                    k
                    A
                    B
                    """);
            sink.clear();
            printSql("EXPLAIN SELECT k FROM mv WHERE k = 'A' " +
                    "UNION ALL SELECT k FROM mv WHERE k = 'B'", sink);
            Assert.assertEquals(2, countOccurrences(sink.toString(), "Index forward scan on: k"));
        });
    }

    @Test
    public void testLatestOnOverExpiringViewUsesIndexedFastPath() throws Exception {
        // End-to-end: LATEST ON over a passthrough EXPIRE ROWS view reads through the injected
        // retention keep-filter using the SYMBOL index the view inherited from its base (indexed
        // latest-by), NOT a LatestBy light full scan - and returns the correct latest row per key within
        // the retained window.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol index, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('A',1.0,'2024-01-01T00:00:00.000000Z'),"
                    + "('A',2.0,'2024-01-03T00:00:00.000000Z'),"
                    + "('B',3.0,'2024-01-01T00:00:00.000000Z'),"
                    + "('B',4.0,'2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z' cleanup every 1h");
            drainWalAndMatViewQueues();
            assertQuery("select k, v from mv latest on ts partition by k")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                LatestByDeferredListValuesFiltered
                                    Interval backward scan on: mv
                                      intervals: [("2024-01-02T00:00:00.000000Z","MAX")]
                            """);
            // correct latest row per key within the retained window (rows before 2024-01-02 expired)
            assertQuery("select k, v from mv latest on ts partition by k order by k")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tv\n"
                            + "A\t2.0\n"
                            + "B\t4.0\n");
        });
    }

    @Test
    public void testKeepHighestAllTiesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v partition by k");
            // Keep every row tied at the per-key max; NULL-group rows survive (v<max is UNKNOWN, kept).
            assertQuery("select k, v, ts from mv order by k, ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    B\t5.0\t2024-01-02T00:00:00.000000Z
                    C\tnull\t2024-01-01T00:00:00.000000Z
                    C\tnull\t2024-01-02T00:00:00.000000Z
                    D\t7.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLowestPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep lowest v partition by k");
            assertQuery("select k, v, ts from mv order by k, ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t1.0\t2024-01-01T00:00:00.000000Z
                    B\t4.0\t2024-01-03T00:00:00.000000Z
                    C\tnull\t2024-01-01T00:00:00.000000Z
                    C\tnull\t2024-01-02T00:00:00.000000Z
                    D\t7.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepTopNPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep 2 highest v partition by k");
            // top-2 by v desc, with the designated timestamp as the deterministic tiebreak.
            assertQuery("select k, v, ts from mv order by k, v desc, ts desc").noLeakCheck().returns("""
                    k\tv\tts
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    A\t2.0\t2024-01-03T00:00:00.000000Z
                    B\t5.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    C\tnull\t2024-01-02T00:00:00.000000Z
                    C\tnull\t2024-01-01T00:00:00.000000Z
                    D\t7.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepTopNNullsSortFirstWithinN() throws Exception {
        // Documents top-N NULL handling for a FLOATING-POINT column (v is DOUBLE): QuestDB has no NULLS LAST
        // and sorts a float NULL (NaN) FIRST under DESC, so under KEEP <N> HIGHEST the NULL takes a leading
        // rank and is kept while within N, ahead of real values. Here N=2 with one NULL: NULL=rank1,
        // 9.0=rank2, so 8.0 and 7.0 expire. (An integer/timestamp NULL sorts LAST under DESC and would be
        // expired first instead -- the position is type-dependent; use KEEP HIGHEST without N to keep all NULLs.)
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 9.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 8.0, '2024-01-02T00:00:00.000000Z'),
                    ('A', 7.0, '2024-01-03T00:00:00.000000Z'),
                    ('A', null, '2024-01-04T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            assertQuery("select k, v from mv order by v").noLeakCheck().returns("""
                    k\tv
                    A\t9.0
                    A\tnull
                    """);
        });
    }

    @Test
    public void testKeepTopNIntegerNullSortsLastExpiredFirst() throws Exception {
        // Type-dependent NULL placement under KEEP <N> HIGHEST. RowExpiryUtil's javadoc: QuestDB has no NULLS
        // LAST and where a NULL sorts is TYPE-DEPENDENT -- an INTEGER NULL (a MIN sentinel) sorts LAST under
        // DESC, so it takes a TRAILING rank and is EXPIRED first (unlike a DOUBLE NaN which sorts FIRST and is
        // kept -- see testKeepTopNNullsSortFirstWithinN). N=2 over an INT column with one NULL: 9=rank1,
        // 8=rank2, 7=rank3, NULL=rank4(last). Kept = {9, 8}; the NULL and 7 expire. Pinned with explicit rows.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, n int, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 9, '2024-01-01T00:00:00.000000Z'),
                    ('A', 8, '2024-01-02T00:00:00.000000Z'),
                    ('A', 7, '2024-01-03T00:00:00.000000Z'),
                    ('A', null, '2024-01-04T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest n partition by k");
            drainWalAndMatViewQueues();
            // ORDER BY n -- with NULLs sorting per QuestDB convention; only the two real top values survive.
            assertQuery("select k, n from mv order by n").noLeakCheck().returns("""
                    k\tn
                    A\t8
                    A\t9
                    """);
            // The NULL row is NOT visible -- integer NULL expired first under KEEP N (opposite of DOUBLE).
            assertQuery("select count() c from mv where n is null").noRandomAccess().expectSize().noLeakCheck().returns("c\n0\n");
        });
    }

    @Test
    public void testKeepTopNTimestampNullSortsLastExpiredFirst() throws Exception {
        // Same type-dependent NULL placement for a TIMESTAMP value column: a TIMESTAMP NULL (also a MIN
        // sentinel) sorts LAST under DESC and is EXPIRED first under KEEP <N> HIGHEST. N=2 over a TIMESTAMP
        // column 'w' with one NULL: the two largest non-null w survive, the NULL expires. Pinned explicitly.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, w timestamp, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', '2024-03-03T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z'),
                    ('A', '2024-02-02T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z'),
                    ('A', '2024-01-01T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z'),
                    ('A', null, '2024-01-04T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest w partition by k");
            drainWalAndMatViewQueues();
            assertQuery("select k, w from mv order by w").timestamp("w").noLeakCheck().returns("""
                    k\tw
                    A\t2024-02-02T00:00:00.000000Z
                    A\t2024-03-03T00:00:00.000000Z
                    """);
            // The NULL-w row is NOT visible -- timestamp NULL expired first under KEEP N.
            assertQuery("select count() c from mv where w is null").noRandomAccess().expectSize().noLeakCheck().returns("c\n0\n");
        });
    }

    @Test
    public void testKeepHighestNoPartition() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v");
            // Global max is 7.0 (D). NULL rows (C) survive (v<max is UNKNOWN). Everything else expires.
            assertQuery("select k, v from mv order by k, ts").noLeakCheck().returns("""
                    k\tv
                    C\tnull
                    C\tnull
                    D\t7.0
                    """);
        });
    }

    @Test
    public void testRawWindowWhen() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows when v < max(v) over (partition by k)");
            // Equivalent to KEEP HIGHEST v PARTITION BY k.
            assertQuery("select k, v, ts from mv order by k, ts").noLeakCheck().returns("""
                    k\tv\tts
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    B\t5.0\t2024-01-02T00:00:00.000000Z
                    C\tnull\t2024-01-01T00:00:00.000000Z
                    C\tnull\t2024-01-02T00:00:00.000000Z
                    D\t7.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testComposesWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v partition by k");
            // The outer predicate filters the already-kept (per-key max) rows.
            assertQuery("select k, v from mv where v > 3 order by k, ts").noLeakCheck().returns("""
                    k\tv
                    B\t5.0
                    B\t5.0
                    D\t7.0
                    """);
        });
    }

    @Test
    public void testSetViaAlter() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n9\n");
            execute("alter materialized view mv set expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n7\n"); // 9 rows -> top-2 per key keeps 7 (A2,B2,C2,D1)
        });
    }

    @Test
    public void testShowCreateRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP 2 HIGHEST v PARTITION BY k");

            execute("create materialized view mv2 as (select * from base) expire rows when v < max(v) over (partition by k)");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv2", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS WHEN v < max(v) over (partition by k)");
        });
    }

    @Test
    public void testReadAndCleanupEscapeTableName() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base (k SYMBOL, v DOUBLE, extra INT, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, 10, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, 20, '2024-01-01T01:00:00.000000Z'),
                    ('A', 3.0, 30, '2024-01-03T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW "m v" AS (SELECT * FROM base)
                    EXPIRE ROWS KEEP HIGHEST v PARTITION BY k""");
            drainWalAndMatViewQueues();

            final String expected = "k\tv\textra\tts\n" +
                    "A\t3.0\t30\t2024-01-03T00:00:00.000000Z\n" +
                    "B\t2.0\t20\t2024-01-01T01:00:00.000000Z\n";
            assertQuery("SELECT * FROM \"m v\" ORDER BY k").noLeakCheck().returns(expected);

            final TableToken token = engine.verifyTableName("m v");
            final String predicate;
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                predicate = metadata.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT * FROM \"m v\" ORDER BY k").noLeakCheck().returns(expected);
            assertQuery("SELECT count() p FROM table_partitions('m v')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    @Test
    public void testShowCreateQuotesKeepColumnNeedingQuoting() throws Exception {
        // A keep column whose name needs quoting (a space) must be re-quoted by SHOW CREATE so the rendered
        // DDL round-trips. (The parser unquote()s the stored column, so the renderer must add the quotes back.)
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, \"my val\" double, ts timestamp) timestamp(ts) partition by day wal");
            // The CREATE accepts the quoted keep column -> proves the parse side of the round-trip.
            execute("create materialized view mv as (select * from base) expire rows keep highest \"my val\" partition by k");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv", sink);
            // The render side must emit it quoted (unquoted "my val" would not re-parse).
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP HIGHEST \"my val\" PARTITION BY k");
        });
    }

    @Test
    public void testCatalogueRendersClause() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k cleanup every 30m");
            drainWalAndMatViewQueues();
            assertQuery("select expire_clause, expire_cleanup_every from tables() where table_name = 'mv'").noRandomAccess().noLeakCheck().returns("""
                    expire_clause\texpire_cleanup_every
                    KEEP HIGHEST v PARTITION BY k\t30m
                    """);
        });
    }

    @Test
    public void testRejectedOnBaseTable() throws Exception {
        assertException(
                "create table t (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal expire rows keep highest v partition by k",
                85,
                "EXPIRE ROWS is only supported on materialized views"
        );
    }

    @Test
    public void testRejectedOnUnpartitionedTable() throws Exception {
        // EXPIRE on an un-partitioned CREATE TABLE must give the SPECIFIC message. The rejection is raised
        // before the PARTITION BY block, so this case does not fall through to a generic "unexpected token".
        assertException(
                "create table t (a int, ts timestamp) timestamp(ts) expire rows when a < 2",
                51,
                "EXPIRE ROWS is only supported on materialized views"
        );
    }

    @Test
    public void testRejectedOnCtas() throws Exception {
        // Same for CREATE TABLE ... AS SELECT (the PARTITION BY block is likewise skipped here).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create table cp as (select * from base) expire rows when v < 2.0",
                    40,
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testAllowedOnAggregatingView() throws Exception {
        // EXPIRE ROWS on an aggregating (SAMPLE BY) view is allowed but advisory: physical reclamation is
        // best-effort (a later refresh can regenerate reclaimed rows), reads stay correct regardless.
        assertMemoryLeak(() -> {
            createBase();
            execute(
                    "create materialized view mvagg as (select k, last(v) v, ts from base sample by 1d) " +
                            "partition by day expire rows keep highest v partition by k"
            );
            drainWalAndMatViewQueues();
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("mvagg"))) {
                Assert.assertNotNull(m.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testRejectedForUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            assertExceptionNoLeakCheck(
                    "create materialized view mvbad as (select * from base) expire rows keep highest nope partition by k",
                    25,
                    "invalid EXPIRE ROWS KEEP column: nope"
            );
        });
    }

    @Test
    public void testRejectedZeroRowCount() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            assertExceptionNoLeakCheck(
                    "create materialized view mvbad as (select * from base) expire rows keep 0 highest v partition by k",
                    72,
                    "positive row count"
            );
        });
    }

    @Test
    public void testKeepExtremeAcceptsEveryOrderableTypeUnderTopN() throws Exception {
        // The top-N form ranks the column with ORDER BY instead of taking its group extreme, so it accepts
        // every comparable type -- including the text-ish ones the bare form has to reject.
        assertMemoryLeak(() -> {
            createTypedBase();
            for (String col : new String[]{"s", "vc", "sy", "c", "b", "u", "ip", "g"}) {
                final String view = "mv_topn_" + col;
                execute("CREATE MATERIALIZED VIEW " + view + " AS (SELECT * FROM typed) EXPIRE ROWS KEEP 2 HIGHEST " + col + " PARTITION BY k");
                drainWalAndMatViewQueues();
                // Three rows under one key, all values distinct: the top two by the column survive.
                assertQuery("SELECT count() FROM " + view).noRandomAccess().expectSize().noLeakCheck().returns("count\n2\n");
            }
        });
    }

    @Test
    public void testKeepExtremeAcceptsNumericTemporalAndDecimalColumns() throws Exception {
        // The types max()/min() genuinely support. Each view must both define AND read: a keep column whose
        // extreme needs an implicit parsing cast would define fine and throw on every read.
        assertMemoryLeak(() -> {
            createTypedBase();
            for (String col : new String[]{"by", "sh", "i", "l", "f", "d", "dt", "l256", "dec", "ts"}) {
                final String view = "mv_ext_" + col;
                execute("CREATE MATERIALIZED VIEW " + view + " AS (SELECT * FROM typed) EXPIRE ROWS KEEP HIGHEST " + col + " PARTITION BY k");
                drainWalAndMatViewQueues();
                // k1 holds the three rows and every column rises with the timestamp, so only the last one
                // sits at the per-key maximum and survives.
                assertQuery("SELECT s FROM " + view).noLeakCheck().returns("s\nc\n");
            }
        });
    }

    @Test
    public void testKeepExtremeRejectsNonNumericColumn() throws Exception {
        // The bare KEEP HIGHEST/LOWEST form desugars to "<col> < max(<col>) OVER (...)". max() takes LONG,
        // DOUBLE, DATE, TIMESTAMP, LONG256 or DECIMAL, so a text-ish column would only reach it through an
        // implicit parsing cast -- accepted at DDL, then thrown on every single read of the view. Reject the
        // policy up front, and say which form does handle the column.
        assertMemoryLeak(() -> {
            createTypedBase();
            assertKeepExtremeRejected("s", "STRING");
            assertKeepExtremeRejected("vc", "VARCHAR");
            assertKeepExtremeRejected("sy", "SYMBOL");
            assertKeepExtremeRejected("c", "CHAR");
            assertKeepExtremeRejected("b", "BOOLEAN");
            assertKeepExtremeRejected("u", "UUID");
            assertKeepExtremeRejected("ip", "IPv4");
            assertKeepExtremeRejected("g", "GEOHASH(8c)");
            assertKeepExtremeRejected("bin", "BINARY");
        });
    }

    @Test
    public void testKeepExtremeRejectsNonNumericColumnOnAlter() throws Exception {
        // Same check on the ALTER path: an existing readable view must not be turned into an unreadable one.
        assertMemoryLeak(() -> {
            createTypedBase();
            execute("CREATE MATERIALIZED VIEW mvalt AS (SELECT * FROM typed) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW mvalt SET EXPIRE ROWS KEEP HIGHEST s PARTITION BY k",
                    46,
                    "EXPIRE ROWS KEEP HIGHEST/LOWEST requires a numeric, temporal or decimal column, but 's' is STRING"
            );
            // The view kept no policy and still reads.
            assertQuery("SELECT count() FROM mvalt").noRandomAccess().expectSize().noLeakCheck().returns("count\n3\n");
        });
    }

    @Test
    public void testKeepExtremeRejectsNonOrderableColumnUnderTopN() throws Exception {
        // BINARY cannot be ordered either, so the top-N form rejects it too, with its own message.
        assertMemoryLeak(() -> {
            createTypedBase();
            assertExceptionNoLeakCheck(
                    "CREATE MATERIALIZED VIEW mvbad AS (SELECT * FROM typed) EXPIRE ROWS KEEP 2 HIGHEST bin PARTITION BY k",
                    25,
                    "EXPIRE ROWS KEEP <N> HIGHEST/LOWEST requires an orderable column, but 'bin' is BINARY"
            );
            Assert.assertNull(engine.getTableTokenIfExists("mvbad"));
        });
    }

    @Test
    public void testKeepInvalidModeTokenRejected() throws Exception {
        // The token after KEEP must be 'latest', 'highest', 'lowest' or a row count; anything else fails.
        assertMemoryLeak(() -> {
            createBase();
            assertExceptionNoLeakCheck(
                    "create materialized view mvbad as (select * from base) expire rows keep bogus v partition by k",
                    72,
                    "'latest', 'highest', 'lowest' or a row count expected"
            );
        });
    }

    @Test
    public void testKeepNWithoutHighestLowestRejected() throws Exception {
        // KEEP <N> must be followed by 'highest' or 'lowest'.
        assertMemoryLeak(() -> {
            createBase();
            assertExceptionNoLeakCheck(
                    "create materialized view mvbad as (select * from base) expire rows keep 3 bogus v partition by k",
                    74,
                    "'highest' or 'lowest' expected"
            );
        });
    }

    @Test
    public void testKeepUnknownColumnRejectedOnAlter() throws Exception {
        // The keep column resolves against the view's metadata before the policy is stored, so it names the
        // column whatever the mode. The create-path counterpart is testRejectedForUnknownColumn.
        assertMemoryLeak(() -> {
            createBase();
            execute("CREATE MATERIALIZED VIEW mvalt AS (SELECT * FROM base) PARTITION BY DAY");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "ALTER MATERIALIZED VIEW mvalt SET EXPIRE ROWS KEEP 2 LOWEST nope PARTITION BY k",
                    46,
                    "invalid EXPIRE ROWS KEEP column: nope"
            );
        });
    }

    @Test
    public void testRejectedEmptyPartitionBy() throws Exception {
        // KEEP HIGHEST/LOWEST with a PARTITION BY keyword but no column list must be rejected, not silently
        // treated as a global (un-partitioned) window (which would change the retention semantics).
        assertMemoryLeak(() -> {
            createBase();
            assertExceptionNoLeakCheck(
                    "create materialized view mvbad as (select * from base) expire rows keep highest v partition by cleanup every 1h",
                    95,
                    "requires a column list"
            );
        });
    }

    @Test
    public void testCleanupDeferredForKeepHighest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // B max -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A max=9)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select k, v, ts from mv order by k").noLeakCheck().returns("""
                    k\tv\tts
                    A\t9.0\t2024-01-03T00:00:00.000000Z
                    B\t8.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCleanupDeferredForKeepLowest() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A min=1)
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +   // B min -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A min=1)
                    "('A', 1.0, '2024-01-03T00:00:00.000000Z')");    // A min (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep lowest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            runCleanup();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select k, v, ts from mv order by k").noLeakCheck().returns("""
                    k\tv\tts
                    A\t1.0\t2024-01-03T00:00:00.000000Z
                    B\t2.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCleanupDeferredForTopN() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // rank2 -> survives in d1
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +   // rank3 -> expired (d1 partial)
                    "('A', 4.0, '2024-01-02T00:00:00.000000Z')," +   // rank4 -> expired (d2 wiped)
                    "('A', 10.0, '2024-01-03T00:00:00.000000Z')");   // rank1 (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            runCleanup();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select k, v, ts from mv order by v desc").noLeakCheck().returns("""
                    k\tv\tts
                    A\t10.0\t2024-01-03T00:00:00.000000Z
                    A\t9.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCleanupRawWindowWhenNonMonotonic() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    (9.0, '2024-01-01T00:00:00.000000Z'),
                    (10.0, '2024-01-02T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("""
                    CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base)
                    EXPIRE ROWS WHEN row_number() OVER (ORDER BY v DESC) = 2""");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("non-monotonic window policy must skip physical cleanup", job.cleanupTable(token, predicate));
            }

            // The current rank-2 row becomes rank 3 after refresh and must become visible again.
            execute("INSERT INTO base VALUES (11.0, '2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT v, ts FROM mv ORDER BY v DESC").noLeakCheck().returns("""
                    v\tts
                    11.0\t2024-01-03T00:00:00.000000Z
                    9.0\t2024-01-01T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCleanupDeferredWithNullValueGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('C', null, '2024-01-01T00:00:00.000000Z')," +  // NULL group -> kept (d1 partial)
                    "('C', null, '2024-01-02T00:00:00.000000Z')," +  // NULL group -> kept (d2 all-kept -> skipped)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            runCleanup();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select k, v, ts from mv order by ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    C\tnull\t2024-01-01T00:00:00.000000Z
                    C\tnull\t2024-01-02T00:00:00.000000Z
                    A\t9.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testCleanupAllSurvivorsNonActivePartitionNotReclaimed() throws Exception {
        // CLEANUP classification edge: a NON-ACTIVE partition in which EVERY row survives (survivors ==
        // rowCount) must NOT be reclaimed -- cleanup must find no work and leave the partition byte-identical.
        // Here every row is tied at the per-key extreme, so KEEP HIGHEST keeps them all; the only expired data
        // lives elsewhere (the active partition is protected, so there is genuinely nothing to reclaim).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    // 01-01 (non-active): A and B each tied at their per-key max -> ALL survive.
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 7.0, '2024-01-01T00:00:00.000000Z')," +
                    // 01-02 active partition (protected).
                    "('C', 1.0, '2024-01-02T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t4\n");
            // Every row is visible (all are per-key maxima or the lone active row).
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n4\n");

            // No expired rows anywhere reclaimable -> cleanup is a NO-OP; partitions unchanged.
            Assert.assertFalse("all-survivors + protected-active partition => no work", runCleanupReturning());
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t4\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n4\n");
        });
    }

    @Test
    public void testCleanupTopNNGreaterThanRowsNotReclaimed() throws Exception {
        // KEEP <N> classification edge: when N >= the group's row count in a NON-ACTIVE partition, every row
        // ranks within N and survives, so that partition must NOT be reclaimed. Here N=5 but key A has only 2
        // rows in the non-active 01-01 partition -> both survive -> nothing to reclaim there; the active
        // partition is protected. Cleanup must report no work and leave all partitions untouched.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // non-active, within N=5
                    "('A', 8.0, '2024-01-01T00:00:00.000000Z')," +   // non-active, within N=5
                    "('A', 7.0, '2024-01-02T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 5 highest v partition by k");
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t3\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n3\n"); // N >= rows -> all visible

            Assert.assertFalse("N >= rows in every non-active partition => no work", runCleanupReturning());
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t3\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n3\n");
        });
    }

    @Test
    public void testCleanupSingleActivePartitionIsNoOp() throws Exception {
        // Edge partition handling: when ALL data lives in the single ACTIVE partition, cleanup never touches it
        // (the active partition is always protected from reclamation), even though the read filter hides the
        // superseded rows. Cleanup must be a no-op and the on-disk rows must remain.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded by A=9 (read-hidden)
                    "('A', 9.0, '2024-01-01T06:00:00.000000Z')," +   // A max
                    "('B', 4.0, '2024-01-01T12:00:00.000000Z')");    // B max -- all in ONE partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            // One physical partition, 3 rows; the read filter shows the two per-key maxima.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n1\t3\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");

            // The only partition is the active one -> protected -> cleanup is a no-op; 3 rows stay on disk.
            Assert.assertFalse("the lone active partition must not be reclaimed", runCleanupReturning());
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n1\t3\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
        });
    }

    // Regression (window-mode designated-timestamp propagation): a window-mode policy (KEEP HIGHEST/LOWEST/
    // top-N or a window WHEN) rewrites the view reference as an explicit projection over an inner
    // "SELECT *, CASE ... __keep" query. That inner projection drops the designated timestamp, so a
    // timestamp-requiring operator over such a view (ASOF/LT/SPLICE JOIN) failed to compile with "TIMESTAMP
    // column is required but not provided". The rewrite now re-asserts timestamp("<ts>") on the sub-query.
    @Test
    public void testWindowPoliciedViewCarriesDesignatedTimestampForJoinsAndSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("""
                    insert into base values
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 3.0, '2024-01-02T00:00:00.000000Z'),
                    ('B', 5.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 2.0, '2024-01-02T00:00:00.000000Z')""");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();
            // kept per key: A -> (3.0 @ 01-02), B -> (5.0 @ 01-01)
            assertQuery("select k, v, ts from mv order by ts").timestamp("ts").noLeakCheck().returns("""
                    k\tv\tts
                    B\t5.0\t2024-01-01T00:00:00.000000Z
                    A\t3.0\t2024-01-02T00:00:00.000000Z
                    """);

            // ASOF JOIN over the window-policied view must compile and read through the keep-filter.
            execute("create table probe (k symbol, p double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into probe values ('A', 100.0, '2024-01-05T00:00:00.000000Z'),('B', 200.0, '2024-01-05T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("select p.k, p.p, mv.v from probe p asof join mv on (k) order by p.k").expectSize().noLeakCheck().returns("""
                    k\tp\tv
                    A\t100.0\t3.0
                    B\t200.0\t5.0
                    """);

            // SAMPLE BY over the same view also needs the designated timestamp.
            assertQuery("select ts, count() c from mv sample by 1d").noRandomAccess().timestamp("ts").noLeakCheck().returns("""
                    ts\tc
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t1
                    """);
        });
    }

    // Runs one cleanup sweep over "mv" and asserts structural cleanup was deferred.
    private void runCleanup() throws Exception {
        final TableToken token = engine.verifyTableName("mv");
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            Assert.assertFalse("structural cleanup must preserve fallback rows", job.cleanupTable(token, predicate));
        }
        drainWalAndMatViewQueues();
    }

    // Runs one cleanup sweep over "mv" and returns whether it reclaimed anything (no assertion).
    private boolean runCleanupReturning() throws Exception {
        final TableToken token = engine.verifyTableName("mv");
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        final boolean worked;
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            worked = job.cleanupTable(token, predicate);
        }
        drainWalAndMatViewQueues();
        return worked;
    }

    private void assertKeepExtremeRejected(String col, String typeName) throws Exception {
        assertExceptionNoLeakCheck(
                "CREATE MATERIALIZED VIEW mvbad AS (SELECT * FROM typed) EXPIRE ROWS KEEP HIGHEST " + col + " PARTITION BY k",
                25,
                "EXPIRE ROWS KEEP HIGHEST/LOWEST requires a numeric, temporal or decimal column, but '" + col
                        + "' is " + typeName + "; use KEEP <N> HIGHEST/LOWEST to rank an orderable column of any type"
        );
        Assert.assertNull(engine.getTableTokenIfExists("mvbad"));
    }

    private void createBase() throws Exception {
        execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
        execute("""
                insert into base values
                ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                ('A', 3.0, '2024-01-02T00:00:00.000000Z'),
                ('A', 2.0, '2024-01-03T00:00:00.000000Z'),
                ('B', 5.0, '2024-01-01T00:00:00.000000Z'),
                ('B', 5.0, '2024-01-02T00:00:00.000000Z'),
                ('B', 4.0, '2024-01-03T00:00:00.000000Z'),
                ('C', null, '2024-01-01T00:00:00.000000Z'),
                ('C', null, '2024-01-02T00:00:00.000000Z'),
                ('D', 7.0, '2024-01-01T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
    }

    /**
     * One row per column type, three rows under a single key so a KEEP HIGHEST policy has something to expire.
     */
    private void createTypedBase() throws Exception {
        execute("""
                CREATE TABLE typed (
                    k SYMBOL, s STRING, vc VARCHAR, sy SYMBOL, c CHAR, b BOOLEAN,
                    by BYTE, sh SHORT, i INT, l LONG, f FLOAT, d DOUBLE, dt DATE,
                    u UUID, ip IPV4, l256 LONG256, dec DECIMAL(10,2), g GEOHASH(8c), bin BINARY,
                    ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY WAL""");
        execute("""
                INSERT INTO typed VALUES
                    ('k1', 'a', 'a', 'sy1', 'a', false, 1, 1, 1, 1, 1.0, 1.0, '2024-01-01',
                     '11111111-1111-1111-1111-111111111111', '1.1.1.1', '0x01'::long256, 1.50::decimal(10,2),
                     #sp052w91, null, '2024-01-01T00:00:00.000000Z'),
                    ('k1', 'b', 'b', 'sy2', 'b', false, 2, 2, 2, 2, 2.0, 2.0, '2024-01-02',
                     '22222222-2222-2222-2222-222222222222', '2.2.2.2', '0x02'::long256, 2.50::decimal(10,2),
                     #sp052w92, null, '2024-01-02T00:00:00.000000Z'),
                    ('k1', 'c', 'c', 'sy3', 'c', true, 3, 3, 3, 3, 3.0, 3.0, '2024-01-03',
                     '33333333-3333-3333-3333-333333333333', '3.3.3.3', '0x03'::long256, 3.50::decimal(10,2),
                     #sp052w93, null, '2024-01-03T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
    }

    private int countOccurrences(String value, String fragment) {
        int count = 0;
        int from = 0;
        while ((from = value.indexOf(fragment, from)) > -1) {
            count++;
            from += fragment.length();
        }
        return count;
    }

    private void createIndexedView(String expireClause) throws Exception {
        execute("""
                CREATE TABLE base (k SYMBOL INDEX, region SYMBOL, v DOUBLE, ts TIMESTAMP)
                TIMESTAMP(ts) PARTITION BY DAY WAL""");
        execute("""
                INSERT INTO base VALUES
                    ('A', 'X', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'X', 9.0, '2024-01-02T00:00:00.000000Z'),
                    ('A', 'Y', 8.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 'Y', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('B', 'X', 7.0, '2024-01-01T00:00:00.000000Z'),
                    ('B', 'X', 3.0, '2024-01-02T00:00:00.000000Z')
                """);
        drainWalAndMatViewQueues();
        execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base), INDEX (k) " + expireClause);
        drainWalAndMatViewQueues();
    }

    private void createViewWith(String expireClause) throws Exception {
        createBase();
        execute("create materialized view mv as (select * from base) " + expireClause);
        drainWalAndMatViewQueues();
    }
}
