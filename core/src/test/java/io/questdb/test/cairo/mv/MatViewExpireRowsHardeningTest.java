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
import io.questdb.cairo.RowExpiryUtil;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.ExpiryValidationResult;
import io.questdb.griffin.SqlCompiler;
import io.questdb.mp.WorkerPool;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Hardening tests for the EXPIRE ROWS feature, covering the level-3-review fixes:
 * <ul>
 *     <li><b>Monotonicity gate</b> — physical cleanup is SKIPPED for a non-monotonic policy (e.g.
 *         {@code ts > now()}, which un-expires rows as time advances), so it can never physically delete a
 *         row a later read would show. The read filter stays authoritative for visibility. Clock-free and
 *         {@code ts < now()}-style (monotonic) policies still reclaim.</li>
 *     <li><b>No policied-view chains</b> — a materialized view cannot be created over a base that carries an
 *         EXPIRE ROWS policy, and {@code SET EXPIRE} is rejected on a view that already has dependents.</li>
 *     <li><b>DROP EXPIRE [ROWS]</b> — both spellings are accepted.</li>
 *     <li><b>Read vs cleanup boundary agreement</b> — for {@code ts < now()} the row exactly at the frozen
 *         {@code now()} boundary is kept by both the read filter and the cleanup classifier.</li>
 * </ul>
 */
public class MatViewExpireRowsHardeningTest extends AbstractCairoTest {

    private static final long JAN_10 = 1_704_844_800_000_000L; // 2024-01-10T00:00:00Z
    private static final long JAN_25 = 1_706_140_800_000_000L; // 2024-01-25T00:00:00Z

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testClockAndRandomThresholdCleanupSkipped() throws Exception {
        assertMemoryLeak(() -> {
            // A pre-epoch clock makes `now() - now()*2` a positive post-epoch threshold. Before the fix,
            // cleanup classified it monotonic and physically dropped the first two partitions.
            setCurrentMicros(-864_000_000_000L); // 1969-12-22, threshold of decreasing transform = 1970-01-11
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " +
                    "(1.0, '1970-01-02T00:00:00.000000Z'), " +
                    "(2.0, '1970-01-05T00:00:00.000000Z'), " +
                    "(3.0, '1970-02-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();

            assertMixedClockCleanupSkipped("mv_direct", "ts < now() + rnd_long()");
            assertMixedClockCleanupSkipped("mv_nested", "ts < dateadd('u', rnd_int(), now())");
            assertMixedClockCleanupSkipped("mv_decreasing", "ts < now() - (now()::long * 2)");
            assertMixedClockCleanupSkipped("mv_nested_decreasing", "ts < (now() - (now()::long * 2)) - 1_000_000L");
        });
    }

    @Test
    public void testCleanupUsesPolicyDeadlinesAndAuthoritativeSnapshot() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (1.0, '2024-01-01T00:00:00.000000Z'), " +
                    "(2.0, '2024-01-02T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " +
                    "EXPIRE ROWS WHEN v < 0 CLEANUP EVERY 1h");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            final TableToken token = engine.verifyTableName("mv");
            Assert.assertTrue(engine.getMetadataCache().mayTableHaveExpiryPolicy(token));
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, job.getPolicyDiscoveryCount());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("unchanged policy version and future deadline avoid discovery",
                        1, job.getPolicyDiscoveryCount());

                execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
                drainWalAndMatViewQueues();
                Assert.assertFalse(engine.getMetadataCache().mayTableHaveExpiryPolicy(token));
                Assert.assertFalse(engine.getMetadataCache().mayHaveExpiryPolicy());
                Assert.assertFalse(job.runNow());
                Assert.assertEquals("policy publication invalidates the deadline immediately",
                        1, job.getPolicyDiscoveryCount());
            }
        });
    }

    @Test
    public void testClockFreeValuePredicateReclaims() throws Exception {
        // Positive control for the monotonicity gate: a CLOCK-FREE value predicate is monotonic (mat-view
        // rows are immutable, so the predicate's per-row value never changes), so cleanup reclaims a
        // wholly-expired non-active partition.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," + // v > 100 -> false -> kept? no: expire WHEN v>100, so v=1 kept
                    "('B', 500.0, '2024-01-02T00:00:00.000000Z')," + // v > 100 -> expired (whole 01-02 partition)
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v > 100");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();

            Assert.assertTrue("clock-free monotonic policy must reclaim", worked);
            // 01-02 wholly expired -> reclaimed; 01-01 (kept v=1) and 01-03 (active) remain.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\nA\t1.0\nC\t3.0\n");
        });
    }

    @Test
    public void testExpiryFilterAcrossCteNestedAndUnion() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z'), " +
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z'), " +
                    "('C', null, '2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 2");
            drainWalAndMatViewQueues();

            assertQuery("WITH x AS (SELECT sym FROM mv) SELECT * FROM x UNION ALL SELECT * FROM x ORDER BY sym")
                    .noLeakCheck().returns("sym\nB\nB\nC\nC\n");
            assertQuery("SELECT sym FROM (SELECT * FROM mv) WHERE sym IN " +
                    "(SELECT sym FROM mv WHERE v IS NULL) ORDER BY sym")
                    .noLeakCheck().returns("sym\nC\n");
            assertQuery("SELECT sym FROM mv WHERE v = 2 UNION ALL SELECT sym FROM mv WHERE v IS NULL ORDER BY sym")
                    .noLeakCheck().returns("sym\nB\nC\n");

        });
    }

    @Test
    public void testExpiryValidationReturnsReusableClassification() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final TableToken token = engine.verifyTableName("x");
            try (
                    TableMetadata metadata = engine.getTableMetadata(token);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final ExpiryValidationResult clockFree = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "v < 0", 0);
                Assert.assertFalse(clockFree.hasClock());
                Assert.assertTrue(clockFree.isDeterministic());
                Assert.assertTrue(clockFree.isMonotonic());
                Assert.assertEquals(1, clockFree.getReferencedColumnIndexes().size());
                Assert.assertEquals(metadata.getColumnIndexQuiet("v"), clockFree.getReferencedColumnIndexes().getQuick(0));

                final ExpiryValidationResult advancingThreshold = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now()", 0);
                Assert.assertTrue(advancingThreshold.hasClock());
                Assert.assertFalse(advancingThreshold.isDeterministic());
                Assert.assertTrue(advancingThreshold.isMonotonic());
                Assert.assertEquals(metadata.getTimestampIndex(), advancingThreshold.getReferencedColumnIndexes().getQuick(0));

                final ExpiryValidationResult reversingThreshold = compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts > now()", 0);
                Assert.assertTrue(reversingThreshold.hasClock());
                Assert.assertFalse(reversingThreshold.isMonotonic());

                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() + rnd_long()", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('u', rnd_int(), now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() - (now()::long * 2)", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < (now() - (now()::long * 2)) - 1_000_000L", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < dateadd('u', 1, now())", 0).isMonotonic());
                Assert.assertFalse(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "ts < now() - 1_000_000L", 0).isMonotonic());
            }

            execute("CREATE TABLE xns (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            try (
                    TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("xns"));
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                Assert.assertTrue(compiler.validateExpiryPredicateOnMetadata(
                        sqlExecutionContext, metadata, "now() > ts", 0).isMonotonic());
                Assert.assertTrue(compiler.isExpiryCleanupMonotonic(
                        sqlExecutionContext, metadata, "xns", "now() > ts", "ts"));
            }
        });
    }

    @Test
    public void testDropExpireRowsAndDropExpireBothWork() throws Exception {
        // DROP EXPIRE and DROP EXPIRE ROWS are both accepted (symmetry with SET EXPIRE ROWS).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertNotNull(expiryPredicate("mv"));

            execute("alter materialized view mv drop expire rows");
            drainWalAndMatViewQueues();
            Assert.assertNull("DROP EXPIRE ROWS must clear the policy", expiryPredicate("mv"));

            // Re-add, then DROP EXPIRE (no ROWS) must also clear it.
            execute("alter materialized view mv set expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            Assert.assertNotNull(expiryPredicate("mv"));
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();
            Assert.assertNull("DROP EXPIRE must clear the policy", expiryPredicate("mv"));
        });
    }

    @Test
    public void testMemoryLimitFailureDefersWithoutWalMutation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base SELECT 'A', x, dateadd('d', x::int, '2024-01-01'::timestamp) FROM long_sequence(4)");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " +
                    "EXPIRE ROWS KEEP HIGHEST v PARTITION BY k CLEANUP EVERY 1s");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate = RowExpiryUtil.encodeKeepBy(1, true, "v", "k");

            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 1L);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck().returns("p\tr\n4\t4\n");

            // A second breach exercises pooled tracker reuse; it must defer again rather than inherit
            // unbalanced accounting or mutate WAL state.
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck().returns("p\tr\n4\t4\n");
        });
    }

    @Test
    public void testMemoryLimitBreachThenRecoveryReclaimsAndBalances() throws Exception {
        // (C12) The breach path must defer WITHOUT mutating the WAL; then, under a sufficient budget, cleanup
        // must SUCCEED and physically reclaim. Success on the same engine after a breach also proves the pooled
        // MAT_VIEW_REFRESH tracker was left balanced -- leaked bytes would re-breach here instead of completing.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // B max -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A max=9)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " +
                    "EXPIRE ROWS KEEP HIGHEST v PARTITION BY k CLEANUP EVERY 1s");
            drainWalAndMatViewQueues();
            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }

            // Breach: a 1-byte budget must DEFER, leaving every partition physically intact.
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 1L);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            // Recovery: a sufficient budget must SUCCEED -- d1 compacted (B survives), d2 wiped, active d3
            // untouched -> 2 partitions / 2 rows. Reaching this reclaimed state after a breach also proves the
            // pooled tracker was left balanced (a leak would re-breach and leave the partitions at 3/4).
            setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 256L * 1024 * 1024);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertTrue(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('mv')")
                    .noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("SELECT k, v FROM mv ORDER BY k").noLeakCheck().returns("k\tv\nA\t9.0\nB\t8.0\n");
        });
    }

    @Test
    public void testNonMonotonicFuturePredicateCleanupSkippedAndRowsSurvive() throws Exception {
        // A non-monotonic policy "ts > now()" expires FUTURE rows; as now() advances past them they un-expire.
        // Cleanup must NOT physically delete them (the gate skips reclamation), so when now() advances they
        // reappear. The read filter stays authoritative throughout.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-05T00:00:00.000000Z')," + // past  -> ts > now() false -> kept
                    "('B', 2.0, '2024-01-15T00:00:00.000000Z')," + // future-> ts > now() true  -> expired (non-active)
                    "('C', 3.0, '2024-01-20T00:00:00.000000Z')");  // future-> expired (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts > now()");
            drainWalAndMatViewQueues();

            // Three logical partitions; read filter shows only the non-expired past row.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\n");

            // Cleanup must be a NO-OP: the policy is non-monotonic, so reclamation is skipped.
            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertFalse("non-monotonic policy must skip physical cleanup", worked);
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\n");

            // Advance the clock past all rows: every row now satisfies ts <= now() -> all kept and VISIBLE.
            // This only holds because cleanup did not delete the future rows while they were expired.
            setCurrentMicros(JAN_25);
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nA\nB\nC\n");
        });
    }

    @Test
    public void testCreateViewOverPoliciedBaseRejected() throws Exception {
        // A materialized view must not derive from a base that carries an EXPIRE ROWS policy: refresh reads
        // the RAW base, so it would copy the base's expired-but-not-yet-reclaimed rows.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view a as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "create materialized view b as (select * from a)",
                    25,
                    "the base carries an EXPIRE ROWS policy"
            );
            Assert.assertNull(engine.getTableTokenIfExists("b"));
        });
    }

    @Test
    public void testSetExpireOnViewWithDependentsRejected() throws Exception {
        // The reverse direction: a view that other materialized views derive from must not GAIN a policy
        // (those dependents would copy its expired rows on refresh).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view a as (select * from base)");
            drainWalAndMatViewQueues();
            execute("create materialized view b as (select * from a)");
            drainWalAndMatViewQueues();

            assertExceptionNoLeakCheck(
                    "alter materialized view a set expire rows when v < 2.0",
                    24,
                    "materialized view(s), which would copy expired rows on refresh"
            );
            Assert.assertNull("policy must not have been set on a", expiryPredicate("a"));
        });
    }

    @Test
    public void testFailedCleanupRetriesOnNextGlobalTick() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("CREATE TABLE base (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base VALUES (1.0, '2024-01-01T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) " +
                    "EXPIRE ROWS WHEN v < 0 CLEANUP EVERY 1h");
            drainWalAndMatViewQueues();
            engine.getMetadataCache().hydrateAllTables();

            final int[] attempts = {0};
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine) {
                @Override
                public boolean cleanupTable(TableToken tableToken, String predicate) {
                    if (attempts[0]++ == 0) {
                        throw new RuntimeException("injected cleanup failure");
                    }
                    return false;
                }
            }) {
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, attempts[0]);
                setCurrentMicros(JAN_10 + 999_999L);
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(1, attempts[0]);
                setCurrentMicros(JAN_10 + 1_000_000L);
                Assert.assertFalse(job.runNow());
                Assert.assertEquals(2, attempts[0]);
            }
        });
    }

    @Test
    public void testOperationalKillSwitchControlsPoolOwnership() throws Exception {
        assertMemoryLeak(() -> {
            try (WorkerPool pool = new WorkerPool(() -> 1)) {
                setProperty(PropertyKey.CAIRO_ROW_EXPIRY_ENABLED, "false");
                Assert.assertFalse(RowExpiryCleanupJob.assignToPool(pool, engine));
                Assert.assertEquals(0, pool.getAssignedJobCount());
                Assert.assertEquals(0, pool.getFreeOnExitJobCount());

                setProperty(PropertyKey.CAIRO_ROW_EXPIRY_ENABLED, "true");
                Assert.assertTrue(RowExpiryCleanupJob.assignToPool(pool, engine));
                Assert.assertEquals(1, pool.getAssignedJobCount());
                Assert.assertEquals(1, pool.getFreeOnExitJobCount());
            }
        });
    }

    @Test
    public void testReadVsCleanupBoundaryAgreementForTsNowPredicate() throws Exception {
        // The read filter flips "ts < now()" to "ts >= now()" for pruning while cleanup classifies via the
        // bounds threshold (now() frozen per sweep). Both must agree at the boundary: a row at EXACTLY now()
        // is KEPT by the read filter (ts < now() is false) and must NOT be deleted by cleanup. The visible
        // set must be identical before and after a cleanup sweep.
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +   // ts < now()  -> expired (non-active, wiped)
                    "('EDGE', 2.0, '2024-01-10T00:00:00.000000Z')," +  // ts == now() -> kept (boundary, non-active)
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");    // ts > now()  -> kept (active)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            final String visibleBefore = "sym\nEDGE\nNEW\n";
            assertQuery("select sym from mv order by sym").noLeakCheck().returns(visibleBefore);
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertTrue("ts < now() is monotonic and must reclaim the wholly-expired old partition", worked);

            // The expired OLD partition is reclaimed; the EDGE row exactly at now() and NEW row survive — the
            // post-cleanup visible set equals the pre-cleanup read-filtered set (no boundary divergence).
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns(visibleBefore);
        });
    }

    @Test
    public void testCleanupDefersWhileRefreshHoldsViewLock() throws Exception {
        // M5 serialization: cleanup and the mat-view refresh job are mutually exclusive per view via the
        // MatViewState lock — so a back-fill can never land between the survivor scan and the REPLACE_RANGE
        // commit. Holding that lock (as an in-progress refresh would) must make cleanup DEFER (no reclamation);
        // releasing it lets the next sweep reclaim normally.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," + // expired (non-active)
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," + // expired (non-active)
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");  // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final io.questdb.cairo.mv.MatViewState state = engine.getMatViewStateStore().getViewState(token);
            Assert.assertNotNull("mat view must have a refresh state", state);

            // Simulate a refresh in progress by holding the per-view lock.
            Assert.assertTrue(state.tryLock());
            try {
                Assert.assertFalse("cleanup must defer while the view lock is held", runCleanup("mv"));
            } finally {
                state.unlock();
            }
            // Nothing reclaimed while deferred.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            // Lock released: the next sweep reclaims the wholly-expired old partition.
            Assert.assertTrue("cleanup must reclaim once the lock is free", runCleanup("mv"));
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    @Test
    public void testCleanupWithStalePredicateDuringUnappliedDropExpireSurvives() throws Exception {
        assertConcurrentPolicyChangeKeepsRows("alter materialized view mv drop expire", null);
    }

    @Test
    public void testCleanupWithStalePredicateDuringUnappliedSetExpireLooseningSurvives() throws Exception {
        // Loosen so every previously-expired row is kept by the new policy: the read filter keeps rows for
        // which the predicate is false, and ts < 2024-01-01 is false for all three rows (all >= 2024-01-01).
        assertConcurrentPolicyChangeKeepsRows(
                "alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'",
                "ts < '2024-01-01T00:00:00.000000Z'"
        );
    }

    // M1 GATE deterministic pin (writer caught up + policy change lands MID-SWEEP). Since the seqTxn baseline
    // is now the reader's own applied txn (readerSeqTxn), even the committed-but-not-applied
    // testCleanupWithStalePredicate...Unapplied tests reach the bounds-DROP fast path (racyOpsAllowed is true
    // against that baseline) and defer at the per-commit gate -- so reverting the M1 one-liner already breaks
    // them. This test pins the same gate under the fully-applied mid-sweep race: the view is fully applied so
    // racyOpsAllowed == true and the bounds-DROP fast path IS entered, and an in-job barrier injects the
    // loosening ALTER exactly before the first destructive commit — advancing the sequencer past the sweep's
    // expectedSeqTxn while the writer was caught up. Only the M1 per-commit gate on the bounds-DROP fast path
    // can defer here; reverting that one-liner makes this test fail (the bounds-DROP would wipe OLD1/OLD2
    // against the stale strict predicate before the loosened policy applies).
    @Test
    public void testM1BoundsDropGateDefersOnMidSweepPolicyChange() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired, bounds-DROP eligible
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired, bounds-DROP eligible
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            // Strict "ts < now()" routes cleanup through the bounds-DROP fast path.
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            // Under the strict policy both old partitions are expired; only NEW is visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            // Writer caught up -> racyOpsAllowed == true at sweep start -> the bounds-DROP fast path is entered.
            Assert.assertEquals("precondition: view fully applied (writer caught up)",
                    tracker.getSeqTxn(), tracker.getWriterTxn());

            // Fire after cleanup has appended and synced its WAL event but before the conditional sequencer
            // allocation. The competing ALTER must take the next txn, reject cleanup's stale fence, and leave
            // no sequenced cleanup event behind.
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try {
                    execute("alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("conditional allocation must defer the bounds-DROP wipe", reclaimed);

            // Apply the loosened policy. Nothing was reclaimed; every row it keeps (all three -- ts < 2024-01-01 is
            // false for all) must survive. OLD1/OLD2 would be physically gone had the bounds-DROP committed.
            drainWalAndMatViewQueues();
            Assert.assertEquals("ts < '2024-01-01T00:00:00.000000Z'", expiryPredicate("mv"));
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
        });
    }

    @Test
    public void testConditionalFenceDefersScanDropOnSetExpire() throws Exception {
        assertConditionalFenceDefersPolicyChange(
                false,
                "alter materialized view mv set expire rows when v < 0",
                "v < 0"
        );
    }

    @Test
    public void testConditionalFenceDefersSurvivorReplaceOnDropExpire() throws Exception {
        assertConditionalFenceDefersPolicyChange(true, "alter materialized view mv drop expire", null);
    }

    @Test
    public void testConditionalFenceRetriesAfterCompetingDataTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxnBefore = tracker.getSeqTxn();
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try (WalWriter competingWriter = engine.getWalWriter(token)) {
                    final TableWriter.Row row = competingWriter.newRow(1_704_412_800_000_000L);
                    row.putSym(0, "KEEP");
                    row.putDouble(1, 3.0);
                    row.append();
                    competingWriter.commit();
                }
            });

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("competing data txn must reject the cleanup fence", job.cleanupTable(token, predicate));
            }
            Assert.assertEquals("rejected cleanup must not consume a sequencer txn", seqTxnBefore + 1, tracker.getSeqTxn());

            drainWalAndMatViewQueues();
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertTrue("next sweep must retry successfully", job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nKEEP\nNEW\n");
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    // C1 (mid-sweep predicate reconciliation, data-loss). A cleanup sweep snapshots (token, predicate) at
    // sweep-start discovery (runSerially) and opens its reader LATER. If an ALTER SET/DROP EXPIRE applied in
    // between and was FULLY applied (writer caught up), the seqTxn per-commit gate alone does NOT defer -- the
    // sequencer moved but the writer is caught up again, so racyOpsAllowed is true and getSeqTxn()==expectedSeqTxn
    // holds. cleanupTable0 must instead re-read the AUTHORITATIVE predicate from its reader and defer when it no
    // longer matches the stale discovery predicate. Reproduced deterministically: apply the loosening/DROP fully,
    // then drive cleanup with the OLD strict predicate exactly as a stale discovery snapshot would. Without the
    // reconciliation this wipes OLD1/OLD2 against the stale strict predicate.
    @Test
    public void testCleanupWithStalePredicateAfterAppliedSetExpireLooseningSurvives() throws Exception {
        assertCleanupWithStaleDiscoveryPredicateKeepsRows(
                "alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'",
                "ts < '2024-01-01T00:00:00.000000Z'"
        );
    }

    @Test
    public void testCleanupWithStalePredicateAfterAppliedDropExpireSurvives() throws Exception {
        assertCleanupWithStaleDiscoveryPredicateKeepsRows("alter materialized view mv drop expire", null);
    }

    @Test
    public void testScalarCleanupSkipsUnchangedPartitionGenerations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                        ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                        ('B', 2.0, '2024-01-02T00:00:00.000000Z'),
                        ('C', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS WHEN v < 0");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate = expiryPredicate("mv");
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals(2, job.getScalarPartitionScanCount());

                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals("unchanged persisted partition generations skip rescans",
                        2, job.getScalarPartitionScanCount());

                execute("INSERT INTO base VALUES ('D', 4.0, '2024-01-01T12:00:00.000000Z')");
                drainWalAndMatViewQueues();
                Assert.assertFalse(job.cleanupTable(token, predicate));
                Assert.assertEquals("historical WAL-range replacement invalidates only the changed partition",
                        3, job.getScalarPartitionScanCount());
            }
        });
    }

    private void assertCleanupWithStaleDiscoveryPredicateKeepsRows(String policyChangeSql, String newPredicateOrNullForDrop) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired under the STALE predicate
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired under the STALE predicate
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");

            // The stale discovery predicate: the strict policy in force at (simulated) discovery time.
            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }

            // Apply the policy change FULLY (writer caught up): a reader opened now reflects the NEW policy, so
            // the seqTxn gate alone would NOT defer -- only the authoritative-predicate re-read does.
            execute(policyChangeSql);
            drainWalAndMatViewQueues();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("precondition: policy change fully applied (writer caught up)",
                    tracker.getSeqTxn(), tracker.getWriterTxn());

            // Drive cleanup with the STALE strict predicate, exactly as a discovery snapshot taken before the change.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("cleanup must defer when the authoritative predicate changed since the discovery snapshot", reclaimed);

            if (newPredicateOrNullForDrop == null) {
                Assert.assertNull("DROP EXPIRE cleared the policy", expiryPredicate("mv"));
            } else {
                Assert.assertEquals(newPredicateOrNullForDrop, expiryPredicate("mv"));
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            if (newPredicateOrNullForDrop == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            }
        });
    }

    private void assertConditionalFenceDefersPolicyChange(
            boolean isReplace,
            String policyChangeSql,
            String expectedPredicate
    ) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute(isReplace
                    ? "insert into base values " +
                      "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +
                      "('KEEP', 3.0, '2024-01-05T01:00:00.000000Z')," +
                      "('NEW', 4.0, '2024-01-20T00:00:00.000000Z')"
                    : "insert into base values " +
                      "('OLD', 1.0, '2024-01-05T00:00:00.000000Z')," +
                      "('NEW', 4.0, '2024-01-20T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate = expiryPredicate("mv");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long seqTxnBefore = tracker.getSeqTxn();
            engine.getTableSequencerAPI().setTestNextTxnIfLastTxnBarrier(() -> {
                try {
                    execute(policyChangeSql);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("policy change must reject the cleanup fence", job.cleanupTable(token, stalePredicate));
            }
            Assert.assertEquals("rejected cleanup must not consume a sequencer txn", seqTxnBefore + 1, tracker.getSeqTxn());

            drainWalAndMatViewQueues();
            Assert.assertEquals(expectedPredicate, expiryPredicate("mv"));
            if (expectedPredicate == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nKEEP\nNEW\nOLD\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD\n");
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
        });
    }

    // M1/M6 DETERMINISTIC data-loss guard (non-fuzz): a cleanup sweep that snapshotted a now-STALE (stricter)
    // predicate must not physically delete rows the CURRENT (loosened / dropped) policy keeps. TableWriter's
    // policy-change apply path does not take the MatViewState lock the sweep holds, so a policy change can
    // apply mid-sweep; the sequencer-txn gate on every destructive commit (incl. the bounds-DROP fast path,
    // see RowExpiryCleanupJob) is what prevents the wipe. As in testDeterministicBackfillBetweenScanAndCommit-
    // Survives, there is no in-job hook to pause between the survivor scan and the destructive commit, so we
    // reproduce the gate's precondition DETERMINISTICALLY: the policy change is left COMMITTED-BUT-NOT-APPLIED
    // (its sequencer txn is published but not applied, so writerTxn < seqTxn). With the reader-txn baseline the
    // bounds-DROP fast path is entered and the per-commit gate (getSeqTxn() != expectedSeqTxn) is what defers.
    // Cleanup with the stale strict predicate MUST defer (reclaim nothing); after the change is applied, every row the new policy keeps must
    // still be physically present. The instruction-level scan-vs-commit interleave with the writer caught up is
    // exercised probabilistically by MatViewRowExpiryFuzzTest.
    private void assertConcurrentPolicyChangeKeepsRows(String policyChangeSql, String loosenedPredicateOrNullForDrop) throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(JAN_10);
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('OLD1', 1.0, '2024-01-05T00:00:00.000000Z')," + // ts < now() -> strict-expired (non-active, bounds-DROP eligible)
                    "('OLD2', 2.0, '2024-01-08T00:00:00.000000Z')," + // ts < now() -> strict-expired (non-active, bounds-DROP eligible)
                    "('NEW', 3.0, '2024-01-20T00:00:00.000000Z')");   // active partition
            drainWalAndMatViewQueues();
            // A scalar "ts < now()" strict policy routes cleanup through the bounds-DROP fast path.
            execute("create materialized view mv as (select * from base) expire rows when ts < now()");
            drainWalAndMatViewQueues();

            // Under the strict policy the two old partitions are expired; only NEW is visible.
            assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\n");
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals("precondition: view fully applied", tracker.getSeqTxn(), tracker.getWriterTxn());

            // Apply the policy change to the view's sequencer but DO NOT apply it (no drain) -> writerTxn < seqTxn.
            execute(policyChangeSql);
            Assert.assertTrue(
                    "precondition: policy change must be committed-but-not-applied (writerTxn < seqTxn)",
                    tracker.getWriterTxn() < tracker.getSeqTxn()
            );

            // Cleanup with the STALE strict predicate must defer: the writer is not caught up, so no destructive
            // commit (bounds-DROP included) fires and nothing the loosened/dropped policy keeps can be wiped.
            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("cleanup must defer while a policy change is committed-but-not-applied", reclaimed);

            // Apply the policy change. No partition may have been reclaimed, and every row the new policy keeps
            // (all three under both the loosened threshold and DROP EXPIRE) must be present and visible.
            drainWalAndMatViewQueues();
            if (loosenedPredicateOrNullForDrop == null) {
                Assert.assertNull("DROP EXPIRE must clear the policy", expiryPredicate("mv"));
            } else {
                Assert.assertEquals(loosenedPredicateOrNullForDrop, expiryPredicate("mv"));
            }
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");
            if (loosenedPredicateOrNullForDrop == null) {
                assertQuery("select sym from mv order by sym").expectSize().noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            } else {
                assertQuery("select sym from mv order by sym").noLeakCheck().returns("sym\nNEW\nOLD1\nOLD2\n");
            }
        });
    }

    private void assertMixedClockCleanupSkipped(String viewName, String predicate) throws Exception {
        execute("CREATE MATERIALIZED VIEW " + viewName + " AS (SELECT * FROM base) EXPIRE ROWS WHEN " + predicate);
        drainWalAndMatViewQueues();
        final TableToken token = engine.verifyTableName(viewName);
        final String storedPredicate = expiryPredicate(viewName);
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            Assert.assertFalse("mixed clock/random threshold must skip physical cleanup", job.cleanupTable(token, storedPredicate));
        }
        drainWalAndMatViewQueues();
        assertQuery("SELECT count() p, sum(numRows) r FROM table_partitions('" + viewName + "')")
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .returns("p\tr\n3\t3\n");
    }

    private String expiryPredicate(String name) {
        final TableToken token = engine.verifyTableName(name);
        try (TableMetadata m = engine.getTableMetadata(token)) {
            return m.getExpiryPredicate();
        }
    }

    private boolean runCleanup(String name) {
        final TableToken token = engine.verifyTableName(name);
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            return job.cleanupTable(token, predicate);
        }
    }
}
