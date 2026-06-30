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

    private static final long JAN_10 = 1704844800000000L; // 2024-01-10T00:00:00Z
    private static final long JAN_25 = 1706140800000000L; // 2024-01-25T00:00:00Z

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();

            Assert.assertTrue("clock-free monotonic policy must reclaim", worked);
            // 01-02 wholly expired -> reclaimed; 01-01 (kept v=1) and 01-03 (active) remain.
            assertSql("p\n2\n", "select count() p from table_partitions('mv')");
            assertSql("sym\tv\nA\t1.0\nC\t3.0\n", "select sym, v from mv order by sym");
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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");
            assertSql("sym\nA\n", "select sym from mv order by sym");

            // Cleanup must be a NO-OP: the policy is non-monotonic, so reclamation is skipped.
            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertFalse("non-monotonic policy must skip physical cleanup", worked);
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");
            assertSql("sym\nA\n", "select sym from mv order by sym");

            // Advance the clock past all rows: every row now satisfies ts <= now() -> all kept and VISIBLE.
            // This only holds because cleanup did not delete the future rows while they were expired.
            setCurrentMicros(JAN_25);
            assertSql("sym\nA\nB\nC\n", "select sym from mv order by sym");
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
            assertSql(visibleBefore, "select sym from mv order by sym");
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

            final boolean worked = runCleanup("mv");
            drainWalAndMatViewQueues();
            Assert.assertTrue("ts < now() is monotonic and must reclaim the wholly-expired old partition", worked);

            // The expired OLD partition is reclaimed; the EDGE row exactly at now() and NEW row survive — the
            // post-cleanup visible set equals the pre-cleanup read-filtered set (no boundary divergence).
            assertSql("p\n2\n", "select count() p from table_partitions('mv')");
            assertSql(visibleBefore, "select sym from mv order by sym");
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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

            // Lock released: the next sweep reclaims the wholly-expired old partition.
            Assert.assertTrue("cleanup must reclaim once the lock is free", runCleanup("mv"));
            drainWalAndMatViewQueues();
            assertSql("p\n2\n", "select count() p from table_partitions('mv')");
        });
    }

    // ----- helpers -----

    private void assertSql(CharSequence expected, CharSequence sql) throws Exception {
        assertQuery(sql).noLeakCheck().returnsOnce(expected);
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
