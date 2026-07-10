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
import io.questdb.cairo.wal.seq.SeqTxnTracker;
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
            assertSql("sym\nNEW\n", "select sym from mv order by sym");

            final TableToken token = engine.verifyTableName("mv");
            final String stalePredicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                stalePredicate = m.getExpiryPredicate();
            }
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            // Writer caught up -> racyOpsAllowed == true at sweep start -> the bounds-DROP fast path is entered.
            Assert.assertEquals("precondition: view fully applied (writer caught up)",
                    tracker.getSeqTxn(), tracker.getWriterTxn());

            final boolean reclaimed;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                // Inject the loosening policy change at the first bounds-DROP commit point (before the walWriter
                // is acquired, so no writer-lock contention). It advances the sequencer past expectedSeqTxn while
                // the writer was caught up, so ONLY the M1 per-commit gate can defer -- racyOpsAllowed does not.
                job.setTestBoundsDropCommitBarrier(() -> {
                    try {
                        execute("alter materialized view mv set expire rows when ts < '2024-01-01T00:00:00.000000Z'");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                reclaimed = job.cleanupTable(token, stalePredicate);
            }
            Assert.assertFalse("M1 gate must defer the bounds-DROP wipe when a policy change lands mid-sweep", reclaimed);

            // Apply the loosened policy. Nothing was reclaimed; every row it keeps (all three -- ts < 2024-01-01 is
            // false for all) must survive. OLD1/OLD2 would be physically gone had the bounds-DROP committed.
            drainWalAndMatViewQueues();
            Assert.assertEquals("ts < '2024-01-01T00:00:00.000000Z'", expiryPredicate("mv"));
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");
            assertSql("sym\nNEW\nOLD1\nOLD2\n", "select sym from mv order by sym");
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

            assertSql("sym\nNEW\n", "select sym from mv order by sym");

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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");
            assertSql("sym\nNEW\nOLD1\nOLD2\n", "select sym from mv order by sym");
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
            assertSql("sym\nNEW\n", "select sym from mv order by sym");
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");
            assertSql("sym\nNEW\nOLD1\nOLD2\n", "select sym from mv order by sym");
        });
    }

    // Bridge: AbstractCairoTest.assertSql(expected, sql) was removed in favor of the QueryAssertion builder
    // (OSS #7195). Drive the builder via returns() (NOT returnsOnce) so both cursor passes plus the
    // calculate-size and variable-column cross-checks run against these deterministic projections.
    // sizeMayVary() keeps the size-vs-iteration cross-check without pinning determinability, and
    // inferRandomAccess()/inferTimestamp() adopt each heterogeneous factory's own capability.
    private void assertSql(CharSequence expected, CharSequence sql) throws Exception {
        assertQuery(sql).noLeakCheck().sizeMayVary().inferRandomAccess().inferTimestamp().returns(expected);
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
