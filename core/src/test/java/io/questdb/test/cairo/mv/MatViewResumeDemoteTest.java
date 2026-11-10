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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * {@code REFRESH MATERIALIZED VIEW v RESUME} on a node that is, or turns, read-only. The node's role is a
 * test-controlled flag on the configuration, as in {@link MatViewRowExpiryDemoteFenceTest}. A read-only node refuses
 * the statement, and a demote that lands while RESUME holds the view latch makes the role-switch commit fence refuse
 * its state event: the view stays invalid, and nothing reaches the view's WAL.
 */
public class MatViewResumeDemoteTest extends AbstractCairoTest {
    // Flips the role when RESUME opens the base table's reader: after it acquired the view's WAL writer and before the
    // commit fence's unlocked read-only check.
    private static final AtomicBoolean demoteOnNextBaseReader = new AtomicBoolean();
    // Flips the role inside the commit fence, after its unlocked read-only check and before it takes the role-switch
    // read lock, where only the re-check under the lock refuses the event.
    private static final AtomicBoolean demoteOnNextRoleSwitchReadLock = new AtomicBoolean();
    private static final AtomicBoolean readOnly = new AtomicBoolean();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isReadOnlyInstance() {
                        return readOnly.get();
                    }
                };
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public TableReader getReader(TableToken tableToken) {
                if ("base".equals(tableToken.getTableName()) && demoteOnNextBaseReader.compareAndSet(true, false)) {
                    readOnly.set(true);
                }
                return super.getReader(tableToken);
            }

            @Override
            public Lock getRoleSwitchReadLock() {
                if (demoteOnNextRoleSwitchReadLock.compareAndSet(true, false)) {
                    readOnly.set(true);
                }
                return super.getRoleSwitchReadLock();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        AbstractCairoTest.tearDownStatic();
        AbstractCairoTest.configurationFactory = null;
        AbstractCairoTest.engineFactory = null;
    }

    @Before
    public void setUp() {
        readOnly.set(false);
        demoteOnNextBaseReader.set(false);
        demoteOnNextRoleSwitchReadLock.set(false);
        super.setUp();
    }

    @Test
    public void testBacklogUpdateOnReadOnlyNodeWritesNothing() throws Exception {
        // A backlog update on a node that turned read-only cannot acquire the view's WAL writer. The refusal is a
        // role switch, not a failure: the backlog stays tracked and nothing reaches the view's WAL, and a later
        // update on the primary brings the backlog up to date.
        assertMemoryLeak(() -> {
            createInvalidView();
            execute("INSERT INTO base VALUES ('a', 2.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            readOnly.set(true);
            try {
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                drainMatViewQueue(engine);
            } finally {
                readOnly.set(false);
            }
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.isLocked());
            Assert.assertNull(state.getBacklogLostReason());
            Assert.assertEquals(-1, state.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
            drainWalAndMatViewQueues();
            Assert.assertEquals(2, state.getRefreshIntervalsBaseTxn());
            Assert.assertEquals(viewSeqTxn + 1, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());
        });
    }

    @Test
    public void testBacklogLostOnReadOnlyNodeKeepsMarkerInMemory() throws Exception {
        // A backlog update that finds the backlog lost after the node turned read-only writes no view state event:
        // the marker lives in memory, and RESUME on the primary finds the backlog lost again.
        assertMemoryLeak(() -> {
            createInvalidView();
            execute("DROP TABLE base");
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            try (MatViewRefreshJob job = new MatViewRefreshJob(0, engine, 0)) {
                job.setOnHoldingLockForTesting(() -> readOnly.set(true));
                engine.getMatViewStateStore().enqueueUpdateRefreshIntervals(viewToken);
                drainMatViewQueue(job);
            } finally {
                readOnly.set(false);
            }
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.isLocked());
            Assert.assertEquals(MatViewBacklog.LOST_BASE_TABLE_REPLACED, state.getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            assertExceptionNoLeakCheck(
                    "REFRESH MATERIALIZED VIEW price_1h RESUME",
                    "REFRESH MATERIALIZED VIEW ".length(),
                    "materialized view backlog is lost [reason=base table replaced]"
            );
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            Assert.assertFalse(state.isInvalid());
        });
    }

    @Test
    public void testDemoteBeforeCommitFenceRefusesStateEvent() throws Exception {
        // The demote lands after RESUME acquired the view's WAL writer: the commit fence's unlocked check refuses the
        // view state event.
        assertDemoteRefusesStateEvent(demoteOnNextBaseReader);
    }

    @Test
    public void testDemoteInsideCommitFenceRefusesStateEvent() throws Exception {
        // The demote lands between the commit fence's unlocked check and its read lock: the re-check under the lock
        // refuses the view state event.
        assertDemoteRefusesStateEvent(demoteOnNextRoleSwitchReadLock);
    }

    @Test
    public void testDemoteDuringBacklogUpdateOfResumeRefusesIt() throws Exception {
        // The demote lands while RESUME brings the backlog up to date: the commit fence refuses the backlog's view
        // state event, and RESUME fails with the read-only refusal instead of finding the backlog lost. The view stays
        // invalid with its backlog, and a RESUME on the primary catches up.
        assertMemoryLeak(() -> {
            createInvalidView();
            execute("INSERT INTO base VALUES ('a', 2.0, '2024-01-01T00:20:00.000000Z')");
            drainWalAndMatViewQueues();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            demoteOnNextRoleSwitchReadLock.set(true);
            try {
                execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.isAuthorizationError());
                TestUtils.assertContains(e.getFlyweightMessage(), "read-only");
            } finally {
                readOnly.set(false);
            }
            Assert.assertFalse(demoteOnNextRoleSwitchReadLock.get());
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.isLocked());
            Assert.assertNull(state.getBacklogLostReason());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            execute("REFRESH MATERIALIZED VIEW price_1h RESUME");
            Assert.assertFalse(state.isInvalid());
            drainWalAndMatViewQueues();
            assertQuery("price_1h ORDER BY ts, sym")
                    .timestamp("ts")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts\tsym\tprice
                            2024-01-01T00:00:00.000000Z\ta\t2.0
                            """);
        });
    }

    @Test
    public void testDemoteWhileResumeHoldsLatchRefusesStateEvent() throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            state.setOnResumeHoldingLockForTesting(() -> readOnly.set(true));
            try {
                execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.isAuthorizationError());
            } finally {
                readOnly.set(false);
            }
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            // back to primary, RESUME succeeds
            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            Assert.assertFalse(state.isInvalid());
            drainWalAndMatViewQueues();
            assertQuery("SELECT view_status FROM materialized_views() WHERE view_name = 'price_1h'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\nvalid\n");
        });
    }

    @Test
    public void testReadOnlyNodeRefusesResume() throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            readOnly.set(true);
            try {
                execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.isAuthorizationError());
                TestUtils.assertContains(e.getFlyweightMessage(), "read-only");
            } finally {
                readOnly.set(false);
            }
            Assert.assertTrue(engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h")).isInvalid());
        });
    }

    private void assertDemoteRefusesStateEvent(AtomicBoolean demoteTrigger) throws Exception {
        assertMemoryLeak(() -> {
            createInvalidView();
            final TableToken viewToken = engine.verifyTableName("price_1h");
            final MatViewState state = engine.getMatViewStateStore().getViewState(viewToken);
            final long viewSeqTxn = engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn();
            demoteTrigger.set(true);
            try {
                execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(e.isAuthorizationError());
                TestUtils.assertContains(e.getFlyweightMessage(), "read-only");
            } finally {
                readOnly.set(false);
            }
            Assert.assertFalse(demoteTrigger.get());
            Assert.assertTrue(state.isInvalid());
            Assert.assertFalse(state.isLocked());
            Assert.assertFalse(state.isResuming());
            Assert.assertEquals(viewSeqTxn, engine.getTableSequencerAPI().getTxnTracker(viewToken).getSeqTxn());

            execute("REFRESH MATERIALIZED VIEW price_1h RESUME SKIP BACKLOG");
            Assert.assertFalse(state.isInvalid());
        });
    }

    private void createInvalidView() throws Exception {
        execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO base VALUES ('a', 1.0, '2024-01-01T00:10:00.000000Z')");
        execute("CREATE MATERIALIZED VIEW price_1h AS (SELECT ts, sym, last(price) AS price FROM base SAMPLE BY 1h) PARTITION BY DAY");
        drainWalAndMatViewQueues();
        engine.getMatViewStateStore().enqueueInvalidate(engine.verifyTableName("price_1h"), "test invalidation");
        drainWalAndMatViewQueues();
        Assert.assertTrue(engine.getMatViewStateStore().getViewState(engine.verifyTableName("price_1h")).isInvalid());
    }
}
