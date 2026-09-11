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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.ForwardingMatViewStateStore;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewStateStore;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.LogCapture;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The demote fence on {@link RowExpiryCleanupJob}. A REPLACE_RANGE reclamation on a materialized view
 * replicates as an ordinary WAL transaction, and the job acquires the view's WAL writer as PRIMARY but
 * commits only after the survivor count and the survivor copy have run. A role flip inside that window
 * would otherwise let the job mint a local-only sequencer transaction on a replicated table - a destructive
 * delete the closing uploader never ships and the new primary never sees.
 * <p>
 * Two gates keep that from happening, and this class drives both against a real sweep rather than asserting
 * they exist: {@code runSerially} refuses to sweep at all while the node is read-only, and
 * {@code commitWithFence} re-checks under the role-switch read lock so a flip that lands mid-sweep refuses
 * the commit instead of externalizing it.
 * <p>
 * The node's role is a test-controlled flag on the configuration ({@code isReadOnlyInstance}), and a
 * forwarding mat-view state store gives the mid-sweep flip a deterministic point to fire from: the job looks
 * the view state up once per sweep, after the entry check and before it touches a writer.
 */
public class MatViewRowExpiryDemoteFenceTest extends AbstractCairoTest {

    private static final String VIEW_NAME = "mv";
    // Fires the mid-sweep demote. Armed for one sweep at a time; the state-store lookup below consumes it.
    private static final AtomicBoolean demoteOnNextViewStateLookup = new AtomicBoolean();
    private static final AtomicBoolean readOnly = new AtomicBoolean();
    private static Supplier<CairoException> walWriterFailure;
    private static int walWriterFailureCount;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // The node's role has to be flippable while the engine runs: a demote does not rebuild the engine,
        // and the whole point of the fence is the window inside one sweep.
        AbstractCairoTest.configurationFactory = (root, telemetry, overrides) ->
                new CairoTestConfiguration(root, telemetry, overrides) {
                    @Override
                    public boolean isReadOnlyInstance() {
                        return readOnly.get();
                    }
                };
        // cleanupTable() resolves the view state once per sweep, after runSerially's entry check and before
        // it acquires a WAL writer or scans anything. That makes it the exact point an in-place demote has to
        // be survivable from, so it is where the test flips the role.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public WalWriter getWalWriter(TableToken tableToken) {
                if (VIEW_NAME.equals(tableToken.getTableName()) && walWriterFailure != null) {
                    walWriterFailureCount++;
                    throw walWriterFailure.get();
                }
                return super.getWalWriter(tableToken);
            }

            @Override
            protected MatViewStateStore createMatViewStateStore() {
                return new ForwardingMatViewStateStore(super.createMatViewStateStore()) {
                    @Override
                    public MatViewState getViewState(TableToken matViewToken) {
                        if (VIEW_NAME.equals(matViewToken.getTableName())
                                && demoteOnNextViewStateLookup.compareAndSet(true, false)) {
                            readOnly.set(true);
                        }
                        return super.getViewState(matViewToken);
                    }
                };
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
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        readOnly.set(false);
        demoteOnNextViewStateLookup.set(false);
        walWriterFailure = null;
        walWriterFailureCount = 0;
    }

    @Test
    public void testDemoteMidSweepRefusesCommitAndLosesNothing() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken viewToken = createPolicedView();
            final long seqTxnBeforeSweep = engine.getTableSequencerAPI().lastTxn(viewToken);

            // A demote lands after the sweep starts and before it commits.
            demoteOnNextViewStateLookup.set(true);
            Assert.assertFalse(runCleanupSweep());
            Assert.assertTrue(readOnly.get()); // the flip really did fire

            // Nothing was externalized: the sequencer is exactly where it was.
            Assert.assertEquals(seqTxnBeforeSweep, engine.getTableSequencerAPI().lastTxn(viewToken));

            // The refusal rolled the prepared reclamation back cleanly, so a later sweep as PRIMARY still
            // reclaims, and reclaims exactly the expired rows.
            readOnly.set(false);
            Assert.assertTrue(runCleanupSweep());
            // One REPLACE_RANGE per fully-expired logical partition: days 01 and 02.
            Assert.assertEquals(seqTxnBeforeSweep + 2, engine.getTableSequencerAPI().lastTxn(viewToken));

            execute("ALTER MATERIALIZED VIEW " + VIEW_NAME + " DROP EXPIRE");
            drainWalAndMatViewQueues();
            // Days 01 and 02 are below the threshold and gone; 03 is the active partition, which the job
            // never touches, so its expired row stays on disk (the read filter hid it while the policy was on).
            assertQuery("SELECT ts, v FROM " + VIEW_NAME)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-03T00:00:00.000000Z\t3.0
                            """);
        });
    }

    @Test
    public void testReadOnlyNodeSweepsNothing() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken viewToken = createPolicedView();
            final long seqTxnBeforeSweep = engine.getTableSequencerAPI().lastTxn(viewToken);

            readOnly.set(true);
            Assert.assertFalse(runCleanupSweep());
            Assert.assertEquals(seqTxnBeforeSweep, engine.getTableSequencerAPI().lastTxn(viewToken));

            readOnly.set(false);
            execute("ALTER MATERIALIZED VIEW " + VIEW_NAME + " DROP EXPIRE");
            drainWalAndMatViewQueues();
            // Every row is still on disk: a replica reclaims nothing, it replays what the primary reclaimed.
            assertQuery("SELECT ts, v FROM " + VIEW_NAME)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:00.000000Z\t1.0
                            1970-01-02T00:00:00.000000Z\t2.0
                            1970-01-03T00:00:00.000000Z\t3.0
                            """);
        });
    }

    @Test
    public void testWriterAcquireAuthorizationFailureKeepsFailureBackoff() throws Exception {
        assertWriterAcquisitionFailure("ts < '1970-01-03T00:00:00.000000Z'", false, false);
    }

    @Test
    public void testWriterAcquireReadOnlyRefusalDefersBoundsWipe() throws Exception {
        assertWriterAcquisitionFailure("ts < '1970-01-03T00:00:00.000000Z'", true, false);
    }

    @Test
    public void testWriterAcquireReadOnlyRefusalDefersCountWipe() throws Exception {
        assertWriterAcquisitionFailure("v < 3", true, false);
    }

    @Test
    public void testWriterAcquireReadOnlyRefusalDefersReplacement() throws Exception {
        assertWriterAcquisitionFailure("v < 3", true, true);
    }

    private void assertWriterAcquisitionFailure(String predicate, boolean isReadOnlyRefusal, boolean hasSurvivors) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken viewToken = createPolicedView();
            if (hasSurvivors) {
                execute("""
                        INSERT INTO base VALUES
                        ('1970-01-01T01:00:00.000000Z', 4.0),
                        ('1970-01-02T01:00:00.000000Z', 4.0)""");
            }
            execute("ALTER MATERIALIZED VIEW mv SET EXPIRE ROWS WHEN " + predicate + " CLEANUP EVERY 1s");
            drainWalAndMatViewQueues();
            final long seqTxnBeforeSweep = engine.getTableSequencerAPI().lastTxn(viewToken);
            setCurrentMicros(0);

            final LogCapture logCapture = new LogCapture();
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                logCapture.start();
                walWriterFailure = () -> {
                    if (isReadOnlyRefusal) {
                        final CairoException refusal = CairoException.readOnlyAccess();
                        // A failed demote can restore PRIMARY before the catch classifies the refusal.
                        readOnly.set(false);
                        return refusal;
                    }
                    return CairoException.authorization().put("injected cleanup authorization failure");
                };
                final int failuresPerSweep = isReadOnlyRefusal ? 1 : 2;
                for (int i = 0; i < 2; i++) {
                    setCurrentMicros(i * 1_000_000L);
                    demoteOnNextViewStateLookup.set(isReadOnlyRefusal);
                    Assert.assertFalse(job.runNow());
                    Assert.assertFalse(readOnly.get());
                    Assert.assertEquals("read-only refusal must stop at the first partition",
                            (i + 1) * failuresPerSweep, walWriterFailureCount);
                    Assert.assertEquals(seqTxnBeforeSweep, engine.getTableSequencerAPI().lastTxn(viewToken));
                    setCurrentMicros(i * 1_000_000L + 999_999L);
                    Assert.assertFalse(job.runNow());
                    Assert.assertEquals((i + 1) * failuresPerSweep, walWriterFailureCount);
                }
                logCapture.drain();
                if (isReadOnlyRefusal) {
                    logCapture.assertNotLogged("row-expiry partition cleanup failed");
                    logCapture.assertNotLogged("row-expiry cleanup failed");
                } else {
                    logCapture.assertLogged("injected cleanup authorization failure");
                }

                walWriterFailure = null;
                setCurrentMicros(2_000_000L);
                if (!isReadOnlyRefusal) {
                    // Genuine failures double the second retry gap to 2s, beyond CLEANUP EVERY 1s.
                    Assert.assertFalse(job.runNow());
                    Assert.assertEquals(seqTxnBeforeSweep, engine.getTableSequencerAPI().lastTxn(viewToken));
                    setCurrentMicros(3_000_000L);
                }
                Assert.assertTrue("read-only deferrals must retry within CLEANUP EVERY", job.runNow());
                Assert.assertEquals(seqTxnBeforeSweep + 2, engine.getTableSequencerAPI().lastTxn(viewToken));
            } finally {
                walWriterFailure = null;
                readOnly.set(false);
                logCapture.stop();
            }

            execute("ALTER MATERIALIZED VIEW mv DROP EXPIRE");
            drainWalAndMatViewQueues();
            assertQuery("SELECT ts, v FROM mv").noLeakCheck().expectSize().timestamp("ts").returns(hasSurvivors ? """
                    ts\tv
                    1970-01-01T01:00:00.000000Z\t4.0
                    1970-01-02T01:00:00.000000Z\t4.0
                    1970-01-03T00:00:00.000000Z\t3.0
                    """ : """
                    ts\tv
                    1970-01-03T00:00:00.000000Z\t3.0
                    """);
        });
    }

    /**
     * A passthrough view over three daily partitions with a monotonic timestamp policy, so the cleanup job
     * classifies days 01 and 02 as fully expired and leaves day 03 alone as the active partition.
     */
    private TableToken createPolicedView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                ('1970-01-01T00:00:00.000000Z', 1.0),
                ('1970-01-02T00:00:00.000000Z', 2.0),
                ('1970-01-03T00:00:00.000000Z', 3.0)""");
        drainWalAndMatViewQueues();
        execute("CREATE MATERIALIZED VIEW " + VIEW_NAME + " REFRESH IMMEDIATE AS (SELECT * FROM base)"
                + " PARTITION BY DAY EXPIRE ROWS WHEN ts < '1970-01-03T00:00:00.000000Z'");
        drainWalAndMatViewQueues();
        return engine.verifyTableName(VIEW_NAME);
    }

    private boolean runCleanupSweep() {
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            boolean isWorkDone = false;
            for (int i = 0; i < 8; i++) {
                isWorkDone |= job.runNow();
            }
            return isWorkDone;
        }
    }
}
