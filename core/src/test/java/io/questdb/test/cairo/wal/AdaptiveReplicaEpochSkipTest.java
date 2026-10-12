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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The adaptive apply-side durable epoch is skipped when the engine's LocalDurabilityPolicy is REPLICA_SKIP
 * (what Enterprise installs on a replica), and fires under the default ALWAYS_ON. The epoch interval is 0,
 * so the epoch is eligible on every apply batch and only the policy gate suppresses it under REPLICA_SKIP.
 */
public class AdaptiveReplicaEpochSkipTest extends AbstractCairoTest {

    @Test
    public void testReplicaSkipFiresNoEpochButDataStillReadable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // epoch eligible every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("x");
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("REPLICA_SKIP must not advance the durable epoch frontier",
                        0L, tracker.getDurableEpochSeqTxn());
                Assert.assertEquals("REPLICA_SKIP must fire no epoch (lastEpochTs stays 0)",
                        0L, tracker.getLastEpochTs());

                // Visibility is unaffected: the epoch governs durability, not apply. Lazy apply still
                // writes the columns, so the row is readable.
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    /**
     * A demote that lands inside the epoch's gate-&gt;fsync window must not let the in-flight apply publish an
     * epoch on the demoted node (on a demote Enterprise installs {@link LocalDurabilityPolicy#REPLICA_SKIP}).
     * {@code ApplyWal2TableJob.maybeAdvanceDurableEpoch} checks the policy at its gate, then {@code advance()}
     * checks it again as its first statement, before {@code fsyncMaterializedState}. Here the policy answers
     * ENABLED at the gate and DISABLED at the re-check, modelling the demote in the window: the durable
     * frontier must not advance and the {@code _epoch.manifest} save point must not change.
     */
    @Test
    public void testDemoteInEpochWindowRecheckSuppressesEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0); // epoch eligible every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // Apply the CREATE under REPLICA_SKIP, so it fires no epoch beyond the enrollment baseline.
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");
            final long baselineEpochSeqTxn;
            final long baselineEpochTxn;
            final int baselineGeneration;
            try (Path markerPath = new Path()) {
                markerPath.of(configuration.getDbRoot()).concat(tt);
                final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), markerPath, markerPath.size(), tt.getTableId());
                Assert.assertTrue("adaptive enrollment baseline must load", saved.length > 0);
                baselineEpochSeqTxn = saved[0].epochSeqTxn;
                baselineEpochTxn = saved[0].epochTxn;
                baselineGeneration = saved[0].generation;
            }

            // Calls 0-3 construct the apply job and writer (including its two structural purge gates).
            // Call 4 reconciles the anchor, call 5 is maybeAdvanceDurableEpoch's gate, and call 6 is
            // advance()'s re-check. Only the re-check and subsequent calls see the demote.
            final AtomicInteger calls = new AtomicInteger();
            final boolean[] sawEnabledAtGate = {false};
            final boolean[] sawDisabledAtRecheck = {false};
            final LocalDurabilityPolicy demoteInWindow = () -> {
                final int call = calls.getAndIncrement();
                final boolean enabled = call < 6;
                if (call == 5) {
                    sawEnabledAtGate[0] = true;
                } else if (call == 6) {
                    sawDisabledAtRecheck[0] = true;
                }
                return enabled;
            };
            engine.setLocalDurabilityPolicy(demoteInWindow);
            try {
                execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                // The gate passed (entered advance()) and the re-check saw DISABLED: advance()'s re-check
                // suppressed the epoch, not the gate.
                Assert.assertTrue("epoch gate must have seen local durability ENABLED (entered advance())",
                        sawEnabledAtGate[0]);
                Assert.assertTrue("advance()'s re-check must have seen local durability DISABLED",
                        sawDisabledAtRecheck[0]);

                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("a demote in the epoch window must not advance the durable epoch frontier",
                        0L, tracker.getDurableEpochSeqTxn());
                Assert.assertEquals("a demote in the epoch window must not publish an epoch (lastEpochTs stays 0)",
                        0L, tracker.getLastEpochTs());

                // Pin the exact sequence so a new policy reader cannot silently move the demote before the gate.
                Assert.assertEquals("setup, reconcile, gate, then disabled re-check", 7, calls.get());

                // Enrollment may have published an epoch-0 baseline before this window. The demoted node must
                // not advance or replace that save point.
                try (Path markerPath = new Path()) {
                    markerPath.of(configuration.getDbRoot()).concat(tt);
                    final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), markerPath, markerPath.size(), tt.getTableId());
                    Assert.assertTrue("enrollment baseline must remain loadable", saved.length > 0);
                    Assert.assertEquals(baselineEpochSeqTxn, saved[0].epochSeqTxn);
                    Assert.assertEquals(baselineEpochTxn, saved[0].epochTxn);
                    Assert.assertEquals(baselineGeneration, saved[0].generation);
                }

                // Visibility is unaffected: lazy apply still writes the columns.
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                count
                                1
                                """);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    @Test
    public void testAlwaysOnFiresEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // default policy = ALWAYS_ON (no setLocalDurabilityPolicy call)
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            Assert.assertTrue("ALWAYS_ON (default) must advance the durable epoch frontier",
                    tracker.getDurableEpochSeqTxn() > 0L);
        });
    }

    private boolean epochArtifactExists(TableToken tt, CharSequence base, CharSequence suffix) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(base);
            if (suffix.length() > 0) {
                p.put(suffix);
            }
            return ff.exists(p.$());
        }
    }
}
