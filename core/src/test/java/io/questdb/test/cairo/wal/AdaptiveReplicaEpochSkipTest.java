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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * S5: the adaptive apply-side durable epoch is skipped when the engine's LocalDurabilityPolicy is
 * REPLICA_SKIP (the behavior Enterprise installs on a replica), and fires under the default
 * ALWAYS_ON. The epoch interval is set to 0 so the epoch is eligible on every apply batch — thus
 * the ONLY thing that suppresses it under REPLICA_SKIP is the new policy gate, not the cadence.
 */
public class AdaptiveReplicaEpochSkipTest extends AbstractCairoTest {

    @Test
    public void testReplicaSkipFiresNoEpochButDataStillReadable() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0); // epoch eligible every batch
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

                // Visibility is unaffected — the epoch governs durability, not apply. Lazy apply
                // still writes the columns, so the row is readable.
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .returnsOnce("""
                                count
                                1
                                """);
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    /**
     * S5 hardening (review Finding 2): a demote (Enterprise installs {@link LocalDurabilityPolicy#REPLICA_SKIP}
     * and clears the epoch trio) that lands INSIDE the epoch's gate-&gt;fsync window must not let the in-flight
     * apply (re-)create the epoch trio on the demoted node. {@code ApplyWal2TableJob.maybeAdvanceDurableEpoch}
     * checks the policy at its gate, then {@code advance()} RE-CHECKS it as its first statement (immediately
     * before {@code fsyncMaterializedState}). Here the policy returns ENABLED at the gate (so we enter
     * {@code advance()}) then DISABLED at the re-check — modelling the demote landing in the window — and the
     * epoch trio ({@code _snapshot}/{@code _txn.epoch}/{@code _cv.epoch}) must NOT be created and the durable
     * frontier must NOT advance. (Without the re-check the epoch was created despite the demote.)
     */
    @Test
    public void testDemoteInEpochWindowRecheckSuppressesEpoch() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0); // epoch eligible every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            // Apply the CREATE under REPLICA_SKIP so it fires no epoch (the table starts with no trio).
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("x");

            // The demote-in-window policy: ENABLED on the gate call (the 1st isLocalDurabilityEnabled() of the
            // apply's maybeAdvanceDurableEpoch), DISABLED on advance()'s re-check (and every call thereafter).
            final AtomicInteger calls = new AtomicInteger();
            final boolean[] sawEnabledAtGate = {false};
            final boolean[] sawDisabledAtRecheck = {false};
            final LocalDurabilityPolicy demoteInWindow = () -> {
                final boolean enabled = calls.getAndIncrement() == 0;
                if (enabled) {
                    sawEnabledAtGate[0] = true;
                } else {
                    sawDisabledAtRecheck[0] = true;
                }
                return enabled;
            };
            engine.setLocalDurabilityPolicy(demoteInWindow);
            try {
                execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                // The gate passed (entered advance()) but the re-check fired — proving the epoch was
                // suppressed by advance()'s re-check, not merely by the gate.
                Assert.assertTrue("epoch gate must have seen local durability ENABLED (entered advance())",
                        sawEnabledAtGate[0]);
                Assert.assertTrue("advance()'s re-check must have seen local durability DISABLED",
                        sawDisabledAtRecheck[0]);

                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("a demote in the epoch window must not advance the durable epoch frontier",
                        0L, tracker.getDurableEpochSeqTxn());
                Assert.assertEquals("a demote in the epoch window must not publish an epoch (lastEpochTs stays 0)",
                        0L, tracker.getLastEpochTs());

                // The epoch trio must NOT have been (re-)created on the demoted node.
                Assert.assertFalse("_snapshot must not be created in the demote window",
                        epochArtifactExists(tt, TableUtils.SNAPSHOT_FILE_NAME, ""));
                Assert.assertFalse("_txn.epoch must not be created in the demote window",
                        epochArtifactExists(tt, TableUtils.TXN_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));
                Assert.assertFalse("_cv.epoch must not be created in the demote window",
                        epochArtifactExists(tt, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.EPOCH_COPY_SUFFIX));

                // Visibility is unaffected — lazy apply still wrote the columns.
                assertQuery("select count() from x")
                        .noLeakCheck()
                        .returnsOnce("""
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
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 0);
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
