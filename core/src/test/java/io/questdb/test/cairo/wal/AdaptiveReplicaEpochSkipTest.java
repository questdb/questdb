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
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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
}
