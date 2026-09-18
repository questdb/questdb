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
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdaptiveMetricsTest extends AbstractCairoTest {

    @Test
    public void testLocalDurableSeqTxnGaugeAdvancesUnderAdaptive() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values (0, 1)");
            execute("insert into x values (1000000, 2)");
            execute("insert into x values (2000000, 3)");
            drainWalQueue();

            SeqTxnTracker tracker = engine.getTableSequencerAPI()
                    .getTxnTracker(engine.verifyTableName("x"));
            long localDurable = tracker.getLocalDurableSeqTxn();
            assertTrue("adaptive commits should make the frontier durable", localDurable > 0);
            // Global gauge == this single table's local-durable frontier.
            assertEquals(localDurable,
                    TestUtils.getMetricValue(engine, "questdb_wal_apply_local_durable_seq_txn"));
        });
    }

    @Test
    public void testLocalDurableSeqTxnGaugeReleasedOnDrop() throws Exception {
        // Review LOW: an adaptive table that advanced the process-wide durable-ack frontier gauge
        // (wal_apply_local_durable_seq_txn) must RELEASE its contribution when the table is dropped, or the
        // gauge leaks the last value forever. notifyOnDrop routes through resetDurableFrontier() so there is
        // exactly ONE decrement (no double-count).
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values (0, 1)");
            execute("insert into x values (1000000, 2)");
            drainWalQueue();

            final SeqTxnTracker tracker = engine.getTableSequencerAPI()
                    .getTxnTracker(engine.verifyTableName("x"));
            final long v = tracker.getLocalDurableSeqTxn();
            assertTrue("adaptive commits should advance the frontier", v > 0);
            assertEquals(v, TestUtils.getMetricValue(engine, "questdb_wal_apply_local_durable_seq_txn"));

            // Real drop routes through TableSequencerAPI.dropTable -> SeqTxnTracker.notifyOnDrop.
            execute("drop table x");
            drainWalQueue();
            assertEquals("drop releases the table's gauge contribution (no observability leak)",
                    0, TestUtils.getMetricValue(engine, "questdb_wal_apply_local_durable_seq_txn"));

            // Idempotent: a second drop notification on the same tracker must NOT double-decrement the gauge
            // below 0 (the dropped guard + resetDurableFrontier's current>0 guard both prevent it).
            tracker.notifyOnDrop();
            assertEquals("second drop is a no-op — no double-count",
                    0, TestUtils.getMetricValue(engine, "questdb_wal_apply_local_durable_seq_txn"));
            assertEquals("frontier reset to the uninitialised -1 on drop", -1, tracker.getLocalDurableSeqTxn());
        });
    }

    @Test
    public void testEpochAdvancesCounterIncrements() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, "0"); // advance every batch
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            long before = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_epoch_advances_total");
            execute("insert into x values (0, 1)");
            drainWalQueue();
            long after = TestUtils.getMetricValue(engine, "questdb_wal_adaptive_epoch_advances_total");
            assertTrue("each adaptive apply batch advances the durable epoch", after > before);
        });
    }
}
