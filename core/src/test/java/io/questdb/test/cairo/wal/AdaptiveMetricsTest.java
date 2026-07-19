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
