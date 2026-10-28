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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * Pinned-client coverage for DROP COLUMN and RENAME COLUMN landing on a
 * SUSPENDED WAL table between two frames of one QWP connection. The
 * connection notices the ALTER only if the sequencer txn tracker follows the
 * log on a suspended table; see {@code QwpTudCache.advanceStructureCheckGate}.
 * <p>
 * The contract pinned here is the one the unsuspended path already honours:
 * a column the frame names that the table no longer has is auto-created, and
 * the acked values land in it.
 * <p>
 * Each test sends two acked frames before the ALTER. The first lookup is a
 * cache miss and does not arm the gate; the second lookup arms it and the
 * second commit moves it onto the connection's own seqTxn, which is the
 * state the ALTER has to be noticed from.
 */
public class QwpSenderSuspendedTableAlterE2ETest extends AbstractQwpWebSocketTest {

    @Test
    public void testDropColumnOnSuspendedTableBetweenFrames() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (QwpWebSocketSender sender = connectWs(port)) {
                sendTwoAckedFrames(sender);

                execute("ALTER TABLE t SUSPEND WAL");
                execute("ALTER TABLE t DROP COLUMN v");
                assertTrackerFollowsLog();

                sender.table("t").longColumn("v", 200).at(1_000_000_002_000L, ChronoUnit.MICROS);
                Assert.assertTrue("frame after the ALTER must be acked", sender.drain(30_000));

                execute("ALTER TABLE t RESUME WAL");
            }
            drainWalQueue();
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            1970-01-12T13:46:40.000000Z\tnull
                            1970-01-12T13:46:40.001000Z\tnull
                            1970-01-12T13:46:40.002000Z\t200
                            """);
        });
    }

    @Test
    public void testDropColumnOnSuspendedTableThenResumeBeforeFrame() throws Exception {
        // The ALTER lands while suspended, the table resumes, and only then does
        // the next frame arrive. RESUME does not move the tracker on its own
        // (resumeTable() notifies with Long.MAX_VALUE), and the apply job only
        // moves writerTxn; CheckWalTransactionsJob is what would close the gap,
        // and in production it ticks every cairo.wal.sequencer.check.interval.
        // Run the apply job alone here so the test models that window instead
        // of drainWalQueue(), which also runs the check job and would mask a
        // tracker that stalled on the suspended ALTER.
        runInContext((port) -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (QwpWebSocketSender sender = connectWs(port)) {
                sendTwoAckedFrames(sender);

                execute("ALTER TABLE t SUSPEND WAL");
                execute("ALTER TABLE t DROP COLUMN v");
                execute("ALTER TABLE t RESUME WAL");
                try (ApplyWal2TableJob job = createWalApplyJob(engine)) {
                    //noinspection StatementWithEmptyBody
                    while (job.run()) ;
                }
                assertTrackerFollowsLog();

                sender.table("t").longColumn("v", 200).at(1_000_000_002_000L, ChronoUnit.MICROS);
                Assert.assertTrue("frame after the ALTER must be acked", sender.drain(30_000));
            }
            drainWalQueue();
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv
                            1970-01-12T13:46:40.000000Z\tnull
                            1970-01-12T13:46:40.001000Z\tnull
                            1970-01-12T13:46:40.002000Z\t200
                            """);
        });
    }

    @Test
    public void testRenameColumnOnSuspendedTableBetweenFrames() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try (QwpWebSocketSender sender = connectWs(port)) {
                sendTwoAckedFrames(sender);

                execute("ALTER TABLE t SUSPEND WAL");
                execute("ALTER TABLE t RENAME COLUMN v TO w");
                assertTrackerFollowsLog();

                sender.table("t").longColumn("v", 200).at(1_000_000_002_000L, ChronoUnit.MICROS);
                Assert.assertTrue("frame after the ALTER must be acked", sender.drain(30_000));

                execute("ALTER TABLE t RESUME WAL");
            }
            drainWalQueue();
            // w keeps the two pre-rename values; v is auto-created for the frame
            // that still names it, exactly as on an unsuspended table.
            assertQuery("SELECT * FROM t")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tw\tv
                            1970-01-12T13:46:40.000000Z\t100\tnull
                            1970-01-12T13:46:40.001000Z\t150\tnull
                            1970-01-12T13:46:40.002000Z\tnull\t200
                            """);
        });
    }

    // Pins the premise the three scenarios depend on: after the ALTER the
    // tracker sits on the sequencer's last txn, so the QWP gate sees it.
    private static void assertTrackerFollowsLog() {
        final TableToken token = engine.getTableTokenIfExists("t");
        Assert.assertNotNull(token);
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        Assert.assertEquals(
                "tracker must follow the sequencer log across an ALTER on a suspended table",
                engine.getTableSequencerAPI().lastTxn(token),
                tracker.getSeqTxn()
        );
    }

    private static void sendTwoAckedFrames(QwpWebSocketSender sender) {
        sender.table("t").longColumn("v", 100).at(1_000_000_000_000L, ChronoUnit.MICROS);
        Assert.assertTrue(sender.drain(30_000));
        sender.table("t").longColumn("v", 150).at(1_000_000_001_000L, ChronoUnit.MICROS);
        Assert.assertTrue(sender.drain(30_000));
    }
}
