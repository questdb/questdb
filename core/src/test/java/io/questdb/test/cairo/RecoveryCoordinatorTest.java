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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit-level oracle for {@link RecoveryCoordinator}: a durable epoch was taken at seqTxn=K, then more
 * rows were applied LAZILY (advancing {@code _txn} to K+M without a new epoch). Recovery must REWIND
 * {@code _txn}/{@code _cv} to the durable epoch cut by copying the immutable {@code _txn.epoch} /
 * {@code _cv.epoch} back over them, so the table opens at exactly {@code epoch.seqTxn}.
 */
public class RecoveryCoordinatorTest extends AbstractCairoTest {

    @Test
    public void testRecoverRestoresTxnToEpochCut() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Epoch interval -1 => the auto-epoch never fires; we drive the durable cut explicitly so the
        // epoch is recorded at a KNOWN seqTxn (K), then apply more after it without a new epoch.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

            execute("create table r (ts timestamp, v long) timestamp(ts) partition by day wal");
            // K rows, fully applied.
            for (int i = 0; i < 3; i++) {
                execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("r");

            // Drive the durable epoch explicitly at the current cut (seqTxn == K): fsyncMaterializedState()
            // writes the immutable _txn.epoch/_cv.epoch copies; then write the _snapshot marker exactly
            // as the apply-worker advance() hook does (this unit test isolates RecoveryCoordinator's
            // restore from the apply-worker cadence — the end-to-end auto-epoch path is covered by the
            // crash oracle).
            final long epochSeqTxn;
            final long epochTxn;
            try (io.questdb.cairo.TableWriter w = getWriter(tt)) {
                w.fsyncMaterializedState();
                epochSeqTxn = w.getSeqTxn();
                epochTxn = w.getTxn();
            }
            Assert.assertTrue("epoch must be at seqTxn >= 3", epochSeqTxn >= 3);
            try (io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
                 Path p = new Path()) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                marker.of(p.$());
                marker.write(epochSeqTxn, epochTxn, 1L);
            }

            // M more rows, applied lazily AFTER the epoch (no new epoch fires).
            for (int i = 3; i < 7; i++) {
                execute("insert into r values ('2024-09-01T0" + i + ":00:00.000000Z', " + i + ")");
            }
            drainWalQueue();

            // Sanity: _txn now reflects the post-epoch frontier (seqTxn advanced past the epoch).
            final long frontierSeqTxn = readTxnSeqTxn(tt);
            Assert.assertTrue("post-epoch _txn seqTxn must be > epoch seqTxn",
                    frontierSeqTxn > epochSeqTxn);

            // Release writers so RecoveryCoordinator can rewrite _txn/_cv unobstructed.
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // ACT: roll forward / restore the durable cut.
            new RecoveryCoordinator(engine).recover();

            // ASSERT: _txn was rewound to exactly the epoch cut.
            final long restoredSeqTxn = readTxnSeqTxn(tt);
            Assert.assertEquals("recovery must restore _txn to the epoch cut", epochSeqTxn, restoredSeqTxn);

            // ASSERT: recoveryIncarnation was bumped exactly once (one successful restore).
            final long incarnation = engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation();
            Assert.assertEquals("recoveryIncarnation must be 1 after one successful restore", 1L, incarnation);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    /**
     * Negative control: a table with NO {@code _snapshot} marker must NOT have its
     * {@code recoveryIncarnation} bumped (the no-op / absent-marker path skips restore).
     */
    @Test
    public void testRecoverWithNoSnapshotLeavesIncarnationZero() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, -1);
        try {
            execute("create table noepoch (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into noepoch values ('2024-09-01T00:00:00.000000Z', 42)");
            drainWalQueue();

            final TableToken tt = engine.verifyTableName("noepoch");

            // Precondition: no _snapshot marker was ever written for this table.
            engine.releaseAllWriters();
            engine.releaseAllReaders();

            // ACT: recovery finds no marker → no restore → incarnation must stay 0.
            new RecoveryCoordinator(engine).recover();

            final long incarnation = engine.getTableSequencerAPI().getTxnTracker(tt).getRecoveryIncarnation();
            Assert.assertEquals("recoveryIncarnation must be 0 when no epoch was restored", 0L, incarnation);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL_MS, 1000);
        }
    }

    private long readTxnSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), io.questdb.cairo.ColumnType.TIMESTAMP_MICRO, io.questdb.cairo.PartitionBy.DAY);
            tx.unsafeLoadAll();
            return tx.getSeqTxn();
        }
    }
}
