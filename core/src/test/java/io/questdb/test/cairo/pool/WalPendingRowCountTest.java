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

package io.questdb.test.cairo.pool;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * wal_pending_row_count in tables() counts WAL rows committed but not yet applied, so it can
 * never be negative.
 * <p>
 * It goes negative when an apply batch subtracts rows the counter never held. The counter is
 * per-process and in-memory: rows committed before this process started counting a table are
 * absent from it, and the table's pending-row floor marks where counting began. A single
 * block-apply batch can span that floor - the common case is the first batch after a restart,
 * which applies a pre-existing backlog together with freshly committed transactions - so the
 * floor has to be applied per transaction, not to the batch as a whole.
 */
public class WalPendingRowCountTest extends AbstractCairoTest {

    private static final long DAY = 86_400_000_000L;

    @Test
    public void testDedupUpsertRewriteDoesNotGoNegative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            engine.getRecentWriteTracker().clear();
            TableToken tt = engine.verifyTableName("t");
            for (int b = 0; b < 10; b++) {
                writeRows(tt, DAY * (b + 1), 500);
            }
            drainWalQueue();
            // rewrite the same partitions - every row is a duplicate
            for (int b = 0; b < 10; b++) {
                writeRows(tt, DAY * (b + 1), 500);
            }
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    @Test
    public void testO3BackfillDoesNotGoNegative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.getRecentWriteTracker().clear();
            TableToken tt = engine.verifyTableName("t");
            writeRows(tt, DAY * 100, 100);
            drainWalQueue();
            // backfill many older partitions, all committed before a single drain so that the
            // apply job block-applies them as one batch
            for (int b = 0; b < 20; b++) {
                writeRows(tt, DAY * (b + 1), 500);
            }
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    @Test
    public void testPendingRowsCountedWhileWalIsOutstanding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            engine.getRecentWriteTracker().clear();
            TableToken tt = engine.verifyTableName("t");
            for (int b = 0; b < 4; b++) {
                writeRows(tt, DAY * (b + 1), 250);
            }
            // nothing applied yet - all 1000 rows are pending
            assertPendingRows("t", 1000);
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    @Test
    public void testReplaceRangeRewriteDoesNotGoNegative() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
            engine.getRecentWriteTracker().clear();
            TableToken tt = engine.verifyTableName("t");
            for (int b = 0; b < 10; b++) {
                writeRows(tt, DAY * (b + 1), 200);
            }
            drainWalQueue();
            for (int b = 0; b < 10; b++) {
                long lo = DAY * (b + 1);
                try (WalWriter ww = engine.getWalWriter(tt)) {
                    for (int i = 0; i < 50; i++) {
                        TableWriter.Row row = ww.newRow(lo + i * 1_000_000L);
                        row.putInt(1, i);
                        row.append();
                    }
                    ww.commitWithParams(lo, lo + DAY, WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE);
                }
            }
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    /**
     * The floor is seeded by whichever component sees the table first. Here the apply job and
     * ingestion get there before CheckWalTransactionsJob's first sweep, which is what happens on
     * a busy server: the sweep then finds the txn tracker already initialised and never sets the
     * floor at all. The seed has to come from the write/apply path for the backlog to be
     * protected.
     */
    @Test
    public void testRestartWithBacklogApplyRacesCheckJob() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tt = engine.verifyTableName("t");
            for (int b = 0; b < 6; b++) {
                writeRows(tt, DAY * (b + 1), 1000);
            }

            restart();

            // ingestion resumes before the check job's first sweep
            for (int b = 6; b < 10; b++) {
                writeRows(tt, DAY * (b + 1), 1000);
            }
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    /**
     * Same restart, but CheckWalTransactionsJob wins the race and does set the floor. The batch
     * still spans it, so the floor has to be honoured per transaction.
     */
    @Test
    public void testRestartWithBacklogCheckJobSetsFloor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            TableToken tt = engine.verifyTableName("t");
            for (int b = 0; b < 6; b++) {
                writeRows(tt, DAY * (b + 1), 1000);
            }

            restart();
            new CheckWalTransactionsJob(engine).run();

            for (int b = 6; b < 10; b++) {
                writeRows(tt, DAY * (b + 1), 1000);
            }
            // the 4000 post-restart rows are pending; the 6000-row backlog is below the floor
            // and was never counted
            assertPendingRows("t", 4000);
            drainWalQueue();
            assertPendingRows("t", 0);
        });
    }

    private void assertPendingRows(String table, long expected) throws Exception {
        assertQuery("SELECT wal_pending_row_count FROM tables() WHERE table_name = '" + table + "'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("wal_pending_row_count\n" + expected + "\n");
    }

    /**
     * Drops all in-memory WAL tracking state, leaving the WAL backlog on disk, the way a server
     * restart does.
     */
    private void restart() {
        engine.getTableSequencerAPI().releaseAll();
        engine.getRecentWriteTracker().clear();
    }

    private void writeRows(TableToken tt, long baseTs, int count) {
        try (WalWriter ww = engine.getWalWriter(tt)) {
            for (int i = 0; i < count; i++) {
                TableWriter.Row row = ww.newRow(baseTs + i * 1_000_000L);
                row.putInt(1, i);
                row.append();
            }
            ww.commit();
        }
    }
}
