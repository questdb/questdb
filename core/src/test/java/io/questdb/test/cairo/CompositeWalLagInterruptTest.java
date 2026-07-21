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
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Composite-partitioning Plan #5, Task 3 -- regression guard for the drain-interrupt data-loss bug in the
 * flag-gated cell-aware WAL lag.
 * <p>
 * The composite accumulate path bumps {@code txWriter.lagTxnCount} for rows that live ONLY in the RAM
 * {@code compositeWalLagBuffer} (unlike the plain lag, whose rows are durable in the last-partition lag
 * column files, the composite buffer has no on-disk representation). If the WAL apply loop is interrupted
 * with the buffer non-empty -- a per-table time-quota expiry or a graceful shutdown, both of which call
 * {@code TableWriter.commitSeqTxn()} -- and the buffer is not flushed first, that {@code lagTxnCount} is
 * persisted to {@code _txn}. The writer is then released (its RAM buffer discarded), and the next apply /
 * a cold restart opens the cursor at {@code getAppliedSeqTxn() == seqTxn + lagTxnCount} and SILENTLY SKIPS
 * those transactions -- their rows exist nowhere durable. This is the exact composite analog of
 * {@code WalTableSqlTest.testWhenApplyJobTerminatesEarlierLagCommitted} (which proves the plain lag
 * survives the same interrupt) and is driven the same way: a {@link FilesFacade} trips the job's
 * {@code isTerminating} signal the instant a WAL segment data file is read, so the apply loop breaks right
 * after a transaction has accumulated -- before any clean forced flush.
 * <p>
 * The table is first seeded with one drained row so its cell registry is non-empty: on a fresh composite
 * table the very first commit is treated as dormant and full-commits (bypassing the lag), so the seed is
 * what makes the subsequent transactions actually accumulate into the RAM buffer.
 * <p>
 * Pre-fix this test is RED: the first accumulated transaction's rows are lost. Post-fix
 * {@code commitSeqTxn} flushes the buffer before persisting {@code lagTxnCount}, so nothing is lost and the
 * composite table matches a plain twin fed the identical stream.
 */
public class CompositeWalLagInterruptTest extends AbstractCairoTest {

    private static final String[] EXCH = {"A", "B", "C"};
    private static final int ROWS_PER_TXN = 12;
    private static final int TOTAL_ROWS = 96;     // 8 transactions of 12 rows
    private static final int TXN_COUNT = TOTAL_ROWS / ROWS_PER_TXN;
    // A single seed row (distinct, late timestamp) drained before the accumulating stream, so the composite
    // table is non-dormant (cell registry populated) and the stream accumulates rather than full-commits.
    private static final String SEED_ROW = "('2020-01-02T12:00:00.000000Z','A',-1.0)";

    @Override
    public void setUp() {
        // Config must be in place BEFORE super.setUp() rebuilds the engine's configuration.
        setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_LAG_ENABLED, "true");
        // getWalMaxLagRows = walLagRowsMultiplier(20) * maxUncommittedRows(1) = 20, so a single 12-row
        // out-of-order transaction stays sub-threshold and ACCUMULATES into the RAM lag buffer instead of
        // force-committing (see needFullCommit's OR-chain in processWalCommit).
        setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, "1");
        // Neutralise the commit-latency force-commit clause so the transactions accumulate rather than
        // flush on a slow CI box.
        setProperty(PropertyKey.CAIRO_COMMIT_LATENCY, "600000000");
        super.setUp();
    }

    @Test
    public void testDrainInterruptMidAccumulationDoesNotLoseComposite() throws Exception {
        final AtomicBoolean isTerminating = new AtomicBoolean(false);
        // Disarmed during the seed drain; armed only for the accumulating stream so exactly one interrupt
        // lands on an accumulating transaction (buffer non-empty).
        final AtomicBoolean interruptArmed = new AtomicBoolean(false);
        final Job.WorkerContext runStatus = new Job.WorkerContext() {
            @Override
            public int carrierId() {
                return 0;
            }

            @Override
            public boolean isTerminating() {
                return isTerminating.get();
            }
        };

        final FilesFacade ff = new TestFilesFacadeImpl() {
            // Terminate the WAL apply job the moment a WAL segment data file is read while armed -- i.e.
            // after the first stream transaction has been accumulated into the RAM lag buffer but before
            // any clean forced flush. px.d is a plain data column, only opened while applying data, so the
            // break lands with the buffer non-empty. One-shot: the latch disarms itself.
            @Override
            public long openRO(LPSZ name) {
                if (interruptArmed.get() && Utf8s.containsAscii(name, "wal") && Utf8s.endsWithAscii(name, "px.d")) {
                    isTerminating.set(true);
                    interruptArmed.set(false);
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(ff, () -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            // Seed both tables (drained) so the composite table is non-dormant before the accumulating run.
            execute("insert into c values " + SEED_ROW);
            execute("insert into p values " + SEED_ROW);
            drainWalQueue();
            engine.releaseInactive();

            // Queue TXN_COUNT out-of-order transactions; each now accumulates into the composite RAM lag.
            ingestShuffledTransactions("c");

            interruptArmed.set(true);
            try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
                // First run: accumulates the first stream transaction, then the FilesFacade trips
                // isTerminating so the apply loop breaks and calls writer.commitSeqTxn() with the RAM lag
                // buffer NON-EMPTY -- the drain-interrupt boundary this test guards.
                walApplyJob.run(runStatus);
                // Cold reopen: release the writer so its RAM buffer is discarded, exactly as a graceful
                // shutdown / writer eviction would. The next apply opens fresh and reads the applied
                // position from _txn -- where, pre-fix, the phantom lagTxnCount has already been persisted.
                engine.releaseInactive();
                isTerminating.set(false);
                interruptArmed.set(false);
                // Drain the remaining transactions cleanly.
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run(runStatus)) ;
            }
            engine.releaseInactive();

            assertWalTableNotSuspended("c");

            // Plain twin fed the identical stream via a normal (uninterrupted) drain.
            ingestShuffledTransactions("p");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // The interrupted composite table must have LOST NOTHING. Pre-fix the first accumulated
            // transaction's rows are silently skipped, so both the row count and the full scan diverge.
            assertSqlCursors("select count() from p", "select count() from c");
            assertSqlCursors(
                    "select ts, exch, px from p order by ts, exch, px",
                    "select ts, exch, px from c order by ts, exch, px");
        });
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    private void ingestShuffledTransactions(String table) throws SqlException {
        // Deterministic permutation of 0..TOTAL_ROWS-1 (gcd(17,96)=1), grouped into TXN_COUNT
        // transactions of ROWS_PER_TXN rows each -- each transaction carries a spread of timestamps
        // (including high ones) while later transactions backfill earlier ones, so commit-to-timestamp
        // stays below each transaction's max lag timestamp and the transaction accumulates.
        for (int t = 0; t < TXN_COUNT; t++) {
            StringBuilder sb = new StringBuilder("insert into ").append(table).append(" values ");
            for (int i = 0; i < ROWS_PER_TXN; i++) {
                int k = ((t * ROWS_PER_TXN + i) * 17) % TOTAL_ROWS;
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(rowValues(k));
            }
            execute(sb.toString());
        }
    }

    // Row k (0..TOTAL_ROWS-1): day = 2020-01-0(1+k%2), exch = EXCH[k%3], ts minute-of-hour = k (distinct
    // within any cell), px = k+0.5 (globally unique -> no ordering ties).
    private String rowValues(int k) {
        String day = "2020-01-0" + (1 + k % 2);
        String exch = EXCH[k % 3];
        int hour = k / 60;
        int minute = k % 60;
        String hh = (hour < 10 ? "0" : "") + hour;
        String mm = (minute < 10 ? "0" : "") + minute;
        return "('" + day + 'T' + hh + ':' + mm + ":00.000000Z','" + exch + "'," + (k + 0.5) + ')';
    }
}
