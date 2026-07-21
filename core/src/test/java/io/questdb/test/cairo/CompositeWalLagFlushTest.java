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
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite-partitioning Plan #5, Task 3 -- the flag-gated cell-aware WAL-lag integration.
 * <p>
 * Two arms:
 * <ul>
 *     <li><b>FLAG-OFF byte-identity</b> ({@link #testFlagOffCompositeByteIdenticalToPlainTwin}): with the
 *     new {@code cairo.wal.composite.lag.enabled} flag at its default (off), a high-frequency multi-cell
 *     composite ingestion is identical to a plain twin fed the same rows -- i.e. the existing composite
 *     full-commit path is untouched. Permanent regression guard for the default path.</li>
 *     <li><b>FLAG-ON equivalence</b> ({@link #testFlagOnCompositeEquivalentToPlainAndSingleCommit}): with
 *     the flag on and a small max-uncommitted-rows (so the WAL apply job routes each transaction through
 *     the single-transaction commit path -- the block path is a separate batching mechanism -- and the
 *     cell-aware RAM lag actually accumulates), a long interleaved multi-cell stream -- with transactions
 *     arriving out of order so later flushes extend cells earlier flushes already wrote to disk (genuine
 *     {@code O3_BLOCK_MERGE}) -- produces data identical to (a) the plain twin fed the same stream and (b)
 *     a composite table fed the same rows in ONE big commit, across every query shape (ordered scan,
 *     per-cell filter, LATEST ON, SAMPLE BY). It also asserts the lag actually batched (fewer physical O3
 *     commits than transactions), so it fails if the flag is ignored or the lag never engages.</li>
 * </ul>
 */
public class CompositeWalLagFlushTest extends AbstractCairoTest {

    private static final String[] EXCH = {"A", "B", "C"};
    // 12 rows/txn: two 12-row transactions (24) overflow getWalMaxLagRows (=20 under maxUncommittedRows=1),
    // so the apply job processes each transaction one at a time (block==1) -- the cell-aware lag's path.
    private static final int ROWS_PER_TXN = 12;
    private static final int TOTAL_ROWS = 96;     // 8 transactions of 12 rows
    private static final int TXN_COUNT = TOTAL_ROWS / ROWS_PER_TXN;

    @Override
    public void setUp() {
        // Config must be in place BEFORE super.setUp() rebuilds the engine's configuration.
        if (testName.getMethodName().contains("FlagOn")) {
            setProperty(PropertyKey.CAIRO_WAL_COMPOSITE_LAG_ENABLED, "true");
            // getWalMaxLagRows = walLagRowsMultiplier(20) * maxUncommittedRows(1) = 20. That makes the
            // apply job's per-transaction block cap 20 rows, so any two 12-row transactions overflow it
            // and are processed one at a time (block==1) -- the path the cell-aware lag lives on. It also
            // makes the lag flush after ~2 transactions (> 20 rows), so flushes interleave with the
            // out-of-order stream and later flushes merge into already-populated cells.
            setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, "1");
        }
        super.setUp();
    }

    @Test
    public void testFlagOffCompositeByteIdenticalToPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertFalse(configuration.isWalCompositeLagEnabled());

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");

            ingestShuffledTransactions("c");
            ingestShuffledTransactions("p");
            drainWalQueue();

            assertWalTableNotSuspended("c");
            assertWalTableNotSuspended("p");
            assertShapesMatch("p", "c");
        });
    }

    @Test
    public void testFlagOnCompositeEquivalentToPlainAndSingleCommit() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(configuration.isWalCompositeLagEnabled());

            execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
            execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
            execute("create table c1 (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");

            // Ingest the composite table (one WAL txn per 12-row group) and drain in isolation so the
            // o3-commit metric delta belongs to `c` alone.
            ingestShuffledTransactions("c");
            long o3CommitsBefore = engine.getMetrics().tableWriterMetrics().getO3CommitCount();
            drainWalQueue();
            long o3CommitsForC = engine.getMetrics().tableWriterMetrics().getO3CommitCount() - o3CommitsBefore;
            assertWalTableNotSuspended("c");

            // The lag must actually batch: on the single-transaction path each composite commit would
            // otherwise do a full O3 commit (one per transaction). With the lag it is strictly fewer.
            Assert.assertTrue(
                    "cell-aware lag should batch commits: o3 commits (" + o3CommitsForC + ") < txns (" + TXN_COUNT + ')',
                    o3CommitsForC < TXN_COUNT);
            Assert.assertTrue("at least one flush expected", o3CommitsForC >= 1);

            // Plain twin, same stream.
            ingestShuffledTransactions("p");
            drainWalQueue();
            assertWalTableNotSuspended("p");

            // Composite reference fed the identical rows in ONE big commit.
            ingestOneCommit("c1");
            drainWalQueue();
            assertWalTableNotSuspended("c1");

            // Lag-batched composite == plain twin == single-commit composite, across every shape.
            assertShapesMatch("p", "c");
            assertShapesMatch("c1", "c");
        });
    }

    private void assertShapesMatch(String ref, String actual) throws SqlException {
        // Full ordered scan.
        assertSqlCursors(
                "select ts, exch, px from " + ref + " order by ts, exch, px",
                "select ts, exch, px from " + actual + " order by ts, exch, px");
        // Table-wide count.
        assertSqlCursors("select count() from " + ref, "select count() from " + actual);
        // Per-cell (per-exch) filtered scans.
        for (String exch : EXCH) {
            String pred = " where exch = '" + exch + "' order by ts, px";
            assertSqlCursors(
                    "select ts, px from " + ref + pred,
                    "select ts, px from " + actual + pred);
        }
        // LATEST ON per exch.
        assertSqlCursors(
                "select ts, exch, px from " + ref + " latest on ts partition by exch order by exch",
                "select ts, exch, px from " + actual + " latest on ts partition by exch order by exch");
        // SAMPLE BY day rollup.
        assertSqlCursors(
                "select ts, count(), sum(px) from " + ref + " sample by 1d",
                "select ts, count(), sum(px) from " + actual + " sample by 1d");
    }

    private void assertWalTableNotSuspended(String tableName) {
        Assert.assertFalse(
                tableName + " must not be suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(tableName)));
    }

    // Row k (0..TOTAL_ROWS-1): day = 2020-01-0(1+k%2), exch = EXCH[k%3], ts minute-of-hour = k (distinct
    // within any cell), px = k+0.5 (globally unique -> no ordering ties). Timestamps are assigned in k
    // order but rows are ingested in a fixed permutation of k, so later transactions carry earlier-ts rows
    // for cells earlier transactions already flushed -> out-of-order merges.
    private String rowValues(int k) {
        String day = "2020-01-0" + (1 + k % 2);
        String exch = EXCH[k % 3];
        int hour = k / 60;
        int minute = k % 60;
        String hh = (hour < 10 ? "0" : "") + hour;
        String mm = (minute < 10 ? "0" : "") + minute;
        return "('" + day + 'T' + hh + ':' + mm + ":00.000000Z','" + exch + "'," + (k + 0.5) + ')';
    }

    private void ingestOneCommit(String table) throws SqlException {
        StringBuilder sb = new StringBuilder("insert into ").append(table).append(" values ");
        for (int k = 0; k < TOTAL_ROWS; k++) {
            if (k > 0) {
                sb.append(", ");
            }
            sb.append(rowValues(k));
        }
        execute(sb.toString());
    }

    private void ingestShuffledTransactions(String table) throws SqlException {
        // Deterministic permutation of 0..TOTAL_ROWS-1 (gcd(17,48)=1), grouped into TXN_COUNT
        // transactions of ROWS_PER_TXN rows each.
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
}
