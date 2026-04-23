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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.InMemoryTable;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewTimerJob;
import io.questdb.cairo.lv.MergeBuffer;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class LiveViewTest extends AbstractCairoTest {

    @Test
    public void testBaseTableDropColumnInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("ALTER TABLE trades DROP COLUMN price");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("drop column operation", instance.getInvalidationReason());
    }

    @Test
    public void testBaseTableDropInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("DROP TABLE trades");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("base table drop", instance.getInvalidationReason());
    }

    @Test
    public void testBaseTableTruncateInvalidatesLiveView() throws Exception {
        createBaseTableAndLiveView();
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        execute("TRUNCATE TABLE trades");
        drainWalQueue();

        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("truncate operation", instance.getInvalidationReason());
    }

    @Test
    public void testBoundedBackfillDropsRowsOlderThanRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // Pre-populate the base table with rows spanning 2 hours. With RETENTION 10m,
        // bootstrap must skip the first 4 rows (older than max_ts - 10m) and seed the
        // view with only the last 2. Window functions see the retained horizon only,
        // so row_number starts at 1 on the earliest retained row.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:30:00.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T01:00:00.000000Z')," +
                " ('AAPL', 103.0, '2024-01-01T01:30:00.000000Z')," +
                " ('AAPL', 104.0, '2024-01-01T01:55:00.000000Z')," +
                " ('AAPL', 105.0, '2024-01-01T02:00:00.000000Z')");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_bb LAG 1s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t104.0\t2024-01-01T01:55:00.000000Z\t1\n" +
                        "AAPL\t105.0\t2024-01-01T02:00:00.000000Z\t2\n",
                "SELECT * FROM live_bb",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_bb");
    }

    @Test
    public void testColdPathDiskReadReplayForUnboundedView() throws Exception {
        // Cold-path disk-read replay for any-unbounded views: a row older than the
        // merge buffer's retention coverage (ts < maxTsSeen - retention) bypasses the
        // merge buffer entirely. The refresh job expands {@code stateHorizonTs} and
        // re-bootstraps from the base table over [horizon, maxTs], so the unbounded
        // window function's accumulator reflects the cold row without reaching for
        // the all-bounded skip counter.
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_cold LAG 1s RETENTION 10s AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:05.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:10.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Sanity check before the cold row: bounded backfill uses ts > maxTs -
        // retention, so the boundary row at exactly 0s sits outside state and the
        // remaining two rows land with rn=1, 2.
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:05.000000Z\t1\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:10.000000Z\t2\n",
                "SELECT * FROM live_cold",
                null,
                "ts",
                true,
                true
        );

        // Insert a row whose ts is four years before the currently visible range.
        // For an unbounded view, this row cannot ride through the merge buffer: its
        // ts sits below the merge buffer's retention coverage, so the refresh
        // diverts it to disk-read replay.
        execute("INSERT INTO trades VALUES ('AAPL', 1.0, '2020-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_cold");
        // All-bounded cold-path skip counter stays at 0 — disk-read replay is a
        // different mechanism.
        Assert.assertEquals(
                "unbounded view must not trigger all-bounded cold-path skip",
                0,
                instance.getColdRowSkipCount()
        );
        // Cold rows skip the merge buffer (the late-row path). Late-row accounting
        // therefore stays at zero even though the row is dramatically out of order.
        Assert.assertEquals(
                "cold row must not pass through the merge buffer",
                0,
                instance.getMergeBuffer().getLateRowCount()
        );
        // Disk-read replay expanded the state horizon to one tick below the cold
        // row's ts — bootstrap uses a strict > lower bound, so the stored horizon
        // is the exclusive bound that keeps the cold row in state.
        Assert.assertEquals(
                "state horizon must expand to include the cold row",
                MicrosFormatUtils.parseUTCTimestamp("2020-01-01T00:00:00.000000Z") - 1,
                instance.getStateHorizonTs()
        );
        // Disk-read rebuild scanned (newHorizon, maxTs], which is (2020 - 1µs,
        // 2024-01-01T00:00:10] — the 0s boundary row is now part of state too.
        // applyRetention evicts ts <= 0s, leaving the two newer rows visible with
        // shifted rn values reflecting the four upstream rows in state.
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:05.000000Z\t3\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:10.000000Z\t4\n",
                "SELECT * FROM live_cold",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_cold");
    }

    @Test
    public void testColdPathHorizonPersistsAcrossWarmPath() throws Exception {
        // Once the state horizon expands via a cold row, subsequent warm-path replays
        // (late rows within retention) must preserve that horizon. Without the
        // disk-read branch in drainAndCommit, a merge-buffer-only replay would
        // rebuild state from the retention window alone and silently drop the cold
        // row's contribution to the accumulator.
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_horizon LAG 1s RETENTION 10s AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:05.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:10.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Cold row: expands the horizon, rn shifts by 1 across the visible range.
        execute("INSERT INTO trades VALUES ('AAPL', 1.0, '2020-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Warm row: within retention but arrives late. For an any-unbounded view
        // whose horizon has been expanded, drainAndCommit must route warm-path
        // replay back through disk-read so the cold row stays in the accumulator.
        execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T00:00:07.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_horizon");
        // Horizon stays at the cold row's exclusive lower bound even after the warm
        // path fired — the disk-read branch in drainAndCommit is what keeps it
        // there.
        Assert.assertEquals(
                "horizon must persist across warm-path disk-read replay",
                MicrosFormatUtils.parseUTCTimestamp("2020-01-01T00:00:00.000000Z") - 1,
                instance.getStateHorizonTs()
        );
        // The warm row is inserted between 5s and 10s. State now carries five rows
        // (cold + 0s + 5s + warm + 10s). applyRetention evicts ts <= 0s, so visible
        // output shows the last three with rn shifted by 2 (two evicted but
        // accumulated rows precede them in state).
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:05.000000Z\t3\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:07.000000Z\t4\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:10.000000Z\t5\n",
                "SELECT * FROM live_horizon",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_horizon");
    }

    @Test
    public void testMultipleColdRowsInOneBatchTakeMinHorizon() throws Exception {
        // When two cold rows arrive in the same WAL batch, the horizon expands to
        // the older of the two — a single disk-read replay picks up both.
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_multi_cold LAG 1s RETENTION 10s AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:05.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:10.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Two cold rows in one batch at different depths.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 2.0, '2021-01-01T00:00:00.000000Z')," +
                " ('AAPL', 1.0, '2020-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_multi_cold");
        // Horizon advances to the older of the two cold rows (stored as the
        // exclusive lower bound used by the disk-read replay).
        Assert.assertEquals(
                "horizon must take the min of all cold timestamps in a batch",
                MicrosFormatUtils.parseUTCTimestamp("2020-01-01T00:00:00.000000Z") - 1,
                instance.getStateHorizonTs()
        );
        // Accumulator sees five rows (2020, 2021, 0s, 5s, 10s). applyRetention
        // evicts ts <= 0s, leaving the two newer rows visible with rn=4, 5.
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:05.000000Z\t4\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:10.000000Z\t5\n",
                "SELECT * FROM live_multi_cold",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_multi_cold");
    }

    @Test
    public void testLateRowWithinRetentionTriggersWarmPathReplay() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_late LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // First batch: three AAPL rows in order. Expected rn=1,2,3.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:02.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:04.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:04.000000Z\t3\n",
                "SELECT * FROM live_late",
                null,
                "ts",
                true,
                true
        );

        // Second batch: a late row at ts=1s (within retention, outside LAG). It must be
        // inserted between the ts=0 and ts=2 rows in sort order, and row_number must
        // restart from 1 on replay so that downstream rn values renumber.
        execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:02.000000Z\t3\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:04.000000Z\t4\n",
                "SELECT * FROM live_late",
                null,
                "ts",
                true,
                true
        );

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_late");
        Assert.assertEquals(1, instance.getMergeBuffer().getLateRowCount());

        execute("DROP LIVE VIEW live_late");
    }

    @Test
    public void testMultipleLateRowsInOneBatchReplayOnce() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_multi LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:05.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:10.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Two late rows in a single WAL batch: 2s and 7s. Both are within retention
        // but before existing drained rows at 5s and 10s. A single warm-path replay
        // renumbers all rows in one pass; pendingLateCount resets after the drain.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:02.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:07.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:05.000000Z\t3\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:07.000000Z\t4\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:10.000000Z\t5\n",
                "SELECT * FROM live_multi",
                null,
                "ts",
                true,
                true
        );

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_multi");
        Assert.assertEquals(2, instance.getMergeBuffer().getLateRowCount());
        Assert.assertEquals(0, instance.getMergeBuffer().getPendingLateCount());

        execute("DROP LIVE VIEW live_multi");
    }

    @Test
    public void testLateRowAcrossPartitionsReplaysIndependentCounters() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_part LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2000.0, '2024-01-01T00:00:02.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:04.000000Z')," +
                " ('GOOG', 2001.0, '2024-01-01T00:00:06.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Late row lands between GOOG's two rows (ts=2s and ts=6s). GOOG's counter must
        // renumber; AAPL's counter must also renumber because row_number replays from the
        // retention horizon across all partitions.
        execute("INSERT INTO trades VALUES ('GOOG', 2500.0, '2024-01-01T00:00:05.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2000.0\t2024-01-01T00:00:02.000000Z\t1\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:04.000000Z\t2\n" +
                        "GOOG\t2500.0\t2024-01-01T00:00:05.000000Z\t2\n" +
                        "GOOG\t2001.0\t2024-01-01T00:00:06.000000Z\t3\n",
                "SELECT * FROM live_part",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_part");
    }

    @Test
    public void testIncrementalRefreshResolvesPerTxnSymbolKeys() throws Exception {
        // Regression for the SYMBOL key collision: the WAL writer assigns symbol
        // keys as `initialSymCount + localId`, with `localId` reset between commits.
        // When initialSymCount stays at zero (no WAL apply has refreshed the clean
        // count), every transaction reuses local ids 0, 1, 2... so two transactions
        // can have key 0 mapping to different symbols. Without per-txn translation,
        // the cumulative symbolMaps in WalReader resolves all rows to the last-seen
        // symbol. Here that would have been MSFT (last INSERT), giving rows
        // GOOG/AAPL/MSFT all the symbol "MSFT".
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_sym LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades");
        drainLiveViewQueue();

        // Bootstrap with one row so the next refresh takes the incremental branch.
        execute("INSERT INTO trades VALUES ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // Three INSERTs queued before drain produce three separate WAL transactions.
        // Each transaction's diff entries use local id 0 for its single new symbol;
        // per-txn overlay must map them to GOOG / AAPL / MSFT respectively.
        execute("INSERT INTO trades VALUES ('GOOG', 200.0, '2024-01-01T00:00:01.000000Z')");
        execute("INSERT INTO trades VALUES ('AAPL', 300.0, '2024-01-01T00:00:02.000000Z')");
        execute("INSERT INTO trades VALUES ('MSFT', 400.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t200.0\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "AAPL\t300.0\t2024-01-01T00:00:02.000000Z\t3\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t4\n",
                "SELECT * FROM live_sym",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_sym");
    }

    @Test
    public void testIncrementalRefreshAccumulatesWindowState() throws Exception {
        // Regression for the broken incremental-refresh path (bugs A/B/C):
        // (A) bootstrap with seqTxn=-1 must advance lastProcessedSeqTxn so the next
        //     refresh enters the incremental branch, not bootstrap;
        // (B) WalSegmentPageFrameCursor.of must not call openSegment() a second time
        //     after the WalReader constructor already opened it;
        // (C) buildColumnMappings must look up writer indexes by name in the base
        //     table's metadata cache (the SQL cursor's metadata reports -1).
        // The non-partitioned row_number() OVER () counter is what the test inspects:
        // with all three bugs in place every refresh fell through to fullRecompute and
        // bounded backfill seeded the counter from the retained horizon (rn=1 each
        // time). With the fixes, the counter accumulates across incremental refreshes.
        execute("CREATE TABLE trades (price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_inc LAG 1s RETENTION 1h AS" +
                " SELECT price, ts, row_number() OVER () AS rn FROM trades");
        drainLiveViewQueue();

        execute("INSERT INTO trades VALUES (100.0, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        execute("INSERT INTO trades VALUES (101.0, '2024-01-01T00:01:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        execute("INSERT INTO trades VALUES (102.0, '2024-01-01T00:02:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "price\tts\trn\n" +
                        "100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "101.0\t2024-01-01T00:01:00.000000Z\t2\n" +
                        "102.0\t2024-01-01T00:02:00.000000Z\t3\n",
                "SELECT * FROM live_inc",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_inc");
    }

    @Test
    public void testBoundedBackfillEmptyBaseTable() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // Base table is empty; bootstrap must succeed and leave the view empty.
        execute("CREATE LIVE VIEW live_empty LAG 1s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n",
                "SELECT * FROM live_empty",
                null,
                "ts",
                true,
                true
        );

        // A subsequent INSERT feeds the view through the incremental path.
        execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n",
                "SELECT * FROM live_empty",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_empty");
    }

    @Test
    public void testBoundedBackfillRetentionExceedsAvailableData() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // Only 3 seconds of data exist; RETENTION 1h greatly exceeds it. Bootstrap
        // must include every row (the lower bound falls below the minimum ts).
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 101.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 102.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_all LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t100.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t101.0\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "AAPL\t102.0\t2024-01-01T00:00:02.000000Z\t3\n",
                "SELECT * FROM live_all",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_all");
    }

    @Test
    public void testCannotAlterLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "ALTER TABLE live_rn ADD COLUMN x INT",
                12,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotDropLiveViewAsTable() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "DROP TABLE live_rn",
                11,
                "table name expected, got live view name: live_rn"
        );
    }

    @Test
    public void testCannotDropTableAsLiveView() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        assertException(
                "DROP LIVE VIEW trades",
                15,
                "live view name expected [name=trades]"
        );
    }

    @Test
    public void testCannotGetReaderForLiveView() throws Exception {
        createBaseTableAndLiveView();
        TableToken token = engine.getTableTokenIfExists("live_rn");
        Assert.assertNotNull(token);
        Assert.assertTrue(token.isLiveView());
        try {
            engine.getReader(token);
            Assert.fail("expected CairoException");
        } catch (CairoException e) {
            Assert.assertTrue(e.getMessage().contains("cannot get a reader for view"));
        }
    }

    @Test
    public void testCannotInsertIntoLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "INSERT INTO live_rn VALUES ('AAPL', 100.0, '2024-01-01T00:00:00.000000Z', 1)",
                12,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotReindexLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "REINDEX TABLE live_rn COLUMN symbol LOCK EXCLUSIVE",
                14,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotRenameLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "RENAME TABLE live_rn TO live_rn2",
                13,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotTruncateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "TRUNCATE TABLE live_rn",
                15,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotUpdateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "UPDATE live_rn SET price = 0",
                0,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCannotVacuumLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertException(
                "VACUUM TABLE live_rn",
                13,
                "cannot modify live view [view=live_rn]"
        );
    }

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));

        execute("DROP LIVE VIEW live_rn");

        Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
    }

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        execute("CREATE TABLE t1 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW lv1 LAG 1s RETENTION 1h AS SELECT val, ts, row_number() OVER () AS rn FROM t1");
        // should not throw
        execute("CREATE LIVE VIEW IF NOT EXISTS lv1 LAG 1s RETENTION 1h AS SELECT val, ts, row_number() OVER () AS rn FROM t1");

        execute("DROP LIVE VIEW lv1");
    }

    @Test
    public void testCreateLiveViewRejectsDedupBase() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, symbol)");
        drainWalQueue();

        // Incremental refresh reads the pre-dedup WAL row stream; rows dropped or replaced
        // at apply time would be double-counted, so DEDUP base tables are rejected for V1.
        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                17,
                "live view cannot be created over a base table with DEDUP keys"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsJoin() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        execute("CREATE TABLE refs (symbol SYMBOL, name STRING, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT t.symbol, t.price, t.ts, row_number() OVER () AS rn" +
                        " FROM trades t JOIN refs r ON (symbol)",
                17,
                "live view select must be a simple scan of a single WAL base table"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsNonZeroPassWindow() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // first_value(...) IGNORE NULLS OVER (PARTITION BY ...) is a TWO_PASS window function;
        // it cannot be maintained incrementally.
        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT symbol, ts, first_value(price) ignore nulls over (partition by symbol) AS fv FROM trades",
                17,
                "live view select may only use window functions that support incremental refresh"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsIndexBackedFilter() throws Exception {
        // Indexed SYMBOL equality combined with a residual predicate makes the planner
        // push the filter into a SymbolIndexFilteredRowCursorFactory. The filter is no
        // longer visible as a wrapping factory, so the live view incremental path would
        // miss it. Verify the validator rejects this shape.
        execute("CREATE TABLE trades (symbol SYMBOL INDEX, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn" +
                        " FROM trades WHERE symbol = 'AAPL' AND price > 100",
                17,
                "live view select cannot use index-backed filters yet"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testCreateLiveViewRejectsSubquery() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn" +
                        " FROM (SELECT symbol, price, ts FROM trades)",
                47,
                "live view requires a single base table in FROM clause"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

@Test
    public void testCreateLiveViewRejectsWindowOrderedByNonTimestamp() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        // Ordering the window by a non-timestamp column forces the planner onto the cached
        // window path, which requires sorting the full base dataset on every refresh.
        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 1h AS" +
                        " SELECT symbol, price, ts, row_number() OVER (ORDER BY price) AS rn FROM trades",
                17,
                "live view select may only use window functions that support incremental refresh"
        );
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("lv_bad"));
    }

    @Test
    public void testDropDuringRefreshDefersFree() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            // Simulate a refresh in flight: hold the refresh latch across the DROP.
            Assert.assertTrue(instance.tryLockForRefresh());
            try {
                execute("DROP LIVE VIEW live_rn");
                // The view is removed from the registry and marked as dropped, but the
                // table is NOT freed yet because we hold the refresh latch.
                Assert.assertTrue(instance.isDropped());
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
            } finally {
                instance.unlockAfterRefresh();
            }
            // The refresh finally hook would normally do this; call it explicitly here.
            instance.tryCloseIfDropped();
        });
    }

    @Test
    public void testDropLiveViewIfExists() throws Exception {
        // should not throw
        execute("DROP LIVE VIEW IF EXISTS nonexistent");
    }

    @Test
    public void testDropNonExistentLiveViewFails() throws Exception {
        try {
            execute("DROP LIVE VIEW nonexistent");
            Assert.fail("expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("live view does not exist"));
        }
    }

    @Test
    public void testDropWithActiveReadLockDefersFree() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            // Simulate an active reader cursor: hold a read pin across the DROP.
            io.questdb.cairo.lv.InMemoryTable pinned = instance.acquireForRead();
            Assert.assertNotNull(pinned);
            try {
                execute("DROP LIVE VIEW live_rn");
                // The view is removed from the registry and marked dropped, but the
                // table is NOT freed yet because we hold the read pin.
                Assert.assertTrue(instance.isDropped());
                Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));
            } finally {
                instance.releaseAfterRead(pinned);
            }
            // The cursor close hook runs tryCloseIfDropped via releaseAfterRead above.
        });
    }

    @Test
    public void testLiveViewAllColumnTypes() throws Exception {
        execute(
                "CREATE TABLE all_types (" +
                        " b BOOLEAN," +
                        " bt BYTE," +
                        " sh SHORT," +
                        " i INT," +
                        " l LONG," +
                        " f FLOAT," +
                        " d DOUBLE," +
                        " ch CHAR," +
                        " sym SYMBOL," +
                        " str STRING," +
                        " vc VARCHAR," +
                        " dt DATE," +
                        " ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY HOUR WAL"
        );
        drainWalQueue();

        execute(
                "CREATE LIVE VIEW lv_all LAG 1s RETENTION 1h AS" +
                        " SELECT b, bt, sh, i, l, f, d, ch, sym, str, vc, dt, ts," +
                        " row_number() OVER () AS rn" +
                        " FROM all_types"
        );

        execute(
                "INSERT INTO all_types VALUES" +
                        " (true, 1, 2, 3, 4, 1.5, 2.5, 'A', 'SYM1', 'hello', 'world'," +
                        "  '2024-01-01', '2024-01-01T00:00:00.000000Z')," +
                        " (false, 10, 20, 30, 40, 10.5, 20.5, 'B', 'SYM2', NULL, 'test'," +
                        "  '2024-01-02', '2024-01-01T00:00:01.000000Z')"
        );
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "b\tbt\tsh\ti\tl\tf\td\tch\tsym\tstr\tvc\tdt\tts\trn\n" +
                        "true\t1\t2\t3\t4\t1.5\t2.5\tA\tSYM1\thello\tworld\t2024-01-01T00:00:00.000Z\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "false\t10\t20\t30\t40\t10.5\t20.5\tB\tSYM2\t\ttest\t2024-01-02T00:00:00.000Z\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM lv_all",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW lv_all");
    }

    @Test
    public void testLiveViewBaseTableMustBeWal() throws Exception {
        execute("CREATE TABLE non_wal (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");

        try {
            execute("CREATE LIVE VIEW lv LAG 1s RETENTION 1h AS SELECT val, ts, row_number() OVER () AS rn FROM non_wal");
            Assert.fail("expected SqlException");
        } catch (SqlException e) {
            Assert.assertTrue(e.getMessage().contains("WAL"));
        }
    }

    @Test
    public void testLiveViewIncrementalRefreshWithWhereClause() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_filtered LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades WHERE price > 200");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 250.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t250.0\t2024-01-01T00:00:02.000000Z\t1\n",
                "SELECT * FROM live_filtered",
                null,
                "ts",
                true,
                true
        );

        // second batch: incremental refresh must also honour the filter, and window state must
        // carry over so the next qualifying AAPL row is numbered 2.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 100.0, '2024-01-01T00:00:03.000000Z')," +
                " ('GOOG', 2900.0, '2024-01-01T00:00:04.000000Z')," +
                " ('AAPL', 300.0, '2024-01-01T00:00:05.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t250.0\t2024-01-01T00:00:02.000000Z\t1\n" +
                        "GOOG\t2900.0\t2024-01-01T00:00:04.000000Z\t2\n" +
                        "AAPL\t300.0\t2024-01-01T00:00:05.000000Z\t2\n",
                "SELECT * FROM live_filtered",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_filtered");
    }

    @Test
    public void testLiveViewMultipleRefreshBatches() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // batch 1
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // batch 2
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')," +
                " ('MSFT', 400.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // batch 3
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:04.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:05.000000Z')," +
                " ('MSFT', 401.0, '2024-01-01T00:00:06.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:04.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:05.000000Z\t3\n" +
                        "MSFT\t401.0\t2024-01-01T00:00:06.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewQueryEmpty() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewQueryWithRefresh() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // insert first batch
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");

        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // insert second batch (incremental)
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:03.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:04.000000Z')");

        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:03.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:04.000000Z\t3\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewRefreshAfterMultipleInserts() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // bootstrap with first batch
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // multiple INSERTs before draining the queue: each INSERT creates
        // a separate WAL transaction, so the refresh job processes multiple
        // WAL txns in one pass
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");
        execute("INSERT INTO trades VALUES" +
                " ('MSFT', 400.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:03.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewRefreshAfterUpdate() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // bootstrap
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // UPDATE produces a non-DATA WAL event, which triggers full recompute
        execute("UPDATE trades SET price = 999.0 WHERE symbol = 'GOOG'");
        drainWalQueue();
        drainLiveViewQueue();

        // the full recompute must reflect the updated price
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t999.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        // verify that subsequent INSERT still works after the recompute
        execute("INSERT INTO trades VALUES" +
                " ('GOOG', 2810.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t999.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:02.000000Z\t2\n" +
                        "GOOG\t2810.0\t2024-01-01T00:00:03.000000Z\t2\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewRefreshRowNumberNoPartition() throws Exception {
        execute("CREATE TABLE events (val INT, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_seq LAG 1s RETENTION 1h AS" +
                " SELECT val, ts, row_number() OVER () AS rn FROM events");

        // bootstrap
        execute("INSERT INTO events VALUES" +
                " (10, '2024-01-01T00:00:00.000000Z')," +
                " (20, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "val\tts\trn\n" +
                        "10\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "20\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM live_seq",
                null,
                "ts",
                true,
                true
        );

        // second batch: row_number continues from where it left off
        execute("INSERT INTO events VALUES" +
                " (30, '2024-01-01T00:00:02.000000Z')," +
                " (40, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        assertQueryNoLeakCheck(
                "val\tts\trn\n" +
                        "10\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "20\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "30\t2024-01-01T00:00:02.000000Z\t3\n" +
                        "40\t2024-01-01T00:00:03.000000Z\t4\n",
                "SELECT * FROM live_seq",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_seq");
    }

    @Test
    public void testLiveViewSurvivesRestart() throws Exception {
        createBaseTableAndLiveView();

        // insert data and refresh before restart
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('GOOG', 2800.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // simulate restart: clear in-memory registry, reload from disk
        engine.getLiveViewRegistry().clear();
        Assert.assertFalse(engine.getLiveViewRegistry().hasView("live_rn"));

        engine.reloadTableNames();
        engine.buildViewGraphs();

        // verify live view was rebuilt from disk
        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertFalse(instance.isInvalid());

        // InMemoryTable is empty after restart; insert new data to trigger refresh
        execute("INSERT INTO trades VALUES ('MSFT', 400.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueue();

        // full recompute includes both pre- and post-restart data
        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "GOOG\t2800.0\t2024-01-01T00:00:01.000000Z\t1\n" +
                        "MSFT\t400.0\t2024-01-01T00:00:02.000000Z\t1\n",
                "SELECT * FROM live_rn",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewSurvivesRestartWithDroppedBaseTable() throws Exception {
        createBaseTableAndLiveView();

        // drop the base table
        execute("DROP TABLE trades");
        drainWalQueue();

        // simulate restart
        engine.getLiveViewRegistry().clear();
        engine.reloadTableNames();
        engine.buildViewGraphs();

        // live view should be loaded but invalidated
        Assert.assertTrue(engine.getLiveViewRegistry().hasView("live_rn"));
        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
        Assert.assertTrue(instance.isInvalid());
        Assert.assertEquals("base table does not exist", instance.getInvalidationReason());

        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testExplainLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "QUERY PLAN\n" +
                        "LiveView\n" +
                        "  name: live_rn\n",
                "EXPLAIN SELECT * FROM live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testInformationSchemaTablesLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "table_type\n" +
                        "LIVE VIEW\n",
                "SELECT table_type FROM information_schema.tables() WHERE table_name = 'live_rn'"
        );
        assertSql(
                "is_insertable_into\n" +
                        "false\n",
                "SELECT is_insertable_into FROM information_schema.tables() WHERE table_name = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewsFunction() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "view_name\tbase_table_name\tlag\tlag_unit\tretention\tretention_unit\tview_status\tinvalidation_reason\tview_sql\tbuffered_row_count\tlate_row_count\n" +
                        "live_rn\ttrades\t1\tSECOND\t1\tHOUR\tvalid\t\tSELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn FROM trades\t0\t0\n",
                "SELECT * FROM live_views()"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testLiveViewsFunctionWithLagAndRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv_lr LAG 5s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades");

        assertSql(
                "view_name\tlag\tlag_unit\tretention\tretention_unit\n" +
                        "lv_lr\t5\tSECOND\t10\tMINUTE\n",
                "SELECT view_name, lag, lag_unit, retention, retention_unit FROM live_views()"
        );
        execute("DROP LIVE VIEW lv_lr");
    }

    @Test
    public void testPgClassRelkindLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "relkind\n" +
                        "v\n",
                "SELECT relkind FROM pg_class() WHERE relname = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testSelectFromInvalidatedLiveViewFails() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            execute("TRUNCATE TABLE trades");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertTrue(instance.isInvalid());

            assertExceptionNoLeakCheck(
                    "SELECT * FROM live_rn",
                    -1,
                    "live view is invalid [name=live_rn, reason=truncate operation]",
                    sqlExecutionContext
            );
        });
    }

    @Test
    public void testShowColumnsLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\n" +
                        "symbol\tSYMBOL\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n" +
                        "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\n" +
                        "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\n",
                "SHOW COLUMNS FROM live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testShowCreateLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "ddl\n" +
                        "CREATE LIVE VIEW 'live_rn' LAG 1s RETENTION 1h AS (\n" +
                        "SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn FROM trades\n" +
                        ");\n",
                "SHOW CREATE LIVE VIEW live_rn"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testShowCreateLiveViewWithLagAndRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW lv_lr LAG 5s RETENTION 10m AS" +
                " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades");

        assertSql(
                "ddl\n" +
                        "CREATE LIVE VIEW 'lv_lr' LAG 5s RETENTION 10m AS (\n" +
                        "SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades\n" +
                        ");\n",
                "SHOW CREATE LIVE VIEW lv_lr"
        );
        execute("DROP LIVE VIEW lv_lr");
    }

    @Test
    public void testTablesTypeLiveView() throws Exception {
        createBaseTableAndLiveView();
        assertSql(
                "table_type\n" +
                        "L\n",
                "SELECT table_type FROM tables() WHERE table_name = 'live_rn'"
        );
        execute("DROP LIVE VIEW live_rn");
    }

    @Test
    public void testTryLockForReadFailsAfterDrop() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            Assert.assertNotNull(instance);

            execute("DROP LIVE VIEW live_rn");
            Assert.assertTrue(instance.isDropped());

            // A reader arriving after the drop must be turned away.
            Assert.assertNull(instance.acquireForRead());
        });
    }

    @Test
    public void testCreateLiveViewRejectsMissingLag() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                24,
                "'lag' expected"
        );
    }

    @Test
    public void testCreateLiveViewRejectsMissingRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                31,
                "'retention' expected"
        );
    }

    @Test
    public void testCreateLiveViewRejectsRetentionLessThanLag() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 10m RETENTION 5s AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                42,
                "retention must be greater than or equal to lag"
        );
    }

    @Test
    public void testCreateLiveViewRejectsZeroLag() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 0s AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                28,
                "lag must be positive"
        );
    }

    @Test
    public void testCreateLiveViewRejectsZeroRetention() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        assertException(
                "CREATE LIVE VIEW lv_bad LAG 1s RETENTION 0h AS" +
                        " SELECT symbol, price, ts, row_number() OVER () AS rn FROM trades",
                41,
                "retention must be positive"
        );
    }

    @Test
    public void testLagHoldsBackRecentRows() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_lag LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        // Insert rows spanning 3 seconds. With LAG=1s and maxTs=2s, the watermark is 1s,
        // so only rows at ts <= 1s drain on this refresh. The row at ts=2s stays buffered.
        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:01.000000Z')," +
                " ('AAPL', 152.0, '2024-01-01T00:00:02.000000Z')");
        drainWalQueue();
        drainLiveViewQueueNoForce();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM live_lag",
                null,
                "ts",
                true,
                true
        );

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_lag");
        Assert.assertEquals(1, instance.getMergeBuffer().size());

        // Inserting a row past ts=3s advances maxTs; the previously-retained row at ts=2s
        // now falls under the watermark (2s) and drains on the next refresh.
        execute("INSERT INTO trades VALUES ('AAPL', 153.0, '2024-01-01T00:00:03.000000Z')");
        drainWalQueue();
        drainLiveViewQueueNoForce();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:01.000000Z\t2\n" +
                        "AAPL\t152.0\t2024-01-01T00:00:02.000000Z\t3\n",
                "SELECT * FROM live_lag",
                null,
                "ts",
                true,
                true
        );

        execute("DROP LIVE VIEW live_lag");
    }

    @Test
    public void testPendingRefreshRetriedViaTimerAfterContention() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            drainLiveViewQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");

            // A reader pins the currently-published buffer. After the incremental
            // refresh below, the writer swaps, and this pinned buffer becomes the
            // write-slot candidate — the force-flush pass inside drainLiveViewQueue
            // will then fail to claim it and set pendingRefresh.
            InMemoryTable pinned = instance.acquireForRead();
            Assert.assertNotNull(pinned);
            try {
                // Two inserts spanning past LAG: the ts=5s row drains on the normal
                // (non-force) refresh, then the ts=10s row stays held back until a
                // force-flush. That force-flush is what hits write buffer contention.
                execute("INSERT INTO trades VALUES" +
                        " ('AAPL', 151.0, '2024-01-01T00:00:05.000000Z')," +
                        " ('AAPL', 152.0, '2024-01-01T00:00:10.000000Z')");
                drainWalQueue();
                drainLiveViewQueue();
                Assert.assertTrue(instance.isPendingRefresh());
            } finally {
                instance.releaseAfterRead(pinned);
            }

            // Timer tick observes pendingRefresh and enqueues a retry. Draining the
            // queue without force-flush ensures the retry path (not the drain helper's
            // own force-refresh) is what commits the pending work.
            LiveViewTimerJob timerJob = new LiveViewTimerJob(engine);
            timerJob.run(0);
            drainLiveViewQueueNoForce();

            Assert.assertFalse(instance.isPendingRefresh());

            assertQueryNoLeakCheck(
                    "symbol\tprice\tts\trn\n" +
                            "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                            "AAPL\t151.0\t2024-01-01T00:00:05.000000Z\t2\n" +
                            "AAPL\t152.0\t2024-01-01T00:00:10.000000Z\t3\n",
                    "SELECT * FROM live_rn",
                    null,
                    "ts",
                    true,
                    true
            );

            execute("DROP LIVE VIEW live_rn");
        });
    }

    @Test
    public void testReaderSeesStableSnapshotAcrossRefresh() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTableAndLiveView();

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')");
            drainWalQueue();
            drainLiveViewQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_rn");
            InMemoryTable pinned = instance.acquireForRead();
            Assert.assertNotNull(pinned);
            try {
                long snapshotRowCount = pinned.getRowCount();
                Assert.assertEquals(1, snapshotRowCount);

                // Two inserts spanning past LAG: the ts=5s row drains on the non-force
                // refresh into the non-pinned write buffer and swaps, leaving the
                // pinned buffer frozen. The ts=10s row stays buffered (held back by
                // LAG) and the force-flush pass is blocked by the pin.
                execute("INSERT INTO trades VALUES" +
                        " ('AAPL', 151.0, '2024-01-01T00:00:05.000000Z')," +
                        " ('AAPL', 152.0, '2024-01-01T00:00:10.000000Z')");
                drainWalQueue();
                drainLiveViewQueue();

                Assert.assertEquals(snapshotRowCount, pinned.getRowCount());
            } finally {
                instance.releaseAfterRead(pinned);
            }

            // A reader opened after the publish sees the post-swap snapshot. Only the
            // first new row made it in — the second is still in the merge buffer
            // waiting for the force-flush retry (not relevant to this test).
            InMemoryTable fresh = instance.acquireForRead();
            Assert.assertNotNull(fresh);
            try {
                Assert.assertEquals(2, fresh.getRowCount());
            } finally {
                instance.releaseAfterRead(fresh);
            }

            execute("DROP LIVE VIEW live_rn");
        });
    }

    @Test
    public void testTimerFlushesIdleBuffer() throws Exception {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();

        execute("CREATE LIVE VIEW live_timer LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");

        execute("INSERT INTO trades VALUES" +
                " ('AAPL', 150.0, '2024-01-01T00:00:00.000000Z')," +
                " ('AAPL', 151.0, '2024-01-01T00:00:01.000000Z')");
        drainWalQueue();
        drainLiveViewQueueNoForce();

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("live_timer");
        Assert.assertEquals(1, instance.getMergeBuffer().size());

        // Simulate wall-clock passing past the LAG window by zeroing the last refresh time.
        instance.setLastRefreshTimeUs(0);

        LiveViewTimerJob timerJob = new LiveViewTimerJob(engine);
        timerJob.run(0);
        drainLiveViewQueueNoForce();

        assertQueryNoLeakCheck(
                "symbol\tprice\tts\trn\n" +
                        "AAPL\t150.0\t2024-01-01T00:00:00.000000Z\t1\n" +
                        "AAPL\t151.0\t2024-01-01T00:00:01.000000Z\t2\n",
                "SELECT * FROM live_timer",
                null,
                "ts",
                true,
                true
        );
        Assert.assertEquals(0, instance.getMergeBuffer().size());

        execute("DROP LIVE VIEW live_timer");
    }

    private static void drainLiveViewQueue() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (job.run(0, Job.RUNNING_STATUS)) ;
            // Force-flush rows still held in each view's merge buffer so tests see
            // the full view state without waiting for the idle timer.
            job.forceFlushAllViews();
        }
    }

    /**
     * Drains only the normal refresh queue without force-flushing. Used by LAG-specific
     * tests that want to observe the buffer's held-back behaviour directly.
     */
    private static void drainLiveViewQueueNoForce() {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (job.run(0, Job.RUNNING_STATUS)) ;
        }
    }

    private void createBaseTableAndLiveView() throws SqlException {
        execute("CREATE TABLE trades (symbol SYMBOL, price DOUBLE, ts TIMESTAMP)" +
                " TIMESTAMP(ts) PARTITION BY HOUR WAL");
        drainWalQueue();
        execute("CREATE LIVE VIEW live_rn LAG 1s RETENTION 1h AS" +
                " SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts) AS rn" +
                " FROM trades");
    }
}
