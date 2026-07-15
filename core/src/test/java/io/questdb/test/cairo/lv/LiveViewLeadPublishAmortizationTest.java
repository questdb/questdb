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

import io.questdb.PropertyKey;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.cairo.security.AllowAllSecurityContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guards the amortised in-memory-tier publish: once the un-flushed lead grows past the
 * {@code cairo.live.view.in.memory.buffer.growth.bytes} budget, publishing the lead must
 * NOT fall back to the O(retained) slow-path slot copy on every cycle when there is nothing
 * to reclaim (the lead never ages out under a long FLUSH EVERY window). Before the fix the
 * gate compared the slot's monotonic allocated footprint against the budget, so any tier
 * larger than the budget copied its whole retained set into the other slot on every publish -
 * O(rows) per publish, which starved the base drain and made the view lag at high ingest.
 * <p>
 * The slow-path swap flips the tier's published slot index; the fast in-place append does
 * not. So a publish that took the slow path is observable as a {@code publishedIdx} flip.
 * The clock is pinned so nothing flushes after the first cycle and the lead only grows, and
 * no reader pins a slot during the drive, so every publish is free to take the fast path.
 * <p>
 * The two budget tests pin the opposite edge of the new gate: the budget decides whether
 * genuinely AGED, reclaimable overlap (rows flushed to disk, then pushed below the IN MEMORY
 * horizon) is evicted now or held in place a while longer. A small budget must still compact
 * (the gate is not a "never compact" degenerate that grows the tier without bound); a budget
 * larger than the aged overlap keeps those rows resident (the fast-path amortisation) until a
 * budget's worth has accumulated.
 */
public class LiveViewLeadPublishAmortizationTest extends AbstractLiveViewTest {

    @Test
    public void testGrowingLeadStaysOnFastPath() throws Exception {
        // Small budget so the lead crosses it after a few batches, keeping the test quick.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, 256 * 1024);
        setCurrentMicros(0L);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE core_price_xxx (timestamp TIMESTAMP, symbol SYMBOL, bid_price DOUBLE) " +
                    "TIMESTAMP(timestamp) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW core_price_lv FLUSH EVERY 60s IN MEMORY 60s " +
                    "START FROM '1970-01-01T00:00:00.000000Z' AS " +
                    "SELECT timestamp, symbol, " +
                    "avg(bid_price) OVER (PARTITION BY symbol ORDER BY timestamp ROWS 300 PRECEDING) AS moving_avg " +
                    "FROM core_price_xxx");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "core_price_lv");

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("core_price_lv");
                final int symbols = 64;
                final int batchRows = 5_000;
                final int batches = 30;
                long ts = 1_000_000L;
                final long tsStep = 1_000L;

                int prevPublishedIdx = -1;
                int flipsAfterCrossover = 0;
                long budgetCrossedAtBatch = -1;

                for (int b = 0; b < batches; b++) {
                    StringBuilder sb = new StringBuilder("INSERT INTO core_price_xxx VALUES ");
                    for (int r = 0; r < batchRows; r++) {
                        if (r > 0) {
                            sb.append(',');
                        }
                        sb.append('(').append(ts).append("::timestamp,'s").append(r % symbols).append("',")
                                .append(100.0 + (r % 50)).append(')');
                        ts += tsStep;
                    }
                    execute(sb.toString());
                    drainWalQueue();
                    drainJob(job);

                    LiveViewInMemoryTier tier = instance.getInMemoryTier();
                    Assert.assertNotNull(tier);
                    int publishedIdx = tier.getPublishedIdx();
                    long footprint = tier.getSlot(publishedIdx).footprintBytes();
                    if (budgetCrossedAtBatch < 0 && footprint >= 256 * 1024) {
                        budgetCrossedAtBatch = b;
                    }
                    // Count slow-path swaps (published-slot flips) only after the lead has
                    // crossed the budget: before that the fast path always applies anyway.
                    if (prevPublishedIdx != -1 && publishedIdx != prevPublishedIdx && budgetCrossedAtBatch >= 0
                            && b > budgetCrossedAtBatch) {
                        flipsAfterCrossover++;
                    }
                    prevPublishedIdx = publishedIdx;
                }

                Assert.assertTrue("the lead must exceed the growth budget for this test to be meaningful",
                        budgetCrossedAtBatch >= 0);
                // The core guarantee: a pure, growing lead with nothing to reclaim and no reader
                // pins never takes the O(retained) slow-path swap. Before the fix this was one
                // flip per batch for every batch past the crossover.
                Assert.assertEquals("a growing un-flushed lead must publish in place, not swap-copy every cycle",
                        0, flipsAfterCrossover);

                // And the lead is fully readable through SELECT * (identity projection routes the
                // in-mem tier), so the fast-path append did not drop or corrupt any rows.
                long expectedMaxTs = ts - tsStep;
                long rows = 0;
                long maxTs = Long.MIN_VALUE;
                try (SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                        .with(AllowAllSecurityContext.INSTANCE, null);
                     RecordCursorFactory factory = engine.select("SELECT * FROM core_price_lv", ctx);
                     RecordCursor cursor = factory.getCursor(ctx)) {
                    int tsIdx = factory.getMetadata().getTimestampIndex();
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        long t = record.getTimestamp(tsIdx);
                        if (t > maxTs) {
                            maxTs = t;
                        }
                        rows++;
                    }
                }
                Assert.assertEquals((long) batchRows * batches, rows);
                Assert.assertEquals("SELECT * must serve the freshest un-flushed lead row", expectedMaxTs, maxTs);
            }
        });
    }

    @Test
    public void testLargeBudgetRetainsAgedOverlap() throws Exception {
        // The amortisation edge: a budget larger than the aged overlap holds those rows in
        // place rather than paying an O(retained) evict-and-swap. Cycle 1's 3 rows have aged
        // out below the IN MEMORY 1s horizon by the time cycle 2 lands, but a 16MB budget is
        // far more than they occupy, so the publish appends in place and the slot still holds
        // all 5 rows (bounded - a budget's worth of aged rows would later trigger a compaction).
        assertMemoryLeak(() -> {
            long slotRows = driveAgedOverlapTwoCycles(16 * 1024 * 1024);
            Assert.assertEquals("aged overlap below the budget stays resident on the fast path", 5, slotRows);
            assertLvReturnsAllFiveRows();
        });
    }

    @Test
    public void testSmallBudgetStillCompactsAgedOverlap() throws Exception {
        // The safety edge: a small non-zero budget must not turn the gate into a "never
        // compact" that grows the tier without bound. With a 16-byte budget (below one row)
        // cycle 1's 3 aged, disk-backed rows exceed it, so the slow path fires, evicts them,
        // and leaves only cycle 2's 2 rows resident - exactly what the growth backstop exists
        // to do. (The old absolute-footprint gate reached the same slot state here; this
        // asserts the row-budget gate still does.)
        assertMemoryLeak(() -> {
            long slotRows = driveAgedOverlapTwoCycles(16);
            Assert.assertEquals("aged overlap beyond a tiny budget must be evicted", 2, slotRows);
            assertLvReturnsAllFiveRows();
        });
    }

    // Asserts SELECT * over the two-cycle view serves the full 5-row union (disk holds all 5;
    // the tier holds either the recent 2 or all 5 depending on the budget) so neither the
    // fast-path append nor the slow-path evict dropped or duplicated a row.
    private void assertLvReturnsAllFiveRows() throws Exception {
        long rows = 0;
        long maxX = Long.MIN_VALUE;
        try (SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                .with(AllowAllSecurityContext.INSTANCE, null);
             RecordCursorFactory factory = engine.select("SELECT * FROM lv", ctx);
             RecordCursor cursor = factory.getCursor(ctx)) {
            Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                long x = record.getInt(1);
                if (x > maxX) {
                    maxX = x;
                }
                rows++;
            }
        }
        Assert.assertEquals("SELECT * must serve the full disk+tier union", 5, rows);
        Assert.assertEquals("the freshest row (x=5) must be present", 5, maxX);
    }

    // Drives a live view through two ingest cycles under the given growth budget and returns
    // the published slot's row count. Cycle 1 inserts 3 rows and flushes them to disk (a clock
    // tick past FLUSH EVERY 100ms), so they become overlap. Cycle 2 inserts 2 rows 5s later -
    // past the IN MEMORY 1s window - so cycle 1's rows are now aged, reclaimable overlap. The
    // publish that lands cycle 2 is where isCompactionWorthwhile decides evict-vs-retain, which
    // is exactly what the budget selects. Mirrors the proven seam-split setup in
    // LiveViewInMemReadTest#createSeamSplitLv, which pins slot == 2 at budget 0.
    private long driveAgedOverlapTwoCycles(long growthBudget) throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_IN_MEMORY_BUFFER_GROWTH_BYTES, growthBudget);
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Pin the CREATE wall clock below the data so every row stays in-frame.
        setCurrentMicros(0L);
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 1s START FROM NOW AS " +
                "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
        final long dataStart = 1_700_000_000_000_000L;
        final long cycle2Start = dataStart + 5_000_000L; // 5s later, beyond IN MEMORY 1s
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("INSERT INTO base (ts, x) VALUES " +
                    "(" + (dataStart + 1) + ", 1), (" + (dataStart + 2) + ", 2), (" + (dataStart + 3) + ", 3)");
            drainWalQueue();
            setCurrentMicros(250_000L); // > FLUSH EVERY 100ms: cycle 1 flushes to disk
            drainJob(job);

            execute("INSERT INTO base (ts, x) VALUES " +
                    "(" + (cycle2Start + 1) + ", 4), (" + (cycle2Start + 2) + ", 5)");
            drainWalQueue();
            setCurrentMicros(500_000L);
            drainJob(job);
        }
        drainWalQueue();

        LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        LiveViewInMemoryTier tier = instance.getInMemoryTier();
        Assert.assertNotNull("view must allocate an in-mem tier for this schema", tier);
        return tier.getSlot(tier.getPublishedIdx()).rowCount();
    }
}
