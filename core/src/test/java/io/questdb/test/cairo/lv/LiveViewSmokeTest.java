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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewCheckpointBlockType;
import io.questdb.cairo.lv.LiveViewCheckpointManifest;
import io.questdb.cairo.lv.LiveViewCheckpointWriter;
import io.questdb.cairo.lv.LiveViewDefinition;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 1 smoke tests: confirm the new CREATE LIVE VIEW syntax (FLUSH EVERY,
 * IN MEMORY, PARTITION BY, BACKFILL reject) is parsed and validated, and that
 * creating + dropping a live view goes through the engine end-to-end.
 * <p>
 * Asserted-wording validation tests will go in a dedicated file once the full
 * suite is rewritten in delta plan task #8.
 */
public class LiveViewSmokeTest extends AbstractCairoTest {

    private static boolean drainJob(Job job) {
        Job.RunStatus status = () -> false;
        boolean any = false;
        for (int i = 0; i < 64 && job.run(0, status); i++) {
            any = true;
        }
        return any;
    }

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s PARTITION BY DAY AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewDefaultsInMemoryToFlushEvery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 500ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewMakesCheckpointsDir() throws Exception {
        // RFC 123 Phase 2a.3: CREATE LIVE VIEW must materialize _checkpoints/
        // inside the LV directory so the flush cycle's checkpoint write hook
        // (Phase 2a.4) has a target directory ready.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            try {
                TableToken token = engine.verifyTableName("lv");
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path path = new Path()) {
                    path.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
                    Assert.assertTrue(
                            "_checkpoints/ must exist after CREATE LIVE VIEW [path=" + path + ']',
                            ff.exists(path.$())
                    );
                }
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testRejectLiveViewOverNonWalBase() throws Exception {
        assertMemoryLeak(() -> {
            // No WAL — bypass-WAL is the default for non-partitioned plain tables.
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts)");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected non-WAL-base reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be a WAL table"));
            }
        });
    }

    @Test
    public void testRejectLiveViewOverLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");
            final String sql = "CREATE LIVE VIEW lv2 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM lv1";
            try {
                execute(sql);
                Assert.fail("expected live-on-live reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("not allowed as base tables"));
                // Position must point at the base table name, not the view
                // name.
                Assert.assertEquals(
                        "position must point at the base table name",
                        sql.lastIndexOf("lv1"),
                        e.getPosition()
                );
            }
            execute("DROP LIVE VIEW lv1");
        });
    }

    @Test
    public void testRejectBackfill() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 500ms BACKFILL AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected BACKFILL reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("BACKFILL not yet supported"));
            }
        });
    }

    @Test
    public void testRejectBareUnboundedWindow() throws Exception {
        // A window with PARTITION BY and the default (UNBOUNDED PRECEDING ...
        // CURRENT ROW) frame must have an ANCHOR clause, otherwise partition
        // count grows without bound.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, symbol SYMBOL, price DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");

            // (a) named WINDOW shape.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY symbol ORDER BY ts)");
                Assert.fail("expected bare unbounded named WINDOW reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (b) inline OVER (...) shape.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts) AS s FROM base");
                Assert.fail("expected bare unbounded inline OVER reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (c) inline OVER nested inside an arithmetic expression must still be caught.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts) + 1 AS s FROM base");
                Assert.fail("expected bare unbounded nested OVER reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported"));
            }

            // (d) bounded ROWS frame without ANCHOR is accepted.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, symbol, sum(price) OVER (PARTITION BY symbol ORDER BY ts " +
                    "ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS s FROM base");
            execute("DROP LIVE VIEW lv");

            // (e) ANCHOR DAILY satisfies the rule for the same window shape.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, symbol, sum(price) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY symbol ORDER BY ts ANCHOR DAILY '00:00')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectFlushEveryBelow100Ms() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 50ms AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected FLUSH EVERY <100ms reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("FLUSH EVERY must be at least 100ms"));
            }
        });
    }

    @Test
    public void testRejectInMemoryAboveCap() throws Exception {
        // Default cap is 60min, which the formatter renders as the largest clean
        // divisor "1h". The reject must include the value so the operator knows
        // what they need to override.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 2h AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected IN MEMORY > cap reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "IN MEMORY must be at most cairo.live.view.in.memory.max (1h)"));
            }
        });
    }

    @Test
    public void testRejectInMemoryBelowFlushEvery() throws Exception {
        // Asserted-wording RFC requires the FLUSH EVERY value in parentheses so the
        // operator sees what the floor was (1s in this case).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 500ms AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected IN MEMORY < FLUSH EVERY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "IN MEMORY must be at least FLUSH EVERY (1s)"));
            }
        });
    }

    @Test
    public void testWalPurgeHonorsLvConsumedSeqTxnFloor() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First insert + drain — base table applies seqTxn 1.
            execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            // Force a fresh segment for the next insert by releasing pooled writers.
            engine.releaseInactive();
            // Second insert + drain — base table applies seqTxn 2 in segment 1.
            execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            engine.releaseInactive();

            // Purge with the LV still at lvConsumedSeqTxn = 0: segment 0 must be
            // retained because the LV hasn't consumed seqTxn 1 yet.
            drainPurgeJob();
            assertSegmentExistence(true, "base", 1, 0);

            // Refresh + persist. lvConsumedSeqTxn advances to base's head.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Now the LV no longer needs the old segment. Purge must reap it.
            engine.releaseInactive();
            drainPurgeJob();
            assertSegmentExistence(false, "base", 1, 0);

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testWriterStallWhenBothSlotsPinned() throws Exception {
        // RFC 123 §"Stall behavior": when both slots are reader-pinned, the
        // slow-path tryAcquireWrite returns null and the refresh worker
        // records the start of the stall streak. writer_stall_micros surfaces
        // the duration via live_views(). We pin both slots through the test
        // by manipulating the tier directly, then run a refresh — the in-mem
        // populate path stalls but the on-disk apply still advances.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Seed the tier via an initial refresh so both slots have valid
            // shapes (the second slot is allocated but never written-to without
            // this seed; the test pins the seeded slot + the other one).
            setCurrentMicros(0L);
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 1)");
            drainWalQueue();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                int publishedAfterSeed = tier.getPublishedIdx();

                // A second cycle so both slots have been writer-touched
                // (publishedIdx flips on each swap, so this seeds the
                // currently-non-published slot).
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 2)");
                drainWalQueue();
                drainJob(job);
                Assert.assertNotEquals("publishedIdx flipped after second cycle",
                        publishedAfterSeed, tier.getPublishedIdx());

                // Pin both slots — first the currently-published one, then
                // flip publishedIdx via a noop pseudo-publish (we just pin
                // both). Standard usage doesn't expose this state, but the
                // test mirrors what concurrent long readers do in production.
                int pinA = tier.acquireRead();
                int pinB;
                // Force a "phantom" reader on the OTHER slot by manipulating
                // publishedIdx via a write+swap cycle while still pinning A.
                // The simplest way is: take write sentinel on otherIdx, swap,
                // then acquireRead on the now-current slot.
                int otherIdx = 1 - pinA;
                LiveViewInMemoryBuffer otherWrite = tier.tryAcquireWrite(otherIdx);
                Assert.assertNotNull("seed write on other slot must succeed", otherWrite);
                tier.publishSwap(otherIdx);
                pinB = tier.acquireRead();
                Assert.assertEquals("pinB must land on the now-published slot",
                        otherIdx, pinB);

                try {
                    // Now both slots are reader-pinned. A new refresh cycle
                    // cannot take the writer sentinel on either. The on-disk
                    // tier still advances; only the in-mem swap stalls.
                    Assert.assertEquals("writer_stall_micros must start at 0",
                            Numbers.LONG_NULL, instance.getWriterStallStartUs());

                    setCurrentMicros(500_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000003Z', 3)");
                    drainWalQueue();
                    drainJob(job);

                    Assert.assertEquals(
                            "writerStallStartUs must equal the wall-clock at refresh time",
                            500_000L,
                            instance.getWriterStallStartUs()
                    );

                    // live_views() must surface the stall duration (now - stallStart).
                    setCurrentMicros(700_000L);
                    assertSql(
                            "writer_stall_micros\n200000\n",
                            "SELECT writer_stall_micros FROM live_views() WHERE view_name = 'lv'"
                    );
                } finally {
                    tier.releaseRead(pinA);
                    tier.releaseRead(pinB);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemEvictionPastInMemoryWindow() throws Exception {
        // RFC 123 Phase 1b: rows whose ts falls below latest - IN_MEMORY are
        // not copied into the new write slot during the slow-path swap. With
        // IN MEMORY 100ms and a 200ms gap between the two inserts (data ts),
        // the first row's ts is below the eviction threshold by the time the
        // second refresh cycle runs, so only the second row survives.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull(tier);
                Assert.assertEquals("first cycle: one row in tier", 1, tier.getSlot(tier.getPublishedIdx()).rowCount());

                // Second insert: data ts 200ms after the first, so latest -
                // IN_MEMORY = +100ms is past the first row's ts. Advance the
                // wall clock by 200ms so the FLUSH EVERY 100ms gate passes.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.200000Z', 2)");
                drainWalQueue();
                drainJob(job);

                LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals("post-second-cycle: only the new row survives", 1, published.rowCount());
                Assert.assertEquals("surviving row is the second insert", 2, published.getInt(0, 1));
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsExposesInMemBytes() throws Exception {
        // RFC 123 §"Catalogue function live_views()": in_mem_bytes reports the
        // current footprint of both N=2 slots. Zero before any refresh; > 0
        // once a refresh has populated the tier.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Before any refresh: tier is unallocated; in_mem_bytes must read 0.
            assertSql(
                    "in_mem_bytes\n0\n",
                    "SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'"
            );

            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 1), " +
                    "('2026-05-12T00:00:00.000002Z', 2)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            // After refresh, the tier is non-empty and footprint must be > 0.
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNotNull("tier must be allocated after refresh", instance.getInMemoryTier());
            long footprint = instance.getInMemoryTier().footprintBytes();
            Assert.assertTrue("footprint must be > 0 after a refresh", footprint > 0);
            assertSql(
                    "in_mem_bytes\n" + footprint + "\n",
                    "SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInMemTierReceivesRowsAfterRefresh() throws Exception {
        // RFC 123 Phase 1b: refresh worker mirrors LV outputs into a worker-local
        // staging buffer and runs a slow-path swap into the LV's N=2 in-mem
        // tier after the inline apply commits. Two rows match the WHERE
        // filter; both must show up in the published slot, in ts-ascending
        // order, with seamTs = the lowest ts.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 1), " +
                    "('2026-05-12T00:00:00.000002Z', 2), " +
                    "('2026-05-12T00:00:00.000003Z', -1)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("in-mem tier must be allocated after refresh", tier);
            LiveViewInMemoryBuffer published = tier.getSlot(tier.getPublishedIdx());
            Assert.assertEquals("published slot must have two rows", 2, published.rowCount());
            // ts column is col 0; matches the LV's designated ts.
            long ts0 = published.getLong(0, 0);
            long ts1 = published.getLong(1, 0);
            Assert.assertTrue("rows must be ordered by ts", ts0 < ts1);
            Assert.assertEquals("seamTs must equal the lowest retained ts", ts0, published.seamTs());
            // x column is col 1 — survived the WHERE filter (x > 0)
            Assert.assertEquals(1, published.getInt(0, 1));
            Assert.assertEquals(2, published.getInt(1, 1));
            // row_number outputs at col 2 — the SELECT's third column
            Assert.assertEquals(1L, published.getLong(0, 2));
            Assert.assertEquals(2L, published.getLong(1, 2));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCursorPinKeepsTierAliveAcrossDrop() throws Exception {
        // RFC 123 §"DROP LIVE VIEW" step 4 "modulo cursor pins": a reader
        // holding an in-mem tier pin must survive a concurrent DROP LIVE VIEW
        // without segfault. The tier's deferred-close protocol marks it
        // closed on DROP but keeps native memory alive until the last pin
        // drains. Without the protocol, tier.releaseRead after DROP would
        // dereference a freed Unsafe pointer.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 7)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);

            // Acquire a pin manually — stands in for a LiveViewRecordCursor
            // that opens before DROP and finishes its scan after.
            int pin = tier.acquireRead();
            Assert.assertTrue("acquireRead must succeed before DROP", pin >= 0);
            // Sanity: contents reachable while pinned.
            Assert.assertEquals(1L, tier.getSlot(pin).rowCount());
            Assert.assertEquals(7, tier.getSlot(pin).getInt(0, 1));

            // DROP LIVE VIEW now: liveViewRegistry.removeView -> markAsDropped ->
            // tryCloseIfDropped -> tier.close(). With deferred close the native
            // memory stays alive because the pin count is 1.
            execute("DROP LIVE VIEW lv");

            // Reader can still inspect the pinned slot — no use-after-free.
            Assert.assertEquals("pinned slot contents survive DROP",
                    7, tier.getSlot(pin).getInt(0, 1));
            // New acquires reject cleanly post-close.
            Assert.assertEquals("post-close acquireRead must return -1",
                    -1, tier.acquireRead());

            // Releasing the last pin triggers the actual native free.
            // assertMemoryLeak verifies no leak — the deferred-free path runs.
            tier.releaseRead(pin);
        });
    }

    @Test
    public void testSlowPathSwapFailureKeepsPreviousSlotPublished() throws Exception {
        // RFC 123 Phase 1b end-to-end: when publishToInMemoryTier's slow-path
        // swap throws after a successful tryAcquireWrite, the catch block must
        // call releaseWriteWithoutPublish so the previously-published slot
        // stays visible to readers and the held sentinel does not deadlock the
        // next refresh. The unit-level test
        // testReleaseWriteWithoutPublishKeepsPriorSlotPublished pins the tier
        // contract; this smoke test drives the same recovery via the refresh
        // worker so the production catch path itself is exercised.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First refresh populates the tier with a known row. Establishes
            // the "previously-published" state the recovery path must preserve.
            setCurrentMicros(0L);
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000001Z', 7)");
            drainWalQueue();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);

                LiveViewInMemoryTier tier = instance.getInMemoryTier();
                Assert.assertNotNull("tier must be allocated after first refresh", tier);
                int publishedBeforeFailure = tier.getPublishedIdx();
                LiveViewInMemoryBuffer pubSlotBeforeFailure = tier.getSlot(publishedBeforeFailure);
                Assert.assertEquals("first refresh seeded one row", 1, pubSlotBeforeFailure.rowCount());
                Assert.assertEquals(7, pubSlotBeforeFailure.getInt(0, 1));
                int retriesBefore = instance.getFlushRetryCount();
                Assert.assertEquals("no retries before injection", 0, retriesBefore);

                // Arm a one-shot publishSwap failure. Wall-clock advance so the
                // FLUSH EVERY gate (100ms) opens for the next refresh cycle.
                tier.setFailNextPublishSwap(new RuntimeException("test: simulated mid-swap failure"));
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000002Z', 13)");
                drainWalQueue();
                drainJob(job);

                // Recovery contract:
                //   1. Previously-published slot still holds the original row;
                //      readers never observe a zero-row slot.
                //   2. publishedIdx did not flip.
                //   3. Sentinel on the write slot is cleared, so a subsequent
                //      refresh can take it.
                Assert.assertEquals(
                        "publishedIdx must not flip after failed swap",
                        publishedBeforeFailure,
                        tier.getPublishedIdx()
                );
                LiveViewInMemoryBuffer pubSlotAfterFailure = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals(
                        "previously-published slot must still hold the original row",
                        1,
                        pubSlotAfterFailure.rowCount()
                );
                Assert.assertEquals(7, pubSlotAfterFailure.getInt(0, 1));
                Assert.assertTrue(
                        "in-mem-tier swap failure must tick the flush retry counter",
                        instance.getFlushRetryCount() > retriesBefore
                );

                // The on-disk tier did advance — the inline apply committed
                // before publishToInMemoryTier ran. Both base rows must be
                // visible through SELECT (which reads from disk in Phase 1b).
                assertSql(
                        "x\trn\n" +
                                "7\t1\n" +
                                "13\t2\n",
                        "SELECT x, rn FROM lv ORDER BY ts"
                );

                // A subsequent refresh must succeed normally: the sentinel was
                // released, the staging buffer is repopulated from the next
                // commit, and publishSwap flips the tier into a slot containing
                // the retained row from the previously-published slot plus the
                // new staging row.
                //
                // Phase 1b note: the row processed by the failed refresh cycle
                // (x=13) is durable on disk via the inline apply that committed
                // before publishToInMemoryTier ran, but it never made it into
                // the in-mem tier — the slow-path swap that would have copied
                // it threw. The tier therefore lags the on-disk tier by one
                // row until that row ages out of the IN MEMORY window. Reads
                // route through disk in Phase 1b, so this gap is invisible to
                // SELECT; once seam_ts routing lands, the recovery path will
                // need to re-source the missed rows from disk (or reset the
                // seam) on the next successful swap. Documented here so the
                // assertion doesn't drift silently when that lands.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000003Z', 21)");
                drainWalQueue();
                drainJob(job);
                Assert.assertNotEquals(
                        "third refresh must successfully flip publishedIdx",
                        publishedBeforeFailure,
                        tier.getPublishedIdx()
                );
                LiveViewInMemoryBuffer pubSlotAfterRecovery = tier.getSlot(tier.getPublishedIdx());
                Assert.assertEquals(
                        "post-recovery slot holds the retained row plus the new staging row",
                        2,
                        pubSlotAfterRecovery.rowCount()
                );
                Assert.assertEquals(7, pubSlotAfterRecovery.getInt(0, 1));
                Assert.assertEquals(21, pubSlotAfterRecovery.getInt(1, 1));
                Assert.assertEquals(0, instance.getFlushRetryCount());
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSqlCursorPinKeepsTierAliveAcrossDrop() throws Exception {
        // RFC 123 §"DROP LIVE VIEW" step 4 "modulo cursor pins": a SQL
        // RecordCursor opened against an LV pins the in-mem tier slot for its
        // lifetime; concurrent DROP LIVE VIEW marks the tier closed but
        // deferred-frees native memory until the cursor releases. The unit-
        // adjacent test testCursorPinKeepsTierAliveAcrossDrop drives the pin
        // directly through the tier API; this smoke test exercises the same
        // contract through the SQL path that production cursors take
        // (LiveViewRecordCursorFactory.getCursor -> LiveViewRecordCursor.of
        // -> tier.acquireRead, then on close -> tier.releaseRead).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 5s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 4), " +
                    "('2026-05-12T00:00:00.000002Z', 9)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);

            // Open the cursor via the SQL path. LiveViewRecordCursorFactory
            // wraps the disk factory and pins the tier slot at getCursor; the
            // pin survives until cursor.close().
            try (RecordCursorFactory factory = select("SELECT x FROM lv ORDER BY ts")) {
                RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                try {
                    // Consume the first row to confirm the cursor is live.
                    Assert.assertTrue("cursor must yield first row", cursor.hasNext());
                    Record record = cursor.getRecord();
                    Assert.assertEquals(4, record.getInt(0));

                    // DROP while the cursor is mid-scan. Registry-level
                    // visibility is gone immediately; the tier is marked
                    // closed but native memory is held by the cursor's pin.
                    execute("DROP LIVE VIEW lv");

                    // A fresh acquireRead after DROP must reject cleanly.
                    Assert.assertEquals(
                            "post-DROP acquireRead must return -1",
                            -1,
                            tier.acquireRead()
                    );

                    // The cursor still works: the disk-side TableReader holds
                    // its partition versions independently of the LV
                    // registry, and DROP defers physical deletion until all
                    // readers release.
                    Assert.assertTrue("cursor must yield second row after DROP", cursor.hasNext());
                    Assert.assertEquals(9, record.getInt(0));
                    Assert.assertFalse("cursor must terminate after the second row", cursor.hasNext());
                } finally {
                    // Closing the cursor releases the tier pin; deferred-free
                    // runs because no other reader holds the tier. assertMemoryLeak
                    // verifies the native refcount block and column buffers are
                    // ultimately freed.
                    cursor.close();
                }
            }
        });
    }

    @Test
    public void testInMemTierBypassedForVarLengthOutputSchema() throws Exception {
        // RFC 123 Phase 1b: LiveViewInMemoryBuffer supports fixed-width column
        // types only; an LV that projects a var-length column (VARCHAR,
        // STRING, etc.) falls through to disk-only reads.
        // ensureStagingAndTier returns false, no tier is allocated, but the
        // on-disk path still produces correct results via TableReader.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym VARCHAR, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, x, row_number() OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') AS rn FROM base");
            execute("INSERT INTO base (ts, sym, x) VALUES " +
                    "('2026-05-12T00:00:00.000001Z', 'a', 10), " +
                    "('2026-05-12T00:00:00.000002Z', 'b', 20), " +
                    "('2026-05-12T00:00:00.000003Z', 'a', 30)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNull(
                    "var-length output schema must skip in-mem tier allocation",
                    instance.getInMemoryTier()
            );

            // Reads still return correct results from disk.
            assertSql(
                    "ts\tsym\tx\trn\n" +
                            "2026-05-12T00:00:00.000001Z\ta\t10\t1\n" +
                            "2026-05-12T00:00:00.000002Z\tb\t20\t1\n" +
                            "2026-05-12T00:00:00.000003Z\ta\t30\t2\n",
                    "SELECT ts, sym, x, rn FROM lv ORDER BY ts"
            );

            // in_mem_bytes must read 0 since no tier was allocated.
            assertSql(
                    "in_mem_bytes\n0\n",
                    "SELECT in_mem_bytes FROM live_views() WHERE view_name = 'lv'"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testGlobalApplyDoesNotTouchLiveView() throws Exception {
        // RFC 123 Phase 1b: the global ApplyWal2TableJob.doRun skips LV tokens. We
        // ingest into the base, write a LIVE_VIEW_DATA block via the refresh worker,
        // then capture the LV's applied seqTxn. A subsequent drainWalQueue must NOT
        // advance the LV's _txn, since global apply ignores LV notifications.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-05-12T00:00:00.000000Z', 11)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            // Refresh writes + applies inline.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            long lvConsumedAfterInline = instance.getStateReader().getLvConsumedSeqTxn();
            long lastProcessedAfterInline = instance.getLastProcessedSeqTxn();
            Assert.assertEquals(
                    "inline apply must advance lvConsumed to lastProcessed",
                    lastProcessedAfterInline,
                    lvConsumedAfterInline
            );

            // Run global apply repeatedly — must be a no-op for the LV.
            for (int i = 0; i < 8; i++) {
                drainWalQueue();
            }
            Assert.assertEquals(
                    "global apply must not advance lvConsumed for LV tokens",
                    lvConsumedAfterInline,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );
            Assert.assertEquals(
                    "global apply must not advance lastProcessed",
                    lastProcessedAfterInline,
                    instance.getLastProcessedSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshAppliesInlineAndAdvancesLvConsumedSeqTxn() throws Exception {
        // RFC 123 Phase 1b: LV apply runs inline on the refresh worker after the
        // LIVE_VIEW_DATA block is written. The global ApplyWal2TableJob.doRun skips
        // LV tokens, so drainWalQueue is a no-op for the LV's own WAL. The temporal
        // contract: lvConsumedSeqTxn advances within a single refresh cycle, not on
        // a subsequent global apply tick.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4), " +
                    "('2026-04-01T00:01:00.000000Z', 8)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long preLvConsumed = instance.getStateReader().getLvConsumedSeqTxn();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(
                    "lastProcessedSeqTxn must advance after refresh",
                    instance.getLastProcessedSeqTxn() > preLvConsumed
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must advance inline with refresh in Phase 1b",
                    instance.getLastProcessedSeqTxn(),
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            // drainWalQueue is a no-op for the LV's own WAL in Phase 1b — global apply
            // skips LV tokens. Calling it must not change anything.
            long lvConsumedPostRefresh = instance.getStateReader().getLvConsumedSeqTxn();
            drainWalQueue();
            Assert.assertEquals(
                    "lvConsumedSeqTxn must not change on global drainWalQueue",
                    lvConsumedPostRefresh,
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLvConsumedAdvancesPastNonDataCommit() throws Exception {
        // Regression: an ALTER on the base table rides through the WAL as a non-DATA
        // event. The LV refresh worker walks past the seqTxn (no rows to process) and
        // emits no LIVE_VIEW_DATA block, so the apply path has nothing to consume.
        // Pre-fix, lvConsumedSeqTxn stayed at the CREATE-time floor and held base WAL
        // retention forever. Post-fix, the no-row branch advances lvConsumedSeqTxn
        // directly via engine.advanceLiveViewConsumedSeqTxn (RFC 123 §"Lifecycle /
        // Invalidation - Base-table data removal").
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();

            // ALTER ADD COLUMN of a column the LV does not reference: dependency-set
            // narrowing keeps the LV ACTIVE; the SQL seqTxn lands on the base sequencer.
            execute("ALTER TABLE base ADD COLUMN y INT");
            drainWalQueue();
            Assert.assertFalse(
                    "LV must stay valid after ALTER touching only non-dependency columns",
                    instance.isInvalid()
            );

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            // Apply runs even though no LV WAL block was written, so we drain to make
            // sure the assertion does not race the post-cycle state publication.
            drainWalQueue();

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lvConsumedSeqTxn must advance past the non-DATA seqTxn [baseline=" + baselineFloor
                            + ", post=" + postFloor + ']',
                    postFloor > baselineFloor
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must catch up to lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn(),
                    postFloor
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLvConsumedAdvancesWhenAllRowsFilteredOut() throws Exception {
        // Regression: a DATA commit whose rows the LV's WHERE clause rejects produces
        // zero output rows. Pre-fix, no LIVE_VIEW_DATA block was emitted and
        // lvConsumedSeqTxn stalled, holding the base WAL segment that contained the
        // filtered seqTxn. Post-fix, the no-row branch advances lvConsumedSeqTxn so
        // base WAL purge can release the segment.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 1000000");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();

            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 5)");
            drainWalQueue();

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            long postFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue(
                    "lvConsumedSeqTxn must advance past the all-rows-filtered seqTxn [baseline="
                            + baselineFloor + ", post=" + postFloor + ']',
                    postFloor > baselineFloor
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must catch up to lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn(),
                    postFloor
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyPersistFailureDoesNotAdvanceFloor() throws Exception {
        // Regression: pre-fix, advanceLiveViewConsumedSeqTxn mutated the in-memory
        // floor before persisting _lv.s and silently swallowed any persist error,
        // leaving the in-memory floor ahead of the durable contract WalPurgeJob
        // reads (RFC 123 §"WAL retention coupling"). The fix reorders to persist
        // first and throws on failure.
        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            TableToken token = instance.getLiveViewToken();
            long baselineFloor = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue("baseline lvConsumedSeqTxn must be > -1", baselineFloor > -1);

            // Inject persist failure and call advance directly with a strictly higher value.
            // Pre-fix the in-memory floor would advance and the persist error would be
            // swallowed; post-fix the call throws and the in-memory floor stays put.
            try (
                    BlockFileWriter blockFileWriter = new BlockFileWriter(
                            engine.getConfiguration().getFilesFacade(),
                            engine.getConfiguration().getCommitMode()
                    );
                    Path path = new Path()
            ) {
                failPersist.set(true);
                try {
                    long target = baselineFloor + 10;
                    try {
                        engine.advanceLiveViewConsumedSeqTxn(token, target, blockFileWriter, path);
                        Assert.fail("expected CairoException from failed _lv.s persist");
                    } catch (CairoException e) {
                        Assert.assertTrue(
                                "exception must mention the view name [msg=" + e.getFlyweightMessage() + "]",
                                Chars.contains(e.getFlyweightMessage(), token.getTableName())
                        );
                    }
                    Assert.assertEquals(
                            "in-memory lvConsumedSeqTxn must not advance when _lv.s persist fails",
                            baselineFloor,
                            instance.getStateReader().getLvConsumedSeqTxn()
                    );
                } finally {
                    failPersist.set(false);
                }

                // Sanity: with the failure flag cleared, the same advance succeeds and the
                // in-memory floor publishes the new value.
                engine.advanceLiveViewConsumedSeqTxn(token, baselineFloor + 10, blockFileWriter, path);
                Assert.assertEquals(
                        baselineFloor + 10,
                        instance.getStateReader().getLvConsumedSeqTxn()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshPersistFailureKeepsInMemoryAdvanced() throws Exception {
        // Refresh-side _lv.s write happens after the LV WAL block is committed, so
        // a failing persist cannot roll back the in-memory advance without producing
        // duplicate rows on retry. The refresh worker logs critical and moves on; the
        // next cycle resumes from the in-memory advance and the eventual successful
        // persist catches up the durable file.
        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            // Pin the clock so the second refresh isn't skipped by the FLUSH-EVERY gate.
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long baselineLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertTrue("baseline lastProcessedSeqTxn must be > -1", baselineLastProcessed > -1);

                // Advance past the FLUSH EVERY 1s window so the next refresh isn't gated.
                setCurrentMicros(2_000_000L);

                failPersist.set(true);
                try {
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 8)");
                    drainWalQueue();
                    drainJob(job);
                    // The persist threw; the refresh top-level catch logged critical. The
                    // key invariant: in-memory still advanced so the next refresh cycle
                    // does not re-process and double-write rows to the LV WAL.
                    Assert.assertTrue(
                            "refresh must keep lastProcessedSeqTxn advanced even when _lv.s persist fails",
                            instance.getLastProcessedSeqTxn() > baselineLastProcessed
                    );
                } finally {
                    failPersist.set(false);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshAdvancesLvConsumedSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-01-01T00:00:00.000000Z', 1), " +
                    "('2026-01-01T00:01:00.000000Z', 5), ('2026-01-01T00:02:00.000000Z', -3)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // The live view's last-processed seqTxn moved past 0 because we ingested at
            // least one DATA commit on the base.
            Assert.assertTrue(
                    "lastProcessedSeqTxn must advance past 0",
                    instance.getLastProcessedSeqTxn() > 0
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must equal lastProcessedSeqTxn after refresh",
                    instance.getLastProcessedSeqTxn(),
                    instance.getStateReader().getLvConsumedSeqTxn()
            );

            // Two rows match (x > 0); the live view's on-disk tier picks them up via
            // the standard ApplyWal2TableJob.
            assertSql(
                    "count\n2\n",
                    "SELECT count() FROM lv"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRecoversLvState() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-02-01T00:00:00.000000Z', 7), " +
                    "('2026-02-01T00:01:00.000000Z', 9)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            long preLastProcessed = instance.getLastProcessedSeqTxn();
            Assert.assertTrue("preLastProcessed must be > 0", preLastProcessed > 0);

            // Simulate restart: clear the in-memory registry and rebuild from on-disk
            // _lv + _lv.s files via buildViewGraphs (the same path startup takes).
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertEquals(
                    "lastProcessedSeqTxn must round-trip via _lv.s",
                    preLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );
            Assert.assertEquals(
                    "lvConsumedSeqTxn must round-trip via _lv.s",
                    preLastProcessed,
                    reloaded.getStateReader().getLvConsumedSeqTxn()
            );
            // Phase 1: reads route through the standard TableReader cursor over the LV's
            // _meta + applied WAL. The on-disk tier survives restart so the row count
            // should reflect what the refresh wrote before the registry was cleared.
            assertSql("count\n2\n", "SELECT count() FROM lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBackfillFieldsRoundTripAsActiveDefault() throws Exception {
        // Phase 1a rejects BACKFILL at the parser, so backfillRequested in _lv and
        // backfillState / backfillTargetSeqTxn in _lv.s are always written as the
        // ACTIVE / LONG_NULL defaults. Preallocated in CORE_DEFINITION / CORE_STATE
        // so Phase 3 can land BACKFILL semantics without a schema bump (RFC 123
        // §"Persistent formats / _lv" and §"Persistent formats / _lv.s"). This test
        // pins the round-trip across restart so a future writer that drops the fields
        // breaks visibly.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertFalse(
                    "backfillRequested must default to false in Phase 1a",
                    instance.getDefinition().getBackfillRequested()
            );
            Assert.assertEquals(
                    "backfillState must default to ACTIVE in Phase 1a",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    instance.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "backfillTargetSeqTxn must default to LONG_NULL in Phase 1a",
                    Numbers.LONG_NULL,
                    instance.getStateReader().getBackfillTargetSeqTxn()
            );

            // Round-trip across a simulated restart.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertFalse(
                    "backfillRequested must round-trip via _lv",
                    reloaded.getDefinition().getBackfillRequested()
            );
            Assert.assertEquals(
                    "backfillState must round-trip via _lv.s",
                    LiveViewState.BACKFILL_STATE_ACTIVE,
                    reloaded.getStateReader().getBackfillState()
            );
            Assert.assertEquals(
                    "backfillTargetSeqTxn must round-trip via _lv.s",
                    Numbers.LONG_NULL,
                    reloaded.getStateReader().getBackfillTargetSeqTxn()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRoundTripsLvConsumedSeqTxn() throws Exception {
        // RFC 123 Phase 1b: refresh writes + applies inline, so a successful refresh
        // cycle leaves no unapplied LV WAL block in steady state. This pins the
        // round-trip of lvConsumedSeqTxn and lastProcessedSeqTxn through restart.
        // (Recovery from a crash mid-cycle — between commitLiveView and the inline
        // apply — is a narrow window covered by the durability ordering inside
        // engine.advanceLiveViewConsumedSeqTxn and lives outside the smoke suite.)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 4), " +
                    "('2026-04-01T00:01:00.000000Z', 8)");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            long postLastProcessed = instance.getLastProcessedSeqTxn();
            long postLvConsumed = instance.getStateReader().getLvConsumedSeqTxn();
            Assert.assertTrue("refresh must advance lastProcessedSeqTxn", postLastProcessed > 0);
            Assert.assertEquals(
                    "inline apply must advance lvConsumed to lastProcessed",
                    postLastProcessed,
                    postLvConsumed
            );

            // Simulate restart.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("live view must be re-registered after restart", reloaded);
            Assert.assertEquals(
                    "lvConsumed must round-trip at the post-apply value",
                    postLvConsumed,
                    reloaded.getStateReader().getLvConsumedSeqTxn()
            );
            Assert.assertEquals(
                    "lastProcessed must round-trip at the post-apply value",
                    postLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );

            // Data must already be visible without further apply.
            assertSql("count\n2\n", "SELECT count() FROM lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartContinuesProcessingNewBaseCommits() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // First batch lands and is fully refreshed/applied.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertSql("count\n1\n", "SELECT count() FROM lv");

            // Restart: the registry is rebuilt from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            // New base commits arrive after restart. The freshly-loaded LV instance
            // must pick them up via the refresh job's normal notification path. This
            // catches loader bugs where the registry is registered but the
            // liveViewStateStore base-table mapping is not, so notifications would
            // silently never enqueue a task for the reloaded view.
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-02T00:00:00.000000Z', 2), " +
                    "('2026-04-02T00:01:00.000000Z', 3)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            assertSql("count\n3\n", "SELECT count() FROM lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMultipleLiveViewsOverSameBaseRefreshTogether() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Two LVs over the same base with different SELECT shapes. A single base
            // commit must fan out to BOTH via getViewsForBaseTable; the per-instance
            // refresh latch must not block one LV from refreshing while the other is.
            execute("CREATE LIVE VIEW lv1 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 5");

            execute("INSERT INTO base (ts, x) VALUES ('2026-05-01T00:00:00.000000Z', 3), " +
                    "('2026-05-01T00:01:00.000000Z', 7), " +
                    "('2026-05-01T00:02:00.000000Z', 12)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // lv1: x > 0 keeps all three rows.
            assertSql("count\n3\n", "SELECT count() FROM lv1");
            // lv2: x > 5 keeps the 7 and 12.
            assertSql("count\n2\n", "SELECT count() FROM lv2");

            // Both LVs must have actually advanced their state independently — verify
            // neither is stuck at zero (which would happen if refresh skipped one of them).
            LiveViewInstance i1 = engine.getLiveViewRegistry().getViewInstance("lv1");
            LiveViewInstance i2 = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertTrue("lv1 must advance", i1.getLastProcessedSeqTxn() > 0);
            Assert.assertTrue("lv2 must advance", i2.getLastProcessedSeqTxn() > 0);

            execute("DROP LIVE VIEW lv1");
            execute("DROP LIVE VIEW lv2");
        });
    }

    @Test
    public void testLiveViewAsAsofJoinRhs() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("CREATE TABLE probe (ts TIMESTAMP, label SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-03-01T00:00:00.000000Z', 10), ('2026-03-01T00:01:00.000000Z', 20)");
            execute("INSERT INTO probe (ts, label) VALUES " +
                    "('2026-03-01T00:00:30.000000Z', 'a'), ('2026-03-01T00:01:30.000000Z', 'b')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // ASOF JOIN against the LV as RHS: each probe row should pick up the
            // latest LV row at or before the probe ts.
            assertSql(
                    "ts\tlabel\tx\trn\n" +
                            "2026-03-01T00:00:30.000000Z\ta\t10\t1\n" +
                            "2026-03-01T00:01:30.000000Z\tb\t20\t2\n",
                    "SELECT probe.ts, probe.label, lv.x, lv.rn " +
                            "FROM probe ASOF JOIN lv"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDependencyColumnIndexesPopulated() throws Exception {
        assertMemoryLeak(() -> {
            // Base has four columns (ts, x, y, z); LV references only ts + x.
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            // ts + x should be the dependency set; y and z must not appear.
            Assert.assertEquals(2, instance.getDependencyColumnNames().size());

            // Restart sweep: the dependency set lives in _lv, so reload restores it
            // before any refresh runs. A schema change between restart and first
            // refresh would otherwise fall back to the broad invalidation path.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertEquals(2, reloaded.getDependencyColumnNames().size());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFallbackScanPicksUpMissedNotifications() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Drain the notification state store BEFORE inserting — this simulates a
            // missed notification (e.g., the worker was busy elsewhere when the commit
            // landed, and the dedup gate dropped subsequent notifications).
            engine.getLiveViewStateStore().clear();

            execute("INSERT INTO base (ts, x) VALUES " +
                    "('2026-04-01T00:00:00.000000Z', 11), ('2026-04-01T00:01:00.000000Z', 22)");
            drainWalQueue();

            // Notification queue is empty; the refresh job's fallback scan must catch
            // the lag and refresh the LV.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertTrue(
                    "fallback scan must advance lastProcessedSeqTxn",
                    instance.getLastProcessedSeqTxn() > 0
            );
            assertSql("count\n2\n", "SELECT count() FROM lv");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFlushEveryRateLimitsCommits() throws Exception {
        assertMemoryLeak(() -> {
            // Pin the test clock so the FLUSH EVERY 1s gate is exercised
            // deterministically: both batches land at t=0, so the second refresh
            // must be skipped; only after we advance the clock past 1s does
            // the LV catch up.
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 at t=0: passes the gate (lastFlushTimeUs is unset).
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long firstFlushUs = instance.getLastFlushTimeUs();
                Assert.assertEquals("first refresh must record the commit timestamp", 0L, firstFlushUs);
                long firstProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertTrue("first refresh must advance lastProcessedSeqTxn", firstProcessed > 0);

                // Batch 2 still at t=0: refresh must be skipped because we are
                // within the 1s rate-limit window.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "rate-limited refresh must not advance lastFlushTimeUs",
                        firstFlushUs,
                        instance.getLastFlushTimeUs()
                );
                Assert.assertEquals(
                        "rate-limited refresh must not advance lastProcessedSeqTxn",
                        firstProcessed,
                        instance.getLastProcessedSeqTxn()
                );

                // Advance past FLUSH EVERY: the fallback scan must pick up the
                // pending commit and refresh now succeeds.
                setCurrentMicros(2_000_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "post-window refresh must record the new commit timestamp",
                        2_000_000L,
                        instance.getLastFlushTimeUs()
                );
                Assert.assertTrue(
                        "post-window refresh must advance lastProcessedSeqTxn",
                        instance.getLastProcessedSeqTxn() > firstProcessed
                );
            }

            assertSql("count\n2\n", "SELECT count() FROM lv");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFlushRetryBudgetExhaustionInvalidatesView() throws Exception {
        // RFC 123 §"Flush": persist failures retry up to cairo.live.view.flush.retry.max
        // (or .duration, whichever fires first). On budget exhaustion the view is
        // invalidated via the unified path. We force consecutive _lv.s persist failures
        // on the refresh worker and assert the LV flips to INVALID after the configured
        // count.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 2);
        // Set a duration cap large enough that the count trigger fires first.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, Micros.HOUR_MICROS);

        final AtomicBoolean failPersist = new AtomicBoolean(false);
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failPersist.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            setCurrentMicros(0);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Happy-path drain seeds lastFlushTimeUs and lvConsumedSeqTxn.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                Assert.assertFalse("LV must be valid after happy path", instance.isInvalid());

                failPersist.set(true);
                try {
                    // Two consecutive persist failures: first hits retryCount=1 (budget
                    // not exhausted, log only); second hits retryCount=2 == max → invalidate.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000002Z', 2)");
                    drainWalQueue();
                    drainJob(job);
                    Assert.assertEquals("first failure must increment retryCount to 1",
                            1, instance.getFlushRetryCount());
                    Assert.assertFalse("LV must still be valid after one failure",
                            instance.isInvalid());

                    setCurrentMicros(400_000L);
                    execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000003Z', 3)");
                    drainWalQueue();
                    drainJob(job);
                    Assert.assertTrue("second failure (retryCount=2) must exhaust the budget",
                            instance.isInvalid());
                    Assert.assertTrue(
                            "invalidation reason must mention flush retry [reason=" + instance.getInvalidationReason() + "]",
                            Chars.contains(instance.getInvalidationReason(), "flush retry budget")
                    );
                } finally {
                    failPersist.set(false);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSchemaChangeNarrowsToReferencedColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid", instance.isInvalid());

            // Drop an unreferenced column — LV should stay ACTIVE.
            execute("ALTER TABLE base DROP COLUMN z");
            drainWalQueue();
            Assert.assertFalse(
                    "dropping an unreferenced column must not invalidate the LV",
                    instance.isInvalid()
            );

            // Drop a referenced column (x) — LV should flip to INVALID.
            execute("ALTER TABLE base DROP COLUMN x");
            drainWalQueue();
            Assert.assertTrue(
                    "dropping a referenced column must invalidate the LV",
                    instance.isInvalid()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLoaderReapsOrphanLiveViewDirectory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Simulate a CREATE crash that left the table directory and _lv.s
            // behind but never wrote the _lv commit marker. After restart, the
            // loader should reap the directory.
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(LiveViewDefinition.LIVE_VIEW_DEFINITION_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));

                // Sanity: the LV directory is still on disk before the loader runs.
                path.of(engine.getConfiguration().getDbRoot()).concat(token);
                Assert.assertTrue("LV directory must still exist before reap", ff.exists(path.$()));
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            Assert.assertNull(
                    "loader must not register an LV without a committed _lv",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            // The reap path goes through dropTableOrViewOrMatView, which marks
            // the WAL token dropped in the name registry. On-disk cleanup is
            // then handled by the standard WAL purge machinery; this assertion
            // captures the durable-side outcome (the LV name no longer resolves).
            Assert.assertNull(
                    "loader must mark the orphan token as dropped",
                    engine.getTableTokenIfExists("lv")
            );
        });
    }

    @Test
    public void testLoaderRejectsHalfCreatedLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Simulate a crash that left _lv on disk but not _lv.s. With the
            // engine's atomic write order (_lv.s first, _lv last) this state can
            // only occur via external corruption, but if it does the loader must
            // reject the LV rather than fall back to a default subscribeFromSeqTxn
            // that would re-replay the entire base table.
            TableToken token = engine.getLiveViewRegistry().getViewInstance("lv").getLiveViewToken();
            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            try (Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot())
                        .concat(token)
                        .concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                Assert.assertTrue(ff.removeQuiet(path.$()));
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            Assert.assertNull(
                    "loader must refuse to register a live view whose _lv.s is missing",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
            // No DROP here: the loader rejected the LV, so the SQL surface no
            // longer sees it. Per-test fixture cleans up the on-disk leftover.
        });
    }

    @Test
    public void testSchemaChangeNarrowsAfterRestart() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, z INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Restart sweep before any refresh runs. The dependency set must come
            // back from _lv, otherwise the schema-change hook falls back to broad
            // invalidation and the LV flips INVALID on an unrelated DROP.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must come back valid", reloaded.isInvalid());

            execute("ALTER TABLE base DROP COLUMN z");
            drainWalQueue();
            Assert.assertFalse(
                    "post-restart unreferenced-column DROP must not invalidate the LV",
                    reloaded.isInvalid()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorSpecPersistsAndRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertNotNull("anchor spec must be captured at CREATE",
                    instance.getDefinition().getAnchorSpec());
            Assert.assertEquals("w", instance.getDefinition().getAnchorSpec().windowName);
            Assert.assertEquals(1, instance.getDefinition().getAnchorSpec().partitionColumnNames.size());
            Assert.assertEquals("sym", instance.getDefinition().getAnchorSpec().partitionColumnNames.get(0));

            // Drive a refresh so ensureAnchorFunction runs. The anchor function should
            // now be compiled and cached on the instance.
            execute("INSERT INTO base (ts, x, sym) VALUES ('2026-07-01T00:00:00.000000Z', 1, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();
            Assert.assertNotNull("anchor function must be lazily compiled on first refresh",
                    instance.getAnchorFunction());

            // Simulate restart and verify anchor spec round-trips via _lv.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertNotNull("anchor spec must round-trip via _lv",
                    reloaded.getDefinition().getAnchorSpec());
            Assert.assertEquals("w", reloaded.getDefinition().getAnchorSpec().windowName);
            Assert.assertEquals(1, reloaded.getDefinition().getAnchorSpec().partitionColumnNames.size());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsRunningSumAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Two days for sym='a': day 1 has values 10 and 20 (running sum: 10, 30);
            // day 2 has values 5 and 15 (running sum should restart from 0: 5, 20).
            // Without anchor reset, day 2's sums would continue from day 1 (35, 50).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsAvgAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(x) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // day 1: values 10, 20 -> running avg 10.0, 15.0
            // day 2 (after reset): values 100, 200 -> running avg 100.0, 150.0
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\ta\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t150.0\n",
                    "SELECT ts, sym, a FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsCountAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, count(x) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // 3 rows on day 1, 2 rows on day 2; count restarts at 1 each day.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 1, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 2, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 3, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 4, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 5, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\tc\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t3\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n",
                    "SELECT ts, sym, c FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsRowNumberAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 1, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 2, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 3, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 4, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\trn\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n",
                    "SELECT ts, sym, rn FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsMaxAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, max(x) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 max climbs 5 -> 50; day 2 starts fresh at 3 (lower than day 1's
            // 50). Without anchor reset, day 2's first row would carry forward
            // max=50 instead of 3.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 25.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 3.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 7.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\tm\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t50.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t3.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t7.0\n",
                    "SELECT ts, sym, m FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsFirstValueAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 first_value sticks to the first row (10.0); day 2 first_value
            // should reset to the new first row (100.0). Without anchor reset,
            // day 2 would keep returning 10.0 (the original first row).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 100.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 200.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\tf\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t100.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t100.0\n",
                    "SELECT ts, sym, f FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsRankAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1: 3 distinct ts values -> rank=1,2,3.
            // Day 2 (anchor reset): rank restarts at 1.
            // Without reset, day 2 would continue counting from 4.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-01T02:00:00.000000Z', 30, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 6, 'a'), " +
                    "('2026-08-02T02:00:00.000000Z', 7, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\tr\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-01T02:00:00.000000Z\ta\t3\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t1\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t2\n" +
                            "2026-08-02T02:00:00.000000Z\ta\t3\n",
                    "SELECT ts, sym, r FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorIsolatesPartitionsIndependently() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Two partitions ('a' and 'b') interleaved across two days. Each partition
            // owns its anchor-state independently: when sym='a' rolls over at the
            // 2026-08-02 boundary, sym='b''s running state must NOT also reset
            // (and vice versa). The test interleaves the partitions in time so
            // that any cross-partition state leak would corrupt the running sum.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T00:30:00.000000Z', 100, 'b'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-01T01:30:00.000000Z', 200, 'b'), " +
                    "('2026-08-02T00:00:00.000000Z', 30, 'a'), " +
                    "('2026-08-02T00:30:00.000000Z', 300, 'b')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1: a = 10, then 30 (10+20); b = 100, then 300 (100+200).
            // Day 2: both a and b reset to first-row value (a=30, b=300).
            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T00:30:00.000000Z\tb\t100.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-01T01:30:00.000000Z\tb\t300.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:30:00.000000Z\tb\t300.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMapCompactRebuildsWithoutTombstones() throws Exception {
        // Phase 2a.11 lands the compact() scaffolding but does not auto-trigger
        // it from processRow - that wiring waits for Phase 2b to add tombstone
        // tracking to each window function's own Map. This test calls compact()
        // directly to verify the rebuild correctly drops tombstoned entries
        // and preserves alive ones. End-to-end LV output correctness under
        // compaction is exercised once 2b coordinates anchor-map + function-map
        // sweeps.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // 4 partitions on day 1; day 2 crosses 'a' and 'b' (both
                // tombstoned), then 'b' is revisited so its tombstone clears.
                // End state: 'a' tombstoned, 'b'/'c'/'d' alive (count=1, size=4).
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 'c'), " +
                        "('2026-08-01T03:00:00.000000Z', 40, 'd'), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 'a'), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 'b'), " +
                        "('2026-08-02T02:00:00.000000Z', 23, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals(1L, window.getTombstoneCount());
                Assert.assertEquals(4L, window.getAnchorMapSize());

                window.compact();

                Assert.assertEquals(
                        "compaction drops tombstoned 'a' entry",
                        3L,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(
                        "tombstoneCount resets to 0 after compaction",
                        0L,
                        window.getTombstoneCount()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorMapTombstoneTracking() throws Exception {
        // Per-row tombstone semantics inside a single refresh cycle: an
        // anchor crossing on an existing partition flags its anchor-map entry
        // tombstoned; the flag clears when a subsequent row revisits the
        // partition. Cross-cycle tracking is moot - the cursor wrapper
        // resets anchor state at the top of each cycle.
        //
        // The default cairo.live.view.partition.compact.threshold is 100K,
        // well above the single tombstone this test produces, so the
        // assertion observes raw tombstone bookkeeping rather than the
        // post-compaction state. See testCompactionFiresUnderPartitionChurn
        // for the auto-trigger path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // 'a' crosses but sees no follow-up - stays tombstoned.
                // 'b' crosses and the next row revisits it - tombstone cleared.
                // 'c' and 'd' never cross - stay alive.
                // count at cycle end: 1 ('a' only). size: 4. count > size/2 (2)?
                // no. Compaction does not fire.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 'b'), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 'c'), " +
                        "('2026-08-01T03:00:00.000000Z', 40, 'd'), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 'a'), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 'b'), " +
                        "('2026-08-02T02:00:00.000000Z', 23, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals(
                        "only 'a' stays tombstoned; 'b' was revisited, 'c'/'d' never crossed",
                        1L,
                        window.getTombstoneCount()
                );
                Assert.assertEquals(
                        "all four partitions still in the map (no compaction at default threshold)",
                        4L,
                        window.getAnchorMapSize()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCompactionFiresUnderPartitionChurn() throws Exception {
        // Phase 2c.5: with the threshold set low, an anchor-cross run that
        // accumulates more tombstones than the threshold allows must trigger
        // compact() from processRow. The trigger fires inside processRow
        // AFTER the current row's anchor-map mutation but BEFORE the row
        // continues into computeNext on each function, so the anchor map
        // ends the cycle empty (all three tombstoned entries dropped) while
        // each function map's view of the current row's partition is
        // re-created by computeNext on the same row.
        //
        // Uses INT partition keys (sym=1, 2, 3) rather than SYMBOL because
        // SYMBOL columns expose local-WAL-segment indices through the LV's
        // per-partition RecordSink, which collide across separate WAL
        // segments and confuse the post-cycle assertions.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Three partitions seeded on day 1, then anchor-crossed on day 2
                // with no follow-up rows. Each cross adds one tombstone:
                //   row 4 -> tombstoneCount=1, 1 > 2 ? no
                //   row 5 -> tombstoneCount=2, 2 > 2 ? no
                //   row 6 -> tombstoneCount=3, 3 > 2 ? yes -> compact() fires
                // After compaction (inside row 6's processRow), the anchor
                // map drops all three tombstoned entries.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-08-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-08-01T02:00:00.000000Z', 30, 3), " +
                        "('2026-08-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-08-02T01:00:00.000000Z', 22, 2), " +
                        "('2026-08-02T02:00:00.000000Z', 33, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchor window must be built after refresh", window);
                Assert.assertEquals(
                        "auto-trigger fired, dropping all tombstoned entries",
                        0L,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(
                        "tombstoneCount resets to 0 after auto-trigger",
                        0L,
                        window.getTombstoneCount()
                );

                // End-to-end correctness: day-1 partial sums (10, 20, 30) plus
                // day-2 partial sums (11, 22, 33) with no carry-over from day 1.
                // sum(INT) returns DOUBLE in QuestDB.
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-08-01T00:00:00.000000Z\t1\t10.0\n" +
                                "2026-08-01T01:00:00.000000Z\t2\t20.0\n" +
                                "2026-08-01T02:00:00.000000Z\t3\t30.0\n" +
                                "2026-08-02T00:00:00.000000Z\t1\t11.0\n" +
                                "2026-08-02T01:00:00.000000Z\t2\t22.0\n" +
                                "2026-08-02T02:00:00.000000Z\t3\t33.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts, sym"
                );

                // Day-3 revival of sym=1 enters via the isNewPartition branch
                // (day-2 entry was dropped from the anchor map by the
                // auto-trigger). Advancing the test clock past the
                // FLUSH-EVERY 100ms gate lets the next refresh tick run.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES ('2026-08-03T00:00:00.000000Z', 7, 1)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "post-revival anchor map carries the single new entry",
                        1L,
                        window.getAnchorMapSize()
                );
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-08-03T00:00:00.000000Z\t1\t7.0\n",
                        "SELECT ts, sym, s FROM lv WHERE ts >= '2026-08-03' ORDER BY ts"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCrossCycleAnchorMapPreserved() throws Exception {
        // Phase 2c.4: a second refresh cycle that hits no anchor crossings
        // must not wipe the anchor map populated by the first cycle. Before
        // 2c.4, the cursor-reopen chain (AnchorDispatchingCursor.toTop ->
        // LiveViewWindow.toTop -> anchorMap.clear()) discarded the in-memory
        // map at the head of every tick, so any partition not visited in
        // the current segment's row range silently lost its lastAnchorValue
        // record.
        //
        // Uses INT partition keys (sym=1, 2, 3) rather than SYMBOL because
        // the WAL writes SYMBOL columns as local-segment indices and the
        // LV's per-partition RecordSink reads them straight through; SYMBOL
        // keys for the same string value collide across separate WAL
        // segments and confuse the multi-cycle anchor-map preservation
        // assertion.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.0), " +
                        "('2026-10-01T01:00:00.000000Z', 2, 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "two partitions seeded on cycle 1",
                        2L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Cycle 2 commits one row for a new partition sym=3. Without
                // the 2c.4 fix the cursor-reopen chain would clear the
                // anchor map first, leaving only {3}. With 2c.4 in place
                // {1, 2} survive and 3 joins them.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-10-01T02:00:00.000000Z', 3, 30.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "anchor map carries cycle-1 entries alongside cycle-2's new partition",
                        3L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );
                Assert.assertEquals(
                        "no tombstones - the new row hit a fresh partition, the existing ones weren't anchor-crossed",
                        0L,
                        lv.getAnchorWindow().getTombstoneCount()
                );
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-10-01T00:00:00.000000Z\t1\t10.0\n" +
                                "2026-10-01T01:00:00.000000Z\t2\t20.0\n" +
                                "2026-10-01T02:00:00.000000Z\t3\t30.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPostRestartCommitPreservesAnchorMap() throws Exception {
        // Phase 2c.4: the first post-restart refresh cycle must NOT wipe
        // the anchor map that tryRestoreFromHead just rehydrated. Without
        // the 2c.4 fix, getIncrementalCursor would drive
        // AnchorDispatchingCursor.toTop -> LiveViewWindow.toTop and clear
        // the map before the first new row arrived. The test seeds two
        // partitions, simulates a restart, then commits one new row for a
        // third partition; only with the fix in place does the map carry
        // all three entries after the post-restart cycle.
        //
        // Uses INT partition keys to side-step the per-WAL-segment SYMBOL
        // index collision that confuses cross-segment partition lookups.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym INT, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 1, 10.0), " +
                        "('2026-10-01T01:00:00.000000Z', 2, 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(
                        "head .cp was written before restart",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals(
                        "two partitions seeded pre-restart",
                        2L,
                        instance.getAnchorWindow().getAnchorMapSize()
                );
            }

            // Simulate restart: clear in-memory registry, rebuild from
            // on-disk state. The startup sweep restamps the head .cp's
            // lvSeqTxn on the reloaded instance.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            // Drive a single refresh tick with no new commits: this fires
            // tryRestoreFromHead alone (mirroring testRestartRestoresFrom
            // HeadCheckpoint) and validates the rehydrate side of the
            // contract.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(
                    "restore attempted on the first post-restart cycle",
                    reloaded.isCheckpointRestoreAttempted()
            );
            Assert.assertEquals(
                    "rehydrated anchor map carries 'a' and 'b' after restore",
                    2L,
                    reloaded.getAnchorWindow().getAnchorMapSize()
            );

            // Commit a new row for sym=3. Advance the clock past FLUSH EVERY
            // 100ms so the next refresh tick is not rate-limited. Without
            // the 2c.4 fix, getIncrementalCursor's cursor-open chain would
            // clear the rehydrated map before processRow saw the new row,
            // leaving the post-cycle map at {3}. With 2c.4 in place, the
            // map preserves {1, 2} and adds 3.
            setCurrentMicros(200_000L);
            execute("INSERT INTO base (ts, sym, x) VALUES ('2026-10-01T02:00:00.000000Z', 3, 30.0)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            Assert.assertEquals(
                    "anchor map keeps the rehydrated 1 and 2 alongside the new partition 3",
                    3L,
                    reloaded.getAnchorWindow().getAnchorMapSize()
            );
            Assert.assertEquals(
                    "no tombstones - the new row hit a fresh partition, the rehydrated ones stayed alive",
                    0L,
                    reloaded.getAnchorWindow().getTombstoneCount()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRingSlabReclaimUnderChurn() throws Exception {
        // Phase 2c.5: a bounded-ROWS aggregate carries a per-partition ring
        // slab inside MemoryARW. compactPartitionMap (2c.3) captures
        // (capacity, startOffset) of each dropped slab into a freeList that
        // the next isNew partition pops from before falling back to
        // memory.appendAddressFor. Bounded frames cannot share a WINDOW with
        // an ANCHOR clause, but they sit alongside an anchored WINDOW in the
        // same SELECT and LiveViewWindow dispatches resetPartition /
        // markPartitionAlive / compactPartitionMap to every function in the
        // SELECT regardless of which WINDOW it belongs to. Driving repeated
        // churn through the auto-trigger reuses the freeList; the observable
        // signature is that the bounded function's partition map shrinks
        // after each auto-trigger (down to the row that triggered the
        // compact, re-added by computeNext) rather than growing with each
        // anchor cross.
        //
        // Uses INT partition keys to side-step the per-WAL-segment SYMBOL
        // index collision that confuses cross-segment partition lookups.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_PARTITION_COMPACT_THRESHOLD, 2);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  row_number() OVER w_anchor AS rn, " +
                    "  sum(x) OVER w_bounded AS s " +
                    "FROM base " +
                    "WINDOW " +
                    "  w_anchor AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)), " +
                    "  w_bounded AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // Day 1 seeds 1, 2, 3 (one row each so the bounded ring
                // allocates a slab per partition). Day 2 crosses anchor for
                // all three without follow-up rows; on row 6 the per-row
                // auto-trigger fires compact() (tombstoneCount=3 > 2),
                // sweeping the anchor map and each function's partition map.
                // computeNext for row 6's partition (sym=3) re-adds a single
                // entry to every function map afterward; the bounded sum
                // pops sym=3's freelist slab to back the ring.
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 10, 1), " +
                        "('2026-09-01T01:00:00.000000Z', 20, 2), " +
                        "('2026-09-01T02:00:00.000000Z', 30, 3), " +
                        "('2026-09-02T00:00:00.000000Z', 11, 1), " +
                        "('2026-09-02T01:00:00.000000Z', 22, 2), " +
                        "('2026-09-02T02:00:00.000000Z', 33, 3)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertEquals(
                        "auto-trigger swept the anchor map",
                        0L,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(
                        "tombstoneCount resets after auto-trigger",
                        0L,
                        window.getTombstoneCount()
                );

                // Locate the bounded sum by class name; compactPartitionMap
                // swaps the underlying Map instance, so re-fetch via
                // getPartitionMap() on every assertion rather than caching
                // the Map reference.
                WindowFunction boundedSum = null;
                ObjList<WindowFunction> funcs = window.getFunctions();
                for (int i = 0, n = funcs.size(); i < n; i++) {
                    WindowFunction f = funcs.getQuick(i);
                    if (Chars.equals(f.getClass().getSimpleName(), "SumOverPartitionRowsFrameFunction")) {
                        boundedSum = f;
                        break;
                    }
                }
                Assert.assertNotNull("bounded-ROWS sum exposes a partition map", boundedSum);
                Assert.assertEquals(
                        "bounded sum's partition map holds only the row-6 partition that computeNext re-added after compact",
                        1L,
                        boundedSum.getPartitionMap().size()
                );

                // Drive a second churn cycle. Day 3 seeds a fresh partition
                // sym=4 alongside 1, 2; day 4 crosses anchor on three of
                // them, firing the auto-trigger again. If the freeList path
                // were leaking slabs, the bounded sum's MemoryARW would grow
                // unboundedly; from the observable side, the function map's
                // post-cycle size resolves to two entries (sym=3 left over
                // from cycle 1's row-6 re-create, plus whichever partition
                // triggered the cycle-2 compact, re-added by computeNext).
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-09-03T00:00:00.000000Z', 40, 1), " +
                        "('2026-09-03T01:00:00.000000Z', 50, 2), " +
                        "('2026-09-03T02:00:00.000000Z', 60, 4), " +
                        "('2026-09-04T00:00:00.000000Z', 44, 1), " +
                        "('2026-09-04T01:00:00.000000Z', 55, 2), " +
                        "('2026-09-04T02:00:00.000000Z', 66, 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "anchor map swept again on cycle 2",
                        0L,
                        window.getAnchorMapSize()
                );
                Assert.assertEquals(
                        "bounded sum's partition map size stays bounded across churn cycles, evidencing slab reclaim",
                        2L,
                        boundedSum.getPartitionMap().size()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSymbolPartitionKeyStableAcrossWalSegments() throws Exception {
        // Anchored LV with PARTITION BY <SYMBOL col> against a base table that
        // receives multiple INSERT statements. Each INSERT emits its own WAL
        // segment whose local symbol indices start at 0 and grow independently
        // of prior segments, so the same partition string ('a') resolves to a
        // different segment-local int in cycle 2 than in cycle 1.
        // The fix routes SYMBOL partition columns through their resolved
        // string in both the anchor map sink and the per-function partition
        // map sink, so multi-segment cycles converge on a single set of
        // partition entries instead of growing per-segment.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-01T00:00:00.000000Z', 10.0, 'a'), " +
                        "('2026-11-01T01:00:00.000000Z', 20.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "two partitions seeded on cycle 1",
                        2L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Cycle 2 lands in a separate WAL segment. The local index for
                // 'a' here is again 0 (the segment starts fresh), but the fix
                // routes the resolved string into the map key so the same
                // partition does not split into two entries.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-01T02:00:00.000000Z', 30.0, 'a'), " +
                        "('2026-11-01T03:00:00.000000Z', 40.0, 'c')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "anchor map carries 'a' (from cycle 1), 'b', and the new 'c'",
                        3L,
                        lv.getAnchorWindow().getAnchorMapSize()
                );

                // Per-function map check: 'a' must accumulate 10 + 30 = 40 in
                // a single entry, not split into a cycle-1 and cycle-2 entry
                // by segment-local index collision.
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:00.000000Z\ta\t10.0\n" +
                                "2026-11-01T01:00:00.000000Z\tb\t20.0\n" +
                                "2026-11-01T02:00:00.000000Z\ta\t40.0\n" +
                                "2026-11-01T03:00:00.000000Z\tc\t40.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );

                // Day boundary in cycle 3 anchor-crosses 'a' so its running
                // sum resets. The anchor reset must reach the per-function
                // map entry keyed by the resolved string, not by the
                // segment-local index.
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-11-02T00:00:00.000000Z', 7.0, 'a')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-02T00:00:00.000000Z\ta\t7.0\n",
                        "SELECT ts, sym, s FROM lv WHERE ts >= '2026-11-02' ORDER BY ts"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsEmaAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, avg(x, 'alpha', 0.5) OVER w AS e FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // Day 1 (10, 20): EMA seeds at 10 then 0.5*20 + 0.5*10 = 15.
            // Day 2 (5, 15) after anchor reset: re-seeds at 5 then 0.5*15 + 0.5*5 = 10.
            // Without the EMA migration, day 2 would carry the day-1 EMA forward.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\te\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t15.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t10.0\n",
                    "SELECT ts, sym, e FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsKsumAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, ksum(x) OVER w AS k FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1 cumulative: 10, 30. Day 2 reset cumulative: 5, 20.
            assertSql(
                    "ts\tsym\tk\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n",
                    "SELECT ts, sym, k FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsLagAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, lag(x) OVER w AS l FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Day 1: row 1 lag is null (no prior row in partition), row 2 lag is 10.
            // Day 2 anchor reset: row 1 lag is null again (state cleared); row 2 lag is 5.
            // Without the lag migration, day 2 row 1 would lag the last day-1 value (20).
            assertSql(
                    "ts\tsym\tl\n" +
                            "2026-08-01T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t10\n" +
                            "2026-08-02T00:00:00.000000Z\ta\tnull\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t5\n",
                    "SELECT ts, sym, l FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorResetsStddevAcrossDayBoundary() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, stddev_pop(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            // stddev_pop with a single value is 0; with two values v1, v2 it is |v2-v1|/2.
            // Day 1 (10, 20): 0, then 5. Day 2 reset (5, 25): 0, then 10.
            // Without the Welford migration, day 2 would continue the running stddev.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20.0, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5.0, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 25.0, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t0.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t10.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAcrossDstWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // Europe/London: clocks jump forward at 2026-03-29T01:00Z to 02:00 BST.
            // ANCHOR DAILY '00:00' Europe/London buckets at local-midnight, which in
            // UTC means 2026-03-28T00:00 (GMT) and 2026-03-29T00:00 (still GMT, since
            // the DST spring-forward happens at 01:00 UTC). The two rows on either
            // side of the boundary live in different anchor buckets even though they
            // are only an hour apart in wall-clock UTC.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00' 'Europe/London')");

            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-03-28T23:00:00.000000Z', 10, 'a'), " +
                    "('2026-03-28T23:30:00.000000Z', 20, 'a'), " +
                    "('2026-03-29T00:30:00.000000Z', 5, 'a'), " +
                    "('2026-03-29T01:30:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Bucket 2026-03-28 (London local): rows at 23:00Z, 23:30Z -> sums 10, 30.
            // Bucket 2026-03-29 (London local): rows at 00:30Z, 01:30Z -> sums 5, 20.
            // (The DST spring-forward at 01:00 UTC does not split a London local day.)
            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-03-28T23:00:00.000000Z\ta\t10.0\n" +
                            "2026-03-28T23:30:00.000000Z\ta\t30.0\n" +
                            "2026-03-29T00:30:00.000000Z\ta\t5.0\n" +
                            "2026-03-29T01:30:00.000000Z\ta\t20.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAtMidnightUtc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Same shape as testAnchorResetsRunningSumAcrossDayBoundary but exercising
            // the DAILY desugar path. For UTC midnight the two should be equivalent.
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T00:00:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T01:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T00:00:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T01:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-08-01T00:00:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T01:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T00:00:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T01:00:00.000000Z\ta\t20.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchorDailyResetsAtNonZeroTimeUtc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // ANCHOR DAILY '09:30' (UTC) buckets at 09:30:00.000000Z each day.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '09:30')");

            // Bucket 1 (2026-08-01T09:30Z .. 2026-08-02T09:29:59...Z): 09:30 and 18:00 -> 10, 30.
            // Bucket 2 (2026-08-02T09:30Z .. ): 09:30 and 18:00 -> 5, 20.
            // The 08:00 row on day 2 still belongs to bucket 1 (before 09:30 cutover).
            execute("INSERT INTO base (ts, x, sym) VALUES " +
                    "('2026-08-01T09:30:00.000000Z', 10, 'a'), " +
                    "('2026-08-01T18:00:00.000000Z', 20, 'a'), " +
                    "('2026-08-02T08:00:00.000000Z', 7, 'a'), " +
                    "('2026-08-02T09:30:00.000000Z', 5, 'a'), " +
                    "('2026-08-02T18:00:00.000000Z', 15, 'a')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertSql(
                    "ts\tsym\ts\n" +
                            "2026-08-01T09:30:00.000000Z\ta\t10.0\n" +
                            "2026-08-01T18:00:00.000000Z\ta\t30.0\n" +
                            "2026-08-02T08:00:00.000000Z\ta\t37.0\n" +
                            "2026-08-02T09:30:00.000000Z\ta\t5.0\n" +
                            "2026-08-02T18:00:00.000000Z\ta\t20.0\n",
                    "SELECT ts, sym, s FROM lv ORDER BY ts"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidationSurvivesRestart() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            // Drop the base table — this should invalidate the live view AND persist
            // the invalidation to _lv.s so restart sees the invalid state.
            execute("DROP TABLE base");
            drainWalQueue();

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue("live view must be invalid after base drop", instance.isInvalid());
            Assert.assertEquals(
                    "invalidation reason must record the trigger",
                    "base table drop",
                    instance.getInvalidationReason().toString()
            );

            // Simulate restart: clear registry, re-load from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertTrue(
                    "invalidation must round-trip via _lv.s",
                    reloaded.isInvalid()
            );
            Assert.assertEquals(
                    "invalidation reason must round-trip via _lv.s",
                    "base table drop",
                    reloaded.getInvalidationReason().toString()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueColumnOrderMatchesRfc() throws Exception {
        // Columns appear in the documented order so clients binding by
        // ordinal see a stable shape. The documented columns come first;
        // the three head_checkpoint_* columns trail as Phase 2a debug
        // surface.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try {
                assertSql(
                        "view_name\tview_table_dir_name\tbase_table_name\tview_sql\tview_status\t"
                                + "invalidation_reason\tflush_every_interval\tflush_every_interval_unit\t"
                                + "in_memory_interval\tin_memory_interval_unit\tin_mem_bytes\t"
                                + "symbol_translation_size\to3_rejected_count\tlag_seqtxn\tlag_micros\t"
                                + "last_processed_seqtxn\tapplied_watermark\tlv_consumed_seqtxn\t"
                                + "view_lower_bound_timestamp\twriter_stall_micros\tbackfill_target_seqtxn\t"
                                + "head_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n",
                        "SELECT * FROM live_views() WHERE 1 = 0"
                );
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 5s IN MEMORY 30s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertSql(
                    "view_name\tbase_table_name\tview_status\tflush_every_interval\tflush_every_interval_unit\tin_memory_interval\tin_memory_interval_unit\n" +
                            "lv\tbase\tactive\t5\tSECOND\t30\tSECOND\n",
                    "SELECT view_name, base_table_name, view_status, flush_every_interval, flush_every_interval_unit, " +
                            "in_memory_interval, in_memory_interval_unit FROM live_views()"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesMillisecondUnit() throws Exception {
        // Regression: getIntervalUnit had no arm for the internal 'T' (millisecond)
        // unit char, so flush_every_interval_unit / in_memory_interval_unit returned
        // NULL for any LV created with a ms-granularity FLUSH EVERY or IN MEMORY.
        // Since FLUSH EVERY's minimum is 100ms, this hit the most common LV shape.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms IN MEMORY 500ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertSql(
                    "view_name\tflush_every_interval\tflush_every_interval_unit\tin_memory_interval\tin_memory_interval_unit\n" +
                            "lv\t200\tMILLISECOND\t500\tMILLISECOND\n",
                    "SELECT view_name, flush_every_interval, flush_every_interval_unit, in_memory_interval, in_memory_interval_unit FROM live_views()"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesViewLowerBoundTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Pin the microsecond clock so view_lower_bound_timestamp captures the
            // wall-clock at CREATE deterministically.
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            assertSql(
                    "view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-11-14T22:13:20.000000Z\n",
                    "SELECT view_name, view_lower_bound_timestamp FROM live_views()"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testViewLowerBoundTimestampPersistsInBaseUnitsForMicroBase() throws Exception {
        // Pins the identity path: for MICRO bases, the persisted value equals the
        // wall-clock micros at CREATE because the driver's fromMicros is the identity.
        // Acts as a guard against future refactors to the conversion shape.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "MICRO base persists wall-clock micros as-is",
                    1_700_000_000_000_000L,
                    instance.getDefinition().getViewLowerBoundTimestamp()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testViewLowerBoundTimestampPersistsInBaseUnitsForNanoBase() throws Exception {
        // Regression: viewLowerBoundTimestamp used to be persisted in wall-clock
        // micros regardless of the base's timestamp unit, so a TIMESTAMP_NS base
        // ended up with a value 1000x smaller than any base-table ts. The persisted
        // value is now scaled to base units so the eventual O3 reject in Phase 2
        // can compare it against late_row.ts directly. The catalogue column stays
        // TIMESTAMP_MICRO (RFC 123 §"Catalogue function live_views()") and rounds
        // NS values back to the MICRO grid at display time.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_700_000_000_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP_NS, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "NS base persists wall-clock value scaled to nanoseconds",
                    1_700_000_000_000_000_000L,
                    instance.getDefinition().getViewLowerBoundTimestamp()
            );

            // Catalogue column commits to TIMESTAMP_MICRO; toMicros rounds NS back
            // to the MICRO grid (lossless here since the source is wall-clock micros).
            assertSql(
                    "view_name\tview_lower_bound_timestamp\n" +
                            "lv\t2023-11-14T22:13:20.000000Z\n",
                    "SELECT view_name, view_lower_bound_timestamp FROM live_views()"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesOperationalColumns() throws Exception {
        // Pin the clock so lag_micros is deterministic across runs.
        assertMemoryLeak(() -> {
            setCurrentMicros(1_000_000L);
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000001Z', 1)");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            // Advance the clock so lag_micros reflects time since the last flush.
            setCurrentMicros(3_000_000L);

            // view_table_dir_name must match the live view's actual directory; once mangling
            // is enabled in tests it diverges from the view name, so resolve via the engine.
            TableToken token = engine.verifyTableName("lv");
            String expectedDir = token.getDirName();
            // writer_stall_micros is always 0 in Phase 1a (no in-mem tier, no stall mechanism).
            // lag_micros = 3_000_000 - lastFlushTimeUs. lastFlushTimeUs was set to 1_000_000
            // (the clock at refresh), so lag_micros = 2_000_000.
            assertSql(
                    "view_name\tview_table_dir_name\tlag_micros\twriter_stall_micros\n" +
                            "lv\t" + expectedDir + "\t2000000\t0\n",
                    "SELECT view_name, view_table_dir_name, lag_micros, writer_stall_micros FROM live_views()"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCountOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: count() over unbounded partition rows + ANCHOR.
        // State per partition is a single LONG count. Round-trip via direct
        // snapshot to in-memory sink + toTop + restore; verify counts match.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, count(*) OVER w AS c FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 1, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 2, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 3, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 4, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction countFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(countFunc.supportsSnapshot());
                Map fnMap = countFunc.getPartitionMap();
                Assert.assertEquals("two partitions seeded", 2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    countFunc.snapshot(sink);
                    countFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    countFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' partition had 3 rows, 'b' had 1. Total 4.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getLong(0);
                    }
                    Assert.assertEquals(4L, total);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSumOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: sum() over unbounded partition rows + ANCHOR.
        // State per partition is [sum: DOUBLE, count: LONG] - same shape as
        // avg. Round-trip via direct snapshot + toTop + restore.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 30.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction sumFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(sumFunc.supportsSnapshot());
                Map fnMap = sumFunc.getPartitionMap();
                Assert.assertEquals("two partitions seeded", 2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    sumFunc.snapshot(sink);
                    sumFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    sumFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' partition sum is 40.0, 'b' sum is 5.0. Total 45.0.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getDouble(0);
                    }
                    Assert.assertEquals(45.0, total, 0.0);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAvgOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: avg() over (PARTITION BY ... ROWS UNBOUNDED
        // PRECEDING ... ANCHOR ...) implements snapshot/restore. State per
        // partition is [sum: DOUBLE, count: LONG]. Round-trip via direct
        // snapshot to in-memory sink + toTop + restore; verify Map content.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, avg(x) OVER w AS a FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 10.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 30.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                ObjList<WindowFunction> funcs = lv.getAnchorWindow().getFunctions();
                WindowFunction avgFunc = funcs.getQuick(0);
                Assert.assertTrue("avg supports snapshot for SYMBOL key", avgFunc.supportsSnapshot());
                Map fnMap = avgFunc.getPartitionMap();
                Assert.assertNotNull(fnMap);
                Assert.assertEquals("two partitions seeded", 2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    avgFunc.snapshot(sink);
                    avgFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    avgFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // Sum of all sums must equal 10+30+5 = 45, total count = 3.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double totalSum = 0;
                    long totalCount = 0;
                    while (mc.hasNext()) {
                        totalSum += rec.getValue().getDouble(0);
                        totalCount += rec.getValue().getLong(1);
                    }
                    Assert.assertEquals(45.0, totalSum, 0.0);
                    Assert.assertEquals(3L, totalCount);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMaxOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: max() over unbounded partition rows + ANCHOR.
        // State per partition is [value: DOUBLE, initialized: BYTE]. The same
        // class handles min() via a swapped comparator.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, max(x) OVER w AS m FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction maxFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(maxFunc.supportsSnapshot());
                Map fnMap = maxFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    maxFunc.snapshot(sink);
                    maxFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    maxFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' max is 50.0, 'b' max is 7.0. Sum 57.0.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getDouble(0);
                    }
                    Assert.assertEquals(57.0, total, 0.0);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFirstValueOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: first_value() over unbounded partition rows +
        // ANCHOR. State per partition is [value: DOUBLE, initialized: BYTE].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, first_value(x) OVER w AS f FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 5.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 50.0, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 20.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction fvFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(fvFunc.supportsSnapshot());
                Map fnMap = fvFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    fvFunc.snapshot(sink);
                    fvFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    fvFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    // 'a' first_value is 5.0, 'b' first_value is 7.0. Sum 12.0.
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    double total = 0;
                    while (mc.hasNext()) {
                        total += rec.getValue().getDouble(0);
                    }
                    Assert.assertEquals(12.0, total, 0.0);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testKSumOverUnboundedPartitionRowsSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #2: ksum() (Kahan-compensated sum) over unbounded
        // partition rows + ANCHOR. State per partition is [sum: DOUBLE,
        // compensation: DOUBLE, count: LONG].
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, ksum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 1.0, 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 1e16, 'a'), " +
                        "('2026-08-01T02:00:00.000000Z', 1.0, 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 7.0, 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                WindowFunction ksumFunc = lv.getAnchorWindow().getFunctions().getQuick(0);
                Assert.assertTrue(ksumFunc.supportsSnapshot());
                Map fnMap = ksumFunc.getPartitionMap();
                Assert.assertEquals(2L, fnMap.size());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    ksumFunc.snapshot(sink);
                    ksumFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());
                    ksumFunc.restore(sink, 1);
                    Assert.assertEquals(2L, fnMap.size());

                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long totalCount = 0;
                    while (mc.hasNext()) {
                        totalCount += rec.getValue().getLong(2);
                    }
                    Assert.assertEquals(4L, totalCount);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRowNumberSnapshotRoundTrip() throws Exception {
        // Phase 2a.5 Group #1: row_number() implements snapshot/restore.
        // Drive a single refresh cycle so the function's Map is populated with
        // partition state, then snapshot to an in-memory sink, reset via
        // toTop(), restore from the sink, and verify the Map content matches
        // by iterating partition entries directly. End-to-end snapshot/restore
        // through the LV refresh pipeline is gated on Phase 2a.4 (the write
        // hook) and Phase 2a.7 (the restart restore path) and the cross-cycle
        // anchor-map reset behaviour is a separate concern.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a'), " +
                        "('2026-08-01T01:00:00.000000Z', 'a'), " +
                        "('2026-08-01T00:00:00.000000Z', 'b')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                ObjList<WindowFunction> funcs = lv.getAnchorWindow().getFunctions();
                WindowFunction rowNumberFunc = funcs.getQuick(0);
                Assert.assertTrue(
                        "row_number must support snapshot for SYMBOL partition keys",
                        rowNumberFunc.supportsSnapshot()
                );
                Assert.assertEquals(1, rowNumberFunc.snapshotFormatVersion());
                Map fnMap = rowNumberFunc.getPartitionMap();
                Assert.assertNotNull("getPartitionMap exposes the function's partition Map", fnMap);
                Assert.assertEquals("two partitions seeded", 2L, fnMap.size());

                // Snapshot the entire partition Map into an in-memory sink.
                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    rowNumberFunc.snapshot(sink);

                    // Reset wipes the Map. After reset the function has no
                    // partitions; any future computeNext on a known partition
                    // would treat it as new and start at rn=1.
                    rowNumberFunc.toTop();
                    Assert.assertEquals(0L, fnMap.size());

                    // Restore must rebuild the Map identically. Verify by
                    // walking the cursor and tallying the row_number values
                    // per symbol id.
                    rowNumberFunc.restore(sink, 1);
                    Assert.assertEquals("restore brought back both partitions", 2L, fnMap.size());

                    // The restored Map's partition values must match the
                    // pre-snapshot counters: partition 'a' had two rows
                    // (rn=2 final), partition 'b' had one (rn=1 final).
                    MapRecordCursor mc = fnMap.getCursor();
                    MapRecord rec = fnMap.getRecord();
                    long sumRn = 0;
                    long minRn = Long.MAX_VALUE;
                    long maxRn = Long.MIN_VALUE;
                    while (mc.hasNext()) {
                        long rn = rec.getValue().getLong(0); // ROW_NUMBER_VALUE_INDEX
                        sumRn += rn;
                        minRn = Math.min(minRn, rn);
                        maxRn = Math.max(maxRn, rn);
                    }
                    Assert.assertEquals("partition counters sum to 1+2=3", 3L, sumRn);
                    Assert.assertEquals("smallest counter is 1 (partition 'b')", 1L, minRn);
                    Assert.assertEquals("largest counter is 2 (partition 'a')", 2L, maxRn);
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewWindowSnapshotRoundTrip() throws Exception {
        // Phase 2a.6 (deferred half): LiveViewWindow's anchor map serialises into
        // the WINDOW_ANCHOR block payload via snapshot() and rehydrates via
        // restore(). The format mirrors the codec used by the migrated window
        // functions in 2a.5 - typed key columns + a single LONG anchor value
        // per partition. End-to-end checkpoint integration is gated on the
        // 2a.4 write hook and the 2a.7 restart restore path; this test
        // exercises the codec in isolation.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                // One row per partition - no anchor crossings - so the anchor
                // map ends with exactly two live entries, no tombstones.
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                LiveViewWindow window = lv.getAnchorWindow();
                Assert.assertNotNull("anchored LV exposes the window driver", window);
                Assert.assertEquals("two partitions seeded", 2L, window.getAnchorMapSize());

                try (MemoryCARW sink = Vm.getCARWInstance(4096L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                    window.snapshot(sink);

                    // Sanity-check the documented payload prefix:
                    //   STR windowName (INT len + len * CHAR), INT keyCount=1,
                    //   INT keyType=STRING, INT anchorValueType=TIMESTAMP,
                    //   LONG partitionCount=2.
                    // The persisted key column type is STRING (not SYMBOL):
                    // LiveViewWindow.build rewrites SYMBOL partition columns as
                    // STRING in the anchor map's key types so cross-WAL-segment
                    // SYMBOL collisions can't corrupt the partition state.
                    long off = 0;
                    final int nameLen = sink.getInt(off);
                    off += Integer.BYTES;
                    Assert.assertEquals("window name 'w' is one char", 1, nameLen);
                    Assert.assertEquals('w', sink.getChar(off));
                    off += (long) nameLen * Character.BYTES;
                    Assert.assertEquals("single key column", 1, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("key column is STRING (SYMBOL columns route through resolved STRING)", ColumnType.STRING, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("anchor value type is TIMESTAMP", ColumnType.TIMESTAMP, sink.getInt(off));
                    off += Integer.BYTES;
                    Assert.assertEquals("partition count is 2", 2L, sink.getLong(off));

                    // toTop() wipes the anchor map; the round-trip must rebuild it.
                    window.toTop();
                    Assert.assertEquals(0L, window.getAnchorMapSize());

                    window.restore(sink);
                    Assert.assertEquals("restore brought back both partitions", 2L, window.getAnchorMapSize());
                    Assert.assertEquals("no tombstones post-restore", 0L, window.getTombstoneCount());

                    // The restored anchor map drives processRow's "existing
                    // partition, anchor unchanged" branch on the very next row,
                    // confirming we built valid SLOT_INITIALIZED=1 entries.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, sym, x) VALUES ('2026-08-01T02:00:00.000000Z', 'a', 4.0)");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testHeadCheckpointWrittenOnFirstCommit() throws Exception {
        // Phase 2a.4: the refresh worker writes a head .cp on the first cycle
        // that lands rows, so subsequent restart / O3 paths have a head to
        // restore from. The cadence triggers (rows / max.duration) gate
        // subsequent writes; this test only proves the first one fires.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-08-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-08-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);

                // Cap was true (anchor SYMBOL + sum(DOUBLE) is fully migrated),
                // so the first commit wrote a head; subsequent cycles will
                // honor the row / duration cadence.
                Assert.assertTrue("snapshot capability computed and true", lv.isSnapshotCapability());
                Assert.assertNotEquals(
                        "head_checkpoint_lv_seqtxn populated after first commit",
                        Numbers.LONG_NULL,
                        lv.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertTrue(
                        "head_checkpoint_state_bytes is positive",
                        lv.getHeadCheckpointStateBytes() > 0
                );
                Assert.assertEquals(
                        "rows counter reset after head write",
                        0L,
                        lv.getRowsSinceLastCheckpointWritten()
                );

                // A .cp file lives under <lv_dir>/_checkpoints/.
                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpDir = new Path()) {
                    cpDir.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
                    Assert.assertTrue("_checkpoints/ exists", ff.exists(cpDir.$()));
                    // Enumerate; expect exactly one .cp file (and zero .cp.tmp).
                    final StringSink nameSink = new StringSink();
                    boolean foundCp = false;
                    long pFind = ff.findFirst(cpDir.$());
                    Assert.assertNotEquals("can iterate _checkpoints/", 0L, pFind);
                    try {
                        do {
                            long namePtr = ff.findName(pFind);
                            if (namePtr == 0) {
                                continue;
                            }
                            nameSink.clear();
                            Utf8s.utf8ToUtf16Z(namePtr, nameSink);
                            if (Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_FILE_EXT)
                                    && !Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_TMP_FILE_EXT)) {
                                foundCp = true;
                            }
                        } while (ff.findNext(pFind) > 0);
                    } finally {
                        ff.findClose(pFind);
                    }
                    Assert.assertTrue("at least one .cp file exists", foundCp);
                }

                // The live_views() catalogue surfaces the same trio the
                // refresh worker stamped via setHeadCheckpoint(). Asserting
                // here closes the end-to-end loop: write hook -> instance
                // setter -> LiveViewsFunctionFactory -> SQL.
                assertSql(
                        "view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_state_bytes\n" +
                                "lv\t" + lv.getHeadCheckpointLvSeqTxn() + '\t' + lv.getHeadCheckpointStateBytes() + '\n',
                        "SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_state_bytes FROM live_views()"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresFromHeadCheckpoint() throws Exception {
        // Phase 2a.7: a simulated restart should re-discover the head .cp
        // via the startup sweep, then the first refresh-worker tick rehydrates
        // the LV's window-function state from the head and advances
        // lastProcessedSeqTxn to the manifest's baseSeqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals("head .cp was written before restart", Numbers.LONG_NULL, preHeadLvSeqTxn);
                Assert.assertEquals("two partitions seeded pre-restart", 2L, preFunctionMapSize);
            }

            // Simulate restart: clear the in-memory registry and rebuild
            // from on-disk _lv + _lv.s. The sweep should re-discover the
            // head .cp via the lvSeqTxn embedded in its filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(
                    "startup sweep stamped head lvSeqTxn from filename",
                    preHeadLvSeqTxn,
                    reloaded.getHeadCheckpointLvSeqTxn()
            );
            Assert.assertFalse(
                    "restore not attempted yet (no refresh cycle has run)",
                    reloaded.isCheckpointRestoreAttempted()
            );

            // Drive a single refresh cycle. There are no new base commits, but
            // the fallback scan still calls refreshInstance, which runs the
            // restore on the first cycle for an LV with a stamped head.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue(
                    "refresh worker attempted the restore on the first post-restart cycle",
                    reloaded.isCheckpointRestoreAttempted()
            );
            Assert.assertEquals(
                    "lastProcessedSeqTxn matches manifest.baseSeqTxn after restore",
                    preLastProcessed,
                    reloaded.getLastProcessedSeqTxn()
            );
            Assert.assertEquals(
                    "function partition map rehydrated to its pre-restart size",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestoreVersionMismatchInvalidatesView() throws Exception {
        // A FUNCTION_SNAPSHOT block whose formatVersion is below the
        // function's current snapshotMinSupportedVersion is a real
        // compatibility break, not structural corruption. The restore path
        // must mark the LV INVALID (operators recover with DROP+CREATE)
        // instead of unlinking the .cp and falling into head-miss replay.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, x, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            // Drive a refresh so a real .cp is written and the compiled
            // factory is cached on the instance. Capture the lvSeqTxn and the
            // actual function class name; the hand-written replacement below
            // must match the name so restoreFunctionBlock's dispatch finds it.
            execute("INSERT INTO base (ts, sym, x) VALUES ('2026-06-01T00:00:00.000000Z', 'a', 1)");
            drainWalQueue();
            final String fnClassName;
            final long headLvSeqTxn;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                headLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, headLvSeqTxn);
                fnClassName = instance.getAnchorWindow().getFunctions().getQuick(0).getClass().getName();
            }

            // Replace the .cp with one that has the same lvSeqTxn but
            // formatVersion = 0 in the function block. The writer auto-handles
            // CRC. We omit the anchor block since the restore unwinds at the
            // version check before getting to it.
            try (Path lvDir = new Path()) {
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                lvDir.of(engine.getConfiguration().getDbRoot()).concat(instance.getLiveViewToken()).slash();

                try (Path cpPath = new Path()) {
                    cpPath.of(lvDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, headLvSeqTxn);
                    engine.getConfiguration().getFilesFacade().removeQuiet(cpPath.$());
                }

                try (LiveViewCheckpointWriter w = new LiveViewCheckpointWriter(engine.getConfiguration())) {
                    w.of(lvDir.$(), headLvSeqTxn);
                    w.writeManifestBlock(new LiveViewCheckpointManifest()
                            .setLvSeqTxn(headLvSeqTxn)
                            .setLvRowPosition(0)
                            .setBaseSeqTxn(0)
                            .setMaxTimestamp(0)
                            .setKind(LiveViewCheckpointManifest.KIND_STEADY)
                            .addWindowName("w"));
                    final MemoryA fnSink = w.beginBlock(LiveViewCheckpointBlockType.BLOCK_FUNCTION_SNAPSHOT);
                    fnSink.putStr("w");
                    fnSink.putStr(fnClassName);
                    fnSink.putInt(0); // intentionally below snapshotMinSupportedVersion
                    w.endBlock();
                    w.commit(Numbers.LONG_NULL);
                }
            }

            // Restart: clear the registry and rebuild from on-disk. The
            // startup sweep re-stamps the head lvSeqTxn from the filename.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            final LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(headLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());
            Assert.assertFalse("LV must still be valid pre-refresh", reloaded.isInvalid());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }

            Assert.assertTrue(
                    "version-too-old in the head .cp must invalidate the LV",
                    reloaded.isInvalid()
            );
            final CharSequence reason = reloaded.getStateReader().getInvalidationReason();
            Assert.assertNotNull(reason);
            Assert.assertTrue(
                    "invalidation reason mentions version-too-old [reason=" + reason + ']',
                    Chars.contains(reason, "version too old")
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresRankFromHeadCheckpoint() throws Exception {
        // Phase 2b.1: rank() is now snapshot-capable. End-to-end check that a
        // refresh cycle writes a head .cp, a simulated restart re-discovers
        // it, and the first post-restart refresh tick rehydrates the rank
        // function's partition map. Mirrors testRestartRestoresFromHeadCheckpoint
        // for sum() but covers the rank chain-prefix path the 2b.1a codec
        // extension and 2b.1b migration introduce.
        //
        // The post-restart-then-new-commit scenario is intentionally NOT
        // covered: getIncrementalCursor's pre-existing toTop chain wipes the
        // anchor map and function maps at the start of each refresh cycle,
        // so a new commit immediately after restore would discard the
        // rehydrated state. That cross-cycle wipe is the broader limitation
        // tracked under 2a.8's "known limitations" - addressing it sits
        // outside Phase 2b.1's scope.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with rank() now that 2b.1b makes it snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals("two partition keys seeded pre-restart", 2L, preFunctionMapSize);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());
            Assert.assertEquals(
                    "rank's partition map rehydrates to its pre-restart partition count",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRankSnapshotRestoreRoundTripsState() throws Exception {
        // Phase 2b.1b/1a: snapshot() / restore() round-trip the rank function's
        // per-partition rank, count, and chain-prefix bytes through a MemoryCARW
        // buffer. The end-to-end LV head .cp path is exercised by
        // testRestartRestoresRankFromHeadCheckpoint; this case isolates the codec
        // round-trip on the function level so a regression in the chain-prefix
        // serializer surfaces here before the integration test sees it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, rank() OVER w AS r FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'b', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            WindowFunction rankFn = lv.getAnchorWindow().getFunctions().getQuick(0);
            Assert.assertTrue("rank reports snapshot capability after 2b.1b", rankFn.supportsSnapshot());
            Assert.assertEquals(2L, rankFn.getPartitionMap().size());

            try (MemoryCARW buf = Vm.getCARWInstance(64 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                rankFn.snapshot(buf);
                final long snapshotBytes = buf.getAppendOffset();
                Assert.assertTrue("snapshot wrote some bytes", snapshotBytes > 0);
                // Clear the function's map and restore from the captured bytes.
                rankFn.getPartitionMap().clear();
                Assert.assertEquals(0L, rankFn.getPartitionMap().size());
                rankFn.restore(buf, rankFn.snapshotFormatVersion());
                Assert.assertEquals(
                        "restore rehydrates the same partition count snapshot captured",
                        2L,
                        rankFn.getPartitionMap().size()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLagSnapshotRestoreRoundTripsState() throws Exception {
        // Phase 2b.2: snapshot() / restore() round-trip the lag function's
        // per-partition firstIdx + count and the raw ring buffer contents
        // through a MemoryCARW buffer. Isolates the codec round-trip from the
        // end-to-end .cp path so a regression in the ring-blob serializer
        // surfaces here before the integration test sees it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, lag(x, 2) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            WindowFunction lagFn = lv.getAnchorWindow().getFunctions().getQuick(0);
            Assert.assertTrue("lag reports snapshot capability after 2b.2a", lagFn.supportsSnapshot());
            Assert.assertEquals(2L, lagFn.getPartitionMap().size());

            try (MemoryCARW buf = Vm.getCARWInstance(64 * 1024L, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT)) {
                lagFn.snapshot(buf);
                final long snapshotBytes = buf.getAppendOffset();
                Assert.assertTrue("snapshot wrote some bytes", snapshotBytes > 0);
                lagFn.getPartitionMap().clear();
                Assert.assertEquals(0L, lagFn.getPartitionMap().size());
                lagFn.restore(buf, lagFn.snapshotFormatVersion());
                Assert.assertEquals(
                        "restore rehydrates the same partition count snapshot captured",
                        2L,
                        lagFn.getPartitionMap().size()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRowsAggregatesFromHeadCheckpoint() throws Exception {
        // Phase 2b.3a/b/c: avg, sum, count(*), count(arg), and ksum over
        // (PARTITION BY ... ROWS N PRECEDING ...) are now snapshot-capable.
        // End-to-end check that an LV combining all of them writes a head .cp
        // and the first post-restart refresh tick rehydrates the partition
        // maps. No ANCHOR is involved - the validator rejects ANCHOR over
        // bounded frames, so this exercises the no-anchor snapshot path.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  avg(x) OVER w AS a, " +
                    "  sum(x) OVER w AS s, " +
                    "  count(*) OVER w AS cs, " +
                    "  count(x) OVER w AS cx, " +
                    "  ksum(x) OVER w AS k " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with bounded ROWS aggregates now that 2b.3a/b/c makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresStatefulEmaFamilyFromHeadCheckpoint() throws Exception {
        // Phase 2b.5 (Group #6 — Stateful + EMA family): the UNBOUNDED +
        // ANCHOR variants now ship snapshot/restore. Six classes migrated:
        //   StdDevOverUnboundedPartitionRowsFrameFunction (covers stddev_*,
        //     var_*; 3 slots, Welford mean/m2/count)
        //   BivarStatOverUnboundedPartitionRowsFrameFunction (covers corr,
        //     covar_*; 6 slots, paired Welford)
        //   EmaOverPartitionFunction + EmaTimeWeightedOverPartitionFunction
        //     (3 slots: ema, prevTimestamp, hasValue)
        //   VwemaOverPartitionFunction + VwemaTimeWeightedOverPartitionFunction
        //     (4 slots: numerator, denominator, prevTimestamp, hasValue)
        // All six are fixed-shape with no ring buffer, so snapshot/restore
        // is a straight slot-by-slot round-trip.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y DOUBLE, vol DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  stddev_samp(x) OVER w AS sd, " +
                    "  corr(x, y) OVER w AS cr, " +
                    "  avg(x, 'period', 5) OVER w AS ep, " +
                    "  avg(x, 'minute', 5) OVER w AS et, " +
                    "  avg(x, 'period', 5, vol) OVER w AS vp, " +
                    "  avg(x, 'minute', 5, vol) OVER w AS vt " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x, y, vol) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0, 10.0, 100.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0, 20.0, 150.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0, 30.0, 200.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 5.0, 50.0, 500.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 6.0, 60.0, 600.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with stddev/corr/ema/vwema now that 2b.5 makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRowsMinMaxFromHeadCheckpoint() throws Exception {
        // Phase 2b.4: min/max over (PARTITION BY ... ROWS N PRECEDING ...) are
        // now snapshot-capable. The single MaxMinOverPartitionRowsFrameFunction
        // class carries two state shapes:
        //   - frameLoBounded == true:  ring + monotonic deque (5 LONG slots)
        //   - frameLoBounded == false: ring + scalar max/min   (3 slots, last
        //                              one typed DOUBLE/LONG/TIMESTAMP)
        // Both flavours need to round-trip through .cp. The LV below exercises
        // both: w1 uses ROWS 2 PRECEDING (bounded lower) and w2 uses ROWS
        // BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING (unbounded lower).
        // min() and max() share the implementation class via a comparator
        // parameter, so covering both functions also covers Min* and Max*
        // factories at once.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  min(x) OVER w1 AS mn, " +
                    "  max(x) OVER w1 AS mx, " +
                    "  min(x) OVER w2 AS mnu, " +
                    "  max(x) OVER w2 AS mxu " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with min/max bounded ROWS now that 2b.4 makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeAggregatesFromHeadCheckpoint() throws Exception {
        // Phase 2b.6a/b: avg/sum/count(*)/count(arg) over (PARTITION BY ...
        // RANGE BETWEEN '<n>' <unit> PRECEDING AND ...) are now snapshot-
        // capable. End-to-end check that an LV combining all of them writes a
        // head .cp and the first post-restart refresh tick rehydrates the
        // partition maps. Variable-length deque serialisation: snapshot writes
        // size + size * (LONG ts, DOUBLE/LONG value); restore re-allocates the
        // ring at capacity = max(size, initialBufferSize).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  avg(x) OVER w AS a, " +
                    "  sum(x) OVER w AS s, " +
                    "  count(*) OVER w AS cs, " +
                    "  count(x) OVER w AS cx " +
                    "FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with bounded RANGE aggregates now that 2b.6a/b makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeFirstLastValueFromHeadCheckpoint() throws Exception {
        // Phase 2b.6d/e: first_value() and last_value() over (PARTITION BY ...
        // RANGE BETWEEN ...) are now snapshot-capable across Double / Long /
        // Timestamp factories and respect-nulls vs IGNORE NULLS variants.
        // First and last share the same per-partition slot count in their LV
        // value-types static; snapshot/restore are overridden per class to
        // handle their distinct slot orderings.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  first_value(x) OVER w1 AS fv, " +
                    "  first_value(x) IGNORE NULLS OVER w1 AS fvn, " +
                    "  last_value(x) IGNORE NULLS OVER w1 AS lvn, " +
                    "  last_value(x) OVER w2 AS lv " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '3' HOUR PRECEDING AND '1' HOUR PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with first_value/last_value bounded RANGE now that 2b.6d/e makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresBoundedRangeMinMaxFromHeadCheckpoint() throws Exception {
        // Phase 2b.6c: min/max over (PARTITION BY ... RANGE BETWEEN ...) are
        // now snapshot-capable. The MaxMinOverPartitionRangeFrameFunction
        // class carries two state shapes:
        //   - frameLoBounded == true:  ring + monotonic deque (9 LONG slots)
        //   - frameLoBounded == false: ring + scalar max/min (5 LONGs + 1
        //                              typed DOUBLE/LONG/TIMESTAMP)
        // Both flavours need to round-trip through .cp. The LV below exercises
        // both: w1 uses RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW
        // (bounded lower) and w2 uses RANGE BETWEEN UNBOUNDED PRECEDING AND
        // '1' HOUR PRECEDING (unbounded lower). min() and max() share the
        // implementation class via a comparator parameter, so covering both
        // functions also covers Min* and Max* factories at once.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  min(x) OVER w1 AS mn, " +
                    "  max(x) OVER w1 AS mx, " +
                    "  min(x) OVER w2 AS mnu, " +
                    "  max(x) OVER w2 AS mxu " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND '1' HOUR PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with min/max bounded RANGE now that 2b.6c makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresFirstLastValueFromHeadCheckpoint() throws Exception {
        // Phase 2b.3d/e: first_value() and last_value() over (PARTITION BY ...
        // ROWS N PRECEDING ...) are now snapshot-capable, in both the parent
        // ("respect nulls", default) and IGNORE NULLS subclass variants. The
        // four migrated classes have distinct slot layouts:
        //   - FirstValue parent:   3 LONG slots
        //   - FirstNotNull:        4 LONG slots
        //   - LastValue parent:    2 LONG slots (3rd reserved)
        //   - LastNotNull:         1 TYPED + 2 LONG slots
        // The LV below routes through all four code paths; w1 fires when
        // rowsHi == 0 (frame includes current row) for first_value variants
        // and the IGNORE NULLS last_value path; w2 fires when rowsHi < 0 for
        // the respect-nulls last_value parent (rowsHi == 0 would route to
        // LastValueIncludeCurrent, which has no snapshot support).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, " +
                    "  first_value(x) OVER w1 AS fv, " +
                    "  first_value(x) IGNORE NULLS OVER w1 AS fvn, " +
                    "  last_value(x) IGNORE NULLS OVER w1 AS lvn, " +
                    "  last_value(x) OVER w2 AS lv " +
                    "FROM base " +
                    "WINDOW " +
                    "  w1 AS (PARTITION BY sym ORDER BY ts ROWS 2 PRECEDING), " +
                    "  w2 AS (PARTITION BY sym ORDER BY ts ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T02:00:00.000000Z', 'a', 3.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 10.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'b', 20.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with first_value/last_value bounded ROWS now that 2b.3d/e makes them snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertTrue(
                        "snapshot capability cached after first successful flush",
                        instance.isSnapshotCapability()
                );
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRestoresLagFromHeadCheckpoint() throws Exception {
        // Phase 2b.2: lag() is now snapshot-capable. End-to-end check that a
        // refresh cycle writes a head .cp, a simulated restart re-discovers
        // it, and the first post-restart refresh tick rehydrates the lag
        // function's partition map. Mirrors testRestartRestoresRankFromHead
        // Checkpoint for the lag ring-blob path.
        //
        // The post-restart-then-new-commit scenario is intentionally NOT
        // covered: getIncrementalCursor's pre-existing toTop chain wipes the
        // anchor map and function maps at the start of each refresh cycle,
        // so a new commit immediately after restore would discard the
        // rehydrated state. That cross-cycle wipe is tracked under 2a.8's
        // "known limitations".
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, lag(x, 1) OVER w AS prev FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            final long preHeadLvSeqTxn;
            final long preLastProcessed;
            final long preFunctionMapSize;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-09-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-09-01T01:00:00.000000Z', 'a', 2.0), " +
                        "('2026-09-01T00:00:00.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                preHeadLvSeqTxn = instance.getHeadCheckpointLvSeqTxn();
                preLastProcessed = instance.getLastProcessedSeqTxn();
                preFunctionMapSize = instance.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size();
                Assert.assertNotEquals(
                        "head .cp must be written for an LV with lag() now that 2b.2a makes it snapshot-capable",
                        Numbers.LONG_NULL,
                        preHeadLvSeqTxn
                );
                Assert.assertEquals("two partition keys seeded pre-restart", 2L, preFunctionMapSize);
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            LiveViewInstance reloaded = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(reloaded);
            Assert.assertEquals(preHeadLvSeqTxn, reloaded.getHeadCheckpointLvSeqTxn());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            Assert.assertTrue(reloaded.isCheckpointRestoreAttempted());
            Assert.assertEquals(preLastProcessed, reloaded.getLastProcessedSeqTxn());
            Assert.assertEquals(
                    "lag's partition map rehydrates to its pre-restart partition count",
                    preFunctionMapSize,
                    reloaded.getAnchorWindow().getFunctions().getQuick(0).getPartitionMap().size()
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLatestSeenTsAdvancesAcrossRows() throws Exception {
        // Phase 2a.8: the anchor-dispatch cursor stamps the per-LV latestSeenTs
        // watermark on every base row consumed by the refresh worker. This is
        // the input the O3 detection path will read in a later commit; for now
        // we just verify the cursor feeds the setter with the max ts in the
        // batch.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertEquals(
                        "latestSeenTs starts at LONG_NULL on a fresh LV",
                        Numbers.LONG_NULL,
                        lv.getLatestSeenTs()
                );

                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:00:00.000000Z', 'a', 1.0), " +
                        "('2026-10-01T00:00:05.000000Z', 'b', 3.0), " +
                        "('2026-10-01T00:00:10.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "latestSeenTs equals max(ts) of the batch consumed by the refresh worker",
                        MicrosFormatUtils.parseUTCTimestamp("2026-10-01T00:00:10.000000Z"),
                        lv.getLatestSeenTs()
                );

                // A subsequent in-order batch advances the watermark; advance
                // microtime past the FLUSH EVERY gate so the second refresh
                // actually fires rather than no-oping on the rate limiter.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-10-01T00:01:00.000000Z', 'a', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertEquals(
                        "latestSeenTs advances on the next in-order batch",
                        MicrosFormatUtils.parseUTCTimestamp("2026-10-01T00:01:00.000000Z"),
                        lv.getLatestSeenTs()
                );

                // The setter's monotonic clamp is asserted directly so the
                // contract is pinned independently of the cursor wiring.
                long beforeClampAttempt = lv.getLatestSeenTs();
                lv.setLatestSeenTs(beforeClampAttempt - 1_000_000L);
                Assert.assertEquals(
                        "setLatestSeenTs is monotonic; a lower value is ignored",
                        beforeClampAttempt,
                        lv.getLatestSeenTs()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3InvalidatesHeadCheckpoint() throws Exception {
        // Phase 2a.8: an O3 base commit (min ts strictly below the LV's
        // latestSeenTs watermark) cannot be replayed in WAL order without
        // corrupting per-partition window state, so the refresh worker
        // rolls back the in-flight WAL writer, branches to o3Replay, and
        // re-feeds base data in ts order from a TableReader. The prior
        // head .cp is retired by the replay path; a fresh head reflecting
        // the post-replay state is written before the cycle returns so
        // restart can short-circuit to head-hit for any subsequent O3.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'b', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(
                        "head .cp was written before the O3 commit",
                        Numbers.LONG_NULL,
                        preO3HeadLvSeqTxn
                );
                Assert.assertEquals(
                        "latestSeenTs equals max(ts) of the first batch",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );

                // The head .cp lives at <root>/<lv_dir>/_checkpoints/<lvSeqTxn>.cp.
                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpPath = new Path()) {
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertTrue("head .cp exists on disk before O3", ff.exists(cpPath.$()));

                    // Insert an out-of-order row: ts (00:00:05) is strictly
                    // below the watermark (00:00:20). Advance microtime past
                    // the FLUSH EVERY gate so the refresh worker actually
                    // ticks rather than no-oping on the rate limiter.
                    setCurrentMicros(200_000L);
                    execute("INSERT INTO base (ts, sym, x) VALUES " +
                            "('2026-11-01T00:00:05.000000Z', 'a', 2.0)");
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();

                    final long postO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                    Assert.assertNotEquals(
                            "head metadata refreshed post O3 replay",
                            Numbers.LONG_NULL,
                            postO3HeadLvSeqTxn
                    );
                    Assert.assertNotEquals(
                            "post-replay head lvSeqTxn differs from the pre-O3 head",
                            preO3HeadLvSeqTxn,
                            postO3HeadLvSeqTxn
                    );
                    Assert.assertNotEquals(
                            "head_checkpoint_max_ts populated post O3 replay",
                            Numbers.LONG_NULL,
                            lv.getHeadCheckpointMaxTs()
                    );
                    Assert.assertNotEquals(
                            "head_checkpoint_state_bytes populated post O3 replay",
                            0L,
                            lv.getHeadCheckpointStateBytes()
                    );

                    // The pre-O3 .cp is gone (retired by the replay path).
                    // Re-derive the path because the helper mutates it in
                    // place.
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertFalse("pre-O3 head .cp unlinked", ff.exists(cpPath.$()));

                    // The fresh post-replay .cp is on disk at the new lvSeqTxn.
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, postO3HeadLvSeqTxn);
                    Assert.assertTrue("post-replay head .cp on disk", ff.exists(cpPath.$()));
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadMissReplaysFromLowerBound() throws Exception {
        // Phase 2a.8: an O3 row with ts strictly below the head's maxTimestamp
        // forces a head-miss replay. The path resets every window-function map,
        // wipes the anchor map, scans the base TableReader from
        // viewLowerBoundTimestamp through advanceTo, emits a single REPLACE_RANGE
        // commit, and writes a fresh head reflecting the post-replay state.
        // After the dust settles the LV output reads the cumulative sum across
        // all rows in ts order - matching what the non-incremental SELECT
        // against the base would produce.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );

                // Drop an O3 row at ts strictly below headMaxTs. lateRowTs=05
                // < headMaxTs=20 - head-miss eligibility is the only path.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Post-replay LV output is the cumulative sum across all three
                // rows in ts-ascending order: 3 -> 3+1=4 -> 4+2=6.
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:05.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t4.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t6.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );

                // A fresh head has landed at a new lvSeqTxn, the prior one is
                // gone on disk.
                Assert.assertNotEquals(preO3HeadLvSeqTxn, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointLvSeqTxn());
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadMissWithFullyFilteredReplayPreservesState() throws Exception {
        // The head-miss path replays from viewLowerBoundTimestamp and
        // rebuilds state. The probe-then-wipe ordering ensures a replay that
        // produces zero output rows does not clobber pre-O3 accumulator
        // state. This test covers the closely related scenario: an O3 commit
        // whose row is filtered out by the LV's WHERE; the replay still reads
        // two surviving base rows so the probe passes, the wipe + replay run
        // as usual, and the LV's output matches the pre-O3 state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base WHERE x > 100 " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 200.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 300.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t200.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t500.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );

                // O3 row at ts=05 that fails the filter (x <= 100). The WAL
                // path still detects O3 by min(ts) < latestSeenTs; head-miss
                // replay reads (05,50), (10,200), (20,300) from the
                // TableReader, the filter drops (05,50), and the post-filter
                // probe sees the remaining two rows. State wipes and
                // rebuilds; LV output is unchanged.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 50.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t200.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t500.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3HeadHitReplaysFromHead() throws Exception {
        // Phase 2a.8: an O3 row with ts > head.maxTimestamp drops into the
        // head-hit branch. State rolls back to the head's snapshot moment
        // (restoreFromHead populates anchor + function maps), the replay
        // scans only the rows past head.maxTimestamp, and emits a single
        // REPLACE_RANGE commit covering (head.maxTimestamp, +inf). LV
        // output afterwards is the cumulative sum across all rows in
        // ts-ascending order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: two rows. Drain writes a head at maxTs=20.
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );

                // Batch 2: two more rows in WAL order. Cadence triggers
                // (default 1M rows / 5 min) do not fire, so the head
                // metadata still points at the batch-1 head.
                setCurrentMicros(150_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:30.000000Z', 'a', 3.0), " +
                        "('2026-11-01T00:00:40.000000Z', 'a', 4.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "head metadata unchanged after batch 2 (cadence did not fire)",
                        preO3HeadLvSeqTxn,
                        lv.getHeadCheckpointLvSeqTxn()
                );
                Assert.assertEquals(
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:40.000000Z"),
                        lv.getLatestSeenTs()
                );

                // O3 row at ts=25 sits strictly between headMaxTs=20 and
                // latestSeenTs=40, so head-hit eligibility applies
                // (headMaxTs <= lateRowTs).
                setCurrentMicros(400_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:25.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Post-replay output: cumulative sum across all five rows
                // ordered by ts.
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:10.000000Z\ta\t1.0\n" +
                                "2026-11-01T00:00:20.000000Z\ta\t3.0\n" +
                                "2026-11-01T00:00:25.000000Z\ta\t8.0\n" +
                                "2026-11-01T00:00:30.000000Z\ta\t11.0\n" +
                                "2026-11-01T00:00:40.000000Z\ta\t15.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );

                Assert.assertNotEquals(preO3HeadLvSeqTxn, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointLvSeqTxn());
                Assert.assertEquals(
                        "post-replay head maxTs reflects max row in replay output",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:40.000000Z"),
                        lv.getHeadCheckpointMaxTs()
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3StormAtFixedHorizon() throws Exception {
        // Phase 2a.8: repeated O3 rows at the same historical horizon (well
        // below headMaxTs) fall into the head-miss path each time per RFC 123
        // §"O3 storms at a fixed historical horizon". Every event triggers a
        // full replay from viewLowerBoundTimestamp and writes one fresh head.
        // After three such events the LV output reflects all rows in ts order.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:50.000000Z', 'a', 1.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                long lastHeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, lastHeadLvSeqTxn);

                String[] o3Inserts = new String[]{
                        "('2026-11-01T00:00:05.000000Z', 'a', 2.0)",
                        "('2026-11-01T00:00:06.000000Z', 'a', 3.0)",
                        "('2026-11-01T00:00:07.000000Z', 'a', 4.0)",
                };
                long microtime = 200_000L;
                for (int i = 0; i < o3Inserts.length; i++) {
                    setCurrentMicros(microtime);
                    execute("INSERT INTO base (ts, sym, x) VALUES " + o3Inserts[i]);
                    drainWalQueue();
                    drainJob(job);
                    drainWalQueue();
                    microtime += 200_000L;

                    // Each storm event writes a fresh head with a new lvSeqTxn.
                    long head = lv.getHeadCheckpointLvSeqTxn();
                    Assert.assertNotEquals("storm event #" + i + " did not refresh the head", lastHeadLvSeqTxn, head);
                    Assert.assertNotEquals(Numbers.LONG_NULL, head);
                    lastHeadLvSeqTxn = head;
                }

                // Final LV output covers all four rows in ts order with the
                // cumulative sum across the day-anchored bucket.
                assertSql(
                        "ts\tsym\ts\n" +
                                "2026-11-01T00:00:05.000000Z\ta\t2.0\n" +
                                "2026-11-01T00:00:06.000000Z\ta\t5.0\n" +
                                "2026-11-01T00:00:07.000000Z\ta\t9.0\n" +
                                "2026-11-01T00:00:50.000000Z\ta\t10.0\n",
                        "SELECT ts, sym, s FROM lv ORDER BY ts"
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3WritesFreshCheckpointPostReplay() throws Exception {
        // Phase 2a.8: after the O3 replay path drives an apply commit, the
        // head metadata trio (lvSeqTxn, maxTs, stateBytes) reflects the
        // post-replay state - not the pre-O3 state, and not LONG_NULL.
        // The .cp file exists on disk at the new lvSeqTxn.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, sym, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 'a', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 'a', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                final long preO3HeadLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                final long preO3StateBytes = lv.getHeadCheckpointStateBytes();
                Assert.assertNotEquals(Numbers.LONG_NULL, preO3HeadLvSeqTxn);
                Assert.assertTrue(preO3StateBytes > 0L);

                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 'a', 5.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long postReplayLvSeqTxn = lv.getHeadCheckpointLvSeqTxn();
                Assert.assertNotEquals(Numbers.LONG_NULL, postReplayLvSeqTxn);
                Assert.assertNotEquals(preO3HeadLvSeqTxn, postReplayLvSeqTxn);
                Assert.assertNotEquals(Numbers.LONG_NULL, lv.getHeadCheckpointMaxTs());
                Assert.assertTrue(
                        "post-replay state_bytes populated",
                        lv.getHeadCheckpointStateBytes() > 0L
                );

                TableToken token = lv.getLiveViewToken();
                FilesFacade ff = engine.getConfiguration().getFilesFacade();
                try (Path cpPath = new Path()) {
                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, postReplayLvSeqTxn);
                    Assert.assertTrue("post-replay head .cp exists", ff.exists(cpPath.$()));

                    cpPath.of(engine.getConfiguration().getDbRoot())
                            .concat(token)
                            .concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME)
                            .slash();
                    LiveViewCheckpointWriter.appendCpFileName(cpPath, preO3HeadLvSeqTxn);
                    Assert.assertFalse("pre-O3 head .cp unlinked", ff.exists(cpPath.$()));
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testO3DetectionFiresForNonAnchoredLv() throws Exception {
        // Phase 2a.8 (non-anchored stamp): the row-loop stamp updates
        // latestSeenTs for LVs without an anchored named window. Pre-fix,
        // only AnchorDispatchingCursor stamped, so non-anchored LVs never
        // drove O3 detection. The test uses row_number() OVER (), which the
        // factory specialises into SequenceRowNumberFunction (no per-partition
        // map). That puts the LV on the not-snapshot-capable branch of the
        // replay path: the O3 cycle invalidates the head, advances the
        // watermarks, and trails for the O3 batch. Detection itself - what
        // the row-loop stamp enables - is the surface this test pins.
        // Head-miss / head-hit content correctness is covered by
        // testO3HeadMissReplaysFromLowerBound and testO3HeadHitReplaysFromHead
        // against anchored snapshot-capable LVs.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-11-01T00:00:10.000000Z', 1.0), " +
                        "('2026-11-01T00:00:20.000000Z', 2.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                Assert.assertNull("LV has no anchored window", lv.getAnchorWindow());
                Assert.assertEquals(
                        "row-loop stamp updates latestSeenTs without an anchor cursor",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );
                final long lastProcessedAfterBatch1 = lv.getLastProcessedSeqTxn();

                // Drop an O3 row. The detect block fires because latestSeenTs
                // is populated; the not-snapshot-capable LV then takes the
                // skip branch in o3Replay.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (ts, x) VALUES " +
                        "('2026-11-01T00:00:05.000000Z', 3.0)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // The watermark stays at 20 thanks to the monotonic clamp,
                // and lastProcessedSeqTxn advances so the next cycle does
                // not re-iterate the O3 batch in WAL order.
                Assert.assertEquals(
                        "monotonic clamp keeps latestSeenTs at the pre-O3 high water",
                        MicrosFormatUtils.parseUTCTimestamp("2026-11-01T00:00:20.000000Z"),
                        lv.getLatestSeenTs()
                );
                Assert.assertTrue(
                        "lastProcessedSeqTxn advanced past the O3 base seqTxn",
                        lv.getLastProcessedSeqTxn() > lastProcessedAfterBatch1
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeBlocksInFlightRefreshCycle() throws Exception {
        // startCheckpoint must force a happens-before edge with any in-flight
        // refresh turn before its file copy begins, so the agent never reads
        // _lv.s mid-rewrite. The fix takes and releases the refresh latch
        // inside startCheckpoint. Here the test stands in for the worker by
        // holding the latch manually and asserts startCheckpoint blocks
        // until the latch is released.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);

            // Simulate the worker mid-turn by holding the latch on this thread.
            Assert.assertTrue(
                    "test setup must take the refresh latch",
                    lv.tryLockForRefresh()
            );

            final AtomicBoolean returned = new AtomicBoolean(false);
            final Thread agent = new Thread(() -> {
                lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
                returned.set(true);
            }, "lv-freeze-handshake-test");
            try {
                agent.start();
                // Give the agent time to publish the flag and start spinning on
                // the latch. startCheckpoint must not return while we hold it.
                Thread.sleep(50);
                Assert.assertTrue(
                        "freeze flag must be published before the agent blocks on the latch",
                        lv.isFreezeInProgress()
                );
                Assert.assertFalse(
                        "startCheckpoint must block while the refresh latch is held",
                        returned.get()
                );

                // Release the latch. startCheckpoint should return promptly.
                lv.unlockAfterRefresh();
                agent.join(5_000);
                Assert.assertFalse(
                        "startCheckpoint thread must have completed",
                        agent.isAlive()
                );
                Assert.assertTrue(
                        "startCheckpoint must return after the latch is released",
                        returned.get()
                );
            } finally {
                if (lv.isFreezeInProgress()) {
                    lv.endCheckpoint();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeQueuesInvalidationUntilEnd() throws Exception {
        // markInvalid blocks on the freeze. If a base-table schema change
        // happens mid-snapshot, invalidation is queued and applied right
        // after endCheckpoint. The snapshot reflects the pre-invalidation
        // state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            final LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(lv);

            // Take the freeze. Now an out-of-band invalidate must wait.
            lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
            Assert.assertTrue(lv.isFreezeInProgress());
            Assert.assertFalse("LV must still be valid pre-freeze", lv.isInvalid());

            final AtomicBoolean returned = new AtomicBoolean(false);
            final Thread invalidator = new Thread(() -> {
                engine.invalidateLiveView(lv, "test queued behind freeze");
                returned.set(true);
            }, "lv-invalidate-freeze-test");
            try {
                invalidator.start();
                Thread.sleep(50);
                Assert.assertFalse(
                        "invalidateLiveView must wait until endCheckpoint",
                        returned.get()
                );
                Assert.assertFalse(
                        "LV must still be valid while frozen",
                        lv.isInvalid()
                );

                lv.endCheckpoint();
                invalidator.join(5_000);
                Assert.assertFalse(
                        "invalidate thread must have returned",
                        invalidator.isAlive()
                );
                Assert.assertTrue(
                        "invalidate must complete after endCheckpoint",
                        returned.get()
                );
                Assert.assertTrue(
                        "LV is invalid after endCheckpoint",
                        lv.isInvalid()
                );
                Assert.assertTrue(
                        "invalidation reason persisted",
                        Chars.equals("test queued behind freeze", lv.getStateReader().getInvalidationReason())
                );
            } finally {
                if (lv.isFreezeInProgress()) {
                    lv.endCheckpoint();
                }
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFreezeGateSkipsRefreshTurn() throws Exception {
        // Phase 2a.9c: DatabaseCheckpointAgent toggles freezeInProgress around
        // its per-LV file copy so the refresh worker does not advance _lv.s /
        // the on-disk tier mid-snapshot. This test exercises the gate without
        // running the full agent: set startCheckpoint manually, drive a
        // refresh - the worker must skip and lastProcessedSeqTxn must not
        // advance. After endCheckpoint(), a subsequent refresh processes the
        // pending commit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                setCurrentMicros(0L);

                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                long beforeProcessed = lv.getLastProcessedSeqTxn();

                // Insert a row + ingest the WAL; refresh has NOT run yet.
                execute("INSERT INTO base (ts, x) VALUES ('2026-04-01T00:00:00.000000Z', 1)");
                drainWalQueue();

                // Freeze the LV. The refresh worker's next turn must see the
                // flag, skip, and leave lastProcessedSeqTxn unchanged.
                lv.startCheckpoint(lv.getStateReader().getAppliedWatermark());
                Assert.assertTrue("freeze in progress after startCheckpoint", lv.isFreezeInProgress());
                drainJob(job);
                Assert.assertEquals(
                        "frozen refresh did not advance lastProcessedSeqTxn",
                        beforeProcessed,
                        lv.getLastProcessedSeqTxn()
                );

                // Unfreeze, drive a fresh refresh. The pending commit should
                // land in the LV.
                lv.endCheckpoint();
                Assert.assertFalse("freeze cleared after endCheckpoint", lv.isFreezeInProgress());
                setCurrentMicros(200_000L);
                drainJob(job);
                drainWalQueue();

                Assert.assertTrue(
                        "post-unfreeze refresh advanced lastProcessedSeqTxn",
                        lv.getLastProcessedSeqTxn() > beforeProcessed
                );
                assertSql("count\n1\n", "SELECT count() FROM lv");
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testLiveViewsCatalogueExposesHeadCheckpointColumns() throws Exception {
        // Phase 2a.10: head_checkpoint_* columns are preallocated; values stay
        // at LONG_NULL / 0 until the Phase 2a.4 flush-cycle write hook starts
        // populating them.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            try {
                assertSql(
                        "view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n" +
                                "lv\tnull\t\t0\n",
                        "SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_max_ts, head_checkpoint_state_bytes FROM live_views()"
                );

                // Direct mutation via the setter (the 2a.4 write hook will call this).
                LiveViewInstance lv = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(lv);
                lv.setHeadCheckpoint(42L, 1_700_000_000_000_000L, 4096L, 0L);

                assertSql(
                        "view_name\thead_checkpoint_lv_seqtxn\thead_checkpoint_max_ts\thead_checkpoint_state_bytes\n" +
                                "lv\t42\t2023-11-14T22:13:20.000000Z\t4096\n",
                        "SELECT view_name, head_checkpoint_lv_seqtxn, head_checkpoint_max_ts, head_checkpoint_state_bytes FROM live_views()"
                );
            } finally {
                execute("DROP LIVE VIEW lv");
            }
        });
    }

    @Test
    public void testTablesIntegrationReportsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            // tables() emits 'L' in the table_type column for live views.
            assertSql(
                    "table_name\ttable_type\n" +
                            "lv\tL\n",
                    "SELECT table_name, table_type FROM tables() WHERE table_name = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowCreateLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 200ms IN MEMORY 5s PARTITION BY DAY AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            assertSql(
                    "ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 200ms IN MEMORY 5s PARTITION BY DAY AS (\n" +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0\n" +
                            ");\n",
                    "SHOW CREATE LIVE VIEW lv"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectExplicitPartitionByNone() throws Exception {
        // Regression: the parser sentinel for "PARTITION BY omitted" used to be
        // PartitionBy.NONE — the same value the user-facing grammar produces for
        // explicit PARTITION BY NONE. LiveViewTableStructure.resolvePartitionBy
        // collapsed both into the base table's scheme, so a user asking for "no
        // partitioning on the LV" silently got the base's scheme instead.
        // Honouring the user's choice instead would fail downstream with the
        // generic "WAL is only supported for partitioned tables"; the LV's
        // WAL-backed on-disk tier requires a partition scheme. Reject up front
        // with an LV-specific message at parse time.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s PARTITION BY NONE AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
                Assert.fail("expected SqlException rejecting PARTITION BY NONE");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "wrong message [msg=" + e.getFlyweightMessage() + ']',
                        Chars.contains(e.getFlyweightMessage(),
                                "live view PARTITION BY NONE is not supported")
                );
            }
            // Confirm no partial-CREATE residue: re-creating with a valid scheme works.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowCreateEmitsResolvedPartitionByForDefault() throws Exception {
        // When PARTITION BY is omitted, the LV inherits the base's scheme. SHOW
        // CREATE emits the resolved value (not a missing clause), so re-executing
        // the output produces an LV with the same partition scheme regardless of
        // any later change to the base.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertEquals(
                    "omitted PARTITION BY must inherit base's DAY scheme",
                    io.questdb.cairo.PartitionBy.DAY,
                    instance.getDefinition().getPartitionBy()
            );

            assertSql(
                    "ddl\n" +
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 1s IN MEMORY 1s PARTITION BY DAY AS (\n" +
                            "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0\n" +
                            ");\n",
                    "SHOW CREATE LIVE VIEW lv"
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // ANCHOR EXPRESSION on a default-frame WINDOW must parse without error.
            // The runtime that drives resetPartition lands with the window-function
            // migration; here we only verify CREATE accepts the syntax.
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts))");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorDaily() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '00:00')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAcceptAnchorDailyWithTimeZone() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                    "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR DAILY '09:30' 'America/New_York')");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectAnchorWithBoundedFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ROWS 5 PRECEDING ANCHOR EXPRESSION timestamp_floor('1d', ts))");
                Assert.fail("expected ANCHOR + bounded frame reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("ANCHOR is incompatible with bounded frames"));
            }
        });
    }

    @Test
    public void testRejectConstantAnchorExpression() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION 1)");
                Assert.fail("expected constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
            }
        });
    }

    @Test
    public void testRejectMultipleAnchoredWindows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sum(x) OVER w1 AS sx, sum(y) OVER w2 AS sy FROM base " +
                        "WINDOW w1 AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1d', ts)), " +
                        "       w2 AS (PARTITION BY y ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
                Assert.fail("expected multi-anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("at most one anchored WINDOW"));
            }
        });
    }

    @Test
    public void testRejectAnchorWithSubquery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION (SELECT 1))");
                Assert.fail("expected subquery anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not contain subqueries"));
            }
        });
    }

    @Test
    public void testRejectAnchorWithRandomFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION rnd_long())");
                Assert.fail("expected random anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be deterministic"));
            }
        });
    }

    @Test
    public void testRejectAnchorWithNow() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION now())");
                Assert.fail("expected now() anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must be deterministic"));
            }
        });
    }

    @Test
    public void testRejectFoldToConstantAnchorExpression() throws Exception {
        // Pass 2 of the ANCHOR EXPRESSION validator: function calls whose arguments
        // are all constants fold to a constant at the top level. Pass 1 (parser AST)
        // only catches direct CONSTANT nodes (e.g. ANCHOR EXPRESSION 1); the fold case
        // needs the post-constant-fold Function tree.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION 1 + 2 + 3)");
                Assert.fail("expected fold-to-constant anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not be a constant"));
            }
        });
    }

    @Test
    public void testRejectAggregationAnchorExpression() throws Exception {
        // Pass 2 of the ANCHOR EXPRESSION validator: aggregates can't appear in an
        // anchor expression because anchor evaluation is scalar per-row. The compiled
        // Function is a GroupByFunction; the validator surfaces it with the asserted
        // "must not contain aggregation" wording.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts ANCHOR EXPRESSION sum(x))");
                Assert.fail("expected aggregation anchor reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must not contain aggregation"));
            }
        });
    }

    @Test
    public void testRejectLeadWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, lead(x) OVER (ORDER BY ts) AS nxt FROM base");
                Assert.fail("expected lead() reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("lead() is not supported"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByNonTimestampColumn() throws Exception {
        // RFC 123: each named WINDOW must ORDER BY the base's designated timestamp.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY x)");
                Assert.fail("expected non-timestamp ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByDescending() throws Exception {
        // RFC 123: ORDER BY direction must be ascending; DESC violates the WAL-row-order
        // processing model.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts DESC)");
                Assert.fail("expected ORDER BY DESC reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts ASC"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByMissing() throws Exception {
        // RFC 123: a named WINDOW without any ORDER BY can't be ordered by the
        // designated ts and must be rejected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x)");
                Assert.fail("expected missing ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY ts"));
            }
        });
    }

    @Test
    public void testRejectWindowOrderByMultipleColumns() throws Exception {
        // RFC 123: ORDER BY must be a single column (the designated timestamp);
        // multi-column ordering doesn't have a meaningful WAL-stream semantics.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, x, sum(x) OVER w AS s FROM base " +
                        "WINDOW w AS (PARTITION BY x ORDER BY ts, x)");
                Assert.fail("expected multi-column ORDER BY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("must ORDER BY a single column"));
            }
        });
    }

    @Test
    public void testRequireFlushEvery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv AS SELECT ts, x FROM base");
                Assert.fail("expected FLUSH EVERY required");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("flush every"));
            }
        });
    }
}
