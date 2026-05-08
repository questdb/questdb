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

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

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
            try {
                execute("CREATE LIVE VIEW lv2 FLUSH EVERY 1s AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM lv1");
                Assert.fail("expected live-on-live reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("not allowed as base tables"));
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
    public void testRejectInMemoryBelowFlushEvery() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 500ms AS " +
                        "SELECT ts, x, row_number() OVER () AS rn FROM base");
                Assert.fail("expected IN MEMORY < FLUSH EVERY reject");
            } catch (SqlException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("IN MEMORY must be at least FLUSH EVERY"));
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
                            "CREATE LIVE VIEW 'lv' FLUSH EVERY 200T IN MEMORY 5s PARTITION BY DAY AS (\n" +
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
