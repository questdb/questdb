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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshTask;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewStateStore;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDL-on-base-table behaviour for live views. Focuses on schema changes that are
 * routed through {@code ApplyWal2TableJob}'s structural path and reach
 * {@code CairoEngine.invalidateLiveViewsForBaseSchemaChange}, plus the base
 * operations that travel the same job's SQL path - notably UPDATE, which rewrites
 * base rows in place and so invalidates every dependent view.
 * <p>
 * The centrepiece is {@code ALTER COLUMN TYPE} on a referenced column. The refresh
 * path derives each column's stride from the cached compile-time factory metadata,
 * so a referenced column whose type changed under the view would be read through the
 * stale stride: wrong results on a widening change, and an out-of-bounds native read
 * (SIGSEGV / corruption) on a narrowing or fixed&lt;-&gt;var-size change. The view
 * must therefore flip to INVALID before it ever refreshes over the changed base. A
 * type change to a column the view does not read must stay transparent.
 */
public class LiveViewBaseDdlTest extends AbstractLiveViewTest {

    // The non-data-commit-among-late-rows cases' view: a per-account cumulative sum that
    // resets at midnight, and what its defining query returns after the burst and the
    // follow-up row.
    private static final String ANCHORED_DAILY_EXPECTED = """
            created_at\taccount_id\tamount\tcumulative_sum
            2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1.0
            2026-01-02T08:35:58.000000Z\tacct-7\t3.0\t3.0
            2026-01-02T12:00:00.000000Z\tacct-7\t1.0\t4.0
            2026-01-03T00:10:00.000000Z\tacct-1\t1.0\t1.0
            2026-01-03T00:54:22.000000Z\tacct-5\t1.0\t1.0
            2026-01-03T01:00:00.000000Z\tacct-2\t1.0\t1.0
            2026-01-03T02:07:07.000000Z\tacct-1\t1.0\t2.0
            2026-01-03T02:30:00.000000Z\tacct-5\t1.0\t2.0
            """;
    // The same sums off the base table: ANCHOR is live-view syntax, so the recompute writes the
    // daily segment out as a partition key.
    private static final String ANCHORED_DAILY_RECOMPUTE = """
            SELECT created_at, account_id, amount, sum(amount) OVER (
                PARTITION BY account_id, day
                ORDER BY created_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_sum
            FROM (SELECT created_at, account_id, amount, timestamp_floor('1d', created_at) AS day FROM tx)
            """;
    private static final String ANCHORED_DAILY_WINDOW = "PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00'";
    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    // First data timestamp (2026-01-01). Data sits well above the pinned test clock,
    // which starts at 0 and only creeps forward 250ms per refresh pass.
    private static final long DATA_EPOCH = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");

    // Pin the test clock below all test data before each test. A non-SEED view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAlterReferencedColumnTypeInvalidatesLiveView() throws Exception {
        // Three transitions exercise the three failure modes the stale stride would
        // produce if the change were not caught:
        //   INT->LONG    - widening, old stride < new data: in-bounds under-read (wrong results).
        //   LONG->INT    - narrowing, old stride > new data: out-of-bounds native read.
        //   INT->VARCHAR - fixed->var-size: the record reads var-size aux offsets over
        //                  fixed bytes - a wild pointer.
        // In every direction the referenced-column type change must invalidate the view
        // with the "change column type operation" reason, so no refresh ever runs over
        // the changed base.
        assertReferencedColumnTypeChangeInvalidates("INT", "LONG");
        assertReferencedColumnTypeChangeInvalidates("LONG", "INT");
        assertReferencedColumnTypeChangeInvalidates("INT", "VARCHAR");
    }

    @Test
    public void testConcurrentRefreshOverRetypedReferencedColumnRecoversThenInvalidates() throws Exception {
        // C1 regression. The raw-WAL lead drain runs at base COMMIT time (the sequencer
        // notifies live views independent of ApplyWal2TableJob), so it can reach data a
        // structural commit wrote AFTER retyping a referenced column - before apply-time
        // invalidation fires. Reading that segment through the cached compile-time stride
        // is the failure mode below. The drain must detect the drift and bail to the
        // recompile-and-recover path (no crash, no drifted rows leaking into the view),
        // and the view must flip INVALID once apply lands the structural change.
        //   LONG->INT    - narrowing: stride 8 over 4-byte data (OOB read).
        //   INT->LONG    - widening: stride 4 over 8-byte data (wrong values).
        //   INT->VARCHAR - fixed->var: var aux offsets over fixed bytes (wild pointer).
        assertConcurrentRetypeRefreshRecoversThenInvalidates("LONG", "INT", "30", "40");
        assertConcurrentRetypeRefreshRecoversThenInvalidates("INT", "LONG", "30", "40");
        assertConcurrentRetypeRefreshRecoversThenInvalidates("INT", "VARCHAR", "'30'", "'40'");
    }

    @Test
    public void testAlterUnreferencedColumnTypeIsTransparent() throws Exception {
        // Changing the TYPE of a column the LV never reads must NOT invalidate it, and
        // the view must keep refreshing correctly across the change (documents the
        // boundary of the type-change invalidation and proves it is not over-broad).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 10, 1), " +
                        "('2026-01-01T00:00:02.000000Z', 20, 2)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertFalse("LV must start valid", instance.isInvalid());

                // Change the type of y, which the view never reads.
                setCurrentMicros(2_000_000L);
                execute("ALTER TABLE base ALTER COLUMN y TYPE LONG");
                drainWalQueue();
                Assert.assertFalse(
                        "changing the type of an unreferenced column must not invalidate the LV",
                        instance.isInvalid()
                );

                // Post-change ingestion must keep refreshing correctly.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', 30, 3), " +
                        "('2026-01-01T00:00:04.000000Z', 40, 4)");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertFalse("LV must stay valid after unreferenced type change", instance.isInvalid());
            }

            assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\t10\t1\n" +
                            "2026-01-01T00:00:02.000000Z\t20\t2\n" +
                            "2026-01-01T00:00:03.000000Z\t30\t3\n" +
                            "2026-01-01T00:00:04.000000Z\t40\t4\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBaseUpdateInvalidatesLiveView() throws Exception {
        // An UPDATE on the base rewrites rows in place, and it does so only in the applied
        // partitions - the WAL segments the refresh worker drains keep the pre-update values.
        // The drain never sees the change either: an UPDATE commits as a SQL-type WAL txn and
        // both drain paths walk past non-DATA txns. So the view would go on serving rows
        // derived from values the base no longer holds, while every recovery path (restart,
        // O3 replay, refresh failure) recomputes the same range from the applied base and
        // emits the post-update values - making the view's contents depend on whether a
        // recovery happened to run. Before the fix it stayed ACTIVE with no
        // invalidation_reason and diverged permanently. It must invalidate instead, with the
        // same "update operation" reason mat views already use.
        //
        // This is the boundary of the data-removal transparency the other tests here lock in
        // (DROP / DETACH PARTITION, TTL): those only retire settled data below the view's
        // replay window and leave its computed rows consistent with the base rows they came
        // from, whereas an UPDATE mutates those very rows.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0)");
                driveRefreshToQuiescence(job);
                assertViewValid();

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");

                // An UPDATE that matches no row rewrites nothing, so it must leave the view
                // alone - the invalidation hangs off the same rowsAffected > 0 guard mat
                // views use, and a no-op UPDATE must not kill a healthy view.
                execute("UPDATE base SET x = 42.0 WHERE sym = 'nonexistent'");
                drainWalQueue();
                drainJob(job);
                Assert.assertFalse(
                        "an UPDATE affecting no rows must not invalidate the LV",
                        instance.isInvalid()
                );

                // Rewrite a base row the view has already consumed and emitted.
                execute("UPDATE base SET x = 999.0 WHERE ts = '2026-01-01T00:00:01.000000Z'");
                drainWalQueue();
                drainJob(job);

                Assert.assertTrue("a base UPDATE must invalidate the LV", instance.isInvalid());
                Assert.assertTrue(
                        "wrong invalidation reason [reason=" + instance.getInvalidationReason() + ']',
                        Chars.contains(instance.getInvalidationReason(), "update operation")
                );
            }

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testConvertBaseToNonWalInvalidatesOrRejects() throws Exception {
        // Converting the base from WAL to non-WAL removes the refresh source (the WAL + sequencer the
        // LV drains). SET TYPE only schedules the conversion via a _convert marker; the flip happens
        // when the table is next opened. This documents the observed behaviour: the ALTER is accepted
        // (there is no dependent-view guard, mirroring mat views), and once the conversion is applied
        // and the view graph rebuilt, the LV flips INVALID with the "base table is not WAL table"
        // reason - it does not silently keep serving stale rows or crash. If a future version wants to
        // block the conversion instead, this test documents the current contract to change.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                Assert.assertFalse("LV must start valid",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            }

            // Accepted with a dependent LV present: SET TYPE writes the _convert marker only.
            execute("ALTER TABLE base SET TYPE BYPASS WAL");

            // Apply the conversion and rebuild the LV registry (mirrors a restart). engine.load()
            // runs TableConverter over the marker, flipping the base to non-WAL and recreating the
            // LV state store; buildViewGraphs then reloads the LV instances and runs the base-is-WAL
            // check that marks the view invalid.
            engine.releaseInactive();
            engine.load();
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();

            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertFalse("base must be non-WAL after conversion", baseToken.isWal());

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull("LV must still be present after the base conversion", instance);
            Assert.assertTrue("LV must be invalid once its base is no longer WAL", instance.isInvalid());
            Assert.assertTrue(
                    "wrong invalidation reason [reason=" + instance.getInvalidationReason() + ']',
                    Chars.contains(instance.getInvalidationReason(), "not WAL")
            );

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDependencyColumnSetIsNonEmpty() throws Exception {
        // The invalidation gate (dependsOnMissingOrRetypedColumn) treats an EMPTY
        // dependency set as "we don't know what the view reads - defer to the broad
        // path" and returns false, so a view that recorded no dependency columns could
        // silently miss a referenced-column DROP / retype. A normally-created view always
        // records the base columns its projection / filter / window read; lock that the
        // set is non-empty and holds exactly the referenced columns, so the defensive
        // empty-set branch is never reached by a real view.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, unused INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                    "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            final ObjList<String> deps = instance.getDependencyColumnNames();
            Assert.assertTrue("a real view must record its base dependency columns", deps.size() > 0);
            Assert.assertTrue("ts must be a dependency", containsDep(deps, "ts"));
            Assert.assertTrue("sym must be a dependency", containsDep(deps, "sym"));
            Assert.assertTrue("x must be a dependency", containsDep(deps, "x"));
            Assert.assertFalse("a column the view never reads must not be a dependency",
                    containsDep(deps, "unused"));

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDetachPartitionIsTransparentToLiveView() throws Exception {
        // DETACH PARTITION removes a settled partition's rows but is a non-structural,
        // non-DATA operation the refresh worker walks past (like DROP PARTITION / TTL
        // eviction): the view stays ACTIVE, its already-emitted rows are frozen (not
        // retracted even though the base rows are gone), and forward ingestion keeps
        // accumulating as if the detached rows still existed. DETACH cannot target the
        // active (last) partition, so the base spans three days and the first is
        // detached while a later day is active.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0), " +
                        "('2026-01-02T00:00:01.000000Z', 'b', 2.0), " +
                        "('2026-01-03T00:00:01.000000Z', 'c', 3.0)");
                driveRefreshToQuiescence(job);
                assertViewValid();

                // Detach the first (non-active) day. Transparent + non-structural.
                execute("ALTER TABLE base DETACH PARTITION LIST '2026-01-01'");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                assertViewValid();

                // The detached day's derived row is frozen, not retracted: the view still
                // holds all three rows even though the base now has only two.
                assertQuery("SELECT count() FROM lv")
                        .noLeakCheck().noRandomAccess().expectSize().returns("count\n3\n");

                // Forward ingestion continues on top of the frozen prefix (rn keeps going).
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-04T00:00:01.000000Z', 'd', 4.0)");
                driveRefreshToQuiescence(job);
                assertViewValid();
            }

            assertQuery("SELECT ts, sym, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\ta\t1.0\t1\n" +
                            "2026-01-02T00:00:01.000000Z\tb\t2.0\t2\n" +
                            "2026-01-03T00:00:01.000000Z\tc\t3.0\t3\n" +
                            "2026-01-04T00:00:01.000000Z\td\t4.0\t4\n");

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidationStillFlipsWhenStatePersistFails() throws Exception {
        // M4 / terminal-circuit-breaker lock for the DDL (multi-view) invalidation path.
        // invalidateLiveViewsForBaseTable0 writes _lv.s durably BEFORE flipping the in-memory
        // invalid bit - WalPurgeJob releases a view's base-WAL purge floor the moment it observes
        // that bit (an unsynchronized read), so persisting first closes the window where a
        // concurrent purge could release the floor while _lv.s still records the view as valid.
        //
        // But when the _lv.s write itself fails - a broken disk - the view must STILL flip invalid
        // in-memory: invalidation is terminal and cannot be blocked by an unwritable state file, or
        // a broken disk would strand the view valid against a base whose referenced column is gone
        // (and would busy-loop the refresh worker). The durable record is re-derived on restart.
        // This locks the flip-even-on-persist-failure behavior for the DDL path (the refresh-worker
        // path is covered by LiveViewSmokeTest#testFlushRetryBudgetExhaustionInvalidatesView).
        final AtomicBoolean failLvStateWrite = new AtomicBoolean(false);
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (failLvStateWrite.get() && Utf8s.endsWithAscii(name, LiveViewState.LIVE_VIEW_STATE_FILE_NAME)) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, price, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");

            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid", instance.isInvalid());

            // Fault every _lv.s write, then drop a referenced column to trigger invalidation.
            failLvStateWrite.set(true);
            execute("ALTER TABLE base DROP COLUMN price");
            drainWalQueue();

            // The durable write failed, but the terminal invalidation still flips the in-memory bit.
            Assert.assertTrue(
                    "invalidation must still flip the in-memory bit when the _lv.s persist fails",
                    instance.isInvalid()
            );

            failLvStateWrite.set(false);
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInvalidationReasonNamesOffendingColumn() throws Exception {
        // For a referenced-column DROP / RENAME / retype, invalidation_reason must
        // name the exact dependency that broke, not just the operation, so an
        // operator can tell from live_views() which column to look at on a wide
        // base with several dependent views. The reason keeps the operation prefix
        // (so existing "contains(operation)" checks still hold) and appends
        // [column=<name>].
        assertReferencedColumnOpNamesColumn("ALTER TABLE base DROP COLUMN price", "drop column operation", "price");
        assertReferencedColumnOpNamesColumn("ALTER TABLE base RENAME COLUMN price TO cost", "rename column operation", "price");
        assertReferencedColumnOpNamesColumn("ALTER TABLE base ALTER COLUMN price TYPE LONG", "change column type operation", "price");
    }

    @Test
    public void testNonDataCommitAmongLateRowsAddColumnKeepsEveryRow() throws Exception {
        assertAnchoredViewKeepsEveryRowAcrossNonDataCommit("", "ALTER TABLE tx ADD COLUMN note INT", false);
    }

    @Test
    public void testNonDataCommitAmongLateRowsDedupDisableKeepsEveryRow() throws Exception {
        // A dedup base refreshes from the applied base until the DISABLE lands; once it has,
        // the pass that reads the burst takes the raw-WAL drain like any other base.
        assertAnchoredViewKeepsEveryRowAcrossNonDataCommit(
                " DEDUP UPSERT KEYS(created_at, account_id)",
                "ALTER TABLE tx DEDUP DISABLE",
                false
        );
    }

    @Test
    public void testNonDataCommitAmongLateRowsDedupEnableKeepsEveryRow() throws Exception {
        // Once DEDUP ENABLE applies, the base refreshes from the applied base. The burst
        // reaches the raw-WAL drain only when the pass routes the view before that apply and
        // the apply lands before the repair pins its reader.
        assertAnchoredViewKeepsEveryRowAcrossNonDataCommit(
                "",
                "ALTER TABLE tx DEDUP ENABLE UPSERT KEYS(created_at, account_id)",
                true
        );
    }

    @Test
    public void testNonDataCommitAmongLateRowsNoOpUpdateKeepsEveryRow() throws Exception {
        // A non-data commit that changes neither the metadata nor a row: the replay raises no
        // metadata drift, so the repair plan alone decides what the view keeps.
        assertAnchoredViewKeepsEveryRowAcrossNonDataCommit(
                "",
                "UPDATE tx SET amount = 42.0 WHERE account_id = 'acct-none'",
                false
        );
    }

    @Test
    public void testNonDataCommitAmongLateRowsRangeFrameKeepsEveryRow() throws Exception {
        // An un-anchored RANGE frame bounds the repair at changeMaxTs + W rather than at a
        // segment end, which a too-low ceiling drops below the open-day row just the same.
        final String window = "PARTITION BY account_id ORDER BY created_at RANGE BETWEEN 2 HOUR PRECEDING AND CURRENT ROW";
        assertNonDataCommitAmongLateRowsKeepsEveryRow(
                "",
                window,
                "SELECT created_at, account_id, amount, sum(amount) OVER (" + window + ") AS cumulative_sum FROM tx",
                "ALTER TABLE tx ADD COLUMN note INT",
                false,
                """
                        created_at\taccount_id\tamount\tcumulative_sum
                        2026-01-02T01:00:00.000000Z\tacct-1\t1.0\t1.0
                        2026-01-02T08:35:58.000000Z\tacct-7\t3.0\t3.0
                        2026-01-02T12:00:00.000000Z\tacct-7\t1.0\t1.0
                        2026-01-03T00:10:00.000000Z\tacct-1\t1.0\t1.0
                        2026-01-03T00:54:22.000000Z\tacct-5\t1.0\t1.0
                        2026-01-03T01:00:00.000000Z\tacct-2\t1.0\t1.0
                        2026-01-03T02:07:07.000000Z\tacct-1\t1.0\t2.0
                        2026-01-03T02:30:00.000000Z\tacct-5\t1.0\t2.0
                        """
        );
    }

    @Test
    public void testNonStructuralAlterIsTransparentToLiveView() throws Exception {
        // Non-structural base ALTERs - SET PARAM, ADD / DROP INDEX, symbol CACHE /
        // NOCACHE, SYMBOL CAPACITY - travel the executeAlter apply path, which never
        // invalidates a live view (only structural referenced-column DROP / RENAME /
        // retype and base DROP / RENAME do). Each op here targets sym, a column the view
        // REFERENCES (PARTITION BY sym): changing a referenced column's index / cache /
        // capacity attributes, none of which touch its name or type, must leave the view
        // ACTIVE and still equal to a from-scratch recompute after post-change data.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES " +
                        "(" + DATA_EPOCH + "::timestamp, 'a', 1.0), " +
                        "(" + (DATA_EPOCH + 1_000_000L) + "::timestamp, 'b', 2.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertViewValid();

                // ADD then DROP INDEX must be ordered (DROP needs an existing index).
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base ALTER COLUMN sym ADD INDEX", 1);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base ALTER COLUMN sym DROP INDEX", 2);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base ALTER COLUMN sym NOCACHE", 3);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base ALTER COLUMN sym CACHE", 4);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base ALTER COLUMN sym SYMBOL CAPACITY 256", 5);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base SET PARAM maxUncommittedRows = 100", 6);
                applyTransparentAlterThenData(job, viewSql, "ALTER TABLE base SET PARAM o3MaxLag = 5s", 7);
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRebaseWalBaseTableInvalidatesDependentLiveView() throws Exception {
        // REBASE WAL mints a new base directory and restarts the sequencer near zero, while the
        // view keeps the watermark it reached against the OLD sequencer. refreshViewsForBaseTable
        // gates on `seqTxn > instance.getLastProcessedSeqTxn()`, so that gate drops every post-rebase
        // commit and nothing ever marks the view INVALID - permanent staleness behind a
        // healthy-looking live_views(). Mat views already force a full refresh of their dependents
        // at the same point (matViewStateStore.enqueueInvalidateDependentViews, "base table
        // rebase"); the identical reasoning applies to live views.
        //
        // REBASE WAL requires suspension to block writes.
        setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, "true");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x, g) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0, 'g1'), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0, 'g1')");
                driveRefreshToQuiescence(job);
                final LiveViewInstance preRebase = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("the LV must be registered before the rebase", preRebase);
                Assert.assertFalse("the LV must be valid before the rebase", preRebase.isInvalid());

                final TableToken oldBase = engine.verifyTableName("base");

                execute("ALTER TABLE base SUSPEND WAL");
                execute("ALTER TABLE base REBASE WAL");
                drainWalQueue();
                drainJob(job);

                // Sanity: the rebase really did mint a new directory and table id, so the view's
                // watermark genuinely no longer maps onto the base it is bound to.
                final TableToken newBase = engine.verifyTableName("base");
                Assert.assertNotEquals(oldBase.getDirName(), newBase.getDirName());
                Assert.assertNotEquals(oldBase.getTableId(), newBase.getTableId());

                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull("the LV must still be registered after the base rebase", instance);
                Assert.assertTrue("REBASE WAL on the base must invalidate the dependent LV", instance.isInvalid());
                Assert.assertTrue(
                        "wrong invalidation reason [reason=" + instance.getInvalidationReason() + ']',
                        Chars.contains(instance.getInvalidationReason(), "base table rebase")
                );
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRecreateBaseSameNameDoesNotRebindInvalidLiveView() throws Exception {
        // Dropping the base terminally invalidates the LV (invalidateLiveViewsForBaseTable).
        // Re-creating a fresh table with the SAME name must NOT resurrect the view: the LV
        // binds to the dropped base's unique TableToken (a per-table directory name), and
        // the invalid flag is terminal, so ingestion into the new same-named table never
        // reaches the view and its materialized data stays frozen at the pre-drop state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:01.000000Z', 'a', 1.0)");
                driveRefreshToQuiescence(job);
                assertViewValid();

                execute("DROP TABLE base");
                drainWalQueue();
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertTrue("dropping the base must invalidate the LV", instance.isInvalid());

                // Re-create a fresh table with the same name and ingest into it.
                execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-05T00:00:01.000000Z', 'z', 9.0)");
                driveRefreshToQuiescence(job);

                // The invalid view must not rebind to the new same-named base.
                Assert.assertTrue("re-creating the base must not revive the invalid LV",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            }

            // The view's data is unchanged - it never saw the new base's row.
            assertQuery("SELECT ts, sym, x, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\tx\trn\n" +
                            "2026-01-01T00:00:01.000000Z\ta\t1.0\t1\n");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testUnreferencedBaseColumnChangeThenDataMatchesRecompute() throws Exception {
        // The refresh path re-resolves each referenced writer column by NAME every cycle
        // (buildColumnMappings), so an unreferenced ADD / DROP / RENAME - each of which shifts the
        // physical column positions of the base - must leave the referenced columns (ts, sym, x)
        // mapping correctly. The existing unreferenced-change tests stop at isInvalid()/seqTxn; this
        // one ingests post-change DATA after every op and asserts the view still equals a from-scratch
        // recompute over the (post-change) base, so a mis-resolved stride would surface as a value
        // mismatch, not just a missed invalidation.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x, y) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 'a', 1.0, 1), " +
                        "('2026-01-01T00:00:02.000000Z', 'b', 2.0, 2)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertViewValid();

                // ADD an unreferenced column: the physical layout grows a trailing column.
                execute("ALTER TABLE base ADD COLUMN z INT");
                drainWalQueue();
                assertViewValid();
                execute("INSERT INTO base (ts, sym, x, y, z) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', 'a', 3.0, 3, 30), " +
                        "('2026-01-01T00:00:04.000000Z', 'b', 4.0, 4, 40)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertViewValid();

                // DROP an unreferenced column: physical positions of later columns shift left.
                execute("ALTER TABLE base DROP COLUMN y");
                drainWalQueue();
                assertViewValid();
                execute("INSERT INTO base (ts, sym, x, z) VALUES " +
                        "('2026-01-01T00:00:05.000000Z', 'a', 5.0, 50), " +
                        "('2026-01-01T00:00:06.000000Z', 'b', 6.0, 60)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertViewValid();

                // RENAME an unreferenced column: the name changes but the referenced columns must
                // still resolve by their own names.
                execute("ALTER TABLE base RENAME COLUMN z TO w");
                drainWalQueue();
                assertViewValid();
                execute("INSERT INTO base (ts, sym, x, w) VALUES " +
                        "('2026-01-01T00:00:07.000000Z', 'a', 7.0, 70), " +
                        "('2026-01-01T00:00:08.000000Z', 'b', 8.0, 80)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
                assertViewValid();
            }

            execute("DROP LIVE VIEW lv");
        });
    }

    // Applies one transparent (non-structural) base ALTER, asserts the view stays valid,
    // then ingests two fresh strictly-increasing rows and asserts the view still equals a
    // from-scratch recompute. step spaces the two rows two seconds apart from every other
    // step so the whole run keeps unique, increasing timestamps.
    private void applyTransparentAlterThenData(LiveViewRefreshJob job, String viewSql, String alterSql, int step) throws Exception {
        execute(alterSql);
        drainWalQueue();
        assertViewValid(); // a non-structural change never invalidates the view

        final long t1 = DATA_EPOCH + (2L * step) * 1_000_000L;
        final long t2 = t1 + 1_000_000L;
        execute("INSERT INTO base (ts, sym, x) VALUES " +
                "(" + t1 + "::timestamp, 'a', " + (step + 1) + ".0), " +
                "(" + t2 + "::timestamp, 'b', " + (step + 2) + ".0)");
        driveRefreshToQuiescence(job);
        assertViewMatchesRecompute(viewSql);
        assertViewValid();
    }

    private static boolean containsDep(ObjList<String> deps, String name) {
        for (int i = 0, n = deps.size(); i < n; i++) {
            if (Chars.equals(deps.getQuick(i), name)) {
                return true;
            }
        }
        return false;
    }

    // The live view must equal the same window recomputed directly over the base table. (lv) and
    // (viewSql) share a schema (the view stores exactly its projection); ORDER BY 2, 1 (sym, ts) gives
    // both a total order and genericStringMatch tolerates the SYMBOL-vs-STRING passthrough difference.
    private void assertViewMatchesRecompute(String viewSql) throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 2, 1",
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
        // A refresh fault self-heals into a full recompute from the applied base, which this
        // oracle would match either way; assert no cycle faulted so an incremental-path
        // regression cannot hide behind the recovery.
        assertNoRefreshFaults("lv");
    }

    private void assertViewValid() {
        Assert.assertFalse(
                "LV must stay valid across the unreferenced change",
                engine.getLiveViewRegistry().getViewInstance("lv").isInvalid()
        );
    }

    private void assertAnchoredViewKeepsEveryRowAcrossNonDataCommit(
            String dedupClause,
            String nonDataSql,
            boolean isAppliedDuringDrain
    ) throws Exception {
        assertNonDataCommitAmongLateRowsKeepsEveryRow(
                dedupClause,
                ANCHORED_DAILY_WINDOW,
                ANCHORED_DAILY_RECOMPUTE,
                nonDataSql,
                isAppliedDuringDrain,
                ANCHORED_DAILY_EXPECTED
        );
    }

    // Four base statements reach one refresh pass together: an in-order row, a non-data
    // commit, a late row in the open day, and a late row in an earlier day. The drain walks
    // the first two, stops on the third, and hands the repair no change ceiling, because the
    // non-data commit can have changed rows anywhere. The base has applied the fourth by then,
    // so the repair classifies it as the apply-ahead range. Folding that range's maximum into
    // the missing ceiling turned "unknown" into the earlier day's timestamp: the repair
    // replaced a range ending below the open-day row and walked the watermark past it, so the
    // view lost the row for good and every later sum over its account came out short.
    //
    // isAppliedDuringDrain holds the base apply back until the pass that reads the burst has
    // routed the view and started its raw-WAL drain, then lands all four statements at the
    // drain's first base WAL event read. That is the order a refresh worker and an apply
    // worker produce when the apply wins the race in the middle of the drain, and it is the
    // only order in which a commit that changes the routing, such as DEDUP ENABLE, still
    // reaches the raw-WAL drain.
    private void assertNonDataCommitAmongLateRowsKeepsEveryRow(
            String dedupClause,
            String window,
            String recomputeSql,
            String nonDataSql,
            boolean isAppliedDuringDrain,
            String expected
    ) throws Exception {
        final String viewSql = "SELECT created_at, account_id, amount, sum(amount) OVER w AS cumulative_sum FROM tx WINDOW w AS (" + window + ")";
        final AtomicBoolean isApplyArmed = new AtomicBoolean();
        final AtomicInteger midDrainApplies = new AtomicInteger();
        final AtomicReference<Throwable> midDrainApplyFailure = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.endsWithAscii(name, WalUtils.EVENT_FILE_NAME)
                        && Utf8s.containsAscii(name, "tx~")
                        && isApplyArmed.compareAndSet(true, false)) {
                    // On a thread of its own, as an apply worker runs it. The drain is reading
                    // the base's sequencer through a cursor the sequencer keeps per thread, and
                    // an apply on this thread would take that same cursor and close it.
                    final Thread applier = new Thread(() -> {
                        try {
                            drainWalQueue();
                        } catch (Throwable th) {
                            midDrainApplyFailure.set(th);
                        } finally {
                            Path.clearThreadLocals();
                        }
                    });
                    applier.start();
                    try {
                        applier.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    midDrainApplies.incrementAndGet();
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                    + "TIMESTAMP(created_at) PARTITION BY DAY WAL" + dedupClause);
            execute("""
                    INSERT INTO tx (created_at, account_id, amount) VALUES
                        ('2026-01-02T01:00:00.000000Z', 'acct-1', 1.0),
                        ('2026-01-02T12:00:00.000000Z', 'acct-7', 1.0),
                        ('2026-01-03T00:10:00.000000Z', 'acct-1', 1.0),
                        ('2026-01-03T01:00:00.000000Z', 'acct-2', 1.0)
                    """);
            drainWalQueue();
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                // No refresh pass runs between the four statements, and the base applies all
                // of them before the repair pins its reader.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T02:07:07.000000Z', 'acct-1', 1.0)");
                execute(nonDataSql);
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T00:54:22.000000Z', 'acct-5', 1.0)");
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-02T08:35:58.000000Z', 'acct-7', 3.0)");
                if (isAppliedDuringDrain) {
                    // The first statement queued a refresh task and closed the notification gate
                    // on the other three, so a pass over that task would stop at the first. Hand
                    // the task back the way a finished pass does, which re-queues it at the
                    // newest commit, and the next pass then reads all four.
                    final LiveViewStateStore stateStore = engine.getLiveViewStateStore();
                    final LiveViewRefreshTask pendingTask = new LiveViewRefreshTask();
                    Assert.assertTrue(
                            "the burst must have queued a refresh task",
                            stateStore.tryDequeueRefreshTask(pendingTask)
                    );
                    stateStore.notifyBaseRefreshed(pendingTask, pendingTask.seqTxn);
                    isApplyArmed.set(true);
                    drainJob(job);
                    if (midDrainApplyFailure.get() != null) {
                        throw new AssertionError("the mid-drain apply failed", midDrainApplyFailure.get());
                    }
                    Assert.assertEquals(
                            "the base must have applied the burst in the middle of the drain",
                            1,
                            midDrainApplies.get()
                    );
                }
                driveRefreshToQuiescence(job);

                // A later in-order row of the open-day row's account, whose sum counts that row.
                execute("INSERT INTO tx (created_at, account_id, amount) VALUES ('2026-01-03T02:30:00.000000Z', 'acct-5', 1.0)");
                driveRefreshToQuiescence(job);

                assertQuery("SELECT * FROM lv")
                        .noLeakCheck()
                        .timestamp("created_at")
                        .expectSize()
                        .returns(expected);
                TestUtils.assertSqlCursors(
                        engine,
                        sqlExecutionContext,
                        "(" + recomputeSql + ") ORDER BY 2, 1",
                        "(lv) ORDER BY 2, 1",
                        LOG,
                        true
                );
                // A structural commit leaves the view's compiled query behind the base's
                // metadata, so the repair's replay raises a metadata drift; the recovery
                // restores the runtime from the timeline and runs the same plan again. That is
                // the one fault these cases may record. A fault recovered any other way
                // recomputes the whole view from the base, which matches the rows above
                // whatever the plan did.
                final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertEquals(
                        "every refresh fault must end in a timeline restore that re-runs the repair plan",
                        instance.getRefreshFaultCount(),
                        instance.getCheckpointRuntimeRestores()
                );
                assertViewValid();
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    private void assertReferencedColumnOpNamesColumn(String alterSql, String opReason, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, price, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid [" + alterSql + ']', instance.isInvalid());

            execute(alterSql);
            drainWalQueue();

            Assert.assertTrue(
                    "referenced-column op must invalidate the LV [" + alterSql + ']',
                    instance.isInvalid()
            );
            final CharSequence reason = instance.getInvalidationReason();
            Assert.assertTrue(
                    "reason must keep the operation prefix [" + alterSql + ", reason=" + reason + ']',
                    Chars.contains(reason, opReason)
            );
            Assert.assertTrue(
                    "reason must name the offending column [" + alterSql + ", reason=" + reason + ']',
                    Chars.contains(reason, "[column=" + columnName + ']')
            );

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    private void assertReferencedColumnTypeChangeInvalidates(String initialType, String newType) throws Exception {
        final String transition = initialType + "->" + newType;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertFalse("LV must start valid [" + transition + ']', instance.isInvalid());

            execute("ALTER TABLE base ALTER COLUMN x TYPE " + newType);
            drainWalQueue();

            Assert.assertTrue(
                    "changing the type of a referenced column must invalidate the LV [" + transition + ']',
                    instance.isInvalid()
            );
            Assert.assertTrue(
                    "wrong invalidation reason [" + transition + ", reason=" + instance.getInvalidationReason() + ']',
                    Chars.contains(instance.getInvalidationReason(), "change column type operation")
            );

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    private void assertConcurrentRetypeRefreshRecoversThenInvalidates(
            String initialType,
            String newType,
            String postValue1,
            String postValue2
    ) throws Exception {
        final String transition = initialType + "->" + newType;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE x > 0");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Activate the view over pre-retype data and flush it to disk.
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:01.000000Z', 10, 1), " +
                        "('2026-01-01T00:00:02.000000Z', 20, 2)");
                driveRefreshToQuiescence(job);

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertFalse("LV must start valid [" + transition + ']', instance.isInvalid());

                // Commit the retype + post-retype data to the sequencer WITHOUT applying, so
                // the base seqTxn is committed but unapplied - no apply-time invalidation yet.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                execute("ALTER TABLE base ALTER COLUMN x TYPE " + newType);
                execute("INSERT INTO base (ts, x, y) VALUES " +
                        "('2026-01-01T00:00:03.000000Z', " + postValue1 + ", 3), " +
                        "('2026-01-01T00:00:04.000000Z', " + postValue2 + ", 4)");

                // Race the refresh worker against the un-applied structural change: the drain
                // reaches the post-retype segment at COMMIT time. Without the guard this is the
                // OOB read; the guard bails to recompile instead of draining the drifted segment.
                drainJob(job);

                // Deferred, not crashed: the view still serves exactly the pre-retype rows -
                // the post-retype commit did not leak into it - and is not yet invalid.
                assertQuery("SELECT ts, x, rn FROM lv ORDER BY ts")
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns("ts\tx\trn\n" +
                                "2026-01-01T00:00:01.000000Z\t10\t1\n" +
                                "2026-01-01T00:00:02.000000Z\t20\t2\n");
                Assert.assertFalse("LV must not be invalid before apply [" + transition + ']', instance.isInvalid());
                // The drift must route through the recompile-and-recover path, not surface
                // as a refresh fault. Without the guard the drain maps the drifted segment
                // and dereferences a missing column / strides a stale width, which the
                // refresh loop records as a failure (or, worse, corrupts the lead above).
                Assert.assertEquals(
                        "drift must be handled cleanly, not recorded as a refresh failure [" + transition + ']',
                        0,
                        instance.getFlushRetryCount()
                );

                // Apply the structural change: it invalidates the view with the type-change reason.
                drainWalQueue();
                drainJob(job);

                Assert.assertTrue(
                        "retype must invalidate the LV once applied [" + transition + ']',
                        instance.isInvalid()
                );
                Assert.assertTrue(
                        "wrong invalidation reason [" + transition + ", reason=" + instance.getInvalidationReason() + ']',
                        Chars.contains(instance.getInvalidationReason(), "change column type operation")
                );
            }

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing the clock each pass so
    // deferred flushes land, and applying the LV's own WAL after each burst. Mirrors the fuzz harness.
}
