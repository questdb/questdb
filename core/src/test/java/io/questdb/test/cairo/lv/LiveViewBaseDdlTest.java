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

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

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

    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    // First data timestamp (2026-01-01). Data sits well above the pinned test clock,
    // which starts at 0 and only creeps forward 250ms per refresh pass.
    private static final long DATA_EPOCH = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");

    // Pin the test clock below all test data before each test. A non-BACKFILL view's
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
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

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
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, row_number() OVER () AS rn FROM base");

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
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, row_number() OVER () AS rn FROM base");

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
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, price, row_number() OVER () AS rn FROM base WHERE price > 0");

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
    public void testRecreateBaseSameNameDoesNotRebindInvalidLiveView() throws Exception {
        // Dropping the base terminally invalidates the LV (invalidateLiveViewsForBaseTable).
        // Re-creating a fresh table with the SAME name must NOT resurrect the view: the LV
        // binds to the dropped base's unique TableToken (a per-table directory name), and
        // the invalid flag is terminal, so ingestion into the new same-named table never
        // reaches the view and its materialized data stays frozen at the pre-drop state.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, sym, x, row_number() OVER () AS rn FROM base");

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
    }

    private void assertViewValid() {
        Assert.assertFalse(
                "LV must stay valid across the unreferenced change",
                engine.getLiveViewRegistry().getViewInstance("lv").isInvalid()
        );
    }

    private void assertReferencedColumnOpNamesColumn(String alterSql, String opReason, String columnName) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, price, row_number() OVER () AS rn FROM base WHERE price > 0");

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
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

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
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                    "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");

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
