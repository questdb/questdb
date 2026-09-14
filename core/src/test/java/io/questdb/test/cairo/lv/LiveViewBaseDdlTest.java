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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
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

    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    // First data timestamp (2026-01-01). Data sits well above the pinned test clock,
    // which starts at 0 and only creeps forward 250ms per refresh pass.
    private static final long DATA_EPOCH = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
    private static final String UPDATE_FIXTURE_VIEW_ROWS = """
            ts\tsym\tx\trn
            2026-01-01T00:00:01.000000Z\ta\t1.0\t1
            2026-01-01T00:00:02.000000Z\tb\t2.0\t2
            """;
    // A test-controlled read-only flip. The OSS engine reads a static isReadOnlyInstance() flag; the
    // injected engine below ORs this in, so a load can run as a read-only node's. Reset before every test.
    private static final AtomicBoolean isReadOnly = new AtomicBoolean();
    // A test-controlled seam on the live view invalidation an UPDATE makes. The injected engine
    // hands the real invalidation to the hook instead of running it, so a test can read what the
    // base holds at that moment and simulate the process dying there. Reset before every test.
    private static final AtomicReference<UpdateInvalidationHook> updateInvalidationHook = new AtomicReference<>();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public void invalidateLiveViewsForBaseTable(TableToken baseTableToken, String reason) {
                final UpdateInvalidationHook hook = updateInvalidationHook.get();
                if (hook != null && UpdateOperation.MAT_VIEW_INVALIDATION_REASON.equals(reason)) {
                    hook.run(() -> super.invalidateLiveViewsForBaseTable(baseTableToken, reason));
                    return;
                }
                super.invalidateLiveViewsForBaseTable(baseTableToken, reason);
            }

            @Override
            public boolean isReadOnlyMode() {
                return isReadOnly.get() || super.isReadOnlyMode();
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    // Pin the test clock below all test data before each test. A non-SEED view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
        isReadOnly.set(false);
        updateInvalidationHook.set(null);
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
                final AtomicInteger invalidationCalls = new AtomicInteger();
                updateInvalidationHook.set(realInvalidation -> {
                    invalidationCalls.incrementAndGet();
                    realInvalidation.run();
                });

                // An UPDATE that matches no row rewrites nothing, so it must leave the view
                // alone - the invalidation runs only ahead of a commit that rewrites a row, the
                // same rowsAffected > 0 rule mat views use, and a no-op UPDATE must not kill a
                // healthy view. Not reached at all, rather than reached and declined: the seam
                // counts the calls.
                execute("UPDATE base SET x = 42.0 WHERE sym = 'nonexistent'");
                drainWalQueue();
                drainJob(job);
                Assert.assertFalse(
                        "an UPDATE affecting no rows must not invalidate the LV",
                        instance.isInvalid()
                );
                Assert.assertEquals("an UPDATE affecting no rows must not reach the invalidation", 0, invalidationCalls.get());

                // Rewrite a base row the view has already consumed and emitted.
                execute("UPDATE base SET x = 999.0 WHERE ts = '2026-01-01T00:00:01.000000Z'");
                drainWalQueue();
                drainJob(job);

                Assert.assertTrue("a base UPDATE must invalidate the LV", instance.isInvalid());
                Assert.assertTrue(
                        "wrong invalidation reason [reason=" + instance.getInvalidationReason() + ']',
                        Chars.contains(instance.getInvalidationReason(), "update operation")
                );
                Assert.assertEquals("a row-rewriting UPDATE must reach the invalidation once", 1, invalidationCalls.get());
            }

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testUpdateInvalidatesTheViewBeforeItsCommit() throws Exception {
        // ApplyWal2TableJob applied an UPDATE through tableWriter.apply, which commits it, and only
        // then invalidated the dependent live views. A process dying between the two left the base
        // rewritten under a view whose _lv.s still recorded it valid, and a committed UPDATE leaves
        // nothing for the next load to find: the seqTxn is applied, the WAL segment is purgeable and
        // the base metadata is unchanged. The restart loaded the view valid, its rows derived from
        // values the base no longer held, and it stayed that way for good - until a recovery
        // recomputed the same range from the applied base and the rows changed under the reader.
        //
        // The seam stands in for the death. It is reached at the invalidation, reads what the base
        // holds at that moment and throws instead of invalidating. Measured on this fixture before
        // the fix: the base already held 999.0 there, and after RESUME WAL the view stayed active
        // holding 1.0 over a base holding 999.0, with nothing to apply and nothing reporting it.
        // The invalidation now runs before the commit, so the death rolls the UPDATE back, and
        // RESUME WAL re-applies it over a view it then invalidates.
        assertMemoryLeak(() -> {
            createUpdateFixture();
            final AtomicReference<String> baseAtInvalidation = new AtomicReference<>();
            updateInvalidationHook.set(realInvalidation -> {
                baseAtInvalidation.set(readBaseX("2026-01-01T00:00:01.000000Z"));
                throw CairoException.critical(0).put("simulated process death at the UPDATE's live view invalidation");
            });
            applyUpdateThatDiesAtItsInvalidation();
            Assert.assertEquals("the UPDATE must not commit ahead of the live view invalidation", "1.0", baseAtInvalidation.get());
            assertBaseHoldsX("1.0");

            // A restart between the death and RESUME WAL: nothing recorded an invalidation, so the
            // view loads valid - over a base the UPDATE has NOT reached, which is what makes that
            // right. Before the fix the base already held 999.0 here.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            assertViewValid();
            assertBaseHoldsX("1.0");

            // The apply resumes, the UPDATE lands, and the view invalidates for it. Before the fix
            // the UPDATE's seqTxn was already applied, so RESUME WAL had nothing to re-apply and
            // the view stayed active.
            updateInvalidationHook.set(null);
            execute("ALTER TABLE base RESUME WAL");
            drainWalQueue();
            assertBaseHoldsX("999.0");
            assertInvalidatedByUpdate();

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testUpdateWhoseCommitFailsAfterTheInvalidationLeavesTheViewInvalid() throws Exception {
        // The other half of the same window: the process dies after the invalidation and before the
        // commit. The invalidation is durable by then, so the view loads invalid from its _lv.s, the
        // UPDATE is rolled back, and RESUME WAL re-applies it over a view that is already invalid -
        // the invalidation runs again and changes nothing. A view invalidated for an UPDATE that
        // then lands anyway is the outcome an uninterrupted UPDATE gives it too. Before the fix the
        // base already held 999.0 at the seam.
        assertMemoryLeak(() -> {
            createUpdateFixture();
            updateInvalidationHook.set(realInvalidation -> {
                realInvalidation.run();
                throw CairoException.critical(0).put("simulated process death after the UPDATE's live view invalidation");
            });
            applyUpdateThatDiesAtItsInvalidation();
            assertBaseHoldsX("1.0");
            assertInvalidatedByUpdate();

            // Durable ahead of the commit: a restart reads the invalidation back from _lv.s.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            assertBaseHoldsX("1.0");
            assertInvalidatedByUpdate();

            updateInvalidationHook.set(null);
            execute("ALTER TABLE base RESUME WAL");
            drainWalQueue();
            assertBaseHoldsX("999.0");
            assertInvalidatedByUpdate();

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

            // The _lv.s on disk still records the view valid, so the restart is what re-derives the
            // invalidation - through the load-time dependency check, which names the column under
            // its own reason since it cannot know which operation dropped it. Before that check the
            // restart loaded the view valid over a base with no price column.
            failLvStateWrite.set(false);
            restartAndAssertInvalidatedOnLoad("price");
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
    public void testLoadInvalidatesAViewOverARetypeItsInvalidationMissed() throws Exception {
        // ApplyWal2TableJob commits a structural change to the base writer and only then invalidates
        // the dependent views, so a process that dies between the two leaves the base retyped and the
        // view's _lv.s recording it valid. applyAlterMissedByInvalidation reproduces exactly that
        // on-disk state. A restart then loaded the view valid, and the refresh worker compiled its SQL
        // against the base's current metadata, where nothing faults on the change.
        //
        // Measured on this fixture before the load-time check: the view stayed active with no refresh
        // fault, and after the restart it served m = 5000000000 for the row it computed over x LONG
        // and m = 705032704 for a later row with the same x = 5000 - x * 1_000_000 had become INT
        // arithmetic, and it wrapped. A later out-of-order row rebuilt the whole view through the new
        // schema, so every x = 5000 row then read 705032704. The apply side, had it run, would have
        // invalidated the view for a referenced-column retype.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                    SELECT ts, sym, x, x * 1_000_000 AS m,
                    count(*) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn
                    FROM base""");
            final String viewRows = """
                    ts\tsym\tx\tm\trn
                    2026-01-01T00:00:01.000000Z\ta\t5000\t5000000000\t1
                    2026-01-01T00:00:02.000000Z\ta\t3\t3000000\t2
                    """;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base VALUES
                        ('2026-01-01T00:00:01.000000Z', 'a', 5_000),
                        ('2026-01-01T00:00:02.000000Z', 'a', 3)""");
                driveRefreshToQuiescence(job);
                assertViewValid();
                assertQuery("SELECT ts, sym, x, m, rn FROM lv").noLeakCheck().timestamp("ts").expectSize().returns(viewRows);
            }

            applyAlterMissedByInvalidation("ALTER TABLE base ALTER COLUMN x TYPE INT");
            final LiveViewInstance instance = restartAndAssertInvalidatedOnLoad("x");

            // An invalid view never refreshes, so neither a forward row nor an out-of-order one reaches
            // it: the rows stay the ones the view's own schema produced.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:03.000000Z', 'a', 5_000)");
                driveRefreshToQuiescence(job);
                execute("INSERT INTO base VALUES ('2026-01-01T00:00:01.500000Z', 'a', 7)");
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("the load must invalidate before any refresh faults", 0, instance.getRefreshFaultCount());
            assertQuery("SELECT ts, sym, x, m, rn FROM lv").noLeakCheck().timestamp("ts").expectSize().returns(viewRows);

            // Durable, like the apply-side invalidation: once x is LONG again the load check passes, so
            // only the _lv.s the first load wrote keeps the view invalid - over a base whose rows the
            // round trip could have changed.
            execute("ALTER TABLE base ALTER COLUMN x TYPE LONG");
            drainWalQueue();
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                capture.drain();
                capture.assertNotLogged("base table no longer resolves a column the live view references");
            } finally {
                capture.stop();
            }
            assertInvalidatedOnLoad("x");

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testLoadKeepsAViewValidOverAnUnreferencedChangeItsInvalidationMissed() throws Exception {
        // The load-time check asks the apply side's question, not a broader one: a missed retype of a
        // column the view never reads must leave it valid, restore it from its timeline and let it keep
        // refreshing to the same rows a recompute gives.
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts "
                + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, sym, x, y) VALUES
                        ('2026-01-01T00:00:01.000000Z', 'a', 1.0, 1),
                        ('2026-01-01T00:00:02.000000Z', 'b', 2.0, 2)""");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
            }

            applyAlterMissedByInvalidation("ALTER TABLE base ALTER COLUMN y TYPE LONG");
            final LogCapture capture = new LogCapture();
            capture.start();
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                assertViewValid();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    execute("INSERT INTO base (ts, sym, x, y) VALUES ('2026-01-01T00:00:03.000000Z', 'a', 3.0, 3)");
                    driveRefreshToQuiescence(job);
                    capture.waitFor("restored live view from checkpoint timeline [view=lv");
                }
                capture.assertNotLogged("base table no longer resolves a column the live view references");
            } finally {
                capture.stop();
            }
            assertViewValid();
            assertViewMatchesRecompute(viewSql);

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testLoadNamesTheReferencedColumnADropOrRenameMissed() throws Exception {
        // A dropped or renamed referenced column does not produce wrong rows after such a restart: the
        // view's SQL no longer compiles. Before the load-time check the restart loaded the view valid,
        // every refresh cycle faulted on the compile, and the fifth invalidated the view under
        // "flush retry budget exhausted" - five critical faults and a reason that names nothing an
        // operator can act on. The load now invalidates it before its first cycle, naming the column.
        assertLoadNamesMissedColumn("ALTER TABLE base DROP COLUMN price", "price");
        assertLoadNamesMissedColumn("ALTER TABLE base RENAME COLUMN price TO cost", "price");
    }

    @Test
    public void testLoadOnAReadOnlyNodeDoesNotDecideOnAMissedRetype() throws Exception {
        // A read-only node skips the load-time check. A replica can register a view before its base has
        // applied the changes the primary's CREATE compiled against, so there a base BEHIND the view's
        // definition reads exactly like a base that moved past it - and an invalidation on a replica is
        // terminal, with no DROP and re-CREATE to undo it. The flip is synthetic: the injected engine
        // reports read-only for the load alone, over the on-disk state a missed retype leaves.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS "
                    + "SELECT ts, price, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");
            applyAlterMissedByInvalidation("ALTER TABLE base ALTER COLUMN price TYPE LONG");

            final LogCapture capture = new LogCapture();
            capture.start();
            isReadOnly.set(true);
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                capture.drain();
                capture.assertNotLogged("base table no longer resolves a column the live view references");
            } finally {
                isReadOnly.set(false);
                capture.stop();
            }
            assertViewValid();

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    @Test
    public void testLoadThatCannotReadTheBaseMetadataKeepsTheViewValid() throws Exception {
        // A base metadata read that fails is a doubt, not a decision: the base can be unreadable for
        // reasons that have nothing to do with the view, and an invalidation is terminal. The load logs
        // it and registers the view as it would have, and the refresh worker, which opens the base for
        // real, is what faults if the base truly is unreadable. The failure must not escape either -
        // the load's outer catch would register the view as a state_unreadable stub.
        final AtomicReference<String> failMetaReadSuffix = new AtomicReference<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                final String suffix = failMetaReadSuffix.get();
                if (suffix != null && Utf8s.endsWithAscii(name, suffix)) {
                    // An I/O error rather than a missing file: TableReaderMetadata spins on a missing
                    // _meta until the spin-lock timeout, which these tests raise to a simulated year.
                    throw CairoException.critical(5).put("test base metadata read fault");
                }
                return super.openRO(name);
            }
        };
        final String viewSql = "SELECT ts, sym, x, sum(x) OVER (PARTITION BY sym ORDER BY ts "
                + "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS s FROM base";
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + viewSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:01.000000Z', 'a', 1.0)");
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute(viewSql);
            }

            final LogCapture capture = new LogCapture();
            capture.start();
            failMetaReadSuffix.set(Files.SEPARATOR + engine.verifyTableName("base").getDirName()
                    + Files.SEPARATOR + TableUtils.META_FILE_NAME);
            try {
                engine.getLiveViewRegistry().clear();
                engine.buildViewGraphs();
                capture.drain();
                capture.assertLogged("could not read base table metadata to check live view dependencies, proceeding [view=lv");
            } finally {
                failMetaReadSuffix.set(null);
                capture.stop();
            }
            assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_status\tinvalidation_reason
                            active\t
                            """);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, sym, x) VALUES ('2026-01-01T00:00:02.000000Z', 'a', 2.0)");
                driveRefreshToQuiescence(job);
            }
            assertViewValid();
            assertViewMatchesRecompute(viewSql);

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
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
    // Drives the UPDATE fixture's row-rewriting UPDATE with updateInvalidationHook armed to throw at
    // the invalidation, and asserts the apply job suspended the base on it.
    private void applyUpdateThatDiesAtItsInvalidation() throws Exception {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            execute("UPDATE base SET x = 999.0 WHERE ts = '2026-01-01T00:00:01.000000Z'");
            drainWalQueue();
            capture.drain();
            capture.assertLogged("job failed, table suspended [table=base");
        } finally {
            capture.stop();
        }
        assertQuery("SELECT name, suspended FROM wal_tables() WHERE name = 'base'")
                .noLeakCheck().noRandomAccess().returns("name\tsuspended\nbase\ttrue\n");
    }

    private void assertBaseHoldsX(String expected) {
        Assert.assertEquals("base row 2026-01-01T00:00:01", expected, readBaseX("2026-01-01T00:00:01.000000Z"));
    }

    // The UPDATE's invalidation, as the view and live_views() report it; and the view's rows, which
    // an invalid view keeps as they were - the pre-update values, the ones its query produced.
    private void assertInvalidatedByUpdate() throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' is not registered", instance);
        Assert.assertTrue("a base UPDATE must invalidate the LV", instance.isInvalid());
        TestUtils.assertEquals(UpdateOperation.MAT_VIEW_INVALIDATION_REASON, instance.getInvalidationReason());
        assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\ninvalid\tupdate operation\n");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        Assert.assertEquals("an invalid view must not spend refresh cycles", 0, instance.getRefreshFaultCount());
        assertQuery("SELECT ts, sym, x, rn FROM lv").noLeakCheck().timestamp("ts").expectSize().returns(UPDATE_FIXTURE_VIEW_ROWS);
    }

    // A base with two rows the view has consumed and emitted, valid and quiescent.
    private void createUpdateFixture() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " +
                "SELECT ts, sym, x, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            execute("""
                    INSERT INTO base (ts, sym, x, g) VALUES
                    ('2026-01-01T00:00:01.000000Z', 'a', 1.0, 'g'),
                    ('2026-01-01T00:00:02.000000Z', 'b', 2.0, 'g')""");
            driveRefreshToQuiescence(job);
            assertViewValid();
            assertQuery("SELECT ts, sym, x, rn FROM lv").noLeakCheck().timestamp("ts").expectSize().returns(UPDATE_FIXTURE_VIEW_ROWS);
        }
    }

    // Reads the base row at ts through a reader of its own, so it sees what the base has committed
    // and nothing the writer holds uncommitted - including from inside the UPDATE's apply.
    private String readBaseX(String ts) {
        try (
                RecordCursorFactory factory = select("SELECT x FROM base WHERE ts = '" + ts + "'");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue("base row " + ts + " must exist", cursor.hasNext());
            return String.valueOf(cursor.getRecord().getDouble(0));
        } catch (SqlException e) {
            throw new AssertionError("could not read base row " + ts, e);
        }
    }

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

    // Applies a base ALTER with the view off the registry's fan-out index, so the apply-side
    // invalidation misses it and _lv.s goes on recording the view valid over the changed base. That
    // is the on-disk state a process dying between the ALTER's commit and the invalidation leaves,
    // and the one an ALTER applied while the refresh pool was off leaves, with no view registered.
    private void applyAlterMissedByInvalidation(String alterSql) throws Exception {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' is not registered", instance);
        Assert.assertSame(instance, engine.getLiveViewRegistry().removeView("lv"));
        execute(alterSql);
        drainWalQueue();
        engine.getLiveViewRegistry().registerView(instance);
        Assert.assertFalse("the apply-side invalidation must have missed the unregistered view", instance.isInvalid());
    }

    private void assertInvalidatedOnLoad(String column) throws Exception {
        // The literal rather than LiveViewInstance.BROKEN_DEPENDENCY_INVALIDATION_REASON: an operator
        // reads this string in live_views(), so a change to it should fail here.
        final String reason = "base schema change to a referenced column [column=" + column + ']';
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' is not registered", instance);
        Assert.assertTrue("the load must invalidate a view whose referenced column broke", instance.isInvalid());
        TestUtils.assertEquals(reason, instance.getInvalidationReason());
        assertQuery("SELECT view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_status\tinvalidation_reason\ninvalid\t" + reason + '\n');
    }

    private void assertLoadNamesMissedColumn(String alterSql, String column) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, price INT, size INT, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS "
                    + "SELECT ts, price, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base WHERE price > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, price, size, g) VALUES ('2026-01-01T00:00:01.000000Z', 10, 1, 'a')");
                driveRefreshToQuiescence(job);
                assertViewValid();
            }

            applyAlterMissedByInvalidation(alterSql);
            final LiveViewInstance instance = restartAndAssertInvalidatedOnLoad(column);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (ts, size, g) VALUES ('2026-01-01T00:00:02.000000Z', 2, 'a')");
                driveRefreshToQuiescence(job);
            }
            Assert.assertEquals("an invalid view must not spend refresh cycles [" + alterSql + ']',
                    0, instance.getRefreshFaultCount());
            // Still the load's reason: no retry budget overwrote it.
            assertInvalidatedOnLoad(column);

            execute("DROP LIVE VIEW lv");
            execute("DROP TABLE base");
        });
    }

    // Rebuilds the registry from disk, as a restart does, and asserts the load invalidated the view
    // for a broken referenced column before any refresh cycle ran.
    private LiveViewInstance restartAndAssertInvalidatedOnLoad(String column) throws Exception {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            capture.drain();
            capture.assertLogged("base table no longer resolves a column the live view references, invalidating [table=base, view=lv");
            capture.assertLogged(", column=" + column + ']');
        } finally {
            capture.stop();
        }
        assertInvalidatedOnLoad(column);
        return engine.getLiveViewRegistry().getViewInstance("lv");
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

    // Receives the real invalidation and decides whether, and when, to run it.
    @FunctionalInterface
    private interface UpdateInvalidationHook {
        void run(Runnable realInvalidation);
    }
}
