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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.mp.Job;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * DDL-on-base-table behaviour for live views. Focuses on schema changes that are
 * routed through {@code ApplyWal2TableJob}'s structural path and reach
 * {@code CairoEngine.invalidateLiveViewsForBaseSchemaChange}.
 * <p>
 * The centrepiece is {@code ALTER COLUMN TYPE} on a referenced column. The refresh
 * path derives each column's stride from the cached compile-time factory metadata,
 * so a referenced column whose type changed under the view would be read through the
 * stale stride: wrong results on a widening change, and an out-of-bounds native read
 * (SIGSEGV / corruption) on a narrowing or fixed&lt;-&gt;var-size change. The view
 * must therefore flip to INVALID before it ever refreshes over the changed base. A
 * type change to a column the view does not read must stay transparent.
 */
public class LiveViewBaseDdlTest extends AbstractCairoTest {

    // > FLUSH EVERY 100ms, so a single driveRefreshToQuiescence pass crosses the flush window.
    private static final long CLOCK_ADVANCE_MICROS = 250_000;

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
    public void testAlterUnreferencedColumnTypeIsTransparent() throws Exception {
        // Changing the TYPE of a column the LV never reads must NOT invalidate it, and
        // the view must keep refreshing correctly across the change (documents the
        // boundary of the type-change invalidation and proves it is not over-broad).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x INT, y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS " + viewSql);

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

    private void assertReferencedColumnTypeChangeInvalidates(String initialType, String newType) throws Exception {
        final String transition = initialType + "->" + newType;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, x " + initialType + ", y INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
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

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing the clock each pass so
    // deferred flushes land, and applying the LV's own WAL after each burst. Mirrors the fuzz harness.
    private void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < 512; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                break;
            }
        }
    }
}
