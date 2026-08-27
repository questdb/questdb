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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.ObjList;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Cross-version restore for the durable shapes {@link LiveViewCheckpointReleaseCompatTest}
 * does not reach.
 * <p>
 * That case reads a released tree carrying one anchored cumulative view, which exercises the
 * boundary root, the legacy anchor root, the function roots and directory, and a partition map
 * one page deep. Four shapes stay unread by it, and each is a decoder this branch would run
 * against released bytes the first time a real instance upgrades:
 * <ul>
 *     <li><b>{@code lv_rows}</b> - a bounded {@code ROWS} frame, whose functions freeze a
 *     whole-state page rather than a ring;</li>
 *     <li><b>{@code lv_range}</b> - a bounded {@code RANGE} frame, whose functions keep the
 *     chunked ring: a timestamp page per chunk, a value page whose kind the ring's value kind
 *     selects, and a scalar continuation state in the partition entry;</li>
 *     <li><b>{@code lv_decimal}</b> - the same frame over {@code DECIMAL(38,6)} and
 *     {@code DECIMAL(60,0)}, which widen the ring value and the scalar past one 64-bit word;</li>
 *     <li><b>{@code lv_keyed}</b> - 100 partition keys, so the partition map has an internal
 *     node and a restore has to descend rather than read one leaf;</li>
 *     <li><b>{@code lv_late}</b> - a timeline a localized out-of-order repair spliced, so its
 *     published generation carries a row-position delta tree correcting the suffix.</li>
 * </ul>
 * <p>
 * The load-bearing assertion is the same one the sibling case rests on:
 * {@link LiveViewInstance#isCheckpointRestoreSucceeded()}. A restore that threw would retire
 * the timeline, replay from the applied base and land on exactly the same rows, so a row
 * oracle alone proves nothing. The rows are compared anyway - against a from-base recompute,
 * not against the runtime's own arithmetic - because a restore that succeeded on a misread
 * page is worse than one that failed.
 * <p>
 * {@link LiveViewCheckpointWireFormatTest} takes the same fixture apart page by page. This
 * class asks only whether the composite path works.
 * <p>
 * To regenerate the fixture, copy {@code /lv/LiveViewReleaseShapesFixtureGenerator.java.txt}
 * into a clean {@code 10.0.1} checkout's {@code io.questdb.test.cairo.lv} package and run it;
 * the constants below are the values it prints.
 */
public class LiveViewCheckpointReleaseShapesCompatTest extends AbstractLiveViewCheckpointCompatTest {

    // The simulated clock the fixture's own run left behind. This one starts above it, so the
    // flush cadence reads a forward-moving clock rather than one that jumped backwards.
    private static final long FIXTURE_END_MICROS = 9_000_000L;
    private static final String FIXTURE_RESOURCE = "/lv/lv_checkpoint_10_0_1_shapes.zip";
    /**
     * The bounded RANGE frame the released {@code lv_range} and {@code lv_decimal} views were
     * declared with, written out as an ordinary window term for the recompute oracles.
     */
    private static final String RANGE_FRAME =
            "PARTITION BY account_id ORDER BY created_at RANGE BETWEEN '30' SECOND PRECEDING AND CURRENT ROW";
    /**
     * The bounded ROWS frame the released {@code lv_rows} and {@code lv_late} views were
     * declared with.
     */
    private static final String ROWS_FRAME =
            "PARTITION BY account_id ORDER BY created_at ROWS BETWEEN 3 PRECEDING AND CURRENT ROW";
    private static final ObjList<ReleasedShape> SHAPES = releasedShapes();

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // Matches the cadence the fixture was sealed under, so a commit made after the upgrade
        // seals a boundary of its own rather than waiting for a row budget to fill.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
    }

    @Test
    public void testEveryReleasedShapeKeepsAccumulatingAfterTheUpgrade() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                for (int i = 0, n = SHAPES.size(); i < n; i++) {
                    Assert.assertTrue(
                            SHAPES.getQuick(i).viewName + ": the upgrade must restore off the released roots",
                            instance(SHAPES.getQuick(i).viewName).isCheckpointRestoreSucceeded()
                    );
                }

                // The first commit after the upgrade seals through this branch's writers. A
                // restored partition that came back empty answers this row's own value rather
                // than the running one, which the recompute catches on the very next row.
                insertDense(job, 60);
                insertWide(job, 60);
                insertLate(job, 130);
            }
            assertEveryShapeMatchesRecompute("after a commit through this branch's writers");

            // A second restart, now reading back whatever this branch sealed rather than the
            // released roots.
            restartCycle();
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                Assert.assertFalse(viewName + ": must stay valid across a restart", instance(viewName).isInvalid());
                Assert.assertTrue(
                        viewName + ": the restart must restore off the root this branch converted to",
                        instance(viewName).isCheckpointRestoreSucceeded()
                );
            }
            assertEveryShapeMatchesRecompute("after a restart off this branch's own seal");
        });
    }

    @Test
    public void testEveryReleasedShapeRestoresRatherThanRebuildingFromTheBase() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();

            // The fixture is genuinely legacy-shaped: 10.0.1 has no fused root to write, so a
            // probe that reported one here would mean the case is testing this branch's own bytes.
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final ReleasedShape shape = SHAPES.getQuick(i);
                Assert.assertFalse(
                        shape.viewName + ": the fixture's head must be a legacy root, not a fused one",
                        isFusedHead(shape.viewName)
                );
                assertReleasedLineage(shape, "the fixture must arrive with the lineage the released build sealed");
            }

            // The upgrade's first refresh cycle.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final ReleasedShape shape = SHAPES.getQuick(i);
                final LiveViewInstance instance = instance(shape.viewName);
                Assert.assertFalse(
                        shape.viewName + ": a released checkpoint must not invalidate the view",
                        instance.isInvalid()
                );
                Assert.assertTrue(shape.viewName + ": the restore must have run", instance.isCheckpointRestoreAttempted());
                Assert.assertTrue(
                        shape.viewName + ": the upgrade must restore off the released roots rather than "
                                + "rebuild from the base",
                        instance.isCheckpointRestoreSucceeded()
                );
                assertNoRefreshFaults(shape.viewName);

                // A rebuild retires the timeline before replaying, so the lineage is the second,
                // independent witness that no fallback ran.
                assertReleasedLineage(
                        shape,
                        "the released lineage must carry forward rather than reset to a new generation"
                );
                Assert.assertEquals(
                        shape.viewName + ": the restored runtime must resume at the boundary the released "
                                + "build sealed",
                        ts(shape.headBoundary),
                        instance.getHeadCheckpointMaxTs()
                );
                Assert.assertFalse(
                        shape.viewName + ": restoring must not rewrite the head; only a later seal converts "
                                + "the shape",
                        isFusedHead(shape.viewName)
                );
            }

            // A restore that succeeded on a misread page is worse than one that failed.
            assertEveryShapeMatchesRecompute("straight off the released roots");
        });
    }

    private void assertEveryShapeMatchesRecompute(String at) throws Exception {
        for (int i = 0, n = SHAPES.size(); i < n; i++) {
            final ReleasedShape shape = SHAPES.getQuick(i);
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    '(' + shape.recompute + ") ORDER BY 2, 1",
                    '(' + shape.viewName + ") ORDER BY 2, 1",
                    LOG,
                    true
            );
            assertNoRefreshFaults(shape.viewName);
        }
        LOG.info().$("released shapes match their from-base recompute [at=").$(at).$(']').$();
    }

    /**
     * Asserts the head the released build sealed is still the head in force: the same number of
     * logical boundaries, standing on the same live-view and base sequencer txns.
     * <p>
     * The boundary timestamp is deliberately not among these. It is runtime state the seal and
     * the restore publish, not something the live-view state file carries, so before the first
     * refresh it reads {@code LONG_NULL} whatever the timeline holds. The case asserts it
     * separately, after the restore that fills it in.
     */
    private void assertReleasedLineage(ReleasedShape shape, String message) {
        final LiveViewInstance instance = instance(shape.viewName);
        Assert.assertEquals(
                shape.viewName + ": " + message + " [sealedBoundaries]",
                shape.sealedBoundaries,
                countSealedBoundaries(shape.viewName)
        );
        Assert.assertEquals(
                shape.viewName + ": " + message + " [headLvSeqTxn]",
                shape.headLvSeqTxn,
                instance.getHeadCheckpointLvSeqTxn()
        );
        Assert.assertEquals(
                shape.viewName + ": " + message + " [headBaseSeqTxn]",
                shape.headBaseSeqTxn,
                instance.getHeadCheckpointBaseSeqTxn()
        );
    }

    private void insertDense(LiveViewRefreshJob job, int second) throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp(second) + "', 'acct-1', "
                + (second + 1.0) + ", " + (second * 1_000L) + ", "
                + "1234512345678901234567890.123456m, 12345678901234567890123456789012345678901m)");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void insertLate(LiveViewRefreshJob job, int second) throws Exception {
        execute("INSERT INTO late VALUES ('" + timestamp(second) + "', 'acct-1', " + (second + 1.0) + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void insertWide(LiveViewRefreshJob job, int second) throws Exception {
        execute("INSERT INTO wide VALUES ('" + timestamp(second) + "', 'k000', " + (second + 1.0) + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    /**
     * Unpacks the fixture and registers its live views, without refreshing them yet, so a case
     * may inspect the released tree before this branch's runtime has touched it.
     */
    private void openFixture() throws IOException {
        replaceDbContent(FIXTURE_RESOURCE);
        engine.buildViewGraphs();
        for (int i = 0, n = SHAPES.size(); i < n; i++) {
            final String viewName = SHAPES.getQuick(i).viewName;
            Assert.assertFalse(viewName + ": the fixture must not carry an invalid view", instance(viewName).isInvalid());
        }
    }

    private void restartCycle() throws Exception {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(resumed);
        }
    }

    private static String timestamp(int secondOfDay) {
        return String.format("2026-01-01T09:%02d:%02d.000000Z", secondOfDay / 60, secondOfDay % 60);
    }

    /**
     * The five released views, the lineage each one's own last seal published, and the
     * from-base recompute its rows have to equal. The numbers are what the generator
     * printed when it wrote the fixture.
     */
    private static ObjList<ReleasedShape> releasedShapes() {
        final ObjList<ReleasedShape> shapes = new ObjList<>();
        shapes.add(new ReleasedShape(
                "lv_rows", 5, 5, 5, "2026-01-01T09:00:41.000000Z",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (" + ROWS_FRAME + ") AS windowed_sum, "
                        + "count(amount) OVER (" + ROWS_FRAME + ") AS windowed_count "
                        + "FROM tx"
        ));
        shapes.add(new ReleasedShape(
                "lv_range", 5, 5, 5, "2026-01-01T09:00:41.000000Z",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (" + RANGE_FRAME + ") AS range_sum, "
                        + "max(amount) OVER (" + RANGE_FRAME + ") AS range_max, "
                        + "first_value(amount) OVER (" + RANGE_FRAME + ") AS range_first, "
                        + "sum(qty) OVER (" + RANGE_FRAME + ") AS range_qty_sum, "
                        + "max(qty) OVER (" + RANGE_FRAME + ") AS range_qty_max, "
                        + "first_value(qty) OVER (" + RANGE_FRAME + ") AS range_qty_first, "
                        + "count(amount) OVER (" + RANGE_FRAME + ") AS range_count "
                        + "FROM tx"
        ));
        shapes.add(new ReleasedShape(
                "lv_decimal", 5, 5, 5, "2026-01-01T09:00:41.000000Z",
                "SELECT created_at, account_id, "
                        + "sum(d128) OVER (" + RANGE_FRAME + ") AS decimal_sum128, "
                        + "max(d128) OVER (" + RANGE_FRAME + ") AS decimal_max128, "
                        + "sum(d256) OVER (" + RANGE_FRAME + ") AS decimal_sum256, "
                        + "max(d256) OVER (" + RANGE_FRAME + ") AS decimal_max256 "
                        + "FROM tx"
        ));
        // ANCHOR is live-view syntax, so the daily bucket is written out as an ordinary
        // partition term.
        shapes.add(new ReleasedShape(
                "lv_keyed", 5, 5, 5, "2026-01-01T09:00:40.000000Z",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum "
                        + "FROM (SELECT created_at, account_id, amount, "
                        + "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket "
                        + "FROM wide)"
        ));
        shapes.add(new ReleasedShape(
                "lv_late", 12, 13, 13, "2026-01-01T09:01:51.000000Z",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (" + ROWS_FRAME + ") AS windowed_sum "
                        + "FROM late"
        ));
        return shapes;
    }

    /**
     * One released view and everything the cases assert about it.
     */
    private static final class ReleasedShape {
        final long headBaseSeqTxn;
        final String headBoundary;
        final long headLvSeqTxn;
        final String recompute;
        final int sealedBoundaries;
        final String viewName;

        ReleasedShape(
                String viewName,
                int sealedBoundaries,
                long headLvSeqTxn,
                long headBaseSeqTxn,
                String headBoundary,
                String recompute
        ) {
            this.viewName = viewName;
            this.sealedBoundaries = sealedBoundaries;
            this.headLvSeqTxn = headLvSeqTxn;
            this.headBaseSeqTxn = headBaseSeqTxn;
            this.headBoundary = headBoundary;
            this.recompute = recompute;
        }
    }
}
