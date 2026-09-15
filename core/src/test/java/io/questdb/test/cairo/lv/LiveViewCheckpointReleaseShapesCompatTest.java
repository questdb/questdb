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
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRestoreRoute;
import io.questdb.cairo.lv.LiveViewCheckpointSuperblock;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * The format block against the released shapes {@link LiveViewCheckpointReleaseCompatTest} does
 * not reach - and, in four of them, against trees that never carried the removed layout at all.
 * <p>
 * That case reads a released tree carrying one anchored cumulative view, which is the shape whose
 * state root this build stopped being able to decode. Five more shapes live in this fixture, and
 * only one of them is anchored:
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
 * Four of those five have no anchored window, so 10.0.1 wrote them no state root of any kind and
 * this build could in principle read every page they hold. They block anyway, and that is the
 * fact this class exists for: the boundary is the <b>timeline's</b> declared format version, not
 * the shape of the roots underneath it. A per-root boundary would leave these four restoring off
 * a directory whose sibling structures this build no longer understands, which is a narrower
 * promise than the format version makes.
 * <p>
 * The rows are the second half. A blocked view keeps serving what the released build materialized
 * - checked here against a snapshot taken before this build's runtime touched the tree, rather
 * than against a recompute, because a recompute is exactly what a block must not have done.
 * <p>
 * {@link LiveViewCheckpointWireFormatTest} takes the same fixture apart page by page. This class
 * asks only what the composite path does with it.
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
    // The layout version 10.0.1 stamped, pinned rather than derived from this build's own
    // constant so a later bump cannot quietly redefine what the fixture is.
    private static final int RELEASED_FORMAT_VERSION = 1;
    private static final ObjList<ReleasedShape> SHAPES = releasedShapes();
    // A valid view holding unmoved rows is the ending of both a block and a decline further down,
    // so the state a case can read afterwards does not say which gate fired. The log does.
    private static final LogCapture capture = new LogCapture();

    @After
    public void resetClock() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpCadence() {
        // Matches the cadence the fixture was sealed under, so a commit made after the upgrade
        // seals a boundary of its own rather than waiting for a row budget to fill.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(2 * FIXTURE_END_MICROS);
        capture.start();
    }

    @Test
    public void testEveryReleasedShapeBlocksWhateverItsStateLayout() throws Exception {
        assertMemoryLeak(() -> {
            final LongList checkpointFilesBefore = openFixture();
            final ObjList<CharSequence> releasedRows = new ObjList<>();
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                releasedRows.add(snapshotRows(viewName));
                Assert.assertEquals(
                        viewName + ": the fixture must declare the version 10.0.1 stamped",
                        RELEASED_FORMAT_VERSION,
                        readSuperblockFormatVersion(checkpointsRoot(viewName))
                );
            }

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            // Nothing rebuilt, whichever shape the tree held: the version is read off the
            // superblock and the decision is taken there, above every structure below it.
            capture.drain();
            capture.assertLogged("live view checkpoint timeline declares an unsupported format version");
            capture.assertNotLogged("could not restore live view from checkpoint timeline, rebuilding derived state");
            capture.assertNotLogged("live view restart rebuilding from applied base");

            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                final LiveViewInstance instance = instance(viewName);
                Assert.assertTrue(viewName + ": must be blocked", instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(viewName + ": blocking must not write _lv.s.invalid", instance.isInvalid());
                Assert.assertEquals(
                        viewName + ": the route must name the decision the refresh turn took",
                        "upgrade_blocked",
                        LiveViewCheckpointRestoreRoute.name(instance.getCheckpointRestoreRoute())
                );
                Assert.assertFalse(viewName + ": no restore may be attempted", instance.isCheckpointRestoreAttempted());
                Assert.assertEquals(viewName, 0, instance.getCheckpointRebuildAttempts());
                Assert.assertEquals(viewName, 0, instance.getCheckpointTimelineResets());
                Assert.assertEquals(
                        viewName + ": not one file of the released directory may move",
                        checkpointFilesBefore.getQuick(i),
                        countFiles(checkpointsRoot(viewName))
                );
                TestUtils.assertEquals(
                        viewName + ": the rows the released build materialized must still be served",
                        releasedRows.getQuick(i),
                        snapshotRows(viewName)
                );
                assertNoRefreshFaults(viewName);
            }

            // A base commit the blocked views will not consume, then a second restart: the
            // disposition is derived from the superblock, so it comes back with no marker of its
            // own and the rows still do not move.
            insertDense(60);
            insertWide(60);
            insertLate(130);
            restartCycle();
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                Assert.assertTrue(viewName + ": must stay blocked", instance(viewName).isCheckpointRecoveryBlocked());
                Assert.assertEquals(
                        viewName,
                        checkpointFilesBefore.getQuick(i),
                        countFiles(checkpointsRoot(viewName))
                );
                TestUtils.assertEquals(viewName, releasedRows.getQuick(i), snapshotRows(viewName));
            }
        });
    }

    @Test
    public void testTheOperatorsReCreateIsTheWayOutForEveryReleasedShape() throws Exception {
        assertMemoryLeak(() -> {
            openFixture();
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                Assert.assertTrue(instance(SHAPES.getQuick(i).viewName).isCheckpointRecoveryBlocked());
            }

            // The documented exit, run as an operator would run it, once per shape. SHOW CREATE
            // LIVE VIEW has to work on a blocked view of every shape - the definition is what the
            // re-create is built from - and its output has to re-execute.
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                printSql("SHOW CREATE LIVE VIEW " + viewName + ';');
                final String releasedDdl = sink.toString().replace("ddl\n", "");
                execute("DROP LIVE VIEW " + viewName);
                execute(releasedDdl);
            }

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int i = 0, n = SHAPES.size(); i < n; i++) {
                    driveSeedToCompletion(job, SHAPES.getQuick(i).viewName);
                }
                driveRefreshToQuiescence(job);
                insertDense(job, 60);
                insertWide(job, 60);
                insertLate(job, 130);
            }

            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                final String viewName = SHAPES.getQuick(i).viewName;
                final LiveViewInstance recreated = instance(viewName);
                Assert.assertFalse(viewName + ": the re-created view must not be blocked",
                        recreated.isCheckpointRecoveryBlocked());
                Assert.assertFalse(viewName, recreated.isInvalid());
                Assert.assertEquals(
                        viewName + ": the re-created view must seal under this build's format version",
                        LiveViewCheckpointSuperblock.SLOT_FORMAT_VERSION,
                        readSuperblockFormatVersion(checkpointsRoot(viewName))
                );
            }
            // The rows are a recomputation from the base rows available today, which is what makes
            // this a separate operation rather than completion of an upgrade.
            assertEveryShapeMatchesRecompute("after the operator's re-create");

            // And they are ordinary views from here: each seals its own timeline and a restart
            // comes back on it.
            restartCycle();
            for (int i = 0, n = SHAPES.size(); i < n; i++) {
                assertRestoredFromTimeline(SHAPES.getQuick(i).viewName);
            }
            assertEveryShapeMatchesRecompute("after a restart off the re-created views' own seals");
        });
    }

    /**
     * The rows a view serves right now, as text. Compared against itself across a refresh turn and
     * a restart, which is how a case says "these did not move" without a literal per shape - and
     * without a from-base recompute, which is the thing a blocked view must not have run.
     */
    private CharSequence snapshotRows(String viewName) throws Exception {
        printSql("SELECT * FROM " + viewName + " ORDER BY 2, 1;");
        return sink.toString();
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

    private void insertDense(int second) throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp(second) + "', 'acct-1', "
                + (second + 1.0) + ", " + (second * 1_000L) + ", "
                + "1234512345678901234567890.123456m, 12345678901234567890123456789012345678901m)");
        drainWalQueue();
    }

    private void insertDense(LiveViewRefreshJob job, int second) throws Exception {
        insertDense(second);
        driveRefreshToQuiescence(job);
    }

    private void insertLate(int second) throws Exception {
        execute("INSERT INTO late VALUES ('" + timestamp(second) + "', 'acct-1', " + (second + 1.0) + ")");
        drainWalQueue();
    }

    private void insertLate(LiveViewRefreshJob job, int second) throws Exception {
        insertLate(second);
        driveRefreshToQuiescence(job);
    }

    private void insertWide(int second) throws Exception {
        execute("INSERT INTO wide VALUES ('" + timestamp(second) + "', 'k000', " + (second + 1.0) + ")");
        drainWalQueue();
    }

    private void insertWide(LiveViewRefreshJob job, int second) throws Exception {
        insertWide(second);
        driveRefreshToQuiescence(job);
    }

    /**
     * Unpacks the fixture and loads its catalogue, which is where the block is decided, and
     * reports the file count each view's released tree arrived with. The counts are taken before
     * the catalogue load, so they are the released inventory rather than one this build has
     * already had an opportunity to change.
     */
    private LongList openFixture() throws IOException {
        replaceDbContent(FIXTURE_RESOURCE);
        final LongList checkpointFiles = new LongList();
        for (int i = 0, n = SHAPES.size(); i < n; i++) {
            final String viewName = SHAPES.getQuick(i).viewName;
            final File checkpointsRoot = new File(
                    new File(engine.getConfiguration().getDbRoot(), engine.getTableTokenIfExists(viewName).getDirName()),
                    LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME
            );
            checkpointFiles.add(countFiles(checkpointsRoot));
        }
        engine.buildViewGraphs();
        for (int i = 0, n = SHAPES.size(); i < n; i++) {
            final String viewName = SHAPES.getQuick(i).viewName;
            Assert.assertFalse(viewName + ": the fixture must not carry an invalid view", instance(viewName).isInvalid());
        }
        return checkpointFiles;
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
     * The five released views and the from-base recompute each one's rows have to equal once an
     * operator has re-created it. The lineage each view's own last seal published is deliberately
     * not among these: this build cannot read a blocked timeline, so a number it could not check
     * would be documentation posing as an assertion.
     */
    private static ObjList<ReleasedShape> releasedShapes() {
        final ObjList<ReleasedShape> shapes = new ObjList<>();
        shapes.add(new ReleasedShape(
                "lv_rows",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (" + ROWS_FRAME + ") AS windowed_sum, "
                        + "count(amount) OVER (" + ROWS_FRAME + ") AS windowed_count "
                        + "FROM tx"
        ));
        shapes.add(new ReleasedShape(
                "lv_range",
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
                "lv_decimal",
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
                "lv_keyed",
                "SELECT created_at, account_id, "
                        + "sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at "
                        + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum "
                        + "FROM (SELECT created_at, account_id, amount, "
                        + "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket "
                        + "FROM wide)"
        ));
        shapes.add(new ReleasedShape(
                "lv_late",
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
        final String recompute;
        final String viewName;

        ReleasedShape(String viewName, String recompute) {
            this.viewName = viewName;
            this.recompute = recompute;
        }
    }
}
