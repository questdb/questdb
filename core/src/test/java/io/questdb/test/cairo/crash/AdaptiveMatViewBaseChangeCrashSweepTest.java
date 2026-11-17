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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewBacklog;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * Does a materialized view moved to another base table recover to a consistent definition, base identity and state
 * after a crash at every durability op of {@code ALTER MATERIALIZED VIEW ... WITH BASE} and the
 * {@code REFRESH ... RESUME SKIP BACKLOG} that follows it?
 *
 * <h3>The workload</h3>
 * Two adaptive WAL tables, {@code base1} and {@code base2}, and an IMMEDIATE view {@code mv} =
 * {@code SELECT ts, count() cnt FROM base1 SAMPLE BY 1h}, refreshed over the rows of {@code base1}. {@code base2}
 * holds rows in other hours, written by more transactions than {@code base1} has, so a refresh position on
 * {@code base2} lies past every transaction of {@code base1}. Setup takes a durable epoch, then disables it, so the
 * swept commit phase applies lazily: the base change (an invalidation event, then the definition event, in the view's
 * WAL), the RESUME SKIP BACKLOG (a valid view state event with {@code base2}'s id), and one {@code base2} row that the
 * resumed view refreshes.
 *
 * <h3>Modelling a fresh restart for the view</h3>
 * The view's definition lives in the dependency graph and its state in the state store, both in memory, which the
 * driver's restart does not rebuild: a crash that loses the definition event would leave the graph on {@code base2}
 * while the disk says {@code base1}. {@link #recoverAfterCrash} therefore drops both and loads them from disk after
 * {@link RecoveryCoordinator#recover()} and before WAL apply, as a booting server runs
 * {@link io.questdb.cairo.CairoEngine#buildViewGraphs()}. The load also clears the in-memory record of the definition
 * event, which a crash may have lost, so a RESUME in the oracle does not wait for it.
 *
 * <h3>A crash the statements or the refresh swallow</h3>
 * A {@link CrashSimulationError} fired in the refresh becomes a view invalidation, and one fired in a statement's
 * best-effort tail may be logged and dropped. Neither is a propagated error or a table suspend, which the driver
 * watches, so the workload's {@code commit()} rethrows the fired crash after each step.
 *
 * <h3>The oracle</h3>
 * The view's durable progress is one of these stages, non-decreasing in k:
 * <ol start="0">
 *   <li>nothing durable: valid on {@code base1}, with {@code base1}'s id, holding {@code base1}'s buckets;</li>
 *   <li>the invalidation durable, the definition not: invalid on {@code base1} ("base table changed"), backlog lost;</li>
 *   <li>the definition durable: invalid on {@code base2}, backlog lost;</li>
 *   <li>RESUME durable: valid on {@code base2}, with {@code base2}'s id and no lost backlog;</li>
 *   <li>the new {@code base2} row durable as well.</li>
 * </ol>
 * At every crash point, after recovery: no table is suspended; the view is in one of the stages, so it never pairs
 * the new base with a valid state of the old one; an invalid view keeps its rows; after the post-boot refresh a valid
 * view stays valid and holds {@code base1}'s buckets plus the {@code base2} rows written after RESUME. The view then
 * reconciles: {@code ALTER ... WITH BASE} again where the definition was lost, {@code RESUME SKIP BACKLOG} where the
 * view is invalid, then {@code REFRESH ... FULL}, which must equal the aggregation of {@code base2}, and a follow-up
 * {@code base2} row must reach the view. At k=N the view reaches the last stage.
 */
public class AdaptiveMatViewBaseChangeCrashSweepTest extends AbstractAdaptiveCrashSweepTest {
    private static final String ALTER_SQL = "ALTER MATERIALIZED VIEW mv WITH BASE base2 AS (SELECT ts, count() cnt FROM base2 SAMPLE BY 1h)";
    private static final String BASE_AGG_SQL = "SELECT ts::LONG t, count() cnt FROM %s SAMPLE BY 1h ORDER BY 1";
    // the hour of the base2 row the commit phase writes after RESUME, and of every later base2 row
    private static final int BASE2_AFTER_RESUME_HOUR = 10;
    private static final int LINUX_COMMIT_DURABILITY_OPS = 45;
    private static final String MV_ROWS_SQL = "SELECT ts::LONG t, cnt FROM mv ORDER BY 1";
    private static final String RESUME_SQL = "REFRESH MATERIALIZED VIEW mv RESUME SKIP BACKLOG";
    private static final int STAGE_BASE2_ROW_DURABLE = 4;
    private static final int STAGE_DEFINITION_DURABLE = 2;
    private static final int STAGE_INVALIDATION_DURABLE = 1;
    private static final int STAGE_NOTHING_DURABLE = 0;
    private static final int STAGE_RESUME_DURABLE = 3;
    private static final int SWEEP_CRASH_POINT_CAP = 512;

    @Test
    public void testBaseChangeAndResumeSweepRecoversConsistentInPart1Of2() throws Exception {
        assertSweepPart(1, 2);
    }

    @Test
    public void testBaseChangeAndResumeSweepRecoversConsistentInPart2Of2() throws Exception {
        assertSweepPart(2, 2);
    }

    /**
     * Drops the view's definition and state and loads them from disk after recovery and before WAL apply, as a
     * booting server does, see the class description.
     */
    @Override
    protected void recoverAfterCrash(TableToken[] tokens) {
        simulateRestart(tokens);
        new RecoveryCoordinator(engine).recover();
        final TableToken mvTt = engine.verifyTableName("mv");
        engine.getDependentViewGraph().removeView(mvTt);
        engine.getMatViewStateStore().removeViewState(mvTt);
        engine.hydrateMatViewStateStore();
        for (TableToken tt : tokens) {
            engine.notifyWalTxnRepublisher(tt);
        }
        drainWalQueue();
    }

    private static List<String> expectedRows(List<String> base1Buckets, List<String> base2BucketsAfterResume) {
        final TreeSet<String> rows = new TreeSet<>((a, b) -> Long.compare(
                Long.parseLong(a.substring(0, a.indexOf('|'))),
                Long.parseLong(b.substring(0, b.indexOf('|')))
        ));
        rows.addAll(base1Buckets);
        rows.addAll(base2BucketsAfterResume);
        return new ArrayList<>(rows);
    }

    private static String hourTimestamp(int hour) {
        return String.format("2024-10-01T%02d:00:00.000000Z", hour);
    }

    private static String insertSql(String table, int hour) {
        return "INSERT INTO " + table + " VALUES ('" + hourTimestamp(hour) + "')";
    }

    private static List<String> readRows(String sql) throws SqlException {
        final List<String> rows = new ArrayList<>();
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                rows.add(record.getLong(0) + "|" + record.getLong(1));
            }
        }
        return rows;
    }

    private void assertSweepPart(int part, int partCount) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                final BaseChangeWorkload workload = new BaseChangeWorkload();
                final SweepResult r = forEachAdaptiveCrashPointInPart(workload, SWEEP_CRASH_POINT_CAP, part, partCount);
                LOG.info().$("[mat-view base change sweep] N=").$(r.n)
                        .$(", part=").$(r.firstSweptK).$("..").$(r.lastSweptK)
                        .$(", stageByK=").$(Arrays.toString(r.recoveredByK())).$();

                Assert.assertFalse("the cap must not truncate the sweep", r.truncated);
                // Each part computes its range from its own count pass, so the pin keeps the parts sweeping every point
                // once; it also fails on a removed flush, which would otherwise only shrink the sweep. Off Linux every
                // flush of a mapped file adds an msync, so the count differs there.
                if (crashFf.isSyncfsFileSystemWide()) {
                    Assert.assertEquals("the commit phase's durability ops changed:\n" + phaseDurabilityOps(),
                            LINUX_COMMIT_DURABILITY_OPS, r.n);
                }
                for (int k = r.firstSweptK + 1; k <= r.lastSweptK; k++) {
                    Assert.assertTrue(
                            "the recovered stage must be non-decreasing at k=" + k + " (" + r.recoveredByK()[k - 1]
                                    + " -> " + r.recoveredByK()[k] + "), op: " + phaseDurabilityOp(k),
                            r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                    );
                }
                if (r.firstSweptK == 1) {
                    Assert.assertEquals("the first crash point must keep nothing of the base change",
                            STAGE_NOTHING_DURABLE, r.recoveredByK()[1]);
                }
                if (r.lastSweptK == r.sweptPoints) {
                    Assert.assertEquals("the last crash point must keep everything the commit phase wrote",
                            STAGE_BASE2_ROW_DURABLE, r.recoveredByK()[r.lastSweptK]);
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private final class BaseChangeWorkload implements AdaptiveCrashWorkload {
        private TableToken base1Tt;
        private TableToken base2Tt;
        private TableToken mvTt;

        @Override
        public void commit() throws Exception {
            step(ALTER_SQL);
            step(RESUME_SQL);
            step(insertSql("base2", BASE2_AFTER_RESUME_HOUR));
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            final String at = "k=" + k + " (op " + phaseDurabilityOp(k) + "): ";
            for (TableToken tt : new TableToken[]{base1Tt, base2Tt, mvTt}) {
                Assert.assertFalse(at + tt.getTableName() + " must not be suspended after recovery",
                        engine.getTableSequencerAPI().isSuspended(tt));
            }

            final List<String> base1Buckets = readRows(String.format(BASE_AGG_SQL, "base1"));
            Assert.assertEquals(at + "base1 is not part of the commit phase", 2, base1Buckets.size());
            final List<String> base2Buckets = readRows(String.format(BASE_AGG_SQL, "base2"));
            // base2's rows written after RESUME, which only the resumed view refreshes
            final List<String> base2BucketsAfterResume = readRows(
                    "SELECT ts::LONG t, count() cnt FROM base2 WHERE ts >= '" + hourTimestamp(BASE2_AFTER_RESUME_HOUR)
                            + "' SAMPLE BY 1h ORDER BY 1"
            );

            final MatViewDefinition definition = engine.getDependentViewGraph().getViewDefinition(mvTt);
            final MatViewState state = engine.getMatViewStateStore().getViewState(mvTt);
            Assert.assertNotNull(at + "the view definition must load", definition);
            Assert.assertNotNull(at + "the view state must load", state);
            final boolean isOnBase2 = "base2".equals(definition.getBaseTableName());
            if (!isOnBase2) {
                Assert.assertEquals(at + "the view must stay on its old base or move to the new one",
                        "base1", definition.getBaseTableName());
            }

            final int stage;
            if (!state.isInvalid()) {
                if (isOnBase2) {
                    // The base change must never leave the new base next to a valid state of the old one.
                    Assert.assertEquals(at + "a valid view on the new base must carry the new base's id",
                            base2Tt.getTableId(), state.getBaseTableId());
                    Assert.assertNull(at + "a resumed view has no lost backlog", state.getBacklogLostReason());
                    stage = base2BucketsAfterResume.isEmpty() ? STAGE_RESUME_DURABLE : STAGE_BASE2_ROW_DURABLE;
                } else {
                    Assert.assertEquals(at + "a valid view on the old base must carry the old base's id",
                            base1Tt.getTableId(), state.getBaseTableId());
                    Assert.assertEquals(at + "a view untouched by the base change must hold base1's buckets",
                            base1Buckets, readRows(MV_ROWS_SQL));
                    stage = STAGE_NOTHING_DURABLE;
                }
            } else {
                Assert.assertEquals(at + "only the base change invalidates the view",
                        MatViewBacklog.LOST_BASE_TABLE_CHANGED, state.getInvalidationReasonForReport());
                Assert.assertEquals(at + "the base change loses the backlog of a refreshed view",
                        MatViewBacklog.LOST_BASE_TABLE_CHANGED, state.getBacklogLostReason());
                Assert.assertEquals(at + "an invalid view keeps its rows", base1Buckets, readRows(MV_ROWS_SQL));
                Assert.assertTrue(at + "the base2 row written after RESUME cannot outlive RESUME",
                        base2BucketsAfterResume.isEmpty());
                stage = isOnBase2 ? STAGE_DEFINITION_DURABLE : STAGE_INVALIDATION_DURABLE;
            }

            // The post-boot refresh: a valid view stays valid and catches up on base2's rows written after RESUME.
            final boolean isInvalid = state.isInvalid();
            drainWalAndMatViewQueues();
            Assert.assertEquals(at + "the post-boot refresh must not change the view's validity: " + state.getInvalidationReasonForReport(),
                    isInvalid, state.isInvalid());
            if (!isInvalid) {
                Assert.assertEquals(at + "a valid view holds the buckets of its refreshes", expectedRows(base1Buckets,
                        stage >= STAGE_RESUME_DURABLE ? base2BucketsAfterResume : List.of()), readRows(MV_ROWS_SQL));
            }

            // Reconcile: finish the base change and the RESUME where the crash lost them.
            if (!isOnBase2) {
                execute(ALTER_SQL);
                drainWalAndMatViewQueues();
            }
            if (state.isInvalid()) {
                execute(RESUME_SQL);
                drainWalAndMatViewQueues();
            }
            final MatViewDefinition reconciledDefinition = engine.getDependentViewGraph().getViewDefinition(mvTt);
            final MatViewState reconciledState = engine.getMatViewStateStore().getViewState(mvTt);
            Assert.assertEquals(at + "the reconciled view must be on base2", "base2", reconciledDefinition.getBaseTableName());
            Assert.assertFalse(at + "the reconciled view must be valid: " + reconciledState.getInvalidationReasonForReport(),
                    reconciledState.isInvalid());
            Assert.assertEquals(at + "the reconciled view must carry base2's id", base2Tt.getTableId(), reconciledState.getBaseTableId());
            Assert.assertEquals(at + "the reconciled view keeps its rows and refreshes base2 after RESUME",
                    expectedRows(base1Buckets, base2BucketsAfterResume), readRows(MV_ROWS_SQL));

            execute("REFRESH MATERIALIZED VIEW mv FULL");
            drainWalAndMatViewQueues();
            Assert.assertEquals(at + "FULL must rebuild the view from base2", base2Buckets, readRows(MV_ROWS_SQL));

            execute(insertSql("base2", BASE2_AFTER_RESUME_HOUR + 2));
            drainWalAndMatViewQueues();
            Assert.assertFalse(at + "the view must stay valid after a follow-up base2 row", reconciledState.isInvalid());
            Assert.assertEquals(at + "a follow-up base2 row must reach the view",
                    readRows(String.format(BASE_AGG_SQL, "base2")), readRows(MV_ROWS_SQL));
            return stage;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // a durable epoch for the setup, then a lazily applied commit phase
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            teardown();
            execute("CREATE TABLE base1 (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE base2 (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT ts, count() cnt FROM base1 SAMPLE BY 1h) PARTITION BY DAY");
            execute("INSERT INTO base1 VALUES ('2024-10-01T01:00:00.000000Z'), ('2024-10-01T02:00:00.000000Z')");
            // more base2 transactions than base1 has: a refresh position on base2 lies past every base1 transaction
            for (int hour = 5; hour < 8; hour++) {
                execute(insertSql("base2", hour));
            }
            drainWalAndMatViewQueues();
            base1Tt = engine.verifyTableName("base1");
            base2Tt = engine.verifyTableName("base2");
            mvTt = engine.verifyTableName("mv");
            final MatViewState state = engine.getMatViewStateStore().getViewState(mvTt);
            Assert.assertFalse("the view must be valid before the commit phase", state.isInvalid());
            Assert.assertEquals(base1Tt.getTableId(), state.getBaseTableId());
            Assert.assertEquals(readRows(String.format(BASE_AGG_SQL, "base1")), readRows(MV_ROWS_SQL));
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{base1Tt, base2Tt, mvTt};
        }

        @Override
        public void teardown() throws Exception {
            execute("DROP MATERIALIZED VIEW IF EXISTS mv");
            execute("DROP TABLE IF EXISTS base1");
            execute("DROP TABLE IF EXISTS base2");
            drainWalAndMatViewQueues();
        }

        // Runs one statement of the commit phase and drives the WAL apply and the refresh, then stops at a crash
        // that fired on the way, however the engine handled it.
        private void step(String sql) throws Exception {
            try {
                execute(sql);
                drainWalAndMatViewQueues();
            } catch (Throwable th) {
                if (crashFf.getFiredCrash() != null) {
                    throw crashFf.getFiredCrash();
                }
                throw th;
            }
            if (crashFf.getFiredCrash() != null) {
                throw crashFf.getFiredCrash();
            }
        }
    }
}
